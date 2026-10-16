package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// evictable returns a store whose free space is unknown (only the size limit
// applies) and whose eviction takes exactly what is needed and spares nothing
// because it is young.
func evictable(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.SetEvictMinAge(0)
	s.slack = func(int64) int64 { return 0 }
	s.free = func(string) (uint64, bool) { return 0, false }
	s.total = func(string) (uint64, bool) { return 0, false }
	return s
}

// putN stores size bytes of content that is unique to tag.
func putN(t *testing.T, s *Store, tag string, size int, put func(string, io.Reader, int64) (int64, error)) (string, error) {
	t.Helper()
	body := []byte(tag + strings.Repeat("x", size-len(tag)))
	d := digestOf(body)
	_, err := put(d, bytes.NewReader(body), int64(size))
	return d, err
}

func mustPut(t *testing.T, s *Store, tag string, size int) string {
	t.Helper()
	d, err := putN(t, s, tag, size, s.PutVerifiedLimit)
	if err != nil {
		t.Fatalf("storing %s: %v", tag, err)
	}
	return d
}

// usedAgo pretends d was last used that long ago.
func usedAgo(s *Store, d string, age time.Duration) {
	s.verifiedMu.Lock()
	s.lastUse[d] = time.Now().Add(-age).UnixNano()
	s.verifiedMu.Unlock()
}

func lastUseOf(s *Store, d string) int64 {
	s.verifiedMu.Lock()
	defer s.verifiedMu.Unlock()
	return s.lastUse[d]
}

func has(s *Store, d string) bool { _, err := s.Stat(d); return err == nil }

func TestEvictsTheLeastRecentlyUsedWhenOverTheLimit(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(300)
	a, b, c := mustPut(t, s, "a", 100), mustPut(t, s, "b", 100), mustPut(t, s, "c", 100)
	usedAgo(s, a, 3*time.Hour)
	usedAgo(s, b, 2*time.Hour)
	usedAgo(s, c, time.Hour)
	s.Touch(a) // a was served just now, which makes b the least recently used

	d := mustPut(t, s, "d", 100)
	if has(s, b) || !has(s, a) || !has(s, c) || !has(s, d) {
		t.Fatalf("a=%v b=%v c=%v d=%v, want only b evicted", has(s, a), has(s, b), has(s, c), has(s, d))
	}
	if s.Used() != 300 {
		t.Fatalf("Used() = %d, want 300", s.Used())
	}
	if n, bytes := s.Evicted(); n != 1 || bytes != 100 {
		t.Fatalf("Evicted() = %d, %d", n, bytes)
	}
	if !s.WasEvicted(b) || s.WasEvicted(a) {
		t.Fatal("WasEvicted must be true for b only")
	}
	if _, ok := s.Resolve("", "x", b); !ok {
		t.Fatal("a digest reference still resolves; fetching it again is the proxy's business")
	}
}

// The free space of the node is what matters; the size limit is optional.
func TestMakesRoomToKeepTheFreeSpaceReserve(t *testing.T) {
	s := evictable(t)
	const disk = 1000
	s.free = func(string) (uint64, bool) { return uint64(disk - s.Used()), true }
	s.SetMinFree(200)
	var ds []string
	for i, tag := range []string{"a", "b", "c", "d"} {
		d := mustPut(t, s, tag, 200)
		usedAgo(s, d, time.Duration(10-i)*time.Hour)
		ds = append(ds, d)
	}
	// 800 used, 200 free, reserve 200: the next 200 bytes need an eviction.
	e := mustPut(t, s, "e", 200)
	if has(s, ds[0]) || !has(s, ds[1]) || !has(s, e) {
		t.Fatal("expected exactly the least recently used blob to be evicted")
	}
}

func TestSlackKeepsNextDownloadsFromScanningAgain(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(500)
	s.slack = func(int64) int64 { return 100 }
	var ds []string
	for i, tag := range []string{"a", "b", "c", "d", "e"} {
		d := mustPut(t, s, tag, 100)
		usedAgo(s, d, time.Duration(10-i)*time.Hour)
		ds = append(ds, d)
	}
	mustPut(t, s, "f", 100) // short by 100; with 100 of slack that takes two blobs
	if n, _ := s.Evicted(); n != 2 {
		t.Fatalf("evicted %d blobs, want 2 (the shortfall plus the slack)", n)
	}
	if has(s, ds[0]) || has(s, ds[1]) || !has(s, ds[2]) {
		t.Fatal("the two oldest blobs should have gone")
	}
}

func TestPercentageReserve(t *testing.T) {
	s := evictable(t)
	s.total = func(string) (uint64, bool) { return 1000, true }
	s.SetMinFreePercent(30)
	if r := s.reserve(); r != 300 {
		t.Fatalf("reserve = %d, want 300", r)
	}
	s.SetMinFree(400) // the larger of the two applies
	if r := s.reserve(); r != 400 {
		t.Fatalf("reserve = %d, want 400", r)
	}
	s.SetMinFree(0)
	s.SetMinFreePercent(500)
	if r := s.reserve(); r != 900 {
		t.Fatalf("a percentage above 90 must count as 90: reserve = %d", r)
	}
	s.total = func(string) (uint64, bool) { return 0, false }
	if r := s.reserve(); r != 0 {
		t.Fatalf("without a known disk size only the byte reserve applies: %d", r)
	}
}

// Emptying the cache cannot help when the content would not fit anyway, so the
// cache must be left alone.
func TestNothingIsEvictedInVain(t *testing.T) {
	s := evictable(t)
	s.total = func(string) (uint64, bool) { return 1000, true }
	s.free = func(string) (uint64, bool) { return uint64(1000 - s.Used()), true }
	s.SetMinFreePercent(30)
	a := mustPut(t, s, "a", 100)
	if _, err := putN(t, s, "big", 800, s.PutVerifiedLimit); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace (800 plus a 300 reserve is more than the disk)", err)
	}
	if !has(s, a) {
		t.Fatal("the cache was emptied for content that could not fit")
	}
	if n, _ := s.Evicted(); n != 0 {
		t.Fatalf("evicted %d blobs", n)
	}

	s.SetMinFreePercent(0)
	s.SetMaxBytes(500)
	if _, err := putN(t, s, "huge", 600, s.PutVerifiedLimit); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("larger than the size limit: err = %v", err)
	}
	if !has(s, a) {
		t.Fatal("the cache was emptied for content larger than the limit")
	}
}

func TestRecentlyUsedBlobsAreProtected(t *testing.T) {
	s := evictable(t)
	s.SetEvictMinAge(time.Hour)
	s.SetMaxBytes(200)
	a, b := mustPut(t, s, "a", 100), mustPut(t, s, "b", 100)
	if _, err := putN(t, s, "c", 100, s.PutVerifiedLimit); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v: blobs stored a moment ago must not be evicted", err)
	}
	if !has(s, a) || !has(s, b) {
		t.Fatal("a young blob was evicted")
	}
	usedAgo(s, a, 2*time.Hour)
	mustPut(t, s, "c", 100)
	if has(s, a) || !has(s, b) {
		t.Fatal("only the old blob may go")
	}
}

// A blob that is being read or written at this moment is skipped; the next one
// in line goes instead.
func TestBlobsInUseAreSkipped(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(200)
	a, b := mustPut(t, s, "a", 100), mustPut(t, s, "b", 100)
	usedAgo(s, a, 3*time.Hour)
	usedAgo(s, b, 2*time.Hour)
	unlock := s.lockBlob(a) // a is being written
	mustPut(t, s, "c", 100)
	unlock()
	if !has(s, a) || has(s, b) {
		t.Fatalf("a=%v b=%v, want a kept (busy) and b evicted", has(s, a), has(s, b))
	}

	// ...and a reader holding the blob protects it just the same.
	s2 := evictable(t)
	s2.SetMaxBytes(200)
	x, y := mustPut(t, s2, "x", 100), mustPut(t, s2, "y", 100)
	usedAgo(s2, x, 3*time.Hour)
	usedAgo(s2, y, 2*time.Hour)
	runlock, ok := s2.tryRLockBlob(x)
	if !ok {
		t.Fatal("could not read-lock")
	}
	mustPut(t, s2, "z", 100)
	runlock()
	if !has(s2, x) || has(s2, y) {
		t.Fatalf("x=%v y=%v, want x kept (being read) and y evicted", has(s2, x), has(s2, y))
	}
}

func TestPutIfRoomNeverEvicts(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(200)
	a := mustPut(t, s, "a", 100)
	mustPut(t, s, "b", 100)
	usedAgo(s, a, time.Hour)
	if _, err := putN(t, s, "c", 100, s.PutIfRoom); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if !has(s, a) {
		t.Fatal("PutIfRoom evicted a blob")
	}
	if n, _ := s.Evicted(); n != 0 {
		t.Fatal("PutIfRoom evicted")
	}
}

// The containerd import opens every blob on every run. If that counted as a use,
// nothing would ever look old.
func TestOpenDoesNotCountAsUse(t *testing.T) {
	s := evictable(t)
	a := mustPut(t, s, "a", 100)
	usedAgo(s, a, 5*time.Hour)
	old := lastUseOf(s, a)
	f, err := s.Open(a)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if lastUseOf(s, a) != old {
		t.Fatal("Open refreshed the last-use time")
	}
	s.Touch(a)
	if lastUseOf(s, a) <= old {
		t.Fatal("Touch did not refresh it")
	}
}

func TestTouchIsThrottled(t *testing.T) {
	s := evictable(t)
	a := mustPut(t, s, "a", 100)
	usedAgo(s, a, 30*time.Second)
	before := lastUseOf(s, a)
	s.Touch(a)
	if lastUseOf(s, a) != before {
		t.Fatal("served twice within a minute: that must not rewrite the record every time")
	}
}

// Last use must not be kept in the file's modification time: the verification
// record relies on it.
func TestTouchLeavesTheFileAlone(t *testing.T) {
	s := evictable(t)
	a := mustPut(t, s, "a", 100)
	usedAgo(s, a, time.Hour)
	p, _ := s.path(a)
	before, _ := os.Stat(p)
	s.Touch(a)
	after, _ := os.Stat(p)
	if stampOf(before) != stampOf(after) {
		t.Fatal("Touch changed the file")
	}
	if err := opens(s, a); err != nil {
		t.Fatal(err)
	}
}

func TestUsedFollowsWritesAndRemovals(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	s.SetEvictMinAge(0)
	a, b := mustPut(t, s, "a", 100), mustPut(t, s, "b", 50)
	if s.Used() != 150 {
		t.Fatalf("Used() = %d, want 150", s.Used())
	}
	damageInPlace(t, s, a)
	if _, removed, _ := s.Scrub(context.Background(), 0); removed != 1 {
		t.Fatal("scrub did not remove the damaged blob")
	}
	if s.Used() != 50 {
		t.Fatalf("after the scrub Used() = %d, want 50", s.Used())
	}
	p, _ := s.path(b)
	os.WriteFile(p, []byte("tampered, other length"), 0644)
	if err := opens(s, b); !errors.Is(err, ErrDigestMismatch) {
		t.Fatal(err)
	}
	if s.Used() != 0 {
		t.Fatalf("after Open removed a corrupt blob Used() = %d, want 0", s.Used())
	}
	mustPut(t, s, "c", 70)
	s2, _ := New(root)
	if s2.Used() != 70 {
		t.Fatalf("after a restart Used() = %d, want the 70 bytes on disk", s2.Used())
	}
}

func TestLastUseSurvivesARestart(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	a, b, c := mustPut(t, s, "a", 100), mustPut(t, s, "b", 100), mustPut(t, s, "c", 100)
	usedAgo(s, a, 5*time.Hour)
	usedAgo(s, b, 4*time.Hour)
	usedAgo(s, c, time.Hour)
	s.verifiedMu.Lock()
	s.usageDirty = true
	s.verifiedMu.Unlock()
	if err := s.FlushVerified(); err != nil {
		t.Fatal(err)
	}

	s2, _ := New(root)
	s2.SetEvictMinAge(0)
	s2.slack = func(int64) int64 { return 0 }
	s2.free = func(string) (uint64, bool) { return 0, false }
	s2.SetMaxBytes(300)
	mustPut(t, s2, "d", 100)
	if has(s2, a) || !has(s2, b) || !has(s2, c) {
		t.Fatal("after a restart the oldest blob must still be the first to go")
	}
}

func TestUsageOnlyChangesAreSavedRarely(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	a := mustPut(t, s, "a", 100)
	if err := s.FlushVerified(); err != nil {
		t.Fatal(err)
	}
	rec := filepath.Join(root, verifiedFile)
	usedAgo(s, a, time.Hour)
	s.Touch(a) // only a last-use time changed
	os.Remove(rec)
	if err := s.flush(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(rec); err == nil {
		t.Fatal("a last-use change alone was written out straight away")
	}
	if err := s.flush(true); err != nil { // shutdown
		t.Fatal(err)
	}
	if _, err := os.Stat(rec); err != nil {
		t.Fatal("it must be written on shutdown")
	}
}

func TestWritesInProgressCountAgainstTheLimit(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(200)
	body := []byte(strings.Repeat("q", 150))
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := s.PutVerifiedLimit(digestOf(body), &gatedReader{started: started, release: release, reader: bytes.NewReader(body)}, 150)
		done <- err
	}()
	<-started
	if _, err := putN(t, s, "other", 100, s.PutVerifiedLimit); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v: 150 bytes are on their way and the limit is 200", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Many downloads at once into a cache that is too small for all of them: some
// may be refused, none may fail otherwise, and the accounting must add up.
func TestConcurrentWritesAndEvictionsStayConsistent(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	s.SetEvictMinAge(0)
	s.SetMaxBytes(1000)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_, err := putN(t, s, fmt.Sprintf("g%di%d-", g, i), 100, s.PutVerifiedLimit)
				if err != nil && !errors.Is(err, ErrNoSpace) {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}
	if got, want := s.Used(), usage(filepath.Join(root, "blobs", "sha256")); got != want {
		t.Fatalf("Used() = %d but %d bytes are on disk", got, want)
	}
	if s.Used() > 1000 {
		t.Fatalf("the cache holds %d bytes, over its limit of 1000", s.Used())
	}
	if n, _ := s.Evicted(); n == 0 {
		t.Fatal("expected evictions")
	}
}

func TestStoringAgainForgetsThatItWasEvicted(t *testing.T) {
	s := evictable(t)
	s.SetMaxBytes(100)
	a := mustPut(t, s, "a", 100)
	usedAgo(s, a, time.Hour)
	b := mustPut(t, s, "b", 100)
	if !s.WasEvicted(a) {
		t.Fatal("a should have been evicted")
	}
	usedAgo(s, b, time.Hour)
	mustPut(t, s, "a", 100)
	if s.WasEvicted(a) {
		t.Fatal("a is stored again and must no longer count as evicted")
	}
}
