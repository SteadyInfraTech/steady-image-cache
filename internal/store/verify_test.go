package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// damageInPlace overwrites the blob with different bytes of the same length and
// puts its modification time back: the file keeps its inode, size and mtime, so
// nothing but hashing it can tell that it changed (like bit rot).
func damageInPlace(t *testing.T, s *Store, d string) {
	t.Helper()
	p, _ := s.path(d)
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte("!"), int(fi.Size())), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(p)
	if stampOf(after) != stampOf(fi) {
		t.Fatal("test fixture must keep the file's stamp")
	}
}

func newPut(t *testing.T, s *Store, body string) string {
	t.Helper()
	d := digestOf([]byte(body))
	if _, err := s.PutVerified(d, strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	return d
}

func opens(s *Store, d string) error {
	f, err := s.Open(d)
	if err == nil {
		f.Close()
	}
	return err
}

// The digest is computed while a blob is written, so the first read must not
// hash it again: that doubled the I/O of every pull and delayed its first byte.
func TestBlobThatWasJustWrittenIsNotHashedAgain(t *testing.T) {
	s, _ := New(t.TempDir())
	d := newPut(t, s, "a layer we have just downloaded and checked")
	damageInPlace(t, s, d)
	if err := opens(s, d); err != nil {
		t.Fatalf("Open hashed a blob that was verified when it was written: %v", err)
	}
	// ...and the scrubber is what still catches it.
	if _, removed, err := s.Scrub(context.Background(), 0); err != nil || removed != 1 {
		t.Fatalf("Scrub removed %d, err %v; want it to find the damaged blob", removed, err)
	}
	if err := opens(s, d); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("after the scrub: %v, want the blob gone", err)
	}
}

func TestVerifiedRecordSurvivesARestart(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := newPut(t, s, "a layer that was verified before the restart")
	if err := s.FlushVerified(); err != nil {
		t.Fatal(err)
	}
	damageInPlace(t, s, d)

	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := opens(s2, d); err != nil {
		t.Fatalf("after a restart Open hashed a blob it had recorded as verified: %v", err)
	}
}

func TestWithoutTheRecordARestartVerifiesAgain(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := newPut(t, s, "a layer whose record was never saved")
	damageInPlace(t, s, d) // no FlushVerified: a crash lost the record

	s2, _ := New(root)
	if err := opens(s2, d); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open = %v, want ErrDigestMismatch: nothing vouched for this blob", err)
	}
}

func TestBlobChangedWhileStoppedIsVerifiedAgain(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := newPut(t, s, "a layer that is changed while SIC is down")
	s.FlushVerified()
	p, _ := s.path(d)
	if err := os.WriteFile(p, []byte("a different layer of another length"), 0644); err != nil {
		t.Fatal(err)
	}
	s2, _ := New(root)
	if err := opens(s2, d); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open = %v, want ErrDigestMismatch", err)
	}
}

func TestStaleRecordEntriesAreDroppedOnLoad(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	keep := newPut(t, s, "stays as it was")
	gone := newPut(t, s, "is deleted later")
	changed := newPut(t, s, "is rewritten later")
	s.FlushVerified()
	pg, _ := s.path(gone)
	os.Remove(pg)
	pc, _ := s.path(changed)
	os.WriteFile(pc, []byte("rewritten, different size"), 0644)

	s2, _ := New(root)
	if len(s2.verified) != 1 {
		t.Fatalf("%d entries kept, want only the unchanged blob", len(s2.verified))
	}
	if _, ok := s2.verified[keep]; !ok {
		t.Fatal("the unchanged blob lost its entry")
	}
}

func TestUnreadableRecordIsIgnored(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := newPut(t, s, "layer")
	os.WriteFile(filepath.Join(root, verifiedFile), []byte("{ not json"), 0644)
	s2, err := New(root)
	if err != nil {
		t.Fatalf("a damaged record must not stop the start: %v", err)
	}
	if err := opens(s2, d); err != nil {
		t.Fatalf("Open = %v: without a record the blob is simply hashed", err)
	}
}

func TestFlushOnlyWritesWhenSomethingChanged(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	newPut(t, s, "layer")
	if err := s.FlushVerified(); err != nil {
		t.Fatal(err)
	}
	rec := filepath.Join(root, verifiedFile)
	if _, err := os.Stat(rec); err != nil {
		t.Fatal("record not written")
	}
	os.Remove(rec)
	s.FlushVerified()
	if _, err := os.Stat(rec); err == nil {
		t.Fatal("an unchanged record was written again")
	}
	newPut(t, s, "another layer")
	s.FlushVerified()
	if _, err := os.Stat(rec); err != nil {
		t.Fatal("a changed record was not written")
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Fatalf("temporary file %q left behind", e.Name())
		}
	}
}

func TestScrubRemovesOnlyCorruptBlobs(t *testing.T) {
	s, _ := New(t.TempDir())
	var mu sync.Mutex
	var logged []string
	s.SetLogf(func(f string, a ...any) { mu.Lock(); logged = append(logged, f); mu.Unlock() })
	good1 := newPut(t, s, "good one")
	bad := newPut(t, s, "damaged one")
	good2 := newPut(t, s, "good two")
	damageInPlace(t, s, bad)

	checked, removed, err := s.Scrub(context.Background(), 0)
	if err != nil || checked != 3 || removed != 1 {
		t.Fatalf("Scrub = %d checked, %d removed, %v; want 3, 1", checked, removed, err)
	}
	if _, err := s.Stat(bad); err == nil {
		t.Fatal("the damaged blob is still there")
	}
	if _, err := s.Stat(good1); err != nil {
		t.Fatal("a good blob was removed")
	}
	if _, err := s.Stat(good2); err != nil {
		t.Fatal("a good blob was removed")
	}
	if s.Corrupt() != 1 {
		t.Fatalf("Corrupt() = %d", s.Corrupt())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logged) != 1 {
		t.Fatalf("logged %d lines, want 1: %v", len(logged), logged)
	}
}

func TestScrubSkipsABlobThatIsBeingWritten(t *testing.T) {
	s, _ := New(t.TempDir())
	d := newPut(t, s, "being replaced")
	damageInPlace(t, s, d)
	unlock := s.lockBlob(d) // a writer holds the digest
	checked, removed, err := s.Scrub(context.Background(), 0)
	unlock()
	if err != nil || checked != 0 || removed != 0 {
		t.Fatalf("Scrub = %d, %d, %v; it must leave a blob alone while it is written", checked, removed, err)
	}
	if _, err := s.Stat(d); err != nil {
		t.Fatal("blob removed while locked")
	}
}

func TestScrubStopsWhenCancelled(t *testing.T) {
	s, _ := New(t.TempDir())
	newPut(t, s, "layer")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if checked, _, err := s.Scrub(ctx, 0); !errors.Is(err, context.Canceled) || checked != 0 {
		t.Fatalf("checked %d, err %v", checked, err)
	}
}

func TestPacedReaderLimitsTheRate(t *testing.T) {
	now := time.Unix(1000, 0)
	var slept time.Duration
	p := &pacedReader{
		r:     bytes.NewReader(make([]byte, 1000)),
		rate:  100, // bytes per second
		start: now,
		now:   func() time.Time { return now },
		sleep: func(_ context.Context, d time.Duration) error { slept += d; now = now.Add(d); return nil },
		ctx:   context.Background(),
	}
	buf := make([]byte, 400)
	for {
		if _, err := p.Read(buf); err != nil {
			break
		}
	}
	if slept < 9900*time.Millisecond || slept > 10100*time.Millisecond {
		t.Fatalf("slept %v for 1000 bytes at 100 B/s, want about 10s", slept)
	}
}

func TestRunBackgroundSavesOnShutdownAndScrubsWhenDue(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	bad := newPut(t, s, "will be damaged")
	damageInPlace(t, s, bad)
	// The store was last scrubbed long ago, so a pass is due at once.
	s.verifiedMu.Lock()
	s.lastScrub = time.Now().Add(-48 * time.Hour)
	s.verifiedMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunBackground(ctx, Background{ScrubEvery: time.Hour, FlushEvery: time.Hour})
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := s.Stat(bad); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the scheduled scrub never removed the damaged blob")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Wait until the pass has been recorded, then shut down.
	for {
		s.verifiedMu.Lock()
		recent := time.Since(s.lastScrub) < time.Minute
		s.verifiedMu.Unlock()
		if recent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the finished pass was not recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	s2, _ := New(root)
	if time.Since(s2.lastScrub) > time.Minute {
		t.Fatalf("the time of the last scrub was not saved on shutdown: %v", s2.lastScrub)
	}
}
