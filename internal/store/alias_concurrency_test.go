package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Saving the alias file ends in an fsync and takes as long as the file is big.
// Every manifest request calls Resolve, so a lookup must not wait for it.
func TestResolveIsNotBlockedWhileAnAliasIsBeingSaved(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := digestOf([]byte("m"))
	if err := s.SetAlias("r", "a", "old", d); err != nil {
		t.Fatal(err)
	}

	saving, release := make(chan struct{}), make(chan struct{})
	s.beforeAliasSave = func() {
		close(saving)
		<-release
	}
	done := make(chan error, 1)
	go func() { done <- s.SetAlias("r", "a", "new", d) }()
	<-saving // the save is under way and stuck

	looked := make(chan struct{})
	go func() {
		defer close(looked)
		if got, ok := s.Resolve("r", "a", "old"); !ok || got != d {
			t.Errorf("Resolve(old) = %q, %v", got, ok)
		}
		// Not published until it has been saved.
		if _, ok := s.Resolve("r", "a", "new"); ok {
			t.Error("an alias is visible before it was saved")
		}
	}()
	select {
	case <-looked:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("Resolve waited for an alias save to finish")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Resolve("r", "a", "new"); !ok || got != d {
		t.Fatalf("alias missing after the save: %q, %v", got, ok)
	}
}

// Writers are serialized, and each saves a copy of the whole map, so none of the
// concurrent additions may be lost from memory or from the file.
func TestConcurrentAliasWritersLoseNothing(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 8, 6
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				d := digestOf([]byte(fmt.Sprintf("m-%d-%d", w, i)))
				if err := s.SetAlias("r", "a", fmt.Sprintf("t-%d-%d", w, i), d); err != nil {
					t.Error(err)
				}
				s.Resolve("r", "a", "t-0-0") // readers keep going meanwhile
			}
		}()
	}
	wg.Wait()

	reopened, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []*Store{s, reopened} {
		for w := 0; w < writers; w++ {
			for i := 0; i < each; i++ {
				want := digestOf([]byte(fmt.Sprintf("m-%d-%d", w, i)))
				if got, ok := st.Resolve("r", "a", fmt.Sprintf("t-%d-%d", w, i)); !ok || got != want {
					t.Fatalf("alias t-%d-%d: %q, %v", w, i, got, ok)
				}
			}
		}
	}
}

// A failed save leaves the map as it was, and the next one still works.
func TestFailedAliasSaveIsNotRememberedAndDoesNotWedgeWriters(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := digestOf([]byte("m"))
	good := s.root
	s.mu.Lock()
	s.root = good + "/missing"
	s.mu.Unlock()
	if err := s.SetAlias("r", "a", "v1", d); err == nil {
		t.Fatal("expected the save to fail")
	}
	if _, ok := s.Resolve("r", "a", "v1"); ok {
		t.Fatal("an alias that was not saved is visible")
	}
	s.mu.Lock()
	s.root = good
	s.mu.Unlock()
	if err := s.SetAlias("r", "a", "v1", d); err != nil {
		t.Fatalf("a later write must not be blocked by the failed one: %v", err)
	}
}

// The tag record has a size of its own: a client that has many distinct tags
// learned must not make every later tag cost a rewrite of an ever bigger file.
func TestAliasRecordIsBounded(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	s.maxAliases = 100
	d := digestOf([]byte("m"))
	base := time.Unix(1_700_000_000, 0)
	for i := 0; i < 500; i++ {
		if err := s.SetAliasAt("r", "a", fmt.Sprintf("t%03d", i), d, base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
		s.mu.RLock()
		n := len(s.aliases)
		s.mu.RUnlock()
		if n > 100 {
			t.Fatalf("%d tags after %d were learned, limit 100", n, i+1)
		}
	}
	// The newest are kept, the oldest are gone, and so are their times.
	if _, ok := s.Resolve("r", "a", "t499"); !ok {
		t.Fatal("the newest tag was dropped")
	}
	if _, ok := s.Resolve("r", "a", "t000"); ok {
		t.Fatal("the oldest tag was kept")
	}
	if _, ok := s.AliasChecked("r", "a", "t000"); ok {
		t.Fatal("the time of a dropped tag was kept")
	}
	// What was saved is what is held.
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Resolve("r", "a", "t499"); !ok {
		t.Fatal("the newest tag is missing after a restart")
	}
	if _, ok := s2.Resolve("r", "a", "t000"); ok {
		t.Fatal("a dropped tag came back after a restart")
	}
}

// A tag is recorded as confirmed while its writer still holds the save lock:
// otherwise another writer can trim it in between (it has no time yet, so it
// counts as the oldest), and its time is then recorded for a tag that is gone.
func TestConfirmationTimesAreOnlyKeptForTagsThatExist(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.maxAliases = 20
	d := digestOf([]byte("m"))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				if err := s.SetAliasAt("r", "a", fmt.Sprintf("g%d-t%03d", g, i), d, time.Now()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.verifiedMu.Lock()
	defer s.verifiedMu.Unlock()
	for k := range s.aliasChecked {
		if _, ok := s.aliases[k]; !ok {
			t.Fatalf("a time is kept for %q, which is not a tag any more", k)
		}
	}
	if len(s.aliases) > 20 {
		t.Fatalf("%d tags, limit 20", len(s.aliases))
	}
}

// A tag that moves is published with its new digest before its new time: a
// reader that sees the time must not find the old digest, which it would take
// for just confirmed.
func TestMovedTagNeverHasANewTimeWithTheOldDigest(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const n = 400
	digests := make([]string, n)
	index := map[string]int{}
	base := time.Unix(1_700_000_000, 0)
	for i := range digests {
		digests[i] = digestOf([]byte(fmt.Sprintf("m%d", i)))
		index[digests[i]] = i
	}
	if err := s.SetAliasAt("r", "a", "v", digests[0], base); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			at, ok := s.AliasChecked("r", "a", "v") // the time first, then the digest
			d, _ := s.Resolve("r", "a", "v")
			if want := int(at.Sub(base) / time.Second); ok && index[d] < want {
				t.Errorf("time of write %d with the digest of write %d", want, index[d])
				return
			}
		}
	}()
	for i := 1; i < n; i++ {
		if err := s.SetAliasAt("r", "a", "v", digests[i], base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// A record that is over the limit when the cache starts (an older version had
// none) is brought within it at once, and what is dropped stays dropped.
func TestOversizedAliasRecordIsTrimmedAtStart(t *testing.T) {
	root := t.TempDir()
	d := digestOf([]byte("m"))
	doc := make(map[string]string, defaultMaxAliases+100)
	for i := 0; i < defaultMaxAliases+100; i++ {
		doc[fmt.Sprintf("r/a@t%06d", i)] = d
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "aliases.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	want := defaultMaxAliases - defaultMaxAliases/10
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.aliases) != want {
		t.Fatalf("%d tags after start, want %d", len(s.aliases), want)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.aliases) != want {
		t.Fatalf("%d tags after another start, want %d (the trimmed record was not saved)", len(s2.aliases), want)
	}
}

// What a complete small blob needs is what it is, not a chunk's worth.
func TestSettleMakesRoomForTheBlobNotForAChunk(t *testing.T) {
	smallChunks(t, 500)
	withProof(t, 100)
	s := evictable(t)
	s.SetMaxBytes(900)
	var held []string
	for i := 0; i < 3; i++ {
		held = append(held, mustPut(t, s, string(rune('a'+i)), 300))
		usedAgo(s, held[i], time.Duration(10-i)*time.Hour)
	}
	// 900 held; an 80-byte blob needs one eviction (300), a chunk (500) would need two.
	body := []byte(strings.Repeat("s", 80))
	d := digestOf(body)
	if _, err := s.PutVerifiedLimit(d, bytes.NewReader(body), 800); err != nil { // declared as an upper bound
		t.Fatal(err)
	}
	if got := cachedCount(s, held); got != 2 {
		t.Fatalf("%d of 3 cached blobs are left, want 2", got)
	}
}
