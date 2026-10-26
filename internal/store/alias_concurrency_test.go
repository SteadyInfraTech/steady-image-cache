package store

import (
	"fmt"
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
