package store

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Content that arrives a second time is checked against what is on disk before
// it is let go, and that check holds the blob's lock. Somebody who asks for the
// blob meanwhile must get it: it is complete, it was verified, and nothing
// about it is changing.
func TestOpenWorksWhileTheSameBlobIsBeingStoredAgain(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(strings.Repeat("0123456789abcdef", 1<<19)) // 8 MiB: the re-check takes a moment
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 300; i++ {
		f, err := s.Open(d)
		if err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("Open %d failed while the blob was being stored again: %v", i, err)
		}
		f.Close()
	}
	stop.Store(true)
	wg.Wait()
}

// A blob that is not known to be good is still a miss while a writer has it:
// the lock is what keeps a reader away from a file that is being checked or
// replaced.
func TestOpenStillMissesAnUnverifiedBlobWhileItIsLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("not verified by this process")
	d := digestOf(body)
	p, _ := s.path(d)
	if err := os.WriteFile(p, body, 0o600); err != nil { // as if left by an earlier run
		t.Fatal(err)
	}
	unlock := s.lockBlob(d)
	defer unlock()
	if f, err := s.Open(d); err == nil {
		f.Close()
		t.Fatal("an unverified blob was opened while a writer held it")
	}
}

// And a verified blob whose file changed under it is not trusted.
func TestOpenDoesNotTrustAChangedFileWhileItIsLocked(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(strings.Repeat("x", 4096))
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	p, _ := s.path(d)
	if err := os.WriteFile(p, []byte(strings.Repeat("y", 5000)), 0o600); err != nil {
		t.Fatal(err)
	}
	unlock := s.lockBlob(d)
	defer unlock()
	if f, err := s.Open(d); err == nil {
		f.Close()
		t.Fatal("a file that no longer matches its verification was opened")
	}
}
