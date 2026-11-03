package store

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestDigestsListsWhatTheStoreHolds(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if ds, err := s.Digests(); err != nil || len(ds) != 0 {
		t.Fatalf("Digests of an empty store = %v, %v", ds, err)
	}
	a, b := []byte("one"), []byte("two")
	for _, body := range [][]byte{a, b} {
		if _, err := s.PutVerified(digestOf(body), bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Digests()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	want := []string{digestOf(a), digestOf(b)}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Digests = %v, want %v", got, want)
	}
}

// A directory that cannot be read is reported, so a caller does not take it for
// an empty store.
func TestDigestsReportsAnUnreadableDirectory(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "blobs", "sha256")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, nil, 0o644); err != nil { // not a directory
		t.Fatal(err)
	}
	if ds, err := s.Digests(); err == nil {
		t.Fatalf("Digests = %v, nil for an unreadable directory", ds)
	}
}
