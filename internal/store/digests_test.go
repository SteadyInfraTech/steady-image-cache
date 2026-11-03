package store

import (
	"bytes"
	"slices"
	"testing"
)

func TestDigestsListsWhatTheStoreHolds(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Digests()) != 0 {
		t.Fatalf("Digests of an empty store = %v", s.Digests())
	}
	a, b := []byte("one"), []byte("two")
	for _, body := range [][]byte{a, b} {
		if _, err := s.PutVerified(digestOf(body), bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	got := s.Digests()
	slices.Sort(got)
	want := []string{digestOf(a), digestOf(b)}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("Digests = %v, want %v", got, want)
	}
}
