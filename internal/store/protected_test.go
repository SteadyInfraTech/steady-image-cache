package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestProtectedMarkSurvivesARestartAndAnEviction(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("blob")
	d := digestOf(b)
	if _, err := s.PutVerified(d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	if s.IsProtected(d) {
		t.Fatal("marked before anything marked it")
	}
	if err := s.MarkProtected(d); err != nil {
		t.Fatal(err)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.IsProtected(d) {
		t.Fatal("the mark did not survive a restart")
	}
	// Eviction does not take it away: a refetch must not come back unmarked.
	s2.forget(d)
	if !s2.IsProtected(d) {
		t.Fatal("the mark went with an eviction")
	}
	if s2.IsProtected("not a digest") || s2.MarkProtected("not a digest") == nil {
		t.Fatal("a bad digest was accepted")
	}
}

// When the mark cannot be looked up, the answer is yes.
func TestProtectedFailsClosed(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "protected"), nil, 0o600); err != nil { // a file where the directory should be
		t.Fatal(err)
	}
	if !s.IsProtected(digestOf([]byte("x"))) {
		t.Fatal("an unreadable mark counted as absent")
	}
}

// A mark outlives its blob and a restart: the bytes may come back from
// elsewhere (containerd's content store, another node), and they are still the
// content of a protected registry.
func TestMarkOutlivesItsBlobAndARestart(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("a private layer")
	d := digestOf(b)
	if err := s.MarkProtected(d); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutVerified(d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "blobs", "sha256", d[len("sha256:"):])); err != nil {
		t.Fatal(err)
	}
	s, err = New(root)
	if err != nil {
		t.Fatal(err)
	}
	if !s.IsProtected(d) {
		t.Fatal("the mark of a blob that is gone was removed at start")
	}
}
