package store

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProtectedMarkSurvivesARestartAndGoesWithTheBlob(t *testing.T) {
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
	// It goes at the next start if the blob is gone.
	if err := os.Remove(filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))); err != nil {
		t.Fatal(err)
	}
	s3, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if s3.IsProtected(d) {
		t.Fatal("the mark of a blob that is gone was kept")
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
