package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenRevalidatesAtomicallyReplacedBlobWithSameMetadata(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("original immutable blob bytes")
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(root, "replacement")
	corrupt := bytes.Repeat([]byte("x"), len(body))
	if bytes.Equal(corrupt, body) {
		t.Fatal("test fixture must differ from the original content")
	}
	if err := os.WriteFile(replacement, corrupt, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(replacement, original.ModTime(), original.ModTime()); err != nil {
		t.Fatal(err)
	}
	replacementInfo, err := os.Stat(replacement)
	if err != nil {
		t.Fatal(err)
	}
	if replacementInfo.Size() != original.Size() || !replacementInfo.ModTime().Equal(original.ModTime()) {
		t.Fatal("test fixture must preserve the original size and modification time")
	}
	if os.SameFile(original, replacementInfo) {
		t.Fatal("replacement must have a different file identity")
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}

	if f, err := s.Open(d); err == nil {
		_ = f.Close()
		t.Fatal("Open must reject an atomically replaced blob with the wrong digest")
	} else if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open error = %v, want ErrDigestMismatch", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupted replacement should be removed, stat error = %v", err)
	}
}
