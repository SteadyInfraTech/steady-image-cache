package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestWritableLeavesNothingBehind(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	before := s.Used()
	for i := 0; i < 3; i++ {
		if err := s.Writable(); err != nil {
			t.Fatalf("a fresh cache directory is writable: %v", err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the probe left %d file(s) behind", len(entries))
	}
	if s.Used() != before {
		t.Fatalf("the probe changed the cache size: %d -> %d", before, s.Used())
	}
}

func TestWritableFailsWhenTheDirectoryCannotBeWritten(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Tests may run as root, which ignores directory modes, so the failure is
	// injected through the same seam the other storage-failure tests use.
	s.createBlobTemp = func(string, string) (blobTempFile, error) {
		return nil, &os.PathError{Op: "open", Path: "x", Err: syscall.EROFS}
	}
	err = s.Writable()
	if !errors.Is(err, ErrLocal) {
		t.Fatalf("a read-only cache directory must report ErrLocal, got %v", err)
	}
}

func TestWritableIgnoresAFullDisk(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.createBlobTemp = func(string, string) (blobTempFile, error) {
		return nil, &os.PathError{Op: "open", Path: "x", Err: syscall.ENOSPC}
	}
	if err := s.Writable(); err != nil {
		t.Fatalf("a full disk still lets a node serve what it holds, so it stays ready: %v", err)
	}
}

func TestWritableReportsAFailedRemoval(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.removeBlob = func(string) error { return errors.New("remove refused") }
	err = s.Writable()
	if err == nil || !strings.Contains(err.Error(), "remove refused") {
		t.Fatalf("got %v, want the removal failure", err)
	}
	// Clean up the file the failing remover left, as the next start would.
	removePartials(filepath.Join(s.root, "blobs", "sha256"))
}
