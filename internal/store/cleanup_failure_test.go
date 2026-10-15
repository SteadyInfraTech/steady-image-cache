package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutVerifiedReportsTemporaryFileCleanupFailure(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	cleanupErr := errors.New("injected temporary file removal failure")
	s.removeBlob = func(path string) error {
		if strings.HasPrefix(filepath.Base(path), ".part-") {
			return cleanupErr
		}
		return os.Remove(path)
	}

	sourceErr := errors.New("injected source read failure")
	_, err = s.PutVerified(digestOf([]byte("expected")), failingReader{err: sourceErr})
	if !errors.Is(err, sourceErr) {
		t.Fatalf("PutVerified error = %v, want original source error", err)
	}
	if !errors.Is(err, ErrLocal) {
		t.Fatalf("PutVerified error = %v, want cleanup failure reported as ErrLocal", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("PutVerified error = %v, want injected cleanup error", err)
	}

	entries, err := os.ReadDir(filepath.Join(root, "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	foundPartial := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".part-") {
			foundPartial = true
		}
	}
	if !foundPartial {
		t.Fatal("injected removal failure should leave the temporary file for startup cleanup")
	}
}
