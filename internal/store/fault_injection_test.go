package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type faultTempFile struct {
	file   *os.File
	failAt string
}

func (f *faultTempFile) Name() string { return f.file.Name() }

func (f *faultTempFile) Write(p []byte) (int, error) {
	if f.failAt == "write" {
		return 0, errors.New("injected write failure")
	}
	return f.file.Write(p)
}

func (f *faultTempFile) Sync() error {
	if f.failAt == "sync" {
		return errors.New("injected sync failure")
	}
	return f.file.Sync()
}

func (f *faultTempFile) Close() error {
	err := f.file.Close()
	if f.failAt == "close" {
		return errors.Join(err, errors.New("injected close failure"))
	}
	return err
}

func TestPutVerifiedFilesystemFailuresLeaveNoPartialBlob(t *testing.T) {
	for _, stage := range []string{"create", "write", "sync", "close", "rename"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			s, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("verified bytes for fault injection")
			d := digestOf(body)

			switch stage {
			case "create":
				s.createBlobTemp = func(string, string) (blobTempFile, error) {
					return nil, errors.New("injected temp creation failure")
				}
			case "write", "sync", "close":
				failAt := stage
				s.createBlobTemp = func(dir, pattern string) (blobTempFile, error) {
					f, err := os.CreateTemp(dir, pattern)
					if err != nil {
						return nil, err
					}
					return &faultTempFile{file: f, failAt: failAt}, nil
				}
			case "rename":
				s.renameBlob = func(string, string) error {
					return errors.New("injected rename failure")
				}
			}

			_, err = s.PutVerified(d, bytes.NewReader(body))
			if !errors.Is(err, ErrLocal) {
				t.Fatalf("PutVerified error = %v, want ErrLocal", err)
			}
			if _, statErr := os.Stat(filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed write published a blob; stat error = %v", statErr)
			}
			entries, err := os.ReadDir(filepath.Join(root, "blobs", "sha256"))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".part-") {
					t.Errorf("failed write left temporary file %q", entry.Name())
				}
			}
		})
	}
}
