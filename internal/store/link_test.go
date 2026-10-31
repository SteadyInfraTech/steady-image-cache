package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func extFile(t *testing.T, body []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "blob")
	if err := os.WriteFile(p, body, 0o444); err != nil {
		t.Fatal(err)
	}
	return p
}

func blobPath(root, d string) string {
	return filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
}

func TestLinkExternalServesWithoutCountingOrCopying(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("x"), 5000)
	d := digestOf(body)
	if err := s.LinkExternal(d, extFile(t, body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, body) {
		t.Fatal("different bytes")
	}
	if s.Used() != 0 {
		t.Fatalf("Used = %d for a link", s.Used())
	}
	if s2, _ := New(root); s2.Used() != 0 {
		t.Fatalf("a restart counts %d bytes for a link", s2.Used())
	}
	if fi, err := os.Lstat(blobPath(root, d)); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("not a link: %v", err)
	}
}

func TestLinkExternalReplacesACorruptCopyAndFixesTheCount(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("a layer")
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if s.Used() != int64(len(body)) {
		t.Fatalf("Used = %d", s.Used())
	}
	if err := s.LinkExternal(d, extFile(t, body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if s.Used() != 0 {
		t.Fatalf("Used = %d after the copy became a link", s.Used())
	}
}

func TestLinkedFileThatDoesNotVerifyCostsTheLinkNotTheTarget(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	body := []byte("the real layer")
	d := digestOf(body)
	target := extFile(t, body)
	if err := s.LinkExternal(d, target, int64(len(body))); err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(target, 0o644)
	if err := os.WriteFile(target, []byte("the real layerX"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(d); err == nil {
		t.Fatal("served a blob that does not verify")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("the target is gone: %v", err)
	}
	if _, err := os.Lstat(blobPath(root, d)); !os.IsNotExist(err) {
		t.Fatalf("the link remains: %v", err)
	}
}

func TestDanglingLinkIsRemovedOnOpen(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	body := []byte("collected")
	d := digestOf(body)
	target := extFile(t, body)
	if err := s.LinkExternal(d, target, int64(len(body))); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(target)
	if _, err := s.Open(d); !os.IsNotExist(err) {
		t.Fatalf("Open = %v", err)
	}
	if _, err := os.Lstat(blobPath(root, d)); !os.IsNotExist(err) {
		t.Fatalf("the dangling link remains: %v", err)
	}
}

func TestLinkExternalRefusesWrongSizeAndRelativePath(t *testing.T) {
	s, _ := New(t.TempDir())
	body := []byte("a layer")
	d := digestOf(body)
	if err := s.LinkExternal(d, extFile(t, body), int64(len(body))+1); err == nil {
		t.Fatal("wrong size accepted")
	}
	if err := s.LinkExternal(d, "relative", int64(len(body))); err == nil {
		t.Fatal("relative path accepted")
	}
	if err := s.LinkExternal("not a digest", extFile(t, body), 1); err == nil {
		t.Fatal("bad digest accepted")
	}
}
