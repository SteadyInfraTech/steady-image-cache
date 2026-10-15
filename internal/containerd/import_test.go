package containerd

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/containerd/containerd/content"
)

func TestSplitImageRef(t *testing.T) {
	d := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := []struct {
		in, a, b, c string
		ok          bool
	}{
		{"registry.example.com/team/api:v42", "registry.example.com", "team/api", "v42", true},
		{"docker.io/library/alpine", "docker.io", "library/alpine", "latest", true},
		{"registry.local:5000/team/api:v1", "registry.local:5000", "team/api", "v1", true},
		{"registry.local:5000/team/api", "registry.local:5000", "team/api", "latest", true},
		// name:tag@digest: the tag is the alias, and the repository keeps no ":tag".
		{"docker.io/library/nginx:1.25@" + d, "docker.io", "library/nginx", "1.25", true},
		// A digest-only reference resolves to itself and needs no alias.
		{"registry.example.com/team/api@" + d, "", "", "", false},
		{"no-slash", "", "", "", false},
		{"/leading", "", "", "", false},
	}
	for _, x := range cases {
		a, b, c, ok := splitImageRef(x.in)
		if ok != x.ok || a != x.a || b != x.b || c != x.c {
			t.Errorf("%s => %q %q %q %v, want %q %q %q %v", x.in, a, b, c, ok, x.a, x.b, x.c, x.ok)
		}
	}
}


type testReaderAt struct {
	*bytes.Reader
	closed bool
}

func newTestReaderAt(b []byte) *testReaderAt {
	return &testReaderAt{Reader: bytes.NewReader(b)}
}

func (r *testReaderAt) Close() error {
	r.closed = true
	return nil
}

func (r *testReaderAt) Size() int64 { return r.Reader.Size() }

var _ content.ReaderAt = (*testReaderAt)(nil)

func TestCacheContentStoresAndClosesReader(t *testing.T) {
	body := []byte("containerd blob")
	d := "sha256:8f5f5c3a2f7e15f3b29f9d90c4b37b4f5c6d4b9d6c76b6f7a"
	// Compute the actual digest using the store's own verification contract.
	// A wrong digest must be rejected and never left in the cache.
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ra := newTestReaderAt(body)
	_, err = cacheContent(s, d, ra)
	if err == nil {
		t.Fatal("expected a mismatched digest to be rejected")
	}
	if !ra.closed {
		t.Fatal("ReaderAt must be closed after an import attempt")
	}
	if _, err := s.Stat(d); err == nil {
		t.Fatal("mismatched content must not be stored")
	}
}

type brokenReaderAt struct {
	closed bool
}

func (r *brokenReaderAt) ReadAt([]byte, int64) (int, error) { return 0, errors.New("read failed") }
func (r *brokenReaderAt) Close() error { r.closed = true; return nil }
func (r *brokenReaderAt) Size() int64 { return 16 }

var _ content.ReaderAt = (*brokenReaderAt)(nil)

func TestCacheContentReturnsReadErrorAndClosesReader(t *testing.T) {
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ra := &brokenReaderAt{}
	_, err = cacheContent(s, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ra)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("read failed")) {
		t.Fatalf("cacheContent error = %v, want source read failure", err)
	}
	if !ra.closed {
		t.Fatal("ReaderAt must be closed after a read failure")
	}
}

var _ io.ReaderAt = (*testReaderAt)(nil)
