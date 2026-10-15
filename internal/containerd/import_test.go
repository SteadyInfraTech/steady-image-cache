package containerd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"

	"github.com/containerd/containerd/content"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestCorruptedCachedContentIsMarkedForReimport(t *testing.T) {
	root := t.TempDir()
	s, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("valid containerd source blob")
	d := testDigest(original)
	if _, err := s.PutVerified(d, bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.WriteFile(path, []byte("corrupted containerd blob"), 0644); err != nil {
		t.Fatal(err)
	}
	cached, err := cachedContentValid(s, d)
	if err != nil {
		t.Fatalf("cachedContentValid: %v", err)
	}
	if cached {
		t.Fatal("corrupted cache entry must be marked for reimport")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupted entry should be removed, stat error = %v", err)
	}

	// Simulate the importer's recovery step using the still-valid containerd
	// source reader, then ensure subsequent validation accepts the restored blob.
	if _, err := cacheContent(s, d, newTestReaderAt(original)); err != nil {
		t.Fatalf("restore from source: %v", err)
	}
	cached, err = cachedContentValid(s, d)
	if err != nil || !cached {
		t.Fatalf("restored cache valid = %v, err = %v", cached, err)
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) || testDigest(got) != d {
		t.Fatalf("restored bytes = %q, digest = %s; want original bytes and %s", got, testDigest(got), d)
	}
}

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

func testDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestCacheContentStoresAndClosesReader(t *testing.T) {
	body := []byte("containerd blob")
	d := testDigest(body)
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ra := newTestReaderAt(body)
	n, err := cacheContent(s, d, ra)
	if err != nil {
		t.Fatalf("cacheContent: %v", err)
	}
	if n != int64(len(body)) {
		t.Fatalf("stored bytes = %d, want %d", n, len(body))
	}
	if !ra.closed {
		t.Fatal("ReaderAt must be closed after an import attempt")
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("stored content = %q, %v", got, err)
	}
}

type brokenReaderAt struct {
	closed bool
}

func (r *brokenReaderAt) ReadAt([]byte, int64) (int, error) { return 0, errors.New("read failed") }
func (r *brokenReaderAt) Close() error                      { r.closed = true; return nil }
func (r *brokenReaderAt) Size() int64                       { return 16 }

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

type closeFailReaderAt struct {
	*bytes.Reader
	closeErr error
	closed   bool
}

func (r *closeFailReaderAt) Close() error {
	r.closed = true
	return r.closeErr
}

func (r *closeFailReaderAt) Size() int64 { return r.Reader.Size() }

var _ content.ReaderAt = (*closeFailReaderAt)(nil)

// A successful copy is not a successful import if the containerd reader
// reports an error while being released.
func TestCacheContentReturnsCloseError(t *testing.T) {
	body := []byte("containerd blob")
	d := testDigest(body)
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("close failed")
	ra := &closeFailReaderAt{Reader: bytes.NewReader(body), closeErr: closeErr}

	_, err = cacheContent(s, d, ra)
	if !errors.Is(err, closeErr) {
		t.Fatalf("cacheContent error = %v, want close error", err)
	}
	if !ra.closed {
		t.Fatal("ReaderAt must be closed after the import attempt")
	}
	// The content was written, but the caller must still see the close error
	// and may treat this import attempt as unsuccessful.
	if _, err := s.Stat(d); err != nil {
		t.Fatalf("stored content should exist after the successful copy: %v", err)
	}
}

// fakeContentWalker drives the same Walk/ReaderAt path used by Importer.Import.
type fakeContentWalker struct {
	infos       []content.Info
	readers     map[string]content.ReaderAt
	readerErr   error
	readerCalls int
	walkErr     error
}

func (f *fakeContentWalker) Walk(_ context.Context, fn content.WalkFunc, _ ...string) error {
	for _, info := range f.infos {
		if err := fn(info); err != nil {
			return err
		}
	}
	return f.walkErr
}

func (f *fakeContentWalker) ReaderAt(_ context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	f.readerCalls++
	if f.readerErr != nil {
		return nil, f.readerErr
	}
	ra, ok := f.readers[desc.Digest.String()]
	if !ok {
		return nil, errors.New("reader not configured")
	}
	return ra, nil
}

func TestImporterContentWalkRepairsCorruptBlobAndCountsImport(t *testing.T) {
	root := t.TempDir()
	s, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("containerd importer orchestration payload")
	d := testDigest(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.WriteFile(path, []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}

	sourceReader := newTestReaderAt(body)
	cs := &fakeContentWalker{
		infos: []content.Info{{Digest: digest.Digest(d), Size: int64(len(body))}},
		readers: map[string]content.ReaderAt{
			d: sourceReader,
		},
	}
	m := &metrics.Metrics{}
	i := &Importer{Store: s, Metrics: m}
	if err := i.importContent(context.Background(), cs); err != nil {
		t.Fatalf("importContent: %v", err)
	}
	if cs.readerCalls != 1 {
		t.Fatalf("ReaderAt calls = %d, want 1", cs.readerCalls)
	}
	if !sourceReader.closed {
		t.Fatal("source reader was not closed")
	}
	if got := m.ImportedBlobs.Load(); got != 1 {
		t.Fatalf("imported blob metric = %d, want 1", got)
	}
	if got := m.ImportedBytes.Load(); got != uint64(len(body)) {
		t.Fatalf("imported bytes metric = %d, want %d", got, len(body))
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatalf("open repaired blob: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("repaired content = %q, want %q", got, body)
	}
}

func TestImporterContentWalkSkipsValidCachedBlob(t *testing.T) {
	body := []byte("already cached")
	d := testDigest(body)
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	cs := &fakeContentWalker{
		infos:   []content.Info{{Digest: digest.Digest(d), Size: int64(len(body))}},
		readers: map[string]content.ReaderAt{},
	}
	m := &metrics.Metrics{}
	i := &Importer{Store: s, Metrics: m}
	if err := i.importContent(context.Background(), cs); err != nil {
		t.Fatalf("importContent: %v", err)
	}
	if cs.readerCalls != 0 {
		t.Fatalf("ReaderAt calls = %d, want 0 for a valid cache hit", cs.readerCalls)
	}
	if m.ImportedBlobs.Load() != 0 || m.ImportedBytes.Load() != 0 {
		t.Fatalf("cache hit changed import metrics: blobs=%d bytes=%d", m.ImportedBlobs.Load(), m.ImportedBytes.Load())
	}
}

func TestImporterContentWalkReturnsReaderAtFailure(t *testing.T) {
	body := []byte("missing from cache")
	d := testDigest(body)
	readerErr := errors.New("containerd ReaderAt unavailable")
	cs := &fakeContentWalker{
		infos:     []content.Info{{Digest: digest.Digest(d), Size: int64(len(body))}},
		readers:   map[string]content.ReaderAt{},
		readerErr: readerErr,
	}
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	i := &Importer{Store: s, Metrics: &metrics.Metrics{}}
	err = i.importContent(context.Background(), cs)
	if !errors.Is(err, readerErr) {
		t.Fatalf("importContent error = %v, want wrapped ReaderAt failure", err)
	}
	if cs.readerCalls != 1 {
		t.Fatalf("ReaderAt calls = %d, want 1", cs.readerCalls)
	}
	if _, err := s.Stat(d); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed source read unexpectedly created blob, stat error = %v", err)
	}
}

func TestImporterContentWalkReturnsWalkFailure(t *testing.T) {
	walkErr := errors.New("containerd content walk failed")
	cs := &fakeContentWalker{walkErr: walkErr}
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	i := &Importer{Store: s, Metrics: &metrics.Metrics{}}

	err = i.importContent(context.Background(), cs)
	if !errors.Is(err, walkErr) {
		t.Fatalf("importContent error = %v, want wrapped Walk failure", err)
	}
	if cs.readerCalls != 0 {
		t.Fatalf("ReaderAt calls = %d, want 0 when Walk fails before visiting content", cs.readerCalls)
	}
}
