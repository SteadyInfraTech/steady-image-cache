package containerd

import (
	"bytes"
	"context"
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
)

// containerdDir is a content directory as containerd lays it out, holding body.
func containerdDir(t *testing.T, body []byte) string {
	t.Helper()
	dir := t.TempDir()
	blobs := filepath.Join(dir, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blobs, strings.TrimPrefix(testDigest(body), "sha256:")), body, 0o444); err != nil {
		t.Fatal(err)
	}
	return dir
}

func importOne(t *testing.T, s *store.Store, m *metrics.Metrics, contentDir string, body []byte, readers bool) error {
	t.Helper()
	d := testDigest(body)
	w := &fakeContentWalker{infos: []content.Info{{Digest: digest.Digest(d), Size: int64(len(body))}}}
	if readers {
		w.readers = map[string]content.ReaderAt{d: newTestReaderAt(body)}
	}
	return (&Importer{Store: s, Metrics: m, ContentDir: contentDir}).importContent(context.Background(), w)
}

func cacheBytes(t *testing.T, root string) int64 {
	t.Helper()
	s, err := store.New(root) // its own count of what it holds
	if err != nil {
		t.Fatal(err)
	}
	return s.Used()
}

// With containerd's directory at hand, a blob is linked, not copied: the cache
// serves it, takes no space for it, and a second copy never reaches the disk.
func TestImportLinksInsteadOfCopying(t *testing.T) {
	body := bytes.Repeat([]byte("a layer "), 1000)
	cdir := containerdDir(t, body)
	root := t.TempDir()
	s, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	// No reader is configured: reading through containerd's API would fail.
	if err := importOne(t, s, m, cdir, body, false); err != nil {
		t.Fatal(err)
	}
	if m.LinkedBlobs.Load() != 1 || m.ImportedBlobs.Load() != 0 {
		t.Fatalf("linked %d, copied %d; want 1, 0", m.LinkedBlobs.Load(), m.ImportedBlobs.Load())
	}
	d := testDigest(body)
	p := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if fi, err := os.Lstat(p); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the blob is not a link: %v %v", fi, err)
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(f)
	f.Close()
	if !bytes.Equal(got, body) {
		t.Fatal("the linked blob reads differently")
	}
	if n := cacheBytes(t, root); n != 0 {
		t.Fatalf("the cache counts %d bytes for a linked blob", n)
	}
	// Run again: nothing to do.
	if err := importOne(t, s, m, cdir, body, false); err != nil || m.LinkedBlobs.Load() != 1 {
		t.Fatalf("second import: %v, linked %d", err, m.LinkedBlobs.Load())
	}
}

// Without the directory, or when the file is not in it, the blob is copied as before.
func TestImportCopiesWhenThereIsNothingToLink(t *testing.T) {
	body := []byte("a layer that is not in the mounted directory")
	for name, cdir := range map[string]string{"no directory": "", "not in it": containerdDir(t, []byte("something else"))} {
		s, err := store.New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		m := &metrics.Metrics{}
		if err := importOne(t, s, m, cdir, body, true); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if m.LinkedBlobs.Load() != 0 || m.ImportedBlobs.Load() != 1 {
			t.Fatalf("%s: linked %d, copied %d", name, m.LinkedBlobs.Load(), m.ImportedBlobs.Load())
		}
	}
}

// A file that is not what it says is refused on first read, and costs the link,
// not containerd's file.
func TestLinkedBlobThatDoesNotVerifyIsDroppedNotDeleted(t *testing.T) {
	body := []byte("the real layer")
	cdir := containerdDir(t, body)
	root := t.TempDir()
	s, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := importOne(t, s, &metrics.Metrics{}, cdir, body, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(cdir, "blobs", "sha256", strings.TrimPrefix(testDigest(body), "sha256:"))
	if err := os.Chmod(target, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("the real layerX"), 0o644); err != nil { // same name, other bytes
		t.Fatal(err)
	}
	if _, err := s.Open(testDigest(body)); err == nil {
		t.Fatal("a blob that does not verify was served")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("containerd's file is gone: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(testDigest(body), "sha256:"))); !os.IsNotExist(err) {
		t.Fatalf("the link is still there: %v", err)
	}
}

// When containerd collects the content, the link to it goes, and the blob is a miss.
func TestLinkToCollectedContentIsRemoved(t *testing.T) {
	body := []byte("a layer containerd will collect")
	cdir := containerdDir(t, body)
	root := t.TempDir()
	s, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := importOne(t, s, &metrics.Metrics{}, cdir, body, false); err != nil {
		t.Fatal(err)
	}
	d := testDigest(body)
	if err := os.RemoveAll(cdir); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(d); !os.IsNotExist(err) {
		t.Fatalf("Open: %v, want not exist", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))); !os.IsNotExist(err) {
		t.Fatalf("the dangling link is still there: %v", err)
	}
}

// The wrong size is not linked.
func TestLinkRefusesAFileOfTheWrongSize(t *testing.T) {
	body := []byte("a layer")
	cdir := containerdDir(t, body)
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(cdir, "blobs", "sha256", strings.TrimPrefix(testDigest(body), "sha256:"))
	if err := s.LinkExternal(testDigest(body), target, int64(len(body))+1); err == nil {
		t.Fatal("linked a file whose size is not the declared one")
	}
	if err := s.LinkExternal(testDigest(body), "relative/path", int64(len(body))); err == nil {
		t.Fatal("linked a relative path")
	}
}

// What the cache evicted is linked again: a link costs it nothing. Without a
// directory it is still left alone, as before.
func TestEvictedBlobIsLinkedAgain(t *testing.T) {
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.SetMaxBytes(100)
	s.SetEvictMinAge(0)
	x := bytes.Repeat([]byte("x"), 100)
	for _, b := range [][]byte{x, bytes.Repeat([]byte("y"), 100)} { // the second evicts the first
		if _, err := s.PutVerifiedLimit(testDigest(b), bytes.NewReader(b), int64(len(b))); err != nil {
			t.Fatal(err)
		}
	}
	if !s.WasEvicted(testDigest(x)) {
		t.Fatal("test setup: x should have been evicted")
	}
	m := &metrics.Metrics{}
	if err := importOne(t, s, m, "", x, true); err != nil || m.ImportedBlobs.Load() != 0 {
		t.Fatalf("without a directory an evicted blob is left alone: %v, copied %d", err, m.ImportedBlobs.Load())
	}
	if err := importOne(t, s, m, containerdDir(t, x), x, false); err != nil || m.LinkedBlobs.Load() != 1 {
		t.Fatalf("with a directory it is linked: %v, linked %d", err, m.LinkedBlobs.Load())
	}
}

// A full cache stops the copying, not the links: they need no room.
func TestImportKeepsLinkingWhenTheCacheIsFull(t *testing.T) {
	big := bytes.Repeat([]byte("b"), 4000) // copied: not in containerd's directory
	small := []byte("a blob that is linked")
	dir := containerdDir(t, small)
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.SetMaxBytes(1000)
	bd, sd := testDigest(big), testDigest(small)
	w := &fakeContentWalker{
		infos: []content.Info{
			{Digest: digest.Digest(bd), Size: int64(len(big))},
			{Digest: digest.Digest(sd), Size: int64(len(small))},
		},
		readers: map[string]content.ReaderAt{bd: newTestReaderAt(big), sd: newTestReaderAt(small)},
	}
	m := &metrics.Metrics{}
	err = (&Importer{Store: s, Metrics: m, ContentDir: dir}).importContent(context.Background(), w)
	if !errors.Is(err, store.ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if _, err := s.Stat(sd); err != nil {
		t.Fatalf("the blob after the one that did not fit was not linked: %v", err)
	}
	if m.LinkedBlobs.Load() != 1 {
		t.Fatalf("LinkedBlobs = %d, want 1", m.LinkedBlobs.Load())
	}
}

// countingWalker counts the blobs the importer is shown.
type countingWalker struct {
	contentWalker
	seen int
}

func (c *countingWalker) Walk(ctx context.Context, fn content.WalkFunc, filters ...string) error {
	return c.contentWalker.Walk(ctx, func(i content.Info) error { c.seen++; return fn(i) }, filters...)
}

// Without a content directory nothing can be linked, so a full cache ends the
// walk instead of having it scan the rest.
func TestImportEndsTheWalkWhenFullAndNothingCanBeLinked(t *testing.T) {
	cs, _ := threeBlobs()
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.SetMinFree(1 << 60)
	w := &countingWalker{contentWalker: cs}
	err = (&Importer{Store: s, Metrics: &metrics.Metrics{}}).importContent(context.Background(), w)
	if !errors.Is(err, store.ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if w.seen != 1 {
		t.Fatalf("the walk showed %d blobs, want 1", w.seen)
	}
}
