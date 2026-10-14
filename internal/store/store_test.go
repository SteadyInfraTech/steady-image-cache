package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func digestOf(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestValidDigest(t *testing.T) {
	good := digestOf([]byte("x"))
	cases := []struct {
		in string
		ok bool
	}{
		{good, true},
		{strings.ToUpper(good), false},
		{"sha256:" + strings.Repeat("A", 64), false},
		{"sha256:" + strings.Repeat("g", 64), false},
		{"sha256:" + strings.Repeat("a", 63), false},
		{"sha512:" + strings.Repeat("a", 64), false},
		{"sha256:../../../../etc/passwd" + strings.Repeat("a", 40), false},
		{"sha256:" + strings.Repeat("a", 60) + "/..", false},
		{"", false},
	}
	for _, c := range cases {
		if got := ValidDigest(c.in); got != c.ok {
			t.Errorf("ValidDigest(%q) = %v, want %v", c.in, got, c.ok)
		}
	}
}

func TestPutVerified(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("hello layer")
	d := digestOf(body)
	if _, err := s.PutVerified(d, strings.NewReader(string(body))); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(s.root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:")))
	if err != nil || string(got) != string(body) {
		t.Fatalf("stored content = %q, %v", got, err)
	}
}

func TestPutVerifiedRejectsMismatchAndLeavesNothing(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := digestOf([]byte("expected"))
	if _, err := s.PutVerified(d, strings.NewReader("something else")); err == nil {
		t.Fatal("expected a digest mismatch")
	}
	if _, err := s.Stat(d); err == nil {
		t.Fatal("a rejected blob must not be visible")
	}
	entries, _ := os.ReadDir(filepath.Join(root, "blobs", "sha256"))
	if len(entries) != 0 {
		t.Fatalf("temporary file left behind: %v", entries)
	}
}

func TestPutVerifiedRejectsBadDigest(t *testing.T) {
	s, _ := New(t.TempDir())
	if _, err := s.PutVerified("sha256:../x", strings.NewReader("x")); err == nil {
		t.Fatal("expected an error for a malformed digest")
	}
}

func TestResolve(t *testing.T) {
	s, _ := New(t.TempDir())
	d := digestOf([]byte("m"))
	if got, ok := s.Resolve("r", "a/b", d); !ok || got != d {
		t.Fatalf("a digest ref resolves to itself, got %q %v", got, ok)
	}
	if _, ok := s.Resolve("r", "a/b", "sha256:nothex"); ok {
		t.Fatal("a malformed digest ref must not resolve")
	}
	if _, ok := s.Resolve("r", "a/b", "v1"); ok {
		t.Fatal("unknown tag must not resolve")
	}
	if err := s.SetAlias("r", "a/b", "v1", d); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Resolve("r", "a/b", "v1"); !ok || got != d {
		t.Fatalf("got %q %v", got, ok)
	}
	if _, ok := s.Resolve("other", "a/b", "v1"); ok {
		t.Fatal("aliases are per registry namespace")
	}
}

func TestSetAliasRejectsBadDigest(t *testing.T) {
	s, _ := New(t.TempDir())
	if err := s.SetAlias("r", "a", "v1", "../../x"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestAliasesPersistAcrossRestart(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := digestOf([]byte("m"))
	if err := s.SetAlias("r", "a", "v1", d); err != nil {
		t.Fatal(err)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := s2.Resolve("r", "a", "v1"); !ok || got != d {
		t.Fatalf("alias lost across restart: %q %v", got, ok)
	}
}

func TestSetAliasIsNoOpWhenUnchanged(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := digestOf([]byte("m"))
	if err := s.SetAlias("r", "a", "v1", d); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(root, "aliases.json")
	if err := os.Remove(f); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAlias("r", "a", "v1", d); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); err == nil {
		t.Fatal("re-setting an identical alias must not rewrite the file")
	}
	// A real change is still written.
	if err := s.SetAlias("r", "a", "v1", digestOf([]byte("n"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); err != nil {
		t.Fatalf("changed alias not written: %v", err)
	}
}

func TestSaveLeavesNoTempFiles(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	for i := 0; i < 5; i++ {
		if err := s.SetAlias("r", "a", "v"+string(rune('0'+i)), digestOf([]byte{byte(i)})); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".aliases-") {
			t.Fatalf("stray temp file %s", e.Name())
		}
	}
}

func TestCorruptAliasFileIsSetAside(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "aliases.json"), []byte("{not json"), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := New(root)
	if err != nil {
		t.Fatalf("a damaged alias file must not stop startup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "aliases.json.corrupt")); err != nil {
		t.Fatalf("damaged file should be kept for inspection: %v", err)
	}
	if err := s.SetAlias("r", "a", "v1", digestOf([]byte("m"))); err != nil {
		t.Fatal(err)
	}
}

func TestStartupRemovesPartialDownloads(t *testing.T) {
	root := t.TempDir()
	blobs := filepath.Join(root, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0755); err != nil {
		t.Fatal(err)
	}
	keep := strings.Repeat("a", 64)
	os.WriteFile(filepath.Join(blobs, ".part-123"), []byte("junk"), 0644)
	os.WriteFile(filepath.Join(blobs, keep), []byte("real"), 0644)
	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(blobs, ".part-123")); err == nil {
		t.Fatal("stale partial file should be removed")
	}
	if _, err := os.Stat(filepath.Join(blobs, keep)); err != nil {
		t.Fatal("completed blobs must be kept")
	}
}

func TestPutVerifiedLimitRejectsExcess(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	body := []byte("0123456789")
	d := digestOf(body)

	if _, err := s.PutVerifiedLimit(d, strings.NewReader(string(body)), int64(len(body))-1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	if _, err := s.Stat(d); err == nil {
		t.Fatal("over-long content must not be stored")
	}
	entries, _ := os.ReadDir(filepath.Join(root, "blobs", "sha256"))
	if len(entries) != 0 {
		t.Fatalf("temporary file left behind: %v", entries)
	}
	if _, err := s.PutVerifiedLimit(d, strings.NewReader(string(body)), int64(len(body))); err != nil {
		t.Fatalf("content of exactly the limit must be accepted: %v", err)
	}
}

type endless struct{ n int64 }

func (e *endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	e.n += int64(len(p))
	return len(p), nil
}

// An endless source must be cut off at the limit rather than read forever.
func TestPutVerifiedLimitStopsAnEndlessStream(t *testing.T) {
	s, _ := New(t.TempDir())
	src := &endless{}
	if _, err := s.PutVerifiedLimit(digestOf([]byte("x")), src, 1<<20); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v", err)
	}
	if src.n > 2<<20 {
		t.Fatalf("read %d bytes for a 1 MiB limit", src.n)
	}
}

func TestZeroLimitAllowsOnlyEmptyContent(t *testing.T) {
	s, _ := New(t.TempDir())
	empty := digestOf(nil)
	if _, err := s.PutVerifiedLimit(empty, strings.NewReader(""), 0); err != nil {
		t.Fatalf("empty blob: %v", err)
	}
	if _, err := s.PutVerifiedLimit(digestOf([]byte("a")), strings.NewReader("a"), 0); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

func TestPutVerifiedLimitChecksFreeSpace(t *testing.T) {
	s, _ := New(t.TempDir())
	body := []byte("0123456789")
	d := digestOf(body)
	free := uint64(100)
	s.free = func(string) (uint64, bool) { return free, true }

	// 10 bytes fit in 100 free bytes with a 50 byte reserve.
	s.SetMinFree(50)
	if _, err := s.PutVerifiedLimit(d, strings.NewReader(string(body)), 10); err != nil {
		t.Fatal(err)
	}

	d2 := digestOf([]byte("another one"))
	// 11 bytes + 95 byte reserve > 100 free.
	s.SetMinFree(95)
	if _, err := s.PutVerifiedLimit(d2, strings.NewReader("another one"), 11); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if _, err := s.Stat(d2); err == nil {
		t.Fatal("nothing may be stored when the disk is too full")
	}
	// Nothing was written, not even a partial file.
	entries, _ := os.ReadDir(filepath.Join(s.root, "blobs", "sha256"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".part-") {
			t.Fatalf("partial file created before the space check: %s", e.Name())
		}
	}
}

func TestUnknownFreeSpaceDoesNotBlock(t *testing.T) {
	s, _ := New(t.TempDir())
	s.free = func(string) (uint64, bool) { return 0, false }
	s.SetMinFree(1 << 40)
	body := []byte("fine")
	if _, err := s.PutVerifiedLimit(digestOf(body), strings.NewReader(string(body)), 4); err != nil {
		t.Fatalf("a platform without free-space support must still work: %v", err)
	}
}

func TestFreeBytesOnRealDisk(t *testing.T) {
	if free, ok := freeBytes(t.TempDir()); ok && free == 0 {
		t.Fatal("freeBytes reported a full disk for the temp dir")
	}
}
