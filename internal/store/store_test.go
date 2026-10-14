package store

import (
	"crypto/sha256"
	"encoding/hex"
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
