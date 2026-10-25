package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
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

func TestLocalWriteFailuresAreTagged(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	blobs := filepath.Join(root, "blobs", "sha256")
	os.RemoveAll(blobs)
	os.WriteFile(blobs, []byte("a file where the directory should be"), 0644)
	body := []byte("x")
	_, err := s.PutVerified(digestOf(body), strings.NewReader(string(body)))
	if !errors.Is(err, ErrLocal) || errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrLocal", err)
	}
}

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }

// A broken source is not a local failure: only the cache's own writes are.
func TestSourceFailuresAreNotLocal(t *testing.T) {
	s, _ := New(t.TempDir())
	_, err := s.PutVerified(digestOf([]byte("x")), failingReader{errors.New("connection reset by peer")})
	if err == nil || errors.Is(err, ErrLocal) || errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want a plain source error", err)
	}
	_, err = s.PutVerified(digestOf([]byte("expected")), strings.NewReader("different"))
	if !errors.Is(err, ErrDigestMismatch) || errors.Is(err, ErrLocal) {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
}

type failingWriter struct{ err error }

func (f failingWriter) Write([]byte) (int, error) { return 0, f.err }

func TestOutOfSpaceFromTheOperatingSystemIsErrNoSpace(t *testing.T) {
	err := local(syscall.ENOSPC)
	if !errors.Is(err, ErrNoSpace) || errors.Is(err, ErrLocal) {
		t.Fatalf("ENOSPC => %v", err)
	}
	if !errors.Is(local(errors.New("disk on fire")), ErrLocal) {
		t.Fatal("other write errors are ErrLocal")
	}
	if local(nil) != nil {
		t.Fatal("no error must stay no error")
	}
	_, err = io.Copy(localWriter{failingWriter{syscall.ENOSPC}}, strings.NewReader("data"))
	if !errors.Is(err, ErrNoSpace) {
		t.Fatalf("a write that hits ENOSPC => %v", err)
	}
}

func TestMinFreeIsReadable(t *testing.T) {
	s, _ := New(t.TempDir())
	s.SetMinFree(12345)
	if s.MinFree() != 12345 {
		t.Fatalf("MinFree = %d", s.MinFree())
	}
}

func TestOpenRejectsAndRemovesCorruptedBlob(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("verified cache data")
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.WriteFile(p, []byte("tampered"), 0644); err != nil {
		t.Fatal(err)
	}
	if f, err := s.Open(d); err == nil {
		_ = f.Close()
		t.Fatal("Open must not return corrupted content")
	} else if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open error = %v, want ErrDigestMismatch", err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupted blob should be removed, stat error = %v", err)
	}
}

type gatedReader struct {
	started chan struct{}
	release <-chan struct{}
	reader  io.Reader
}

func (r *gatedReader) Read(p []byte) (int, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	<-r.release
	return r.reader.Read(p)
}

func TestConcurrentDifferentDigestWritesReserveCapacity(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Keep the reported free-space value fixed so this test specifically
	// exercises reservation accounting rather than relying on filesystem timing.
	s.free = func(string) (uint64, bool) { return 100, true }
	s.SetMinFree(20)

	firstBody := []byte("first blob")
	firstDigest := digestOf(firstBody)
	releaseFirst := make(chan struct{})
	started := make(chan struct{}, 1)
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.PutVerifiedLimit(firstDigest, &gatedReader{
			started: started,
			release: releaseFirst,
			reader:  bytes.NewReader(firstBody),
		}, 60)
		firstDone <- err
	}()

	// The first write has reserved 60 bytes before its reader is entered.
	<-started
	secondBody := []byte("second blob")
	_, err = s.PutVerifiedLimit(digestOf(secondBody), bytes.NewReader(secondBody), 21)
	if !errors.Is(err, ErrNoSpace) {
		t.Fatalf("second concurrent write error = %v, want ErrNoSpace", err)
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Once the first write is complete, its unused reservation is released.
	thirdBody := []byte("third blob")
	if _, err := s.PutVerifiedLimit(digestOf(thirdBody), bytes.NewReader(thirdBody), 80); err != nil {
		t.Fatalf("write after reservation release: %v", err)
	}
}

func TestPutVerifiedReplacesCorruptedExistingBlob(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("the expected immutable layer")
	d := digestOf(body)
	if _, err := s.PutVerified(d, strings.NewReader(string(body))); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.WriteFile(p, []byte("corrupted bytes"), 0644); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PutVerified(d, strings.NewReader(string(body))); err != nil || n != int64(len(body)) {
		t.Fatalf("PutVerified = %d, %v", n, err)
	}
	got, err := os.ReadFile(p)
	if err != nil || string(got) != string(body) {
		t.Fatalf("blob after repair = %q, %v", got, err)
	}
}

func TestConcurrentPutVerifiedSameDigest(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(strings.Repeat("same layer content", 4096))
	d := digestOf(body)
	const writers = 24
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.PutVerified(d, strings.NewReader(string(body)))
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent PutVerified: %v", err)
		}
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("final blob has %d bytes, read error %v; want %d valid bytes", len(got), err, len(body))
	}
}

func TestSetAliasSaveFailureDoesNotPublishAlias(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	// Replacing a directory with the aliases file must fail on supported
	// filesystems, without leaving an in-memory alias that was never persisted.
	if err := os.Mkdir(filepath.Join(root, "aliases.json"), 0755); err != nil {
		t.Fatal(err)
	}
	d := digestOf([]byte("manifest"))
	if err := s.SetAlias("registry", "team/app", "v1", d); err == nil {
		t.Fatal("SetAlias should report the failed aliases.json rename")
	}
	if _, ok := s.Resolve("registry", "team/app", "v1"); ok {
		t.Fatal("alias became visible despite failed persistence")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".aliases-") {
			t.Errorf("temporary alias file left behind: %s", entry.Name())
		}
	}
}

// BenchmarkOpenRead compares the normal cached read path (which verifies the
// digest before returning the file) with opening and reading the same cache
// file without verification. The difference approximates the extra I/O and
// hashing cost paid by callers that consume the full blob.
func BenchmarkOpenRead(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			root := b.TempDir()
			s, err := New(root)
			if err != nil {
				b.Fatal(err)
			}
			body := bytes.Repeat([]byte("x"), size)
			d := digestOf(body)
			if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
				b.Fatal(err)
			}
			path, err := s.path(d)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(size))

			b.Run("verified", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f, err := s.Open(d)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := io.Copy(io.Discard, f); err != nil {
						_ = f.Close()
						b.Fatal(err)
					}
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("open-and-read-only", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f, err := os.Open(path)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := io.Copy(io.Discard, f); err != nil {
						_ = f.Close()
						b.Fatal(err)
					}
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// A cached verification must be invalidated when the same file is modified
// without changing its size or inode.
func TestOpenRevalidatesBlobWhenMetadataChanges(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("verified cache data")
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

	p := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.WriteFile(p, []byte("tampered cache data"), 0644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Hour)
	if err := os.Chtimes(p, future, future); err != nil {
		t.Fatal(err)
	}
	if f, err := s.Open(d); err == nil {
		_ = f.Close()
		t.Fatal("Open must re-hash a blob whose metadata changed")
	} else if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("Open error = %v, want ErrDigestMismatch", err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupted blob should be removed, stat error = %v", err)
	}
}

type partialFailureReader struct {
	data []byte
	err  error
}

func (r partialFailureReader) Read(p []byte) (int, error) {
	return copy(p, r.data), r.err
}

func TestPartialSourceFailureReleasesReservationAndCleansTempFile(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	// A deterministic free-space report lets the follow-up write prove that
	// the failed partial write released all of its reservation.
	s.free = func(string) (uint64, bool) { return 100, true }
	s.SetMinFree(20)

	partial := []byte("partial bytes")
	d := digestOf([]byte("complete expected blob"))
	_, err = s.PutVerifiedLimit(d, partialFailureReader{
		data: partial,
		err:  errors.New("injected source read failure"),
	}, 60)
	if err == nil || errors.Is(err, ErrLocal) || errors.Is(err, ErrNoSpace) {
		t.Fatalf("partial source write error = %v, want source error", err)
	}
	if _, err := s.Stat(d); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial blob became visible: stat error = %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".part-") {
			t.Errorf("partial source failure left temporary file %q", entry.Name())
		}
	}

	next := []byte("next complete blob")
	if _, err := s.PutVerifiedLimit(digestOf(next), bytes.NewReader(next), 80); err != nil {
		t.Fatalf("follow-up write after reservation cleanup: %v", err)
	}
}

// A writer holds the digest lock for the whole download. A reader must not
// queue behind it: a peer asking for a layer that is still arriving would run
// into its timeout and be skipped as if it were dead.
func TestOpenDoesNotWaitForAnInProgressWrite(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("a layer that is still arriving")
	d := digestOf(body)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := s.PutVerifiedLimit(d, &gatedReader{started: started, release: release, reader: bytes.NewReader(body)}, int64(len(body)))
		done <- err
	}()
	<-started

	opened := make(chan error, 1)
	go func() {
		f, err := s.Open(d)
		if err == nil {
			f.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Open during a write = %v, want os.ErrNotExist", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Open waited for the download to finish")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatalf("Open after the write finished: %v", err)
	}
	f.Close()
}

// Several clients pulling the same layer open it at the same moment; none of
// them may be told it is missing.
func TestConcurrentOpensOfOneBlobAllSucceed(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(strings.Repeat("popular layer", 1000))
	d := digestOf(body)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	const readers = 32
	start := make(chan struct{})
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f, err := s.Open(d)
			if err == nil {
				f.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Open: %v", err)
		}
	}
}

// When a tag was last confirmed against its registry is kept across a restart,
// so that restarting does not make every tag stale at once.
func TestAliasConfirmationTimeSurvivesARestart(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := digestOf([]byte("m"))
	at := time.Unix(1_700_000_000, 0)
	if err := s.SetAliasAt("r", "a", "v1", d, at); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.AliasChecked("r", "a", "v1"); !ok || !got.Equal(at) {
		t.Fatalf("AliasChecked = %v %v", got, ok)
	}
	if _, ok := s.AliasChecked("r", "a", "v2"); ok {
		t.Fatal("a tag that was never confirmed has no time")
	}
	if err := s.FlushVerified(); err != nil {
		t.Fatal(err)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := s2.AliasChecked("r", "a", "v1"); !ok || !got.Equal(at) {
		t.Fatalf("after a restart: %v %v", got, ok)
	}
	if got, ok := s2.Resolve("r", "a", "v1"); !ok || got != d {
		t.Fatalf("alias lost: %q %v", got, ok)
	}
}

// Confirming a tag again changes only its time, and that is not worth a write
// of the alias file.
func TestConfirmingAnUnchangedAliasRefreshesTheTimeOnly(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d := digestOf([]byte("m"))
	t1, t2 := time.Unix(1_700_000_000, 0), time.Unix(1_700_000_600, 0)
	if err := s.SetAliasAt("r", "a", "v1", d, t1); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(root, "aliases.json")
	if err := os.Remove(f); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAliasAt("r", "a", "v1", d, t2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f); err == nil {
		t.Fatal("an unchanged alias must not rewrite the alias file")
	}
	if got, _ := s.AliasChecked("r", "a", "v1"); !got.Equal(t2) {
		t.Fatalf("time = %v, want %v", got, t2)
	}
	// A failed save must not claim the alias was confirmed.
	other := digestOf([]byte("n"))
	s.mu.Lock()
	s.root = filepath.Join(root, "gone")
	s.mu.Unlock()
	if err := s.SetAliasAt("r", "a", "v1", other, time.Unix(1_700_001_200, 0)); err == nil {
		t.Fatal("expected the save to fail")
	}
	if got, _ := s.AliasChecked("r", "a", "v1"); !got.Equal(t2) {
		t.Fatalf("time moved to %v although the alias was not saved", got)
	}
	if d2, _ := s.Resolve("r", "a", "v1"); d2 != d {
		t.Fatal("an alias that could not be saved must not be visible")
	}
}

// An alias learned from containerd never replaces one the cache has, and does
// not count as confirmed.
func TestSetAliasIfAbsent(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	d1, d2 := digestOf([]byte("one")), digestOf([]byte("two"))
	if added, err := s.SetAliasIfAbsent("r", "a", "v1", d1); err != nil || !added {
		t.Fatalf("added=%v err=%v", added, err)
	}
	if _, ok := s.AliasChecked("r", "a", "v1"); ok {
		t.Fatal("an imported alias was never confirmed")
	}
	if added, err := s.SetAliasIfAbsent("r", "a", "v1", d2); err != nil || added {
		t.Fatalf("added=%v err=%v: an existing alias must be left alone", added, err)
	}
	if got, _ := s.Resolve("r", "a", "v1"); got != d1 {
		t.Fatalf("alias = %s, want the first", got)
	}
	// A confirmed alias keeps its time when containerd mentions it again.
	at := time.Unix(1_700_000_000, 0)
	if err := s.SetAliasAt("r", "a", "v2", d1, at); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAliasIfAbsent("r", "a", "v2", d2); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.AliasChecked("r", "a", "v2"); !ok || !got.Equal(at) {
		t.Fatalf("time = %v %v", got, ok)
	}
	if got, _ := s.Resolve("r", "a", "v2"); got != d1 {
		t.Fatal("a confirmed alias was replaced")
	}
	if _, err := s.SetAliasIfAbsent("r", "a", "v3", "../../x"); err == nil {
		t.Fatal("a bad digest must be refused")
	}
}

// A record written before tag times were kept loads, and its tags are simply
// unconfirmed.
func TestRecordWithoutTagTimesLoads(t *testing.T) {
	root := t.TempDir()
	s, _ := New(root)
	if err := s.SetAlias("r", "a", "v1", digestOf([]byte("m"))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "verified.json"), []byte(`{"blobs":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.AliasChecked("r", "a", "v1"); ok {
		t.Fatal("no time was recorded")
	}
	if _, ok := s2.Resolve("r", "a", "v1"); !ok {
		t.Fatal("the alias itself must still be there")
	}
}

// "null" is valid JSON and decodes to a nil map; the first alias written
// afterwards must not panic.
func TestAliasFileContainingNullIsHarmless(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aliases.json"), []byte("null"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := "sha256:" + strings.Repeat("0", 64)
	if err := s.SetAlias("docker.io", "lib/app", "v1", d); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.Resolve("docker.io", "lib/app", "v1"); !ok || got != d {
		t.Fatalf("Resolve = %q, %v", got, ok)
	}
}

// A blob that cannot be read is not a corrupt blob: reading it failed, so
// nothing is known about its content, and deleting it could destroy the only
// copy a node has. Here a directory stands in for a file that fails to read.
func TestPutVerifiedDoesNotDeleteWhatItCannotRead(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("a layer that is there but cannot be read right now")
	d := digestOf(body)
	p := filepath.Join(root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err = s.PutVerified(d, strings.NewReader(string(body)))
	if !errors.Is(err, ErrLocal) {
		t.Fatalf("PutVerified error = %v, want ErrLocal", err)
	}
	if fi, statErr := os.Stat(p); statErr != nil || !fi.IsDir() {
		t.Fatalf("the unreadable entry was removed: %v", statErr)
	}
	if got := s.Corrupt(); got != 0 {
		t.Fatalf("Corrupt() = %d, an unreadable blob was counted as corrupt", got)
	}
}
