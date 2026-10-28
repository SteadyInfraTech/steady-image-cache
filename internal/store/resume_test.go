package store

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// cutAfter yields the first n bytes of b and then fails, as a connection that
// drops does.
func cutAfter(b []byte, n int) io.Reader {
	return io.MultiReader(bytes.NewReader(b[:n]), errReader{io.ErrUnexpectedEOF})
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func partials(t *testing.T, s *Store) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.root, "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".part-") {
			out = append(out, e.Name())
		}
	}
	return out
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// A source that fails partway leaves the start of the blob, and the next source
// is asked only for the rest.
func TestResumeContinuesWhereTheLastSourceStopped(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("0123456789", 10000))
	d := digestOf(body)
	var res Resume

	if _, err := s.PutResumable(d, cutAfter(body, 60000), int64(len(body)), nil, &res); err == nil {
		t.Fatal("a cut transfer reported success")
	}
	if res.Size() != 60000 || res.Total() != int64(len(body)) {
		t.Fatalf("kept %d of %d bytes", res.Size(), res.Total())
	}
	if _, err := s.Stat(d); err == nil {
		t.Fatal("an unfinished blob is visible")
	}

	// Cut again: what the second source delivered is added.
	if _, err := s.PutResumable(d, cutAfter(body[60000:], 15000), int64(len(body)), nil, &res); err == nil {
		t.Fatal("a cut transfer reported success")
	}
	if res.Size() != 75000 {
		t.Fatalf("kept %d bytes, want 75000", res.Size())
	}

	n, err := s.PutResumable(d, bytes.NewReader(body[75000:]), int64(len(body)), nil, &res)
	if err != nil || n != int64(len(body)) {
		t.Fatalf("stored %d bytes, %v", n, err)
	}
	if res.Size() != 0 || len(partials(t, s)) != 0 {
		t.Fatalf("a partial file is left: %d kept, %v", res.Size(), partials(t, s))
	}
	f, err := s.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if got, _ := io.ReadAll(f); !bytes.Equal(got, body) {
		t.Fatal("the stored blob differs")
	}
}

// Nothing a source says about the start is trusted: the digest is checked over
// all of it, and a blob whose start was wrong is not kept.
func TestResumeWithAWrongStartIsRefusedAndForgotten(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("abcdefghij", 5000))
	d := digestOf(body)
	bad := append([]byte(strings.Repeat("X", 20000)), body[20000:]...)
	var res Resume
	if _, err := s.PutResumable(d, cutAfter(bad, 20000), int64(len(body)), nil, &res); err == nil {
		t.Fatal("expected a failure")
	}
	if res.Size() != 20000 {
		t.Fatalf("kept %d", res.Size())
	}
	_, err := s.PutResumable(d, bytes.NewReader(body[20000:]), int64(len(body)), nil, &res)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
	if res.Size() != 0 || len(partials(t, s)) != 0 {
		t.Fatalf("a bad partial was kept: %d, %v", res.Size(), partials(t, s))
	}
	if _, err := s.Stat(d); err == nil {
		t.Fatal("a blob with a wrong start was stored")
	}
}

// Content that is too long, and a failure of this node's own disk, are not the
// source's fault and leave nothing to continue.
func TestOnlyASourceFailureIsResumable(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{io.ErrUnexpectedEOF, true},
		{errors.New("connection reset by peer"), true},
		{ErrDigestMismatch, false},
		{ErrTooLarge, false},
		{local(errors.New("disk")), false},
		{ErrNoSpace, false},
		{nil, false},
	} {
		if got := Resumable(tc.err); got != tc.want {
			t.Errorf("Resumable(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestDropResumeRemovesThePartialFile(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("z", 30000))
	var res Resume
	s.PutResumable(digestOf(body), cutAfter(body, 9000), int64(len(body)), nil, &res)
	if len(partials(t, s)) != 1 {
		t.Fatalf("partials: %v", partials(t, s))
	}
	s.DropResume(&res)
	if res.Size() != 0 || len(partials(t, s)) != 0 {
		t.Fatalf("after DropResume: %d, %v", res.Size(), partials(t, s))
	}
	s.DropResume(&res) // harmless twice
	s.DropResume(nil)
}

// A partial that was started for a blob of another size is not continued.
func TestResumeForAnotherSizeStartsOver(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("q", 40000))
	d := digestOf(body)
	var res Resume
	s.PutResumable(d, cutAfter(body, 10000), int64(len(body))+1, nil, &res) // claimed one byte too many
	if res.Size() != 10000 {
		t.Fatalf("kept %d", res.Size())
	}
	if _, err := s.PutResumable(d, bytes.NewReader(body), int64(len(body)), nil, &res); err != nil {
		t.Fatalf("a fresh transfer after a mismatching partial failed: %v", err)
	}
	if len(partials(t, s)) != 0 {
		t.Fatalf("partials: %v", partials(t, s))
	}
}

// If the blob turned up whole in the meantime (another way of fetching it), the
// partial is not needed.
func TestResumeIsDroppedWhenTheBlobIsAlreadyThere(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("w", 30000))
	d := digestOf(body)
	var res Resume
	s.PutResumable(d, cutAfter(body, 5000), int64(len(body)), nil, &res)
	if _, err := s.PutVerified(d, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutResumable(d, bytes.NewReader(body[5000:]), int64(len(body)), nil, &res); err != nil {
		t.Fatal(err)
	}
	if res.Size() != 0 || len(partials(t, s)) != 0 {
		t.Fatalf("%d, %v", res.Size(), partials(t, s))
	}
}

// A reader of the file while it grows sees one file across attempts, and the
// progress it is told about continues from where the last attempt stopped.
type progressLog struct {
	mu     sync.Mutex
	begins []string
	wrote  []int64
	ends   []error
}

func (p *progressLog) Begin(path string, max int64) {
	p.mu.Lock()
	p.begins = append(p.begins, path)
	p.mu.Unlock()
}
func (p *progressLog) Wrote(total int64) {
	p.mu.Lock()
	p.wrote = append(p.wrote, total)
	p.mu.Unlock()
}
func (p *progressLog) End(err error) { p.mu.Lock(); p.ends = append(p.ends, err); p.mu.Unlock() }

func TestResumeKeepsOneFileAndContinuesTheProgress(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("0123456789", 3000))
	d := digestOf(body)
	var res Resume
	var pl progressLog
	s.PutResumable(d, cutAfter(body, 12000), int64(len(body)), &pl, &res)
	if _, err := s.PutResumable(d, bytes.NewReader(body[12000:]), int64(len(body)), &pl, &res); err != nil {
		t.Fatal(err)
	}
	if len(pl.begins) != 2 || pl.begins[0] != pl.begins[1] {
		t.Fatalf("the attempts used different files: %v", pl.begins)
	}
	// Totals count from the start of the blob, never from the attempt's own.
	last := int64(0)
	for _, w := range pl.wrote {
		if w < last {
			t.Fatalf("progress went backwards: %v", pl.wrote)
		}
		last = w
	}
	if last != int64(len(body)) || len(pl.ends) != 2 || pl.ends[0] == nil || pl.ends[1] != nil {
		t.Fatalf("progress %v, ends %v", pl.wrote, pl.ends)
	}
}

// peek runs f the first time it is read from, while the write is in flight.
type peek struct {
	r io.Reader
	f func()
}

func (p *peek) Read(b []byte) (int, error) {
	if p.f != nil {
		p.f()
		p.f = nil
	}
	return p.r.Read(b)
}

// What a resumed download already has on disk counts against the cache size
// limit while it continues, even though only the rest is new to free space.
func TestResumeCountsThePrefixAgainstTheSizeLimit(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("0123456789", 10000))
	d := digestOf(body)
	var res Resume
	if _, err := s.PutResumable(d, cutAfter(body, 60000), int64(len(body)), nil, &res); err == nil {
		t.Fatal("a cut transfer reported success")
	}

	var pending, reserved int64
	rest := &peek{r: bytes.NewReader(body[60000:]), f: func() {
		s.capacityMu.Lock()
		pending, reserved = s.pending, s.reserved
		s.capacityMu.Unlock()
	}}
	if _, err := s.PutResumable(d, rest, int64(len(body)), nil, &res); err != nil {
		t.Fatal(err)
	}
	if pending != int64(len(body)) {
		t.Fatalf("size limit saw %d bytes in flight, want the whole %d", pending, len(body))
	}
	if reserved != int64(len(body))-60000 {
		t.Fatalf("free space saw %d bytes reserved, want only the missing %d", reserved, len(body)-60000)
	}
	if s.pending != 0 || s.reserved != 0 {
		t.Fatalf("reservation left behind: pending %d reserved %d", s.pending, s.reserved)
	}
}

// A source that fails before sending anything leaves no file behind and no
// state that points at one.
func TestResumeAfterAnEmptyFailureLeavesNoFile(t *testing.T) {
	s := newTestStore(t)
	body := []byte(strings.Repeat("0123456789", 1000))
	d := digestOf(body)
	var res Resume
	for i := 0; i < 3; i++ {
		if _, err := s.PutResumable(d, errReader{io.ErrUnexpectedEOF}, int64(len(body)), nil, &res); err == nil {
			t.Fatal("a failed transfer reported success")
		}
		if res.Size() != 0 {
			t.Fatalf("kept %d bytes of nothing", res.Size())
		}
	}
	if left := partials(t, s); len(left) != 0 {
		t.Fatalf("left behind %v", left)
	}
}
