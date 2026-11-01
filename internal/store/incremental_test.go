package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func smallChunks(t *testing.T, n int64) {
	t.Helper()
	old, oldProof := reserveChunk, proofBytes
	reserveChunk, proofBytes = n, 0 // no allowance: room is made from the first byte
	t.Cleanup(func() { reserveChunk, proofBytes = old, oldProof })
}

func cachedCount(s *Store, ds []string) int {
	n := 0
	for _, d := range ds {
		if _, err := s.Stat(d); err == nil {
			n++
		}
	}
	return n
}

// A source that declares a large blob and then sends a little must not cost the
// cache what it declared: room is made a chunk at a time, as the bytes come.
func TestDeclaredSizeDoesNotEmptyTheCache(t *testing.T) {
	smallChunks(t, 200)
	s := evictable(t)
	s.SetMaxBytes(1000)
	var held []string
	for i := 0; i < 3; i++ {
		held = append(held, mustPut(t, s, string(rune('a'+i)), 300))
		usedAgo(s, held[i], time.Duration(10-i)*time.Hour)
	}
	body := []byte("big" + strings.Repeat("y", 797)) // 800 bytes declared
	d := digestOf(body)
	_, err := s.PutVerifiedLimit(d, io.MultiReader(bytes.NewReader(body[:50]), errReader{io.ErrUnexpectedEOF}), int64(len(body)))
	if err == nil {
		t.Fatal("a cut transfer reported success")
	}
	// The first chunk (200) needed 100 more than fit: one blob went, not three.
	if got := cachedCount(s, held); got != 2 {
		t.Fatalf("%d of 3 cached blobs are left, want 2", got)
	}
	if s.pending != 0 || s.reserved != 0 {
		t.Fatalf("reservation left behind: pending %d reserved %d", s.pending, s.reserved)
	}
}

// A blob that arrives in full is stored, whatever it took to make room.
func TestIncrementalReservationStoresTheWholeBlob(t *testing.T) {
	smallChunks(t, 200)
	s := evictable(t)
	s.SetMaxBytes(1000)
	for i := 0; i < 3; i++ {
		d := mustPut(t, s, string(rune('a'+i)), 300)
		usedAgo(s, d, time.Duration(10-i)*time.Hour)
	}
	body := []byte("big" + strings.Repeat("y", 797))
	d := digestOf(body)
	if _, err := s.PutVerifiedLimit(d, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(d); err != nil {
		t.Fatal(err)
	}
	if s.Used() > 1000 {
		t.Fatalf("cache holds %d bytes of 1000", s.Used())
	}
	if s.pending != 0 || s.reserved != 0 {
		t.Fatalf("reservation left behind: pending %d reserved %d", s.pending, s.reserved)
	}
}

// A blob that could not fit even in an empty cache is refused before anything
// is evicted for it.
func TestImpossibleBlobEvictsNothing(t *testing.T) {
	smallChunks(t, 200)
	s := evictable(t)
	s.SetMaxBytes(1000)
	var held []string
	for i := 0; i < 3; i++ {
		held = append(held, mustPut(t, s, string(rune('a'+i)), 300))
		usedAgo(s, held[i], time.Hour)
	}
	body := []byte(strings.Repeat("z", 5000))
	_, err := s.PutVerifiedLimit(digestOf(body), bytes.NewReader(body), int64(len(body)))
	if !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}
	if got := cachedCount(s, held); got != 3 {
		t.Fatalf("%d of 3 blobs left", got)
	}
}

func withProof(t *testing.T, n int64) {
	t.Helper()
	old := proofBytes
	proofBytes = n
	t.Cleanup(func() { proofBytes = old })
}

// Eviction is paid for by the cache, so a source has to deliver something
// before it can cause one: a registry that declares a size and fails at once
// must not cost a blob.
func TestSourceThatDeliversNothingEvictsNothing(t *testing.T) {
	smallChunks(t, 200)
	withProof(t, 100)
	s := evictable(t)
	s.SetMaxBytes(1000)
	var held []string
	for i := 0; i < 3; i++ {
		held = append(held, mustPut(t, s, string(rune('a'+i)), 300))
		usedAgo(s, held[i], time.Duration(10-i)*time.Hour)
	}
	body := []byte("big" + strings.Repeat("y", 797))
	d := digestOf(body)
	for name, r := range map[string]io.Reader{
		"nothing":           errReader{io.ErrUnexpectedEOF},
		"less than proof":   io.MultiReader(bytes.NewReader(body[:60]), errReader{io.ErrUnexpectedEOF}),
		"exactly the proof": io.MultiReader(bytes.NewReader(body[:100]), errReader{io.ErrUnexpectedEOF}),
	} {
		if _, err := s.PutVerifiedLimit(d, r, int64(len(body))); err == nil {
			t.Fatalf("%s: a cut transfer reported success", name)
		}
		if got := cachedCount(s, held); got != 3 {
			t.Fatalf("%s: %d of 3 cached blobs are left, want 3", name, got)
		}
		if s.pending != 0 || s.reserved != 0 {
			t.Fatalf("%s: reservation left behind: pending %d reserved %d", name, s.pending, s.reserved)
		}
	}
	// Past the allowance room is made, as before.
	if _, err := s.PutVerifiedLimit(d, io.MultiReader(bytes.NewReader(body[:101]), errReader{io.ErrUnexpectedEOF}), int64(len(body))); err == nil {
		t.Fatal("a cut transfer reported success")
	}
	if got := cachedCount(s, held); got != 2 {
		t.Fatalf("after the allowance: %d of 3 cached blobs are left, want 2", got)
	}
}

// A blob that is complete within the allowance is stored, and the room for it
// is made once it has been verified.
func TestSmallBlobWithinTheAllowanceStillMakesRoom(t *testing.T) {
	smallChunks(t, 200)
	withProof(t, 100)
	s := evictable(t)
	s.SetMaxBytes(900)
	var held []string
	for i := 0; i < 3; i++ {
		held = append(held, mustPut(t, s, string(rune('a'+i)), 300))
		usedAgo(s, held[i], time.Duration(10-i)*time.Hour)
	}
	body := []byte(strings.Repeat("s", 80))
	d := digestOf(body)
	if _, err := s.PutVerifiedLimit(d, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(d); err != nil {
		t.Fatal("the blob was not stored")
	}
	if got := cachedCount(s, held); got != 2 {
		t.Fatalf("%d of 3 cached blobs are left, want 2 (one made room)", got)
	}
	if s.pending != 0 || s.reserved != 0 {
		t.Fatalf("reservation left behind: pending %d reserved %d", s.pending, s.reserved)
	}
}

// Bytes that arrived under the allowance are on the disk already: once it is
// crossed they are not outstanding, and what is reserved shrinks with every
// byte that follows.
func TestCrossingTheAllowanceDoesNotReserveTheEarlierBytesTwice(t *testing.T) {
	smallChunks(t, 200)
	withProof(t, 100)
	s := evictable(t)
	s.SetMaxBytes(1000)
	for i := 0; i < 3; i++ {
		usedAgo(s, mustPut(t, s, string(rune('a'+i)), 300), time.Duration(10-i)*time.Hour)
	}
	body := []byte("big" + strings.Repeat("y", 797))
	d := digestOf(body)
	var seen, seenPending int64
	checked := false
	src := &hookReader{r: iotest.OneByteReader(bytes.NewReader(body)), before: func(n int) {
		if n != 150 {
			return
		}
		checked = true
		s.capacityMu.Lock()
		seen, seenPending = s.reserved, s.pending
		s.capacityMu.Unlock()
	}}
	if _, err := s.PutVerifiedLimit(d, src, int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("the check did not run")
	}
	// 100 bytes are on the disk already and count against the size limit; the
	// 200 reserved are for what follows, of which 50 have come: 150 outstanding.
	if seen != 150 {
		t.Fatalf("%d bytes outstanding after 150 bytes, want 150 (the first 100 are not asked of free space again)", seen)
	}
	if seenPending != 300 {
		t.Fatalf("%d counted against the limit, want 300 (100 written and a chunk of 200)", seenPending)
	}
	if s.pending != 0 || s.reserved != 0 {
		t.Fatalf("reservation left behind: pending %d reserved %d", s.pending, s.reserved)
	}
}

// hookReader calls before with the number of bytes read so far, before each read.
type hookReader struct {
	r      io.Reader
	n      int
	before func(n int)
}

func (h *hookReader) Read(p []byte) (int, error) {
	h.before(h.n)
	n, err := h.r.Read(p)
	h.n += n
	return n, err
}

// Bytes that are on the disk when the blob is complete have been seen by free
// space: only what is still lacking is made, not the blob's size on top of it.
func TestSettleDoesNotAskFreeSpaceForBytesAlreadyWritten(t *testing.T) {
	smallChunks(t, 500)
	withProof(t, 100)
	s := evictable(t)
	var held []string
	for i := 0; i < 90; i++ {
		held = append(held, mustPut(t, s, fmt.Sprintf("h%02d", i), 10))
		usedAgo(s, held[i], time.Duration(100-i)*time.Hour)
	}
	// A disk of 1000 bytes with 900 held: 100 free before the blob is written, 20
	// after it, and the floor is 30, so 10 bytes have to be made, not 90.
	s.free = func(string) (uint64, bool) { return uint64(1000 - onDisk(s)), true }
	s.SetMinFree(30)
	body := []byte(strings.Repeat("s", 80))
	d := digestOf(body)
	if _, err := s.PutVerifiedLimit(d, bytes.NewReader(body), 80); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(d); err != nil {
		t.Fatal("the blob was not stored")
	}
	if got := cachedCount(s, held); got != 89 {
		t.Fatalf("%d of 90 cached blobs are left, want 89 (one makes the 10 bytes that were lacking)", got)
	}
}
