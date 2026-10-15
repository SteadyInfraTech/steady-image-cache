package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const verifiedFile = "verified.json"

// SetLogf sets where the store reports blobs that failed verification. Optional.
func (s *Store) SetLogf(f func(string, ...any)) { s.logf = f }

// Corrupt returns how many blobs failed verification and were removed.
func (s *Store) Corrupt() uint64 { return s.corrupt.Load() }

func (s *Store) noteCorrupt(d string) {
	s.corrupt.Add(1)
	if s.logf != nil {
		s.logf("cache: blob %s failed verification and was removed; it will be fetched again", d)
	}
}

func (s *Store) markVerified(d string, st stamp) {
	s.verifiedMu.Lock()
	if s.verified[d] != st {
		s.verified[d] = st
		s.verifiedDirty = true
	}
	s.verifiedMu.Unlock()
}

func (s *Store) forgetVerified(d string) {
	s.verifiedMu.Lock()
	if _, ok := s.verified[d]; ok {
		delete(s.verified, d)
		s.verifiedDirty = true
	}
	s.verifiedMu.Unlock()
}

// verifiedDoc is the on-disk form of the record of verified blobs.
type verifiedDoc struct {
	Scrubbed int64            `json:"scrubbed,omitempty"` // when a full pass last finished, unix seconds
	Blobs    map[string]stamp `json:"blobs"`
}

// loadVerified reads the record of verified blobs that the previous run saved,
// so that a restart does not mean hashing the whole cache again on first use.
// Entries are kept only for blobs that still exist with the same stamp; a
// missing or unreadable record just means everything is verified again.
func (s *Store) loadVerified() {
	b, err := os.ReadFile(filepath.Join(s.root, verifiedFile))
	if err != nil {
		return
	}
	var doc verifiedDoc
	if json.Unmarshal(b, &doc) != nil {
		return
	}
	for d, st := range doc.Blobs {
		p, err := s.path(d)
		if err != nil {
			continue
		}
		if fi, err := os.Stat(p); err == nil && stampOf(fi) == st {
			s.verified[d] = st
		}
	}
	if doc.Scrubbed > 0 {
		s.lastScrub = time.Unix(doc.Scrubbed, 0)
	}
}

// FlushVerified saves the record of verified blobs if it changed. A record that
// is lost in a crash only costs hashing those blobs again.
func (s *Store) FlushVerified() error {
	s.verifiedMu.Lock()
	if !s.verifiedDirty {
		s.verifiedMu.Unlock()
		return nil
	}
	doc := verifiedDoc{Blobs: make(map[string]stamp, len(s.verified))}
	for d, st := range s.verified {
		doc.Blobs[d] = st
	}
	if !s.lastScrub.IsZero() {
		doc.Scrubbed = s.lastScrub.Unix()
	}
	s.verifiedDirty = false
	s.verifiedMu.Unlock()

	b, err := json.Marshal(doc)
	if err == nil {
		err = writeFileAtomic(s.root, verifiedFile, b)
	}
	if err != nil {
		s.verifiedMu.Lock()
		s.verifiedDirty = true
		s.verifiedMu.Unlock()
	}
	return err
}

// writeFileAtomic replaces dir/name through a temporary file and a rename, so a
// crash never leaves a half-written file behind.
func writeFileAtomic(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, ".tmp-"+name+"-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, name))
}

// pacedReader limits how fast r is read to rate bytes per second, so that a
// background pass over the whole cache does not compete with pulls for the disk.
type pacedReader struct {
	r     io.Reader
	rate  int64
	start time.Time
	n     int64
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
	ctx   context.Context
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if len(b) > 256<<10 {
		b = b[:256<<10] // keep the pacing fine-grained
	}
	n, err := p.r.Read(b)
	p.n += int64(n)
	if p.rate > 0 && n > 0 {
		due := time.Duration(float64(p.n) / float64(p.rate) * float64(time.Second))
		if wait := due - p.now().Sub(p.start); wait > 0 {
			if serr := p.sleep(p.ctx, wait); serr != nil {
				return n, serr
			}
		}
	}
	return n, err
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Scrub checks every blob against its digest, reading at most rate bytes per
// second (zero: no limit), and removes the ones that do not match. Serving does
// not wait for verification any more; this is what still finds blobs that were
// damaged on disk. Blobs that are being written are skipped. It returns how many
// blobs it checked and how many it removed, and stops early with the context's
// error if ctx ends.
func (s *Store) Scrub(ctx context.Context, rate int64) (checked, removed int, err error) {
	dir := filepath.Join(s.root, "blobs", "sha256")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return checked, removed, err
		}
		d := "sha256:" + e.Name()
		if !e.Type().IsRegular() || !ValidDigest(d) {
			continue
		}
		ok, bad, err := s.scrubOne(ctx, d, rate)
		if err != nil {
			return checked, removed, err
		}
		if ok {
			checked++
		}
		if bad {
			removed++
		}
	}
	return checked, removed, nil
}

// scrubOne hashes one blob. checked is false if it was skipped (gone, or being
// written); removed is true if it was corrupt and has been deleted.
func (s *Store) scrubOne(ctx context.Context, d string, rate int64) (checked, removed bool, err error) {
	p, _ := s.path(d)
	unlock, ok := s.tryRLockBlob(d)
	if !ok {
		return false, false, nil
	}
	f, err := os.Open(p)
	if err != nil {
		unlock()
		return false, false, nil
	}
	before, err := f.Stat()
	if err != nil {
		f.Close()
		unlock()
		return false, false, nil
	}
	h := sha256.New()
	src := io.Reader(f)
	if rate > 0 {
		src = &pacedReader{r: f, rate: rate, start: time.Now(), now: time.Now, sleep: sleepCtx, ctx: ctx}
	}
	_, copyErr := io.Copy(h, src)
	f.Close()
	unlock()
	if copyErr != nil {
		if ctx.Err() != nil {
			return false, false, ctx.Err()
		}
		return false, false, nil // unreadable now; leave it for the next pass
	}
	if hex.EncodeToString(h.Sum(nil)) == strings.TrimPrefix(d, "sha256:") {
		s.markVerified(d, stampOf(before))
		return true, false, nil
	}
	// Corrupt, unless the file was replaced while we were reading it: take the
	// write lock and look again before deleting anything.
	unlockW := s.lockBlob(d)
	defer unlockW()
	now, err := os.Stat(p)
	if err != nil || stampOf(now) != stampOf(before) {
		return false, false, nil
	}
	s.forgetVerified(d)
	s.noteCorrupt(d)
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, false, nil
	}
	return true, true, nil
}

// Background settings for RunBackground.
type Background struct {
	FlushEvery time.Duration // how often the verified record is saved (default 30s)
	// ScrubEvery is the time between complete passes of Scrub; zero turns the
	// scrubber off. ScrubRate is its read limit in bytes per second.
	ScrubEvery time.Duration
	ScrubRate  int64
	Logf       func(string, ...any)
}

// firstScrubDelay is how long a store that has never been scrubbed waits after
// start-up, so that a restart does not begin with a pass over the whole cache.
const firstScrubDelay = 10 * time.Minute

// RunBackground saves the record of verified blobs regularly and runs the
// scrubber. It returns when ctx ends, after saving once more.
func (s *Store) RunBackground(ctx context.Context, b Background) {
	logf := b.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if b.FlushEvery <= 0 {
		b.FlushEvery = 30 * time.Second
	}
	flushNow := func() {
		if err := s.FlushVerified(); err != nil {
			logf("cache: saving the verified-blob record: %v", err)
		}
	}
	flush := time.NewTicker(b.FlushEvery)
	defer flush.Stop()

	// A nil channel never fires, which is how a disabled scrubber is expressed.
	var scrubC <-chan time.Time
	var scrubT *time.Timer
	if b.ScrubEvery > 0 {
		s.verifiedMu.Lock()
		last := s.lastScrub
		s.verifiedMu.Unlock()
		wait := firstScrubDelay
		if !last.IsZero() {
			wait = max(time.Until(last.Add(b.ScrubEvery)), 0)
		}
		scrubT = time.NewTimer(wait)
		defer scrubT.Stop()
		scrubC = scrubT.C
	}
	for {
		select {
		case <-ctx.Done():
			flushNow()
			return
		case <-flush.C:
			flushNow()
		case <-scrubC:
			checked, removed, err := s.Scrub(ctx, b.ScrubRate)
			switch {
			case err != nil && ctx.Err() != nil:
				// shutting down; the next run starts a new pass
			case err != nil:
				logf("cache: scrub stopped after %d blobs: %v", checked, err)
			default:
				if removed > 0 {
					logf("cache: scrub checked %d blobs and removed %d that were corrupt", checked, removed)
				}
				s.verifiedMu.Lock()
				s.lastScrub = time.Now()
				s.verifiedDirty = true
				s.verifiedMu.Unlock()
			}
			scrubT.Reset(b.ScrubEvery)
		}
	}
}
