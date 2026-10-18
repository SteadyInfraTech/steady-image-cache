package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrTooLarge means the content was longer than the limit it was given.
	ErrTooLarge = errors.New("content exceeds the size limit")
	// ErrDigestMismatch means the content does not hash to the expected digest.
	ErrDigestMismatch = errors.New("digest mismatch")
	// ErrLocal marks a failure of the local cache directory (it cannot be
	// created, written, synced or renamed), as opposed to a problem with the
	// content or with the source it was read from.
	ErrLocal = errors.New("cache write failed")
	// ErrNoSpace means storing the content would leave less free disk space
	// than the configured reserve.
	ErrNoSpace = errors.New("not enough free disk space")
)

type blobTempFile interface {
	io.Writer
	Sync() error
	Close() error
	Name() string
}

type Store struct {
	// Blob file operations are narrow seams for deterministic storage-failure tests.
	// Alias persistence uses its own path and remains independently tested.
	createBlobTemp func(string, string) (blobTempFile, error)
	renameBlob     func(string, string) error
	removeBlob     func(string) error
	root           string
	writeMu        sync.Mutex // guards the per-digest writer-lock map
	writes         map[string]*blobWriteLock
	mu             sync.RWMutex
	aliases        map[string]string

	minFree    atomic.Int64
	minFreePct atomic.Int64
	maxBytes   atomic.Int64
	free       func(path string) (uint64, bool) // replaced in tests
	total      func(path string) (uint64, bool) // replaced in tests

	capacityMu sync.Mutex // guards reservations across distinct digest writes
	reserved   int64      // promised to writers and not yet reflected in free space
	pending    int64      // promised to writers and not yet committed to used

	used         atomic.Int64 // bytes in blobs/
	evictMu      sync.Mutex   // one eviction scan at a time
	minAge       atomic.Int64 // nanoseconds
	slack        func(used int64) int64
	evictedBlobs atomic.Uint64
	evictedBytes atomic.Uint64
	evMu         sync.Mutex
	evicted      map[string]struct{}

	verifiedMu     sync.Mutex // guards verified, lastUse, their dirty flags and lastScrub
	verified       map[string]stamp
	verifiedDirty  bool
	lastUse        map[string]int64 // unix nanoseconds
	aliasChecked   map[string]int64 // when each alias was last confirmed, unix seconds; guarded by verifiedMu
	usageDirty     bool
	lastUsageFlush time.Time
	lastScrub      time.Time

	corrupt atomic.Uint64
	logf    func(string, ...any)
}

// stamp identifies one version of a blob file. A blob whose digest was checked
// is trusted for as long as its stamp is unchanged: the file is replaced
// atomically, never edited, so a different inode, size or modification time
// means it is not the file that was checked.
type stamp struct {
	Size  int64  `json:"size"`
	MTime int64  `json:"mtime"`
	Ino   uint64 `json:"ino"`
}

func stampOf(fi os.FileInfo) stamp {
	return stamp{Size: fi.Size(), MTime: fi.ModTime().UnixNano(), Ino: inode(fi)}
}

type blobWriteLock struct {
	mu   sync.RWMutex // writers hold it exclusively, readers share it
	refs int
}

// blobLock returns the lock for d and counts a reference to it. Reference
// counting removes idle locks from the map.
func (s *Store) blobLock(d string) *blobWriteLock {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writes == nil {
		s.writes = make(map[string]*blobWriteLock)
	}
	l := s.writes[d]
	if l == nil {
		l = &blobWriteLock{}
		s.writes[d] = l
	}
	l.refs++
	return l
}

func (s *Store) dropBlobLock(d string, l *blobWriteLock) {
	s.writeMu.Lock()
	l.refs--
	if l.refs == 0 {
		delete(s.writes, d)
	}
	s.writeMu.Unlock()
}

// lockBlob serializes operations on one digest without blocking unrelated
// layer downloads.
func (s *Store) lockBlob(d string) func() {
	l := s.blobLock(d)
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.dropBlobLock(d, l)
	}
}

// tryLockBlob takes the lock exclusively but gives up instead of waiting.
func (s *Store) tryLockBlob(d string) (func(), bool) {
	l := s.blobLock(d)
	if !l.mu.TryLock() {
		s.dropBlobLock(d, l)
		return nil, false
	}
	return func() {
		l.mu.Unlock()
		s.dropBlobLock(d, l)
	}, true
}

// tryRLockBlob takes the lock for reading, sharing it with other readers, but
// gives up instead of waiting for a writer. A writer holds the lock for the
// whole download, so a reader that waited would stall until it finished.
func (s *Store) tryRLockBlob(d string) (func(), bool) {
	l := s.blobLock(d)
	if !l.mu.TryRLock() {
		s.dropBlobLock(d, l)
		return nil, false
	}
	return func() {
		l.mu.RUnlock()
		s.dropBlobLock(d, l)
	}, true
}

// local tags an error as a local storage failure. A full disk is reported as
// ErrNoSpace, whether the reserve check or the operating system noticed it.
func local(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: %w", ErrNoSpace, err)
	}
	return fmt.Errorf("%w: %w", ErrLocal, err)
}

// localWriter tags errors from writing to the cache, so that they can be told
// apart from errors from the source being copied.
type localWriter struct{ w io.Writer }

func (l localWriter) Write(p []byte) (int, error) {
	n, err := l.w.Write(p)
	return n, local(err)
}

// MinFree returns the reserve set by SetMinFree.
func (s *Store) MinFree() int64 { return s.minFree.Load() }

// SetMinFree sets how many bytes of free disk space must remain after a
// download. Zero only requires that the download itself fits.
func (s *Store) SetMinFree(n int64) { s.minFree.Store(n) }

// capacityReservation tracks only the portion of a write that has not yet
// been reflected in the filesystem's free-space measurement.
type capacityReservation struct {
	store     *Store
	remaining int64
	total     int64
}

func (r *capacityReservation) consume(n int64) {
	if n <= 0 {
		return
	}
	r.store.capacityMu.Lock()
	if n > r.remaining {
		n = r.remaining
	}
	r.remaining -= n
	r.store.reserved -= n
	r.store.capacityMu.Unlock()
}

func (r *capacityReservation) release() {
	r.store.capacityMu.Lock()
	r.store.reserved -= r.remaining
	r.store.pending -= r.total
	r.remaining, r.total = 0, 0
	r.store.capacityMu.Unlock()
}

// reservingWriter reduces the outstanding reservation as bytes are actually
// written. This avoids double-counting bytes already reflected in free space.
type reservingWriter struct {
	w           io.Writer
	reservation *capacityReservation
}

func (w reservingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		w.reservation.consume(int64(n))
	}
	return n, err
}

// reserveSpace makes sure need more bytes fit within the size limit and the
// free-space reserve, and reserves them for an in-flight write. Outstanding
// reservations cover only bytes not yet written, because written bytes are
// already reflected in the filesystem's free-space measurement. If they do not
// fit and evict is set, the least recently used blobs are removed to make room,
// unless even removing all of them would not be enough: emptying the cache for
// nothing would only cost the cache.
func (s *Store) reserveSpace(need int64, evict bool) (*capacityReservation, error) {
	for attempt := 0; ; attempt++ {
		s.capacityMu.Lock()
		short := s.shortfallLocked(need)
		if short <= 0 {
			s.reserved += need
			s.pending += need
			r := &capacityReservation{store: s, remaining: need, total: need}
			s.capacityMu.Unlock()
			return r, nil
		}
		s.capacityMu.Unlock()
		if !evict || attempt >= 2 || short > s.used.Load() {
			return nil, ErrNoSpace
		}
		s.evict(short)
	}
}

// ValidDigest reports whether d is a well-formed sha256 digest: "sha256:"
// followed by exactly 64 lowercase hex characters. Digests end up in file
// paths, so everything that reaches the store must pass this check.
func ValidDigest(d string) bool {
	h, ok := strings.CutPrefix(d, "sha256:")
	if !ok || len(h) != 64 {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func New(root string) (*Store, error) {
	blobs := filepath.Join(root, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "manifests"), 0755); err != nil {
		return nil, err
	}
	removePartials(blobs)
	s := &Store{
		root:           root,
		aliases:        map[string]string{},
		free:           freeBytes,
		verified:       map[string]stamp{},
		lastUse:        map[string]int64{},
		aliasChecked:   map[string]int64{},
		evicted:        map[string]struct{}{},
		total:          totalBytes,
		slack:          defaultSlack,
		createBlobTemp: func(dir, pattern string) (blobTempFile, error) { return os.CreateTemp(dir, pattern) },
		renameBlob:     os.Rename,
		removeBlob:     os.Remove,
	}
	s.minAge.Store(int64(defaultEvictMinAge))
	s.used.Store(usage(blobs))
	s.loadVerified()
	aliasFile := filepath.Join(root, "aliases.json")
	b, err := os.ReadFile(aliasFile)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.aliases); err != nil {
			// Aliases can be rebuilt from peers and the containerd import, so
			// set the damaged file aside instead of refusing to start.
			_ = os.Rename(aliasFile, aliasFile+".corrupt")
			s.aliases = map[string]string{}
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	return s, nil
}

// removePartials deletes temporary files left behind by an interrupted download.
func removePartials(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".part-") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func (s *Store) path(d string) (string, error) {
	if !ValidDigest(d) {
		return "", errors.New("unsupported digest")
	}
	return filepath.Join(s.root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:")), nil
}

// Open returns the stored blob d after checking it against its digest. While
// the blob is being written Open does not wait for the write to finish: it
// reports the blob as missing (os.ErrNotExist), since it is not available yet.
// Waiting would hold up a peer asking for a layer this node is still
// downloading until the peer's timeout expires, and get a healthy node skipped.
func (s *Store) Open(d string) (*os.File, error) {
	p, e := s.path(d)
	if e != nil {
		return nil, e
	}
	unlock, ok := s.tryRLockBlob(d)
	if !ok {
		return nil, fmt.Errorf("blob %s is still being written: %w", d, os.ErrNotExist)
	}
	defer unlock()
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	before, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// A blob whose digest was checked is trusted while its stamp is unchanged.
	// Without this every read would hash the whole file first.
	s.verifiedMu.Lock()
	known, ok := s.verified[d]
	s.verifiedMu.Unlock()
	if ok && known == stampOf(before) {
		return f, nil
	}

	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		_ = f.Close()
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	unchanged := stampOf(before) == stampOf(after)
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") || !unchanged {
		_ = f.Close()
		s.noteCorrupt(d)
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, local(err)
		}
		s.goneCorrupt(d)
		return nil, ErrDigestMismatch
	}
	s.markVerified(d, stampOf(after))
	return f, nil
}

func (s *Store) Stat(d string) (os.FileInfo, error) {
	p, e := s.path(d)
	if e != nil {
		return nil, e
	}
	return os.Stat(p)
}

// PutVerified stores the content of r under digest d. The content only becomes
// visible once it has been fully written, flushed and checked against d. It
// places no limit on the size; use PutVerifiedLimit for untrusted sources.
func (s *Store) PutVerified(d string, r io.Reader) (int64, error) {
	return s.PutVerifiedLimit(d, r, -1)
}

// PutVerifiedLimit is PutVerified for content that may be hostile: the digest
// can only be checked once everything has been written, so without a bound a
// peer or registry could stream data until the disk is full. max is an upper
// bound on the size (negative means none). A positive max also has to fit in
// the free disk space, above the reserve set by SetMinFree, before any data is
// written.
func (s *Store) PutVerifiedLimit(d string, r io.Reader, max int64) (int64, error) {
	return s.put(d, r, max, true, nil)
}

// Progress observes a store in progress so that its content can be passed on
// while it is still arriving. Calls come from the goroutine doing the store.
type Progress interface {
	// Begin is called once the partial file exists and before anything is
	// written, only when the size bound is positive. The file can be opened
	// and read up to the number of bytes reported by Wrote. Its content is
	// not verified: it may still turn out wrong, which End reports.
	Begin(path string, max int64)
	// Wrote reports the total number of bytes now readable from the file.
	Wrote(total int64)
	// End is called exactly once, after the content is stored (err is nil) or
	// has been abandoned. It can come without Begin, for content that was
	// stored already or is empty.
	End(err error)
}

// PutStream is PutVerifiedLimit that reports its progress to prog.
func (s *Store) PutStream(d string, r io.Reader, max int64, prog Progress) (int64, error) {
	return s.put(d, r, max, true, prog)
}

type progressWriter struct {
	w     io.Writer
	prog  Progress
	total int64
}

func (w *progressWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 {
		w.total += int64(n)
		w.prog.Wrote(w.total)
	}
	return n, err
}

// PutIfRoom is PutVerifiedLimit that never evicts: when the content does not fit
// it fails with ErrNoSpace. The containerd import uses it, because copying
// content that containerd already holds must not push out blobs that were
// fetched for a pull.
func (s *Store) PutIfRoom(d string, r io.Reader, max int64) (int64, error) {
	return s.put(d, r, max, false, nil)
}

func (s *Store) put(d string, r io.Reader, max int64, evict bool, prog Progress) (int64, error) {
	if prog == nil {
		return s.putLocked(d, r, max, evict, nil)
	}
	n, err := s.putLocked(d, r, max, evict, prog)
	// Only now, with the partial file renamed or removed and the blob's lock
	// released, so that End(nil) means the blob can be opened.
	prog.End(err)
	return n, err
}

func (s *Store) putLocked(d string, r io.Reader, max int64, evict bool, prog Progress) (written int64, retErr error) {
	unlock := s.lockBlob(d)
	defer unlock()
	p, e := s.path(d)
	if e != nil {
		return 0, e
	}
	if st, e := os.Stat(p); e == nil {
		// A digest-named file is not proof that its bytes are valid: the file
		// may have been truncated or modified after the original write.
		// Re-hash it before trusting it. Remove corrupt content so the caller
		// can fetch and store a verified replacement.
		f, openErr := os.Open(p)
		if openErr == nil {
			h := sha256.New()
			_, hashErr := io.Copy(h, f)
			closeErr := f.Close()
			if hashErr == nil && closeErr == nil && hex.EncodeToString(h.Sum(nil)) == strings.TrimPrefix(d, "sha256:") {
				return st.Size(), nil
			}
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, local(err)
		}
		s.noteCorrupt(d)
		s.goneCorrupt(d)
	} else if !errors.Is(e, os.ErrNotExist) {
		return 0, local(e)
	}
	var reservation *capacityReservation
	if max > 0 {
		var err error
		reservation, err = s.reserveSpace(max, evict)
		if err != nil {
			return 0, err
		}
		defer reservation.release()
	}
	tmp, e := s.createBlobTemp(filepath.Dir(p), ".part-")
	if e != nil {
		return 0, local(e)
	}
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		if removeErr := s.removeBlob(tmp.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, local(removeErr))
		}
	}()
	src := r
	if max >= 0 {
		src = io.LimitReader(r, max+1) // one byte more than allowed, to notice an excess
	}
	h := sha256.New()
	var dst io.Writer = localWriter{tmp}
	if reservation != nil {
		dst = reservingWriter{w: dst, reservation: reservation}
	}
	if prog != nil && max > 0 {
		prog.Begin(tmp.Name(), max)
		dst = &progressWriter{w: dst, prog: prog}
	}
	n, e := io.Copy(io.MultiWriter(dst, h), src)
	if e != nil {
		if closeErr := tmp.Close(); closeErr != nil {
			e = errors.Join(e, local(closeErr))
		}
		return n, e
	}
	if max >= 0 && n > max {
		e = ErrTooLarge
		if closeErr := tmp.Close(); closeErr != nil {
			e = errors.Join(e, local(closeErr))
		}
		return n, e
	}
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") {
		e = ErrDigestMismatch
		if closeErr := tmp.Close(); closeErr != nil {
			e = errors.Join(e, local(closeErr))
		}
		return n, e
	}
	if e = tmp.Sync(); e != nil {
		e = local(e)
		if closeErr := tmp.Close(); closeErr != nil {
			e = errors.Join(e, local(closeErr))
		}
		return n, e
	}
	if e = tmp.Close(); e != nil {
		return n, local(e)
	}
	if e = s.renameBlob(tmp.Name(), p); e != nil {
		return n, local(e)
	}
	cleanup = false
	// The digest was computed while the content was written, so this is a
	// verified blob already; recording that saves hashing it again on first read.
	if fi, err := os.Stat(p); err == nil {
		s.markVerified(d, stampOf(fi))
	}
	s.used.Add(n)
	s.markUsed(d)
	s.forgetEvicted(d)
	return n, nil
}

func key(ns, repo, ref string) string { return ns + "/" + repo + "@" + ref }

// SetAlias points ns/repo:ref at digest d and notes that it was confirmed just
// now. Writing an alias that already exists with the same digest does not
// rewrite the alias file, so a tag that is confirmed again and again costs
// nothing on disk.
func (s *Store) SetAlias(ns, repo, ref, d string) error {
	return s.SetAliasAt(ns, repo, ref, d, time.Now())
}

// SetAliasAt is SetAlias with the time of the confirmation, for callers that
// keep their own clock.
func (s *Store) SetAliasAt(ns, repo, ref, d string, at time.Time) error {
	if !ValidDigest(d) {
		return errors.New("unsupported digest")
	}
	k := key(ns, repo, ref)
	if err := s.setAlias(k, d, false); err != nil {
		return err
	}
	s.noteAliasChecked(k, at)
	return nil
}

// SetAliasIfAbsent records an alias that was learned from somewhere that is
// not the registry (containerd's own image names) and so says nothing about
// what the tag points at now: it never replaces an alias that exists, and the
// alias it creates counts as unconfirmed. It reports whether it created one.
func (s *Store) SetAliasIfAbsent(ns, repo, ref, d string) (bool, error) {
	if !ValidDigest(d) {
		return false, errors.New("unsupported digest")
	}
	k := key(ns, repo, ref)
	s.mu.RLock()
	_, exists := s.aliases[k]
	s.mu.RUnlock()
	if exists {
		return false, nil
	}
	if err := s.setAlias(k, d, true); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) setAlias(k, d string, onlyIfAbsent bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, existed := s.aliases[k]
	if existed && (previous == d || onlyIfAbsent) {
		return nil
	}
	s.aliases[k] = d
	if err := s.saveLocked(); err != nil {
		// Do not expose an alias in memory when its durable update failed.
		if existed {
			s.aliases[k] = previous
		} else {
			delete(s.aliases, k)
		}
		return err
	}
	return nil
}

func (s *Store) noteAliasChecked(k string, at time.Time) {
	s.verifiedMu.Lock()
	s.aliasChecked[k] = at.Unix()
	s.usageDirty = true
	s.verifiedMu.Unlock()
}

// AliasChecked reports when ns/repo:ref was last confirmed against its source,
// or false if it never was (an alias imported from containerd, or one saved
// before the time was kept).
func (s *Store) AliasChecked(ns, repo, ref string) (time.Time, bool) {
	s.verifiedMu.Lock()
	defer s.verifiedMu.Unlock()
	t, ok := s.aliasChecked[key(ns, repo, ref)]
	return time.Unix(t, 0), ok
}

// saveLocked writes the alias file via a temporary file and a rename, so a
// crash can never leave a half-written aliases.json behind.
func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.aliases, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.root, ".aliases-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
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
	return os.Rename(tmp.Name(), filepath.Join(s.root, "aliases.json"))
}

// Resolve maps a tag to a digest. A digest reference resolves to itself,
// whether or not its content is present locally.
func (s *Store) Resolve(ns, repo, ref string) (string, bool) {
	if strings.HasPrefix(ref, "sha256:") {
		return ref, ValidDigest(ref)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.aliases[key(ns, repo, ref)]
	return d, ok
}
