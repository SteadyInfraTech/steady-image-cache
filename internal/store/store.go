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
	root    string
	writeMu sync.Mutex // guards the per-digest writer-lock map
	writes  map[string]*blobWriteLock
	mu      sync.RWMutex
	aliases map[string]string

	minFree atomic.Int64
	free    func(path string) (uint64, bool) // replaced in tests

	capacityMu sync.Mutex // guards reservations across distinct digest writes
	reserved   int64

	verifiedMu sync.Mutex // guards metadata for blobs whose digest was verified
	verified   map[string]os.FileInfo
}

type blobWriteLock struct {
	mu   sync.Mutex
	refs int
}

// lockBlob serializes operations on one digest without blocking unrelated
// layer downloads. Reference counting removes idle locks from the map.
func (s *Store) lockBlob(d string) func() {
	s.writeMu.Lock()
	if s.writes == nil {
		s.writes = make(map[string]*blobWriteLock)
	}
	l := s.writes[d]
	if l == nil {
		l = &blobWriteLock{}
		s.writes[d] = l
	}
	l.refs++
	s.writeMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.writeMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(s.writes, d)
		}
		s.writeMu.Unlock()
	}
}

// local tags an error as a local storage failure. A full disk is reported as
// ErrNoSpace, whether the reserve check or the operating system noticed it.
func local(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: %v", ErrNoSpace, err)
	}
	return fmt.Errorf("%w: %v", ErrLocal, err)
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

// checkSpace reports ErrNoSpace if need more bytes plus the reserve do not fit.
// Where free space cannot be determined the check is skipped.
func (s *Store) checkSpace(need int64) error {
	free, ok := s.free(s.root)
	if !ok {
		return nil
	}
	reserve := s.minFree.Load()
	if reserve < 0 {
		reserve = 0
	}
	if free < uint64(need)+uint64(reserve) {
		return ErrNoSpace
	}
	return nil
}

// capacityReservation tracks only the portion of a write that has not yet
// been reflected in the filesystem's free-space measurement.
type capacityReservation struct {
	store     *Store
	remaining int64
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
	r.remaining = 0
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

// reserveSpace checks the configured free-space reserve and reserves capacity
// for an in-flight write. Outstanding reservations cover only bytes not yet
// written, because written bytes are already reflected in the filesystem's
// free-space measurement.
func (s *Store) reserveSpace(need int64) (*capacityReservation, error) {
	s.capacityMu.Lock()
	free, ok := s.free(s.root)
	reserve := s.minFree.Load()
	if reserve < 0 {
		reserve = 0
	}
	if ok {
		required := uint64(need) + uint64(reserve) + uint64(s.reserved)
		if free < required {
			s.capacityMu.Unlock()
			return nil, ErrNoSpace
		}
	}
	s.reserved += need
	r := &capacityReservation{store: s, remaining: need}
	s.capacityMu.Unlock()
	return r, nil
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
		root: root, aliases: map[string]string{}, free: freeBytes, verified: map[string]os.FileInfo{},
		createBlobTemp: func(dir, pattern string) (blobTempFile, error) { return os.CreateTemp(dir, pattern) },
		renameBlob: os.Rename,
		removeBlob: os.Remove,
	}
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

func (s *Store) Open(d string) (*os.File, error) {
	unlock := s.lockBlob(d)
	defer unlock()
	p, e := s.path(d)
	if e != nil {
		return nil, e
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	before, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	// A previously verified immutable blob can be trusted while its file
	// identity and metadata remain unchanged. This avoids hashing every read.
	s.verifiedMu.Lock()
	verified := s.verified[d]
	s.verifiedMu.Unlock()
	if verified != nil && os.SameFile(verified, before) &&
		verified.Size() == before.Size() && verified.ModTime().Equal(before.ModTime()) {
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
	unchanged := os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime().Equal(after.ModTime())
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") || !unchanged {
		_ = f.Close()
		s.verifiedMu.Lock()
		delete(s.verified, d)
		s.verifiedMu.Unlock()
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, local(err)
		}
		return nil, ErrDigestMismatch
	}
	s.verifiedMu.Lock()
	s.verified[d] = after
	s.verifiedMu.Unlock()
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
	} else if !errors.Is(e, os.ErrNotExist) {
		return 0, local(e)
	}
	var reservation *capacityReservation
	if max > 0 {
		var err error
		reservation, err = s.reserveSpace(max)
		if err != nil {
			return 0, err
		}
		defer reservation.release()
	}
	tmp, e := s.createBlobTemp(filepath.Dir(p), ".part-")
	if e != nil {
		return 0, local(e)
	}
	defer s.removeBlob(tmp.Name())
	src := r
	if max >= 0 {
		src = io.LimitReader(r, max+1) // one byte more than allowed, to notice an excess
	}
	h := sha256.New()
	var dst io.Writer = localWriter{tmp}
	if reservation != nil {
		dst = reservingWriter{w: dst, reservation: reservation}
	}
	n, e := io.Copy(io.MultiWriter(dst, h), src)
	if e != nil {
		tmp.Close()
		return n, e
	}
	if max >= 0 && n > max {
		tmp.Close()
		return n, ErrTooLarge
	}
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") {
		tmp.Close()
		return n, ErrDigestMismatch
	}
	if e = tmp.Sync(); e != nil {
		tmp.Close()
		return n, local(e)
	}
	if e = tmp.Close(); e != nil {
		return n, local(e)
	}
	if e = s.renameBlob(tmp.Name(), p); e != nil {
		return n, local(e)
	}
	return n, nil
}

func key(ns, repo, ref string) string { return ns + "/" + repo + "@" + ref }

// SetAlias points ns/repo:ref at digest d. Writing an alias that already
// exists with the same digest is a no-op, so the periodic containerd import
// does not rewrite the alias file on every run.
func (s *Store) SetAlias(ns, repo, ref, d string) error {
	if !ValidDigest(d) {
		return errors.New("unsupported digest")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(ns, repo, ref)
	if s.aliases[k] == d {
		return nil
	}
	previous, existed := s.aliases[k]
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
