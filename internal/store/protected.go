package store

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

// A blob that came from a protected registry carries a mark: an empty file named
// after the hash of its digest (a name made here, not one taken from a
// request). The mark outlives a restart, goes when the blob is evicted, and is
// all the cache knows about where a blob came from; the proxy uses it to ask
// the registry before it gives such a blob to a client of another namespace.

func (s *Store) markPath(d string) (string, error) {
	if !ValidDigest(d) {
		return "", os.ErrInvalid
	}
	sum := sha256.Sum256([]byte(d))
	return filepath.Join(s.root, "protected", hex.EncodeToString(sum[:])), nil
}

// MarkProtected records that blob d was given out under a protected registry.
func (s *Store) MarkProtected(d string) error {
	p, err := s.markPath(d)
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	return f.Close()
}

// IsProtected reports whether blob d carries the mark.
func (s *Store) IsProtected(d string) bool {
	p, err := s.markPath(d)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

func (s *Store) unmark(d string) {
	if p, err := s.markPath(d); err == nil {
		_ = os.Remove(p)
	}
}
