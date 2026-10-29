package store

import (
	"os"
	"path/filepath"
	"strings"
)

// A blob that came from a protected registry carries a mark: an empty file named
// after it. The mark outlives a restart, goes when the blob is evicted, and is
// all the cache knows about where a blob came from; the proxy uses it to ask
// the registry before it gives such a blob to a client of another namespace.

func (s *Store) markPath(d string) (string, error) {
	if !ValidDigest(d) {
		return "", os.ErrInvalid
	}
	return filepath.Join(s.root, "protected", strings.TrimPrefix(d, "sha256:")), nil
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
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o640)
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
