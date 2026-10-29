package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

// IsProtected reports whether blob d carries the mark. When that cannot be
// told (the file system fails), it says yes: the caller then asks the registry,
// which is the safe way to be wrong.
func (s *Store) IsProtected(d string) bool {
	p, err := s.markPath(d)
	if err != nil {
		return false // not a digest; nothing is stored under it
	}
	_, err = os.Stat(p)
	return err == nil || !errors.Is(err, os.ErrNotExist)
}

// pruneMarks removes the marks of blobs that are no longer stored. It runs at
// start, before anything is served: a mark is never removed while the cache is
// in use, because a blob that is evicted and fetched again must not pass,
// however briefly, for one that came from anywhere else.
func (s *Store) pruneMarks() {
	dir := filepath.Join(s.root, "protected")
	marks, err := os.ReadDir(dir)
	if err != nil || len(marks) == 0 {
		return
	}
	have := map[string]bool{}
	if blobs, err := os.ReadDir(filepath.Join(s.root, "blobs", "sha256")); err == nil {
		for _, b := range blobs {
			sum := sha256.Sum256([]byte("sha256:" + b.Name()))
			have[hex.EncodeToString(sum[:])] = true
		}
	}
	for _, m := range marks {
		if !have[m.Name()] {
			_ = os.Remove(filepath.Join(dir, m.Name()))
		}
	}
}
