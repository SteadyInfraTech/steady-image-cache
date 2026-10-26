package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// Writable reports whether the cache directory can still take a new file. It is
// what a readiness probe asks: a node whose cache directory has become read-only,
// unmounted or unwritable would otherwise stay in every other node's list of
// peers while every download it takes fails.
//
// A full disk is not a failure here. A node that cannot store anything new can
// still serve what it holds, which is all a peer is asked to do, so it stays
// ready; the disk-full counters and the 507 answers report that condition.
//
// The probe file carries the ".part-" prefix, so the usage count, eviction and
// the scrubber ignore it, and a crash between creating and removing it is
// cleaned up at the next start.
func (s *Store) Writable() error {
	dir := filepath.Join(s.root, "blobs", "sha256")
	f, err := s.createBlobTemp(dir, ".part-ready-")
	if err != nil {
		return probeResult(err)
	}
	_, werr := f.Write([]byte{0})
	cerr := f.Close()
	rerr := s.removeBlob(f.Name())
	if errors.Is(rerr, os.ErrNotExist) {
		rerr = nil
	}
	return probeResult(errors.Join(werr, cerr, rerr))
}

// probeResult tags err as a local failure, except that a full disk is not one.
func probeResult(err error) error {
	if err == nil || errors.Is(err, syscall.ENOSPC) {
		return nil
	}
	return local(err)
}
