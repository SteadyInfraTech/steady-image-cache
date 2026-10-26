package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// Writable reports whether the cache can still take new files. It is what a
// readiness probe asks: a node whose cache directory has become read-only,
// unmounted or unwritable would otherwise stay in every other node's list of
// peers while every download it takes fails.
//
// Two places are probed, because the store writes to both and their permissions
// can change independently: the blob directory (downloads) and the cache root
// (aliases.json and verified.json, written when a tag is confirmed or learned).
//
// A full disk is not a failure here. A node that cannot store anything new can
// still serve what it holds, which is all a peer is asked to do, so it stays
// ready; the disk-full counters and the 507 answers report that condition. But
// only a full disk is excused: it never hides another error from the same probe.
//
// The probe file in the blob directory carries the ".part-" prefix, so the usage
// count, eviction and the scrubber ignore it, and a crash between creating and
// removing it is cleaned up at the next start. The one in the root is ".ready-".
func (s *Store) Writable() error {
	return errors.Join(
		s.probeDir(s.root, ".ready-"),
		s.probeDir(filepath.Join(s.root, "blobs", "sha256"), ".part-ready-"),
	)
}

// probeDir creates, writes, syncs, closes and removes one small file in dir.
func (s *Store) probeDir(dir, pattern string) error {
	f, err := s.createBlobTemp(dir, pattern)
	if err != nil {
		return probeResult(err)
	}
	// The write path syncs before it renames, and a filesystem can take a
	// buffered write and fail only on the sync, so the probe has to sync too.
	_, werr := f.Write([]byte{0})
	var serr error
	if werr == nil {
		serr = f.Sync()
	}
	cerr := f.Close()
	rerr := s.removeBlob(f.Name())
	if errors.Is(rerr, os.ErrNotExist) {
		rerr = nil
	}
	return probeResult(werr, serr, cerr, rerr)
}

// probeResult reports the errors of one probe as a local failure. A full disk is
// left out, but only that: any other error among them still fails the probe.
func probeResult(errs ...error) error {
	var bad []error
	for _, err := range errs {
		if err != nil && !errors.Is(err, syscall.ENOSPC) {
			bad = append(bad, err)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	return local(errors.Join(bad...))
}
