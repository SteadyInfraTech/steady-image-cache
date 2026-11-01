package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// A blob can be a link to a file that is not the cache's: containerd's own copy
// of the same content. The cache then holds no second copy of it. The link is a
// symbolic one, so that everything that reads a blob follows it, and everything
// that walks the cache for what it owns (its size, what to evict, what to
// verify in the background) sees a link and leaves it alone: it is not the
// cache's disk space, and not the cache's to delete. What is read through it is
// verified like anything else, once, and again whenever the file changes; a file
// that is wrong costs the link, never the target.

var linkSeq atomic.Uint64

// LinkExternal makes target, a regular file of size bytes that holds blob d, the
// blob's content. It replaces what was at the blob's place (nothing, or a blob
// that did not verify); a blob that verifies is left as it is. The target is not read here.
func (s *Store) LinkExternal(d, target string, size int64) error {
	p, err := s.path(d)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(target) {
		return fmt.Errorf("link target %q is not absolute", target)
	}
	fi, err := os.Stat(target)
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() != size {
		return fmt.Errorf("link target %s is not a regular file of %d bytes", target, size)
	}
	unlock := s.lockBlob(d)
	defer unlock()
	replaced := false // a regular file, counted when it was stored, is being replaced
	if oi, err := os.Lstat(p); err == nil && oi.Mode().IsRegular() {
		// A download may have stored the blob since the caller looked: a copy
		// that verifies is kept, whatever the target is (it is only the size
		// that was checked here).
		match, hashErr := fileHashes(p, d)
		switch {
		case hashErr == nil && match:
			return nil
		case hashErr != nil && !errors.Is(hashErr, os.ErrNotExist):
			return local(hashErr)
		}
		replaced = true
	}
	tmp := p + ".part-link-" + strconv.FormatUint(linkSeq.Add(1), 10)
	_ = os.Remove(tmp)
	if err := os.Symlink(target, tmp); err != nil {
		return local(err)
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return local(err)
	}
	if replaced {
		// The damaged copy may have another size than the one that was counted
		// (see goneCorrupt): the total is measured again, not adjusted.
		s.remeasure()
	}
	return nil
}

// dropDanglingLink removes the link at p when its target is gone (containerd
// collected the content), under the blob's lock, which the caller holds.
func dropDanglingLink(p string) {
	if li, err := os.Lstat(p); err == nil && li.Mode()&os.ModeSymlink != 0 {
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(p)
		}
	}
}
