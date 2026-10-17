package store

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// A blob counts as recently used for this long after SIC stored or served it,
// and eviction leaves it alone: it was fetched a moment ago for a request that
// has not finished reading it yet.
const defaultEvictMinAge = time.Minute

// touchEvery bounds how often serving a blob updates its last-use time.
const touchEvery = time.Minute

// maxRemembered bounds the set of recently evicted digests.
const maxRemembered = 10000

// SetMinFreePercent sets the share of the filesystem (0 to 90) that must stay
// free after a download; the larger of this and SetMinFree applies. The kubelet
// starts evicting pods when a node's image filesystem is more than 85% full, so
// a cache that fills the disk up to a gigabyte from the end hurts the node long
// before it hurts itself.
func (s *Store) SetMinFreePercent(n int) {
	if n < 0 {
		n = 0
	}
	if n > 90 {
		n = 90
	}
	s.minFreePct.Store(int64(n))
}

// SetMaxBytes limits the total size of the cache; zero means no limit.
func (s *Store) SetMaxBytes(n int64) { s.maxBytes.Store(n) }

// SetEvictMinAge sets how long a stored or served blob is protected from
// eviction (one minute by default).
func (s *Store) SetEvictMinAge(d time.Duration) { s.minAge.Store(int64(d)) }

// Used returns the number of bytes held in blobs and manifests.
func (s *Store) Used() int64 { return s.used.Load() }

// Evicted returns how many blobs and bytes eviction has removed.
func (s *Store) Evicted() (blobs, bytes uint64) { return s.evictedBlobs.Load(), s.evictedBytes.Load() }

// WasEvicted reports whether d was removed by eviction and has not been stored
// since. The containerd import uses it to avoid copying back what was just
// thrown out.
func (s *Store) WasEvicted(d string) bool {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	_, ok := s.evicted[d]
	return ok
}

func (s *Store) rememberEvicted(d string) {
	s.evMu.Lock()
	if len(s.evicted) >= maxRemembered {
		s.evicted = map[string]struct{}{}
	}
	s.evicted[d] = struct{}{}
	s.evMu.Unlock()
}

func (s *Store) forgetEvicted(d string) {
	s.evMu.Lock()
	delete(s.evicted, d)
	s.evMu.Unlock()
}

// Touch records that blob d was just served, so that eviction keeps it longer.
// It does not touch the file: its modification time is part of what vouches for
// the blob's verification. Open does not call it, so that the containerd import,
// which opens every blob, does not make everything look recently used.
func (s *Store) Touch(d string) {
	if !ValidDigest(d) {
		return
	}
	now := time.Now().UnixNano()
	s.verifiedMu.Lock()
	if last, ok := s.lastUse[d]; !ok || now-last > int64(touchEvery) {
		s.lastUse[d] = now
		s.usageDirty = true
	}
	s.verifiedMu.Unlock()
}

func (s *Store) markUsed(d string) {
	s.verifiedMu.Lock()
	s.lastUse[d] = time.Now().UnixNano()
	s.usageDirty = true
	s.verifiedMu.Unlock()
}

// forget drops what is recorded about blob d, which was removed from disk.
func (s *Store) forget(d string) {
	s.verifiedMu.Lock()
	if _, ok := s.verified[d]; ok {
		delete(s.verified, d)
		s.verifiedDirty = true
	}
	if _, ok := s.lastUse[d]; ok {
		delete(s.lastUse, d)
		s.usageDirty = true
	}
	s.verifiedMu.Unlock()
}

// goneCorrupt is called when blob d was removed because it did not match its
// digest. Such a file may have changed size since it was counted, so the total
// is measured again instead of adjusted; corruption is rare enough for that.
func (s *Store) goneCorrupt(d string) {
	s.forget(d)
	s.used.Store(usage(filepath.Join(s.root, "blobs", "sha256")))
}

// reserve is the free space that has to remain after a download.
func (s *Store) reserve() int64 {
	r := s.minFree.Load()
	if pct := s.minFreePct.Load(); pct > 0 {
		if total, ok := s.total(s.root); ok {
			if byPct := int64(total / 100 * uint64(pct)); byPct > r {
				r = byPct
			}
		}
	}
	if r < 0 {
		r = 0
	}
	return r
}

// shortfallLocked is how many bytes would have to be freed before need more
// bytes can be stored: the larger of what the size limit and the free-space
// reserve ask for. Zero or less means it fits. Where free space cannot be
// determined only the size limit applies. The caller holds capacityMu.
func (s *Store) shortfallLocked(need int64) int64 {
	var short int64
	if max := s.maxBytes.Load(); max > 0 {
		if over := s.used.Load() + s.pending + need - max; over > short {
			short = over
		}
	}
	if free, ok := s.free(s.root); ok {
		if free > math.MaxInt64 {
			free = math.MaxInt64
		}
		if lack := need + s.reserve() + s.reserved - int64(free); lack > short {
			short = lack
		}
	}
	return short
}

type candidate struct {
	name string
	size int64
	last int64 // last use, unix nanoseconds
}

// evict removes the least recently used blobs until at least short bytes are
// freed, plus a little more so that the next few downloads do not each have to
// scan the cache again. Blobs used within the protection time, and blobs that
// are being read or written at this moment, are left alone.
func (s *Store) evict(short int64) {
	s.evictMu.Lock()
	defer s.evictMu.Unlock()
	target := short + s.slack(s.used.Load())
	dir := filepath.Join(s.root, "blobs", "sha256")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	s.verifiedMu.Lock()
	uses := make(map[string]int64, len(s.lastUse))
	for d, u := range s.lastUse {
		uses[d] = u
	}
	s.verifiedMu.Unlock()
	var cands []candidate
	for _, e := range entries {
		if !e.Type().IsRegular() || strings.HasPrefix(e.Name(), ".part-") {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		// Blobs without a record (stored before last use was kept, say) count as
		// used when they were written.
		last := fi.ModTime().UnixNano()
		if u, ok := uses["sha256:"+e.Name()]; ok {
			last = u
		}
		cands = append(cands, candidate{e.Name(), fi.Size(), last})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].last < cands[j].last })
	cutoff := time.Now().UnixNano() - s.minAge.Load()
	var freed int64
	for _, c := range cands {
		if freed >= target || c.last > cutoff {
			break // everything after this one is more recently used
		}
		d := "sha256:" + c.name
		unlock, ok := s.tryLockBlob(d)
		if !ok {
			continue // being read or written
		}
		err := os.Remove(filepath.Join(dir, c.name))
		unlock()
		if err != nil && !os.IsNotExist(err) {
			continue
		}
		freed += c.size
		s.used.Add(-c.size)
		s.forget(d)
		s.rememberEvicted(d)
		s.evictedBlobs.Add(1)
		s.evictedBytes.Add(uint64(c.size))
	}
}

func defaultSlack(used int64) int64 { return min(used/20, 1<<30) }

// usage adds up the size of the blobs in dir.
func usage(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var n int64
	for _, e := range entries {
		if !e.Type().IsRegular() || strings.HasPrefix(e.Name(), ".part-") {
			continue
		}
		if fi, err := e.Info(); err == nil {
			n += fi.Size()
		}
	}
	return n
}
