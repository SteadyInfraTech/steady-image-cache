// Package peer implements SIC peer discovery and digest-index advertisement.
//
// This file implements a counting Bloom filter used to advertise which blob
// digests a node holds. Nodes exchange these filters periodically; a querying
// node checks the filter before sending an HTTP request, turning blind
// sequential probing into a near-deterministic 1-hop lookup.
//
// Design choices:
//   - 4-bit counters, packed two to a byte: the serialised filter is 64 KiB
//     (131 072 counters), whatever the cache holds.
//   - k=5 hash functions give a false-positive rate under 1% at 10 000 digests,
//     and the filter fills up (more false positives) beyond some tens of thousands.
//   - The filter is Add-only between full rebuilds; removals happen by
//     rebuilding from the store periodically (see DigestIndex.Rebuild).

package peer

import (
	"sync"
)

const (
	// filterBits is the number of counter slots in the counting Bloom filter.
	// 1<<17 = 131 072 slots; at 4 bits each that is 64 KiB. At 10 000 digests
	// and k=5 the false-positive rate is well under 1%.
	filterBits = 1 << 17
	filterK    = 5 // number of hash functions
)

// countingFilter is a counting Bloom filter with 4-bit counters packed two
// per byte. It is safe for concurrent reads but mutations must be serialised by
// the caller.
type countingFilter struct {
	// data holds filterBits 4-bit counters, two per byte.
	data [filterBits / 2]byte
}

// hashPositions returns the k slot positions for key.
func hashPositions(key string) [filterK]uint32 {
	// Two independent 64-bit hashes via FNV-inspired mixing give k independent
	// positions without extra hash calls.
	var h1, h2 uint64
	for i := 0; i < len(key); i++ {
		h1 = h1*1099511628211 ^ uint64(key[i])
		h2 = h2*14695981039346656037 ^ uint64(key[i])
	}
	h1 ^= h1 >> 33
	h1 *= 0xff51afd7ed558ccd
	h1 ^= h1 >> 33
	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33

	var out [filterK]uint32
	for i := range out {
		// Double hashing: h1 + i*h2 mod filterBits
		combined := h1 + uint64(i)*h2
		// Fast mod for power-of-two
		out[i] = uint32(combined) & (filterBits - 1)
	}
	return out
}

func (f *countingFilter) get(pos uint32) uint8 {
	b := f.data[pos>>1]
	if pos&1 == 0 {
		return b & 0x0f
	}
	return b >> 4
}

func (f *countingFilter) set(pos uint32, v uint8) {
	idx := pos >> 1
	if pos&1 == 0 {
		f.data[idx] = (f.data[idx] & 0xf0) | (v & 0x0f)
	} else {
		f.data[idx] = (f.data[idx] & 0x0f) | (v << 4)
	}
}

// Add increments the counters for key. Counters saturate at 15 (no wrap).
func (f *countingFilter) Add(key string) {
	for _, pos := range hashPositions(key) {
		if c := f.get(pos); c < 15 {
			f.set(pos, c+1)
		}
	}
}

// Test reports whether key is probably in the set (may false-positive, never
// false-negative).
func (f *countingFilter) Test(key string) bool {
	for _, pos := range hashPositions(key) {
		if f.get(pos) == 0 {
			return false
		}
	}
	return true
}

// marshalBinary serialises the filter as its raw nibble-packed bytes.
// The result is always filterBits/2 bytes (65 536 bytes for filterBits=131 072).
func (f *countingFilter) marshalBinary() []byte {
	out := make([]byte, len(f.data))
	copy(out, f.data[:])
	return out
}

// unmarshalBinary loads a previously serialised filter. The input must be
// exactly filterBits/2 bytes; otherwise it is silently ignored.
func (f *countingFilter) unmarshalBinary(b []byte) bool {
	if len(b) != len(f.data) {
		return false
	}
	copy(f.data[:], b)
	return true
}

// estimateCount estimates the number of distinct items added using the formula
// n ≈ -(m/k) * ln(1 - X/m) where X is the number of zero slots.
func (f *countingFilter) estimateCount() int {
	zeros := 0
	for _, b := range f.data {
		if b&0x0f == 0 {
			zeros++
		}
		if b>>4 == 0 {
			zeros++
		}
	}
	nonZero := filterBits - zeros
	if nonZero == 0 {
		return 0
	}
	// Approximate with integer arithmetic: n ≈ nonZero * filterBits / (filterK * filterBits - filterK * nonZero)
	// Simplified: avoid floating point in hot path
	denom := filterK * (filterBits - nonZero)
	if denom <= 0 {
		return nonZero
	}
	return nonZero * filterBits / (filterK*(filterBits-nonZero) + 1)
}

// DigestIndex is a thread-safe digest advertisement index. Each node maintains
// one for itself (rebuilt from the store) and one per known peer (received via
// gossip). The Proxy uses Holders to route a miss to the most-likely peer
// before falling back to random probing.
type DigestIndex struct {
	mu       sync.RWMutex
	self     countingFilter
	peers    map[string]*countingFilter // base URL → filter
	popcount map[string]int             // estimated item counts per peer
}

// indexedPeer is used for sorting candidates by filter score.
type indexedPeer struct {
	base  string
	score int // higher → prefer this peer
}

// NewDigestIndex creates an empty DigestIndex.
func NewDigestIndex() *DigestIndex {
	return &DigestIndex{
		peers:    make(map[string]*countingFilter),
		popcount: make(map[string]int),
	}
}

// Rebuild replaces the self filter with a fresh one built from digests.
// It is called periodically by the importer / store background pass.
func (idx *DigestIndex) Rebuild(digests []string) {
	var f countingFilter
	for _, d := range digests {
		f.Add(d)
	}
	idx.mu.Lock()
	idx.self = f
	idx.mu.Unlock()
}

// SelfBytes serialises the self filter for gossip.
func (idx *DigestIndex) SelfBytes() []byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.self.marshalBinary()
}

// UpdatePeer stores a received filter for the peer at base.
func (idx *DigestIndex) UpdatePeer(base string, data []byte) bool {
	var f countingFilter
	if !f.unmarshalBinary(data) {
		return false
	}
	idx.mu.Lock()
	idx.peers[base] = &f
	idx.popcount[base] = f.estimateCount()
	idx.mu.Unlock()
	return true
}

// Holders returns the subset of candidates that probably hold digest, ranked
// by estimated fill (most-loaded last, so the caller iterates front-first and
// prefers the peer least likely to be saturated). Peers with no known filter
// are returned last (we have no information, treat as potential holder).
func (idx *DigestIndex) Holders(digest string, candidates []string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var known, unknown []indexedPeer
	for _, base := range candidates {
		f, ok := idx.peers[base]
		if !ok {
			unknown = append(unknown, indexedPeer{base: base, score: 0})
			continue
		}
		if f.Test(digest) {
			// Score by estimated sparseness: a filter with fewer items is less
			// loaded, prefer it first.
			known = append(known, indexedPeer{base: base, score: filterBits - idx.popcount[base]})
		}
	}
	sortIndexedPeers(known)
	sortIndexedPeers(unknown)

	out := make([]string, 0, len(known)+len(unknown))
	for _, s := range known {
		out = append(out, s.base)
	}
	for _, s := range unknown {
		out = append(out, s.base)
	}
	return out
}

// Classify splits candidates by what the filters say about digest: the peers whose
// filter has it, the peers there is no filter for, and the peers whose filter does
// not have it. Each list keeps the order of candidates, so a ranking the caller
// already made (same zone first, say) survives.
func (idx *DigestIndex) Classify(digest string, candidates []string) (holders, unknown, absent []string) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, base := range candidates {
		f, ok := idx.peers[base]
		switch {
		case !ok:
			unknown = append(unknown, base)
		case f.Test(digest):
			holders = append(holders, base)
		default:
			absent = append(absent, base)
		}
	}
	return holders, unknown, absent
}

// keepOnly drops the filters of the peers that are not in active.
func (idx *DigestIndex) keepOnly(active map[string]bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for base := range idx.peers {
		if !active[base] {
			delete(idx.peers, base)
			delete(idx.popcount, base)
		}
	}
}

// hasPeer reports whether a filter is held for the peer at base.
func (idx *DigestIndex) hasPeer(base string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.peers[base]
	return ok
}

// PeerCount returns the number of peers for which a filter is held.
func (idx *DigestIndex) PeerCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.peers)
}

// sortIndexedPeers sorts by score descending (prefer less-loaded peers first).
// Insertion sort: peer lists are typically <200 elements.
func sortIndexedPeers(s []indexedPeer) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].score > s[j-1].score; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// FilterSize returns the on-wire size in bytes of a serialised filter.
func FilterSize() int { return filterBits / 2 }

// FilterPopBits returns the total number of counter slots (for sizing metrics).
func FilterPopBits() int { return filterBits }

// Snapshot returns the serialised self filter and its fingerprint, read under
// one lock: the pair always describes the same state of the filter.
func (idx *DigestIndex) Snapshot() (fp uint64, data []byte) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return fingerprintOf(idx.self.data[:]), idx.self.marshalBinary()
}

func fingerprintOf(data []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, b := range data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}
