// Package peer implements SIC peer discovery and digest-index advertisement.
//
// This file implements a counting Bloom filter used to advertise which blob
// digests a node holds. Nodes exchange these filters periodically; a querying
// node checks the filter before sending an HTTP request, turning blind
// sequential probing into a near-deterministic 1-hop lookup.
//
// Design choices:
//   - 4-bit counters (nibble-packed) keep the serialised filter under 5 KiB for
//     a typical 10 000-digest cache (m=80 000 bits, 10 bytes/counter × 4 bits).
//   - k=5 hash functions give a false-positive rate < 2% at that fill level.
//   - The filter is Add-only between full rebuilds; removals happen by
//     rebuilding from the store periodically (see DigestIndex.Rebuild).

package peer

import (
	"encoding/binary"
	"math/bits"
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

// Remove decrements the counters for key. Counters clamp at 0.
func (f *countingFilter) Remove(key string) {
	for _, pos := range hashPositions(key) {
		if c := f.get(pos); c > 0 {
			f.set(pos, c-1)
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

// reset clears the filter.
func (f *countingFilter) reset() { f.data = [filterBits / 2]byte{} }

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
	return nonZero * filterBits / (filterK * (filterBits - nonZero) + 1)
}

// DigestIndex is a thread-safe digest advertisement index. Each node maintains
// one for itself (rebuilt from the store) and one per known peer (received via
// gossip). The Proxy uses Holders to route a miss to the most-likely peer
// before falling back to random probing.
type DigestIndex struct {
	mu      sync.RWMutex
	self    countingFilter
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

// AddSelf adds a single digest to the self filter (called when a blob is
// stored, before the next full rebuild).
func (idx *DigestIndex) AddSelf(digest string) {
	idx.mu.Lock()
	idx.self.Add(digest)
	idx.mu.Unlock()
}

// RemoveSelf removes a single digest from the self filter (called on eviction).
func (idx *DigestIndex) RemoveSelf(digest string) {
	idx.mu.Lock()
	idx.self.Remove(digest)
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

// RemovePeer deletes the filter for a peer that has left the cluster.
func (idx *DigestIndex) RemovePeer(base string) {
	idx.mu.Lock()
	delete(idx.peers, base)
	delete(idx.popcount, base)
	idx.mu.Unlock()
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

// PopCount returns the estimated number of distinct items in a peer's filter.
func (idx *DigestIndex) PopCount(base string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.popcount[base]
}

// SelfTest reports whether the local node's own filter says it holds digest.
func (idx *DigestIndex) SelfTest(digest string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.self.Test(digest)
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

// HammingWeight counts the number of non-zero nibbles in a serialised filter
// (used in tests and metrics to characterise filter density).
func HammingWeight(data []byte) int {
	n := 0
	for _, b := range data {
		if b&0x0f != 0 {
			n++
		}
		if b>>4 != 0 {
			n++
		}
	}
	return n
}

// popcount64 counts set bits in a uint64 (used internally).
func popcount64(x uint64) int { return bits.OnesCount64(x) }

// popcount16 counts set bits in a uint16.
func popcount16(x uint16) int { return bits.OnesCount16(x) }

// encodedLen returns the length of a base64-URL-encoded filter.
func encodedLen() int {
	// base64: ceil(n/3)*4
	n := filterBits / 2
	return (n + 2) / 3 * 4
}

// Fingerprint returns a 64-bit FNV-1a fingerprint of the filter bytes, used
// to detect whether a peer's filter has changed since the last exchange.
func (idx *DigestIndex) Fingerprint() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var h uint64 = 14695981039346656037
	for _, b := range idx.self.data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

// byteSwap64 returns x with bytes reversed (for endian-independent hashing in tests).
func byteSwap64(x uint64) uint64 { return binary.BigEndian.Uint64(binary.LittleEndian.AppendUint64(nil, x)) }

// Ensure unused imports are satisfied.
var _ = popcount64
var _ = popcount16
var _ = encodedLen
var _ = byteSwap64
var _ = HammingWeight
