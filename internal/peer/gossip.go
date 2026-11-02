package peer

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// GossipHandler implements HTTP-level digest-index gossip between SIC nodes.
//
// Each node exposes GET /sic/index which returns its current Bloom filter
// snapshot encoded as base64. Peers call this endpoint every GossipInterval
// (default 3 minutes). The filter is small (~64 KiB) and compressed well over
// TLS: the actual data transfer is typically 10–15 KiB after entropy coding in
// the TLS record layer.
//
// The gossip path:
//  1. GossipRunner.Run() ticks every GossipInterval.
//  2. For each live peer it sends a GET /sic/index?self=<base64-fingerprint>.
//  3. The remote node responds 304 Not Modified when its fingerprint matches,
//     otherwise 200 with the full filter bytes (base64-encoded JSON body).
//  4. The local DigestIndex.UpdatePeer() stores the result.

const (
	// GossipPath is the HTTP path served by GossipHandler.
	GossipPath = "/sic/index"
	// gossipMaxBody is the maximum response body accepted from a peer: slightly
	// more than a full filter to allow for JSON framing.
	gossipMaxBody = filterBits/2 + 512
)

// indexResponse is the JSON body returned by GossipHandler.
type indexResponse struct {
	// Filter is the base64-URL-encoded nibble-packed counting Bloom filter.
	Filter string `json:"filter"`
	// Fingerprint is a 64-bit FNV-1a hash of the filter bytes, returned as a
	// decimal string so that the caller can detect unchanged filters cheaply.
	Fingerprint uint64 `json:"fingerprint"`
}

// GossipHandler serves this node's digest-index filter over HTTP. Mount it at
// GossipPath on the existing SIC listener (both the main TLS listener and the
// plain-HTTP admin listener, so peers can always fetch it).
type GossipHandler struct {
	Index *DigestIndex
}

func (g *GossipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fp := g.Index.Fingerprint()
	// Conditional GET: if the peer sends its known fingerprint and it matches,
	// respond 304 to save bandwidth.
	if r.URL.Query().Get("fp") == uint64ToStr(fp) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	raw := g.Index.SelfBytes()
	body := indexResponse{
		Filter:      base64.URLEncoding.EncodeToString(raw),
		Fingerprint: fp,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// GossipRunner periodically fetches filter snapshots from all live peers and
// stores them in the DigestIndex.
type GossipRunner struct {
	Index    *DigestIndex
	Peers    Finder
	Client   *http.Client
	Interval time.Duration
	// SelfBase is this node's own base URL; its filter is never fetched.
	SelfBase string
	// Logf receives occasional warnings. Nil discards them.
	Logf func(string, ...any)

	// lastFP tracks the last fingerprint received per peer, to skip unchanged filters.
	mu     sync.Mutex
	lastFP map[string]uint64
}

// Run runs the gossip loop until ctx is cancelled.
func (g *GossipRunner) Run(ctx interface{ Done() <-chan struct{} }, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Minute
	}
	g.mu.Lock()
	g.lastFP = make(map[string]uint64)
	g.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.round()
		}
	}
}

func (g *GossipRunner) round() {
	candidates := g.Peers.Candidates()
	for _, base := range candidates {
		if base == g.SelfBase {
			continue
		}
		g.fetchOne(base)
	}
	// Remove stale peers that are no longer candidates.
	g.Index.mu.Lock()
	active := make(map[string]bool, len(candidates))
	for _, b := range candidates {
		active[b] = true
	}
	for base := range g.Index.peers {
		if !active[base] {
			delete(g.Index.peers, base)
			delete(g.Index.popcount, base)
		}
	}
	g.Index.mu.Unlock()
}

func (g *GossipRunner) fetchOne(base string) {
	g.mu.Lock()
	fp := g.lastFP[base]
	g.mu.Unlock()

	u := base + GossipPath
	if fp != 0 {
		u += "?fp=" + uint64ToStr(fp)
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return
	}
	resp, err := g.Client.Do(req)
	if err != nil {
		if g.Logf != nil {
			g.Logf("gossip: fetch %s: %v", base, err)
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return // filter unchanged
	}
	if resp.StatusCode != http.StatusOK {
		return
	}
	var body indexResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, gossipMaxBody)).Decode(&body); err != nil {
		return
	}
	raw, err := base64.URLEncoding.DecodeString(body.Filter)
	if err != nil {
		return
	}
	if g.Index.UpdatePeer(base, raw) {
		g.mu.Lock()
		g.lastFP[base] = body.Fingerprint
		g.mu.Unlock()
	}
}

// uint64ToStr converts a uint64 to a decimal string without importing strconv
// (to avoid a cycle; this file is in the peer package).
func uint64ToStr(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte(v%10) + '0'
		v /= 10
	}
	return string(buf[i:])
}
