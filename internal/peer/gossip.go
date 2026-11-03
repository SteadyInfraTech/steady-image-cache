package peer

import (
	"context"
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
	// gossipMaxBody is the most a response may hold: the filter as base64, and
	// room for the JSON around it.
	gossipMaxBody = (filterBits/2+2)/3*4 + 1024
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
// GossipPath on the main listener only (the admin listener has no
// authentication), with Allow set to the check that tells another SIC node from
// a client: which digests a node holds is for the nodes to know.
type GossipHandler struct {
	Index *DigestIndex
	// Allow decides who may fetch the filter. Nil allows everyone.
	Allow func(*http.Request) bool
}

func (g *GossipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if g.Allow != nil && !g.Allow(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
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
	Index *DigestIndex
	Peers Finder
	// Client returns the client to use for one round (it may change when the
	// certificates are rotated).
	Client func() *http.Client
	// Header is sent with every request (it names this node a peer).
	Header map[string]string
	// SelfBase is this node's own base URL; its filter is never fetched.
	SelfBase string
	// Logf receives occasional warnings. Nil discards them.
	Logf func(string, ...any)

	// lastFP tracks the last fingerprint received per peer, to skip unchanged filters.
	mu     sync.Mutex
	lastFP map[string]uint64
}

// Run fetches the filters now and then every interval until ctx ends.
func (g *GossipRunner) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 3 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		g.Round(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Round fetches the filter of every candidate once and forgets the peers that
// are no longer candidates.
func (g *GossipRunner) Round(ctx context.Context) {
	candidates := g.Peers.Candidates()
	active := make(map[string]bool, len(candidates))
	for _, base := range candidates {
		active[base] = true
		if base == g.SelfBase || ctx.Err() != nil {
			continue
		}
		g.fetchOne(ctx, base)
	}
	// A peer that is not a candidate any more has its filter dropped, and its
	// fingerprint with it: when it comes back with the same filter it must be
	// sent again, not answered "not modified" about something we threw away.
	g.mu.Lock()
	for base := range g.lastFP {
		if !active[base] {
			delete(g.lastFP, base)
		}
	}
	g.mu.Unlock()
	g.Index.keepOnly(active)
}

func (g *GossipRunner) fetchOne(ctx context.Context, base string) {
	g.mu.Lock()
	fp, known := g.lastFP[base]
	g.mu.Unlock()

	u := base + GossipPath
	if known && g.Index.hasPeer(base) {
		u += "?fp=" + uint64ToStr(fp)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return
	}
	for k, v := range g.Header {
		req.Header.Set(k, v)
	}
	resp, err := g.Client().Do(req)
	if err != nil {
		g.logf("gossip: fetch %s: %v", base, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return // filter unchanged
	}
	if resp.StatusCode != http.StatusOK {
		g.logf("gossip: %s answered %d", base, resp.StatusCode)
		return
	}
	var body indexResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, gossipMaxBody)).Decode(&body); err != nil {
		g.logf("gossip: %s sent an unreadable filter: %v", base, err)
		return
	}
	raw, err := base64.URLEncoding.DecodeString(body.Filter)
	if err != nil {
		g.logf("gossip: %s sent an unreadable filter: %v", base, err)
		return
	}
	if g.Index.UpdatePeer(base, raw) {
		g.mu.Lock()
		if g.lastFP == nil {
			g.lastFP = make(map[string]uint64)
		}
		g.lastFP[base] = body.Fingerprint
		g.mu.Unlock()
	}
}

func (g *GossipRunner) logf(format string, args ...any) {
	if g.Logf != nil {
		g.Logf(format, args...)
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
