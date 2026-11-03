package peer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type gossipPeers struct{ list atomic.Pointer[[]string] }

func (g *gossipPeers) set(l ...string)      { g.list.Store(&l) }
func (g *gossipPeers) Candidates() []string { return *g.list.Load() }

func digestN(i int) string { return fmt.Sprintf("sha256:%064x", i) }

// node is a peer that holds the digests 0..n-1 and serves its filter.
func node(t *testing.T, n int, allow func(*http.Request) bool) (*httptest.Server, *int32) {
	t.Helper()
	idx := NewDigestIndex()
	var ds []string
	for i := range n {
		ds = append(ds, digestN(i))
	}
	idx.Rebuild(ds)
	var served int32
	mux := http.NewServeMux()
	h := &GossipHandler{Index: idx, Allow: allow}
	mux.HandleFunc(GossipPath, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&served, 1)
		h.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &served
}

func runner(idx *DigestIndex, p Finder, srv *httptest.Server) *GossipRunner {
	return &GossipRunner{Index: idx, Peers: p, Client: func() *http.Client { return srv.Client() },
		Header: map[string]string{"X-SIC-Peer": "1"}}
}

// A filter is a 64 KiB block; sent as base64 it has to fit the limit the
// runner puts on a response (it did not, and no filter was ever stored).
func TestGossipDeliversAFullSizeFilter(t *testing.T) {
	srv, _ := node(t, 5000, nil)
	idx := NewDigestIndex()
	var p gossipPeers
	p.set(srv.URL)
	runner(idx, &p, srv).Round(context.Background())
	if idx.PeerCount() != 1 {
		t.Fatalf("no filter was stored from the peer")
	}
	if got := idx.Holders(digestN(42), []string{srv.URL}); len(got) != 1 {
		t.Fatalf("Holders = %v for a digest the peer holds", got)
	}
	if got := idx.Holders(digestN(999999), []string{srv.URL}); len(got) != 0 {
		t.Fatalf("Holders = %v for a digest the peer does not hold", got)
	}
}

// A peer that stops being a candidate loses its filter; when it comes back with
// the same filter it must be sent again, not answered "not modified".
func TestGossipPeerThatComesBackGetsItsFilterAgain(t *testing.T) {
	srv, _ := node(t, 10, nil)
	idx := NewDigestIndex()
	var p gossipPeers
	p.set(srv.URL)
	g := runner(idx, &p, srv)
	g.Round(context.Background())
	g.Round(context.Background()) // unchanged: answered 304, filter kept
	if idx.PeerCount() != 1 {
		t.Fatal("an unchanged filter was lost")
	}
	p.set() // not ready for a while
	g.Round(context.Background())
	if idx.PeerCount() != 0 {
		t.Fatal("the filter of a peer that left was kept")
	}
	p.set(srv.URL)
	g.Round(context.Background())
	if idx.PeerCount() != 1 {
		t.Fatal("a peer that came back with an unchanged filter stayed unknown")
	}
}

func TestGossipHandlerRefusesWhoIsNotAPeer(t *testing.T) {
	srv, _ := node(t, 1, func(r *http.Request) bool { return r.Header.Get("X-SIC-Peer") != "" })
	resp, err := http.Get(srv.URL + GossipPath)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a client got %d, want 403", resp.StatusCode)
	}
	idx := NewDigestIndex()
	var p gossipPeers
	p.set(srv.URL)
	runner(idx, &p, srv).Round(context.Background())
	if idx.PeerCount() != 1 {
		t.Fatal("a peer was refused")
	}
}

// A peer that sends its headers and then stalls must not hold up the others.
func TestGossipStalledPeerDoesNotBlockTheRound(t *testing.T) {
	release := make(chan struct{})
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release
	}))
	defer stall.Close()
	defer close(release)
	good, _ := node(t, 3, nil)
	idx := NewDigestIndex()
	var p gossipPeers
	p.set(stall.URL, good.URL)
	g := runner(idx, &p, good)
	g.FetchTimeout = 200 * time.Millisecond
	done := make(chan struct{})
	go func() { g.Round(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled peer blocked the round")
	}
	if !idx.hasPeer(good.URL) {
		t.Fatal("the peer after the stalled one got no update")
	}
}
