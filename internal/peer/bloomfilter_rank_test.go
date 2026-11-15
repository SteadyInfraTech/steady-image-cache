package peer

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// Classify sorts candidates three ways and keeps the caller's order in each.
func TestClassifySplitsByWhatTheFiltersSay(t *testing.T) {
	idx := NewDigestIndex()
	has := NewDigestIndex()
	has.Rebuild([]string{digestN(1), digestN(2)})
	lacks := NewDigestIndex()
	lacks.Rebuild([]string{digestN(50)})
	for base, src := range map[string]*DigestIndex{"https://h1": has, "https://h2": has, "https://a1": lacks, "https://a2": lacks} {
		if !idx.UpdatePeer(base, src.SelfBytes()) {
			t.Fatalf("the filter of %s was refused", base)
		}
	}
	cands := []string{"https://a1", "https://u1", "https://h2", "https://a2", "https://u2", "https://h1"}
	holders, unknown, absent := idx.Classify(digestN(1), cands)
	if want := []string{"https://h2", "https://h1"}; !reflect.DeepEqual(holders, want) {
		t.Errorf("holders %v, want %v", holders, want)
	}
	if want := []string{"https://u1", "https://u2"}; !reflect.DeepEqual(unknown, want) {
		t.Errorf("unknown %v, want %v", unknown, want)
	}
	if want := []string{"https://a1", "https://a2"}; !reflect.DeepEqual(absent, want) {
		t.Errorf("absent %v, want %v", absent, want)
	}
	if h, u, a := idx.Classify(digestN(1), nil); len(h)+len(u)+len(a) != 0 {
		t.Errorf("no candidates gave %v %v %v", h, u, a)
	}
}

func TestSelfBytesIsAFilterAPeerCanUse(t *testing.T) {
	idx := NewDigestIndex()
	idx.Rebuild([]string{digestN(7)})
	b := idx.SelfBytes()
	if len(b) != FilterSize() {
		t.Fatalf("%d bytes, want %d", len(b), FilterSize())
	}
	other := NewDigestIndex()
	if !other.UpdatePeer("https://x", b) {
		t.Fatal("a filter from SelfBytes was refused")
	}
	if h, _, _ := other.Classify(digestN(7), []string{"https://x"}); len(h) != 1 {
		t.Fatal("the digest the filter was built from is not found")
	}
	if other.UpdatePeer("https://y", b[:10]) {
		t.Fatal("a truncated filter was accepted")
	}
	if FilterPopBits() != FilterSize()*2 {
		t.Fatalf("FilterPopBits %d is not twice FilterSize %d (4-bit counters)", FilterPopBits(), FilterSize())
	}
}

// Of two peers that both probably hold a digest, the less loaded comes first;
// peers without a filter come after those that have one.
func TestHoldersPrefersTheLessLoadedFilter(t *testing.T) {
	idx := NewDigestIndex()
	light, heavy := NewDigestIndex(), NewDigestIndex()
	light.Rebuild([]string{digestN(0)})
	var many []string
	for i := range 4000 {
		many = append(many, digestN(i))
	}
	heavy.Rebuild(many)
	idx.UpdatePeer("https://heavy", heavy.SelfBytes())
	idx.UpdatePeer("https://light", light.SelfBytes())
	got := idx.Holders(digestN(0), []string{"https://heavy", "https://nofilter", "https://light"})
	if want := []string{"https://light", "https://heavy", "https://nofilter"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order %v, want %v", got, want)
	}
}

// Run fetches at once and then every interval, until the context ends.
func TestGossipRunFetchesRepeatedlyUntilCancelled(t *testing.T) {
	srv, served := node(t, 10, nil)
	idx := NewDigestIndex()
	var p gossipPeers
	p.set(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	var stopped atomic.Bool
	done := make(chan struct{})
	go func() { runner(idx, &p, srv).Run(ctx, 20*time.Millisecond); stopped.Store(true); close(done) }()
	waitFor(t, "a stored filter", func() bool { return idx.PeerCount() == 1 })
	// The filter does not change, so later rounds are "not modified", but the
	// peer is still asked every interval.
	waitFor(t, "repeated rounds", func() bool { return atomic.LoadInt32(served) >= 3 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop when its context ended")
	}
	if !stopped.Load() {
		t.Fatal("Run returned without stopping")
	}
}
