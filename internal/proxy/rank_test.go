package proxy

import (
	"slices"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
)

func filterOf(t *testing.T, idx *peer.DigestIndex, base string, digests ...string) {
	t.Helper()
	src := peer.NewDigestIndex()
	src.Rebuild(digests)
	if !idx.UpdatePeer(base, src.SelfBytes()) {
		t.Fatal("filter refused")
	}
}

// Peers whose filter has the digest come first, then those nothing is known
// about, then those whose filter says no (which may be out of date, so they stay).
func TestRankByFiltersAsksLikelyHoldersFirst(t *testing.T) {
	idx := peer.NewDigestIndex()
	const d = "sha256:aaaa"
	filterOf(t, idx, "https://has", d)
	filterOf(t, idx, "https://hasnot", "sha256:bbbb")
	got := rankByFilters(idx, d, []string{"https://hasnot", "https://unknown", "https://has"})
	want := []string{"https://has", "https://unknown", "https://hasnot"}
	if !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

type fixedPeers []string

func (f fixedPeers) Candidates() []string { return f }

// With a zone, the cap on tries must never leave out a same-zone peer for a
// cross-zone one: the random order is inside a group only.
func TestAskablePeersKeepsSameZonePeersFirstUnderTheCap(t *testing.T) {
	r := peer.NewZoneResolver()
	same := []string{"https://a1", "https://a2"}
	var all []string
	for _, b := range same {
		r.SetZone(b, "z1")
	}
	for _, b := range []string{"https://b1", "https://b2", "https://b3", "https://b4", "https://b5"} {
		r.SetZone(b, "z2")
		all = append(all, b)
	}
	all = append(all, same...)
	p := &Proxy{
		peers:    &peer.ZoneAwareFinder{Inner: fixedPeers(all), LocalZone: "z1", Resolver: r},
		down:     map[string]time.Time{},
		now:      time.Now,
		maxTries: 2,
		metrics:  &metrics.Metrics{},
	}
	for range 100 {
		got := p.askablePeers("", "")
		slices.Sort(got)
		if !slices.Equal(got, same) {
			t.Fatalf("asked %v, want the two same-zone peers", got)
		}
	}
}
