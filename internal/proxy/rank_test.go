package proxy

import (
	"slices"
	"strconv"
	"strings"
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

// Among the peers that match, the order given (nearest first) stays: a sparser
// filter does not move a farther peer ahead.
func TestRankByFiltersKeepsTheGivenOrderAmongMatches(t *testing.T) {
	idx := peer.NewDigestIndex()
	const d = "sha256:aaaa"
	dense := []string{d}
	for i := range 3000 {
		dense = append(dense, "sha256:"+strings.Repeat("c", 3)+strconv.Itoa(i))
	}
	filterOf(t, idx, "https://near", dense...)
	filterOf(t, idx, "https://far", d) // sparser than near
	got := rankByFilters(idx, d, []string{"https://near", "https://far"})
	if !slices.Equal(got, []string{"https://near", "https://far"}) {
		t.Fatalf("order = %v, want the nearest first", got)
	}
}

// A peer the groups lack (discovery changed between two reads) is still asked.
type shrinking struct {
	calls int
}

func (s *shrinking) Candidates() []string { return []string{"https://a", "https://b"} }
func (s *shrinking) CandidateGroups() [][]string {
	return [][]string{{"https://a"}} // b was removed after Candidates was read
}

func TestShuffledPeersKeepsALivePeerTheGroupsLack(t *testing.T) {
	p := &Proxy{peers: &shrinking{}, down: map[string]time.Time{}, now: time.Now, metrics: &metrics.Metrics{}}
	got := p.shuffledPeers()
	slices.Sort(got)
	if !slices.Equal(got, []string{"https://a", "https://b"}) {
		t.Fatalf("peers = %v, want both", got)
	}
}

// Through askablePeers, with a digest and filters: under the cap the same-zone
// holder is asked, not a cross-zone holder whose filter is sparser.
func TestAskablePeersWithFiltersKeepsTheSameZoneHolderUnderTheCap(t *testing.T) {
	const d = "sha256:aaaa"
	r := peer.NewZoneResolver()
	r.SetZone("https://near", "z1")
	r.SetZone("https://far", "z2")
	r.SetZone("https://other", "z1")
	idx := peer.NewDigestIndex()
	dense := []string{d}
	for i := range 3000 {
		dense = append(dense, "sha256:c"+strconv.Itoa(i))
	}
	filterOf(t, idx, "https://near", dense...)
	filterOf(t, idx, "https://far", d) // matches, sparser
	filterOf(t, idx, "https://other", "sha256:bbbb")
	p := &Proxy{
		peers:    &peer.ZoneAwareFinder{Inner: fixedPeers{"https://far", "https://other", "https://near"}, LocalZone: "z1", Resolver: r},
		index:    idx,
		down:     map[string]time.Time{},
		now:      time.Now,
		maxTries: 1,
		metrics:  &metrics.Metrics{},
	}
	for range 50 {
		if got := p.askablePeers("", d); !slices.Equal(got, []string{"https://near"}) {
			t.Fatalf("asked %v, want only the same-zone holder", got)
		}
	}
}
