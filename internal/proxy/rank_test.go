package proxy

import (
	"slices"
	"testing"

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
