package peer

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// The filter has a fixed size, so what it can say about a cache depends on how
// many blobs the cache holds. These tests measure that, so that the numbers in
// the README ("Large clusters") are measured and not guessed, and fail if the
// filter stops behaving like a Bloom filter of its size.

// theoreticalFP is the false-positive rate of a Bloom filter with filterBits
// slots and filterK hashes holding n keys.
func theoreticalFP(n int) float64 {
	return math.Pow(1-math.Exp(-float64(filterK)*float64(n)/float64(filterBits)), filterK)
}

// measuredFP builds a filter of n blobs and asks it about absent ones.
func measuredFP(n, probes int) float64 {
	var f countingFilter
	for i := range n {
		f.Add(digestN(i))
	}
	hits := 0
	for i := range probes {
		if f.Test(digestN(1<<40 + i)) {
			hits++
		}
	}
	return float64(hits) / float64(probes)
}

func TestFilterFalsePositiveRateFollowsTheory(t *testing.T) {
	const probes = 200000
	for _, n := range []int{1000, 5000, 10000, 25000, 50000, 100000} {
		got, want := measuredFP(n, probes), theoreticalFP(n)
		t.Logf("blobs=%6d  false positives: measured %.4f, theory %.4f", n, got, want)
		// Generous: the hashes are not ideal. A filter that is far off its theory
		// (a worse hash, a lost bit) fails here.
		if got > want*1.5+0.002 || got < want*0.5-0.002 {
			t.Errorf("n=%d: measured false-positive rate %.4f, theory %.4f", n, got, want)
		}
	}
	// The README says "under 1% at 10 000 blobs".
	if fp := measuredFP(10000, probes); fp >= 0.01 {
		t.Errorf("at 10 000 blobs the false-positive rate is %.4f, the README says under 1%%", fp)
	}
}

// TestGossipCostModel prints what a cluster of N nodes pays for gossip. It is
// arithmetic on measured sizes; run it with -v (SIC_COST_TABLE=1 for the table).
func TestGossipCostModel(t *testing.T) {
	idx := NewDigestIndex()
	var ds []string
	for i := range 5000 {
		ds = append(ds, digestN(i))
	}
	idx.Rebuild(ds)
	h := &GossipHandler{Index: idx}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, GossipPath, nil))
	wire := rec.Body.Len()
	if rec.Code != 200 || wire < FilterSize() {
		t.Fatalf("a full answer is %d bytes (status %d), want at least %d", wire, rec.Code, FilterSize())
	}
	t.Logf("a full answer: %d bytes on the wire before TLS; the filter in memory: %d bytes", wire, FilterSize())
	if os.Getenv("SIC_COST_TABLE") == "" {
		return
	}
	const intervalSec = 180 // SIC_GOSSIP_INTERVAL default
	fmt.Printf("\n%7s %14s %16s %18s %20s\n", "nodes", "filters held", "requests/s/node", "requests/s cluster", "worst round, MB/node")
	for _, n := range []int{10, 50, 100, 500, 1000, 5000} {
		peers := n - 1
		fmt.Printf("%7d %11.1f MB %16.1f %18.0f %20.1f\n", n,
			float64(peers*FilterSize())/1e6, float64(peers)/intervalSec, float64(n*peers)/intervalSec, float64(peers*wire)/1e6)
	}
	fmt.Printf("\n%9s %12s %8s   %s\n", "blobs", "false pos.", "of 999", "peers wrongly ranked first, of 999 that lack the blob")
	for _, b := range []int{1000, 5000, 10000, 25000, 50000, 100000} {
		fp := measuredFP(b, 200000)
		fmt.Printf("%9d %11.2f%% %8.1f\n", b, fp*100, fp*999)
	}
}
