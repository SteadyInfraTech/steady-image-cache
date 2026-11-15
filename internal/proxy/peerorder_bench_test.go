package proxy

import (
	"fmt"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
)

// The cost of choosing whom to ask on one miss: the whole peer list is copied,
// shuffled and cut to PeerMaxTries. Run with
//
//	go test ./internal/proxy -run '^$' -bench PeerOrder -benchmem
//
// to see what a miss pays at 100, 1 000 and 5 000 peers.
func benchProxy(b *testing.B, n int, withIndex bool) *Proxy {
	b.Helper()
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://10.%d.%d.%d:5000", i>>16&255, i>>8&255, i&255)
	}
	var idx *peer.DigestIndex
	if withIndex {
		idx = peer.NewDigestIndex()
		src := peer.NewDigestIndex()
		var ds []string
		for i := range 2000 {
			ds = append(ds, fmt.Sprintf("sha256:%064x", i))
		}
		src.Rebuild(ds)
		data := src.SelfBytes()
		for _, u := range urls {
			if !idx.UpdatePeer(u, data) {
				b.Fatal("filter refused")
			}
		}
	}
	p, err := New(nil, peer.NewStatic(urls), &metrics.Metrics{}, Options{PeerMaxTries: 4, DigestIndex: idx})
	if err != nil {
		b.Fatal(err)
	}
	return p
}

func BenchmarkPeerOrder(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("peers=%d", n), func(b *testing.B) {
			p := benchProxy(b, n, false)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := p.askablePeers("docker.io", ""); len(got) != 4 {
					b.Fatalf("%d peers", len(got))
				}
			}
		})
	}
	// With a digest filter per peer (64 KiB each, so only up to 1 000 here).
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("peers=%d/with-filters", n), func(b *testing.B) {
			p := benchProxy(b, n, true)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := p.askablePeers("docker.io", "sha256:"+fmt.Sprintf("%064x", 7)); len(got) != 4 {
					b.Fatalf("%d peers", len(got))
				}
			}
		})
	}
}
