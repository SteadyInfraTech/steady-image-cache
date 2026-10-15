package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct{ LocalHits, PeerHits, UpstreamHits, Misses, PeerErrors, VerifyErrors, ImportedBlobs, ImportedBytes, DiskFull, StoreErrors atomic.Uint64 }

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	vals := []struct {
		n string
		v *atomic.Uint64
	}{{"sic_local_hits_total", &m.LocalHits}, {"sic_peer_hits_total", &m.PeerHits}, {"sic_upstream_hits_total", &m.UpstreamHits}, {"sic_misses_total", &m.Misses}, {"sic_peer_errors_total", &m.PeerErrors}, {"sic_verify_errors_total", &m.VerifyErrors}, {"sic_containerd_imported_blobs_total", &m.ImportedBlobs}, {"sic_containerd_imported_bytes_total", &m.ImportedBytes}, {"sic_disk_full_total", &m.DiskFull}, {"sic_store_errors_total", &m.StoreErrors}}
	for _, x := range vals {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", x.n, x.n, x.v.Load())
	}
}
