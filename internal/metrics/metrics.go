package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Func is a metric whose value is read when /metrics is scraped, for numbers
// that live elsewhere (the store's counters, say). Type is "counter" or "gauge".
type Func struct {
	Name, Type string
	Value      func() int64
}

type Metrics struct {
	LocalHits, PeerHits, UpstreamHits, Misses, PeerErrors, VerifyErrors, ImportedBlobs, ImportedBytes, ImportErrors, DiskFull, StoreErrors, Streamed, StreamAborts, UpstreamChallenges, TagChecks, TagChanges, StaleTags, PeerStreams, PeerCapped, CoordHits, CoordServed, CoordFallbacks, BytesToClients, BytesToPeers, BytesFromPeers, BytesFromUpstream, Resumed, ResumedBytes, AccessChecks, AccessDenied atomic.Uint64
	// Funcs are set before the metrics are served.
	Funcs []Func
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	vals := []struct {
		n string
		v *atomic.Uint64
	}{{"sic_local_hits_total", &m.LocalHits}, {"sic_peer_hits_total", &m.PeerHits}, {"sic_upstream_hits_total", &m.UpstreamHits}, {"sic_misses_total", &m.Misses}, {"sic_peer_errors_total", &m.PeerErrors}, {"sic_verify_errors_total", &m.VerifyErrors}, {"sic_containerd_imported_blobs_total", &m.ImportedBlobs}, {"sic_containerd_imported_bytes_total", &m.ImportedBytes}, {"sic_containerd_import_errors_total", &m.ImportErrors}, {"sic_disk_full_total", &m.DiskFull}, {"sic_store_errors_total", &m.StoreErrors}, {"sic_streamed_responses_total", &m.Streamed}, {"sic_stream_aborts_total", &m.StreamAborts}, {"sic_upstream_auth_challenges_total", &m.UpstreamChallenges}, {"sic_tag_checks_total", &m.TagChecks}, {"sic_tag_changes_total", &m.TagChanges}, {"sic_stale_tags_served_total", &m.StaleTags}, {"sic_peer_streams_total", &m.PeerStreams}, {"sic_peer_cap_reached_total", &m.PeerCapped}, {"sic_coordinator_hits_total", &m.CoordHits}, {"sic_coordinator_served_total", &m.CoordServed}, {"sic_coordinator_fallbacks_total", &m.CoordFallbacks}, {"sic_bytes_to_clients_total", &m.BytesToClients}, {"sic_bytes_to_peers_total", &m.BytesToPeers}, {"sic_bytes_from_peers_total", &m.BytesFromPeers}, {"sic_bytes_from_upstream_total", &m.BytesFromUpstream}, {"sic_resumed_downloads_total", &m.Resumed}, {"sic_resumed_bytes_total", &m.ResumedBytes}, {"sic_access_checks_total", &m.AccessChecks}, {"sic_access_denied_total", &m.AccessDenied}}
	for _, x := range vals {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", x.n, x.n, x.v.Load())
	}
	for _, f := range m.Funcs {
		fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", f.Name, f.Type, f.Name, f.Value())
	}
}
