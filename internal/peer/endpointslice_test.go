package peer

import (
	"slices"
	"testing"
)

func slice(name string, deleted bool, port int32, eps ...[3]string) endpointSlice {
	var s endpointSlice
	s.Metadata.Name = name
	if deleted {
		d := "2026-11-03T00:00:00Z"
		s.Metadata.DeletionTimestamp = &d
	}
	s.Ports = append(s.Ports, struct {
		Port *int32 `json:"port"`
	}{&port})
	for _, e := range eps { // address, zone, "ready" or "notready"
		var ep struct {
			Addresses  []string `json:"addresses"`
			Conditions struct {
				Ready *bool `json:"ready"`
			} `json:"conditions"`
			Zone      *string `json:"zone"`
			TargetRef *struct {
				Name string `json:"name"`
			} `json:"targetRef"`
		}
		ep.Addresses = []string{e[0]}
		z, ready := e[1], e[2] == "ready"
		ep.Zone, ep.Conditions.Ready = &z, &ready
		s.Endpoints = append(s.Endpoints, ep)
	}
	return s
}

func TestEndpointSlicePeersAreReadyOthersWithTheirZones(t *testing.T) {
	r := NewZoneResolver()
	e := &EndpointSliceKubernetes{selfIP: "10.0.0.1", port: 5000, scheme: "https", resolver: r,
		slices: map[string]endpointSlice{}}
	e.slices["a"] = slice("a", false, 5000,
		[3]string{"10.0.0.1", "z1", "ready"},    // this node
		[3]string{"10.0.0.2", "z1", "ready"},    // peer
		[3]string{"10.0.0.3", "z2", "notready"}, // not ready
		[3]string{"10.0.0.4", "z2", "ready"})
	e.slices["b"] = slice("b", true, 5000, [3]string{"10.0.0.9", "z3", "ready"}) // being deleted
	e.recomputeLocked()
	want := []string{"https://10.0.0.2:5000", "https://10.0.0.4:5000"}
	if !slices.Equal(e.peers, want) {
		t.Fatalf("peers = %v, want %v", e.peers, want)
	}
	if r.ZoneOf("https://10.0.0.4:5000") != "z2" {
		t.Fatalf("zones = %v", r.Zones())
	}
	// 10.0.0.4 goes away: its zone must go too.
	delete(e.slices, "b")
	e.slices["a"] = slice("a", false, 5000, [3]string{"10.0.0.2", "z1", "ready"})
	e.recomputeLocked()
	if z := r.ZoneOf("https://10.0.0.4:5000"); z != "" {
		t.Fatalf("a departed peer keeps its zone %q", z)
	}
}

// Same-zone peers first, then those whose zone is unknown, then other zones.
func TestZoneAwareFinderOrdersSameZoneFirst(t *testing.T) {
	r := NewZoneResolver()
	r.SetZone("https://a", "z2")
	r.SetZone("https://b", "z1")
	r.SetZone("https://d", "z1")
	var in gossipPeers
	in.set("https://a", "https://b", "https://c", "https://d")
	z := &ZoneAwareFinder{Inner: &in, LocalZone: "z1", Resolver: r}
	want := []string{"https://b", "https://d", "https://c", "https://a"}
	if got := z.Candidates(); !slices.Equal(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	z.LocalZone = ""
	if got := z.Candidates(); !slices.Equal(got, []string{"https://a", "https://b", "https://c", "https://d"}) {
		t.Fatalf("without a local zone the order must be the inner one, got %v", got)
	}
}
