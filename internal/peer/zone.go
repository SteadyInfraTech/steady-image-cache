package peer

import (
	"sort"
	"strings"
	"sync"
)

// ZoneAwareFinder wraps another Finder and re-orders its candidates so that
// peers in the same availability zone as this node appear first.
//
// Zone information is obtained from the Kubernetes downward API via the
// NODE_ZONE environment variable (topology.kubernetes.io/zone label). When
// NODE_ZONE is empty, ZoneAwareFinder degrades to a pass-through, so no
// configuration is required on clusters that do not use zone labels.
//
// The peer base URL encodes the node's IP address only, not its zone. Zone
// membership is determined through the ZoneResolver, which is updated by the
// Kubernetes discovery layer when pod metadata is refreshed.
type ZoneAwareFinder struct {
	// Inner is the wrapped Finder.
	Inner Finder
	// LocalZone is this node's zone (e.g. "us-east-1a"). Empty means unknown.
	LocalZone string
	// Resolver maps peer base URLs to zones. It is updated concurrently by the
	// Kubernetes watcher.
	Resolver *ZoneResolver
}

// Candidates returns the inner finder's candidates, with same-zone peers
// sorted to the front.
func (z *ZoneAwareFinder) Candidates() []string {
	candidates := z.Inner.Candidates()
	if z.LocalZone == "" || z.Resolver == nil {
		return candidates
	}
	return z.sortByZone(candidates)
}

// Poke forwards to the inner finder if it supports pokes.
func (z *ZoneAwareFinder) Poke() {
	if p, ok := z.Inner.(Poker); ok {
		p.Poke()
	}
}

// sortByZone partitions candidates into same-zone and cross-zone groups,
// preserving original order within each group.
func (z *ZoneAwareFinder) sortByZone(candidates []string) []string {
	sameZone := make([]string, 0, len(candidates))
	crossZone := make([]string, 0, len(candidates))
	unknownZone := make([]string, 0)

	for _, c := range candidates {
		zone := z.Resolver.ZoneOf(c)
		switch {
		case zone == "":
			unknownZone = append(unknownZone, c)
		case zone == z.LocalZone:
			sameZone = append(sameZone, c)
		default:
			crossZone = append(crossZone, c)
		}
	}
	// Order: same-zone → unknown zone → cross-zone.
	out := make([]string, 0, len(candidates))
	out = append(out, sameZone...)
	out = append(out, unknownZone...)
	out = append(out, crossZone...)
	return out
}

// ZoneResolver maps peer base URLs to their zone labels. It is safe for
// concurrent reads and writes.
type ZoneResolver struct {
	mu    sync.RWMutex
	zones map[string]string // base URL → zone label
}

// NewZoneResolver creates an empty ZoneResolver.
func NewZoneResolver() *ZoneResolver {
	return &ZoneResolver{zones: make(map[string]string)}
}

// SetZone records the zone for a peer.
func (r *ZoneResolver) SetZone(base, zone string) {
	r.mu.Lock()
	r.zones[base] = zone
	r.mu.Unlock()
}

// DeleteZone removes a peer's zone entry.
func (r *ZoneResolver) DeleteZone(base string) {
	r.mu.Lock()
	delete(r.zones, base)
	r.mu.Unlock()
}

// ZoneOf returns the zone for a peer base URL, or "" if unknown.
func (r *ZoneResolver) ZoneOf(base string) string {
	r.mu.RLock()
	z := r.zones[base]
	r.mu.RUnlock()
	return z
}

// Zones returns a snapshot of all known zones, keyed by base URL.
func (r *ZoneResolver) Zones() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(r.zones))
	for k, v := range r.zones {
		out[k] = v
	}
	return out
}

// ZoneStats returns a map from zone name to the number of peers in that zone.
func (r *ZoneResolver) ZoneStats() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int)
	for _, z := range r.zones {
		if z != "" {
			out[z]++
		}
	}
	return out
}

// ZonesForCandidates returns the sorted, deduplicated set of zones present in
// a set of candidate peer base URLs.
func (r *ZoneResolver) ZonesForCandidates(candidates []string) []string {
	seen := make(map[string]bool)
	for _, c := range candidates {
		if z := r.ZoneOf(c); z != "" {
			seen[z] = true
		}
	}
	zones := make([]string, 0, len(seen))
	for z := range seen {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	return zones
}

// splitBaseHost extracts the host (IP) portion of a base URL like
// "https://10.0.0.1:5000" for zone lookup when the full base URL is not the
// map key.
func splitBaseHost(base string) string {
	// Strip scheme.
	s := base
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	// Strip port.
	if i := strings.LastIndex(s, ":"); i >= 0 {
		s = s[:i]
	}
	// Strip brackets for IPv6.
	s = strings.Trim(s, "[]")
	return s
}

// ensure splitBaseHost is reachable from tests.
var _ = splitBaseHost
