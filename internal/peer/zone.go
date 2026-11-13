package peer

import "sync"

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
// first, then those whose zone is unknown, then the others.
func (z *ZoneAwareFinder) Candidates() []string {
	groups := z.CandidateGroups()
	if len(groups) == 1 {
		return groups[0]
	}
	var out []string
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// Poke forwards to the inner finder if it supports pokes.
func (z *ZoneAwareFinder) Poke() {
	if p, ok := z.Inner.(Poker); ok {
		p.Poke()
	}
}

// Grouper is a Finder whose candidates come in ranked groups: everything in an
// earlier group is preferred to everything in a later one, and the order inside
// a group means nothing, so a caller that spreads load shuffles within groups.
type Grouper interface {
	CandidateGroups() [][]string
}

// CandidateGroups returns the candidates as same-zone peers, peers whose zone is
// not known, and peers in other zones, in that order. Without a local zone it is
// one group.
func (z *ZoneAwareFinder) CandidateGroups() [][]string {
	candidates := z.Inner.Candidates()
	if z.LocalZone == "" || z.Resolver == nil {
		return [][]string{candidates}
	}
	var same, unknown, cross []string
	for _, c := range candidates {
		switch zone := z.Resolver.ZoneOf(c); {
		case zone == "":
			unknown = append(unknown, c)
		case zone == z.LocalZone:
			same = append(same, c)
		default:
			cross = append(cross, c)
		}
	}
	return [][]string{same, unknown, cross}
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
