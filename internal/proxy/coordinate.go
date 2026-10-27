package proxy

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"strings"
)

// Coordination keeps nodes that miss on the same blob at the same moment from
// all going to the registry. Every node ranks the nodes it knows for a digest
// by a hash of the digest and the node's address (rendezvous hashing), so they
// agree on one coordinator per digest without talking to each other, and the
// coordinators of different digests are spread over the cluster.
//
// A node that misses asks the coordinator first. The coordinator starts one
// download (its peers, then the registry) and streams it to everyone who asked.
// If the coordinator is unreachable, slow to start, or does not think it is the
// coordinator (the nodes' lists can differ for a moment), the asking node does
// what it did before. Nobody waits on a node that is not the coordinator in
// its own view, and the coordinator asks the other nodes without CoordHeader,
// which they answer from what they hold at once, so waits cannot form a cycle.

// rank is the node's weight for the digest; the highest wins.
func rank(d, node string) uint64 {
	h := sha256.Sum256([]byte(d + "\x00" + node))
	return binary.BigEndian.Uint64(h[:8])
}

// topNode returns the node that coordinates d among this node and the peers
// that have not failed recently.
func (p *Proxy) topNode(d string) string {
	best, bestRank := p.self, rank(d, p.self)
	for _, n := range p.livePeers() {
		if r := rank(d, n); r > bestRank || r == bestRank && n > best {
			best, bestRank = n, r
		}
	}
	return best
}

// coordinates reports whether this node is the coordinator of d.
func (p *Proxy) coordinates(d string) bool {
	return p.self != "" && p.topNode(d) == p.self
}

// otherCoordinator returns the coordinator of d when it is another node.
func (p *Proxy) otherCoordinator(d string) (string, bool) {
	if p.self == "" {
		return "", false
	}
	top := p.topNode(d)
	return top, top != p.self
}

// fetchCoordinator asks the coordinator for the blob and stores what it
// streams. It sends no credentials: the coordinator fetches for everyone, so
// it can only get what the registry gives without them, and a node that needs
// more falls back to asking with its own.
func (p *Proxy) fetchCoordinator(in info, base, path, d string) fetchResult {
	// Not done means nothing was stored; res is then at most "insufficient
	// storage", which the caller treats like an empty answer.
	res, _ := p.tryPeer(in, p.coordHTTP(), base, path, d, map[string]string{PeerHeader: "1", CoordHeader: "1"}, true)
	return res
}

// coordinate serves a coordinator request: it joins or starts the download of d
// and streams it. It returns false, having written nothing, when there is
// nothing to stream, and the caller answers as for any peer.
func (p *Proxy) coordinate(w http.ResponseWriter, r *http.Request, repo, d string) bool {
	in := newInfo(r)
	in.auth = "" // a peer's request carries none, and none is borrowed from another client
	dl := p.join(d, "", p.missFetch(in, "/v2/"+repo+"/blobs/"+d, d, false))
	if p.stream(w, r, d, dl, false, true) {
		p.metrics.CoordServed.Add(1)
		return true
	}
	return false
}

// isSlowStart reports whether a request failed only because the peer took
// longer than CoordWait to start answering. A connection that could not be made
// in time is a different matter: that peer is down, and is to be left alone for
// the back-off like any other.
func isSlowStart(e error) bool {
	var ne net.Error
	if !errors.As(e, &ne) || !ne.Timeout() {
		return false
	}
	// net/http has no exported value for its response header timeout.
	return strings.Contains(e.Error(), "timeout awaiting response headers")
}
