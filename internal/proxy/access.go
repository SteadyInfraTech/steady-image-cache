package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

// Protected registries.
//
// A blob or tag that was fetched with someone's credentials is in the cache for
// everybody who can reach it. For a registry listed in SIC_PROTECTED_REGISTRIES
// that is not left to the network: before a client is answered from the cache
// (or from a peer, or by a fetch) the registry is asked, with that client's own
// Authorization, whether it may read the repository, and only a yes lets the
// request go on. A client with no credentials gets the registry's challenge, so
// it fetches a token and asks again, exactly as it would at the registry.
//
// Peers are not asked: a node's answer to another node is covered by mutual TLS,
// and the node that has the client in front of it has already checked.

// accessTTL is how long a yes is remembered for one credential, repository and
// registry. It bounds how long a revoked token still reads the cache; registry
// tokens live for minutes anyway.
const accessTTL = 60 * time.Second

// maxAccessEntries bounds the remembered answers. Past it, the expired ones are
// dropped, and if that is not enough all of them are: the next request of each
// client then costs one more check, nothing else.
const maxAccessEntries = 10000

type accessCache struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func (c *accessCache) ok(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, found := c.until[key]
	if found && !now.Before(t) {
		delete(c.until, key)
		return false
	}
	return found
}

func (c *accessCache) grant(key string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.until == nil {
		c.until = map[string]time.Time{}
	}
	if len(c.until) >= maxAccessEntries {
		for k, t := range c.until {
			if !now.Before(t) {
				delete(c.until, k)
			}
		}
		if len(c.until) >= maxAccessEntries {
			c.until = map[string]time.Time{}
		}
	}
	c.until[key] = now.Add(accessTTL)
}

// admit reports whether a request for repo may go on. When it may not it has
// answered the client already.
func (p *Proxy) admit(w http.ResponseWriter, r *http.Request, repo string) bool {
	namespace := ns(r)
	if len(p.protected) == 0 {
		return true
	}
	d := pathDigest(r.URL.Path)
	// The namespace is the client's word (?ns=, Host), and the cache is kept by
	// digest, so a protected blob would come out under any other namespace.
	// Content that came from a protected registry is therefore guarded wherever
	// it is asked for: the registry it is asked of is the one to say.
	guarded := p.protected[namespace]
	if !guarded && (d == "" || !p.store.IsProtected(d)) {
		return true
	}
	if !validRepo(repo) || !validNamespace(namespace) {
		return true // the handler refuses it
	}
	if guarded && isPeer(r) {
		// The header is set by the sender, so it proves nothing: any holder of a
		// client certificate could set it. Protected content is not shared
		// between nodes at all (askablePeers), so there is nothing to serve.
		http.Error(w, "this registry's content is not shared between nodes", http.StatusForbidden)
		return false
	}
	in := newInfo(r)
	sum := sha256.Sum256([]byte(in.auth))
	key := namespace + "|" + r.URL.Path + "|" + hex.EncodeToString(sum[:])
	if p.access.ok(key, p.now()) {
		return p.markedOrRefuse(w, namespace, d)
	}
	p.metrics.AccessChecks.Add(1)
	res, err := p.flights.DoContext(r.Context(), "access:"+key, func() fetchResult {
		return p.probe(in, r.URL.Path, key)
	})
	if err != nil {
		return false // the client gave up
	}
	if res.ok {
		return p.markedOrRefuse(w, namespace, d)
	}
	p.metrics.AccessDenied.Add(1)
	switch {
	case res.status == http.StatusUnauthorized && len(res.challenge) > 0:
		relayChallenge(w, res.challenge)
	case res.status == http.StatusTooManyRequests:
		relayThrottle(w, res.retryAfter)
	default:
		http.Error(w, "the registry did not confirm that this client may read the repository: "+http.StatusText(res.status), res.status)
	}
	return false
}

// probe asks the registry whether the credentials in in may read what path
// names. A redirect is an answer, not something to follow: the registry has
// authorised the request before it redirects to storage, and a HEAD at a signed
// storage address would fail for no reason of the client's.
func (p *Proxy) probe(in info, path, key string) fetchResult {
	host, ok := p.upstreamHost(in.ns)
	if !ok {
		return fetchResult{status: http.StatusForbidden}
	}
	hdr := map[string]string{"Authorization": in.auth}
	if strings.Contains(path, "/manifests/") {
		hdr["Accept"] = in.accept
	}
	c := *p.upstreamClient
	c.CheckRedirect = noRedirect
	resp, e := p.do(in.ctx, http.MethodHead, &c, "https://"+host+path, hdr)
	if e != nil {
		p.upstreamFailed(host, e)
		return fetchResult{status: http.StatusServiceUnavailable} // cannot confirm: closed, not open
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400:
		p.access.grant(key, p.now())
		return fetchResult{ok: true}
	case resp.StatusCode == http.StatusUnauthorized:
		if r, ok := p.upstreamChallenge(resp); ok {
			return r
		}
		p.upstreamAnswered(host, resp)
		return fetchResult{status: http.StatusForbidden}
	case resp.StatusCode == http.StatusTooManyRequests:
		r, _ := p.upstreamThrottle(host, resp)
		return r
	case resp.StatusCode == http.StatusForbidden, resp.StatusCode == http.StatusNotFound:
		return fetchResult{status: resp.StatusCode}
	default:
		p.upstreamAnswered(host, resp)
		return fetchResult{status: http.StatusBadGateway}
	}
}

// errMark marks a failure to record where content came from.
var errMark = errors.New("cannot record where content came from")

// markProtected notes that d is being given out under a protected registry. An
// error means it could not be noted, and then d must not be given out.
func (p *Proxy) markProtected(namespace, d string) error {
	if d == "" || !p.protected[namespace] {
		return nil
	}
	if err := p.store.MarkProtected(d); err != nil {
		p.warn("mark", "cannot mark %s as protected: %v", d, err)
		return fmt.Errorf("%w: %w", errMark, err)
	}
	return nil
}

// markedOrRefuse is markProtected for a request that was just admitted: when
// the mark cannot be made the request is refused, since the content could then
// be read through another namespace.
func (p *Proxy) markedOrRefuse(w http.ResponseWriter, namespace, d string) bool {
	if p.markProtected(namespace, d) != nil {
		http.Error(w, "cannot record where this content came from", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// pathDigest is the digest a blob or manifest path names, or "".
func pathDigest(path string) string {
	d := path[strings.LastIndex(path, "/")+1:]
	if store.ValidDigest(d) {
		return d
	}
	return ""
}
