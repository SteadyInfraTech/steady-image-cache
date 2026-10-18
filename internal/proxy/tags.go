package proxy

import (
	"net/http"
	"strings"

	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

// A tag is a name for whatever digest the registry says it is today, so what the
// cache knows about it can be wrong in two ways.
//
// It can be out of date: "latest" moves. A tag that the registry can still be
// asked about is therefore confirmed again once it is older than the tag TTL.
//
// It can be false. A tag has no digest to check an answer against, so what a
// peer says it points at must be taken on trust, and one faulty or compromised
// node could then give every other node a different image under a name they
// trust. When the registry can be asked, it is the one that says what a tag
// points at; the content is then fetched by digest, from a peer if one has it,
// where it is verified. Peers are the source of a tag only when the registry
// cannot be asked (air-gapped nodes, a registry that is not allowed), and then
// the CA that vouches for them is the trust boundary.

// tagFresh reports whether a cached tag can be served as it is. That holds when
// there is nothing to check it against (no way to ask its registry, or a TTL of
// zero) or when it was confirmed recently. An alias that was never confirmed,
// because it came from containerd, is checked at its first use.
func (p *Proxy) tagFresh(namespace, repo, ref string) bool {
	if strings.HasPrefix(ref, "sha256:") || p.tagTTL <= 0 {
		return true
	}
	if _, ok := p.upstreamHost(namespace); !ok {
		return true
	}
	at, ok := p.store.AliasChecked(namespace, repo, ref)
	return ok && p.now().Sub(at) < p.tagTTL
}

// tagState is what the registry said when asked which digest a tag points at.
type tagState int

const (
	tagResolved    tagState = iota // it names a digest
	tagChallenge                   // it wants the client to authenticate
	tagNoDigest                    // it has the tag but does not say the digest
	tagNotFound                    // it does not have the tag
	tagUnavailable                 // it could not be asked, or failed
)

type tagAnswer struct {
	state  tagState
	digest string
	res    fetchResult // the challenge, for tagChallenge
}

// headManifest asks the registry which digest ref points at. A HEAD returns it
// in Docker-Content-Digest without the manifest, and registries do not count it
// against pull rate limits.
func (p *Proxy) headManifest(in info, host, path string) tagAnswer {
	resp, e := p.do(in.ctx, http.MethodHead, p.upstreamClient, "https://"+host+path,
		map[string]string{"Authorization": in.auth, "Accept": in.accept})
	if e != nil {
		p.upstreamFailed(host, e)
		return tagAnswer{state: tagUnavailable}
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		if d := resp.Header.Get("Docker-Content-Digest"); store.ValidDigest(d) {
			return tagAnswer{state: tagResolved, digest: d}
		}
		return tagAnswer{state: tagNoDigest}
	case resp.StatusCode == http.StatusNotFound:
		return tagAnswer{state: tagNotFound}
	}
	if r, ok := p.upstreamChallenge(resp); ok {
		return tagAnswer{state: tagChallenge, res: r}
	}
	p.upstreamAnswered(host, resp)
	return tagAnswer{state: tagUnavailable}
}

// fetchManifest gets the manifest ref names into the store and, for a tag,
// points the tag at it. It is the work behind a manifest request that the cache
// could not answer: a manifest it does not have, or a tag it cannot vouch for.
func (p *Proxy) fetchManifest(in info, namespace, repo, ref, path string) fetchResult {
	byDigest := strings.HasPrefix(ref, "sha256:")
	prev, havePrev := p.store.Resolve(namespace, repo, ref)
	prevHere := false
	if havePrev {
		_, err := p.store.Stat(prev)
		prevHere = err == nil
	}
	if prevHere && p.tagFresh(namespace, repo, ref) {
		return fetched
	}
	// The cached answer, old as it is, when the registry cannot give a better one.
	stale := func(why string) (fetchResult, bool) {
		if !prevHere || byDigest {
			return fetchResult{}, false
		}
		p.metrics.StaleTags.Add(1)
		p.warn("stale:"+namespace+"/"+repo+"@"+ref, "serving %s/%s:%s from the cache without confirming it: %s", namespace, repo, ref, why)
		return fetched, true
	}
	point := func(d string) fetchResult {
		if byDigest {
			return fetched
		}
		if err := p.store.SetAliasAt(namespace, repo, ref, d, p.now()); err != nil {
			p.metrics.StoreErrors.Add(1)
			p.warn("alias:"+namespace+"/"+repo+"@"+ref, "cannot persist manifest alias: %v", err)
			return fetchResult{status: http.StatusInternalServerError}
		}
		if havePrev && prev != d {
			p.metrics.TagChanges.Add(1)
		}
		return fetched
	}
	// fromAnywhere fetches a manifest by the path given, peers first.
	fromAnywhere := func(path, ref string) (string, fetchResult) {
		d, r := p.fetchManifestPeers(in, path, ref)
		if r.ok {
			p.metrics.PeerHits.Add(1)
		} else if r.status == 0 {
			if d, r = p.fetchManifestUpstream(in, path, ref); r.ok {
				p.metrics.UpstreamHits.Add(1)
			}
		}
		return d, r
	}

	if host, ok := p.upstreamHost(in.ns); ok && !byDigest {
		p.metrics.TagChecks.Add(1)
		ans := p.headManifest(in, host, path)
		switch ans.state {
		case tagResolved:
			// The registry says what the tag is; the manifest itself can come
			// from anywhere, since it is checked against that digest.
			if _, err := p.store.Stat(ans.digest); err == nil {
				return point(ans.digest)
			}
			d, r := fromAnywhere("/v2/"+repo+"/manifests/"+ans.digest, ans.digest)
			if !r.ok && r.status == 0 {
				// Not by digest, then by the tag, from the registry itself.
				if d, r = p.fetchManifestUpstream(in, path, ref); r.ok {
					p.metrics.UpstreamHits.Add(1)
				}
			}
			if !r.ok {
				if s, ok := stale("the registry names a newer manifest that could not be fetched"); ok {
					return s
				}
				return r
			}
			return point(d)
		case tagChallenge:
			if s, ok := stale("the registry wants credentials to confirm it"); ok {
				return s
			}
			return ans.res
		case tagNoDigest:
			// The registry has it but does not say which manifest it is, so only
			// the registry itself can be believed.
			d, r := p.fetchManifestUpstream(in, path, ref)
			if r.ok {
				p.metrics.UpstreamHits.Add(1)
				return point(d)
			}
			if r.status == 0 || r.status == http.StatusUnauthorized {
				if s, ok := stale("the registry would not confirm it"); ok {
					return s
				}
			}
			return r
		case tagUnavailable:
			if s, ok := stale("the registry could not be reached"); ok {
				return s
			}
			// With no registry and nothing cached, a peer is all there is.
		case tagNotFound:
			// The registry does not have it: a tag that was removed, or one that
			// only exists on the nodes. What is cached stays, and is not asked
			// about again until the TTL is up; with nothing cached, the peers
			// may have it.
			if prevHere {
				return point(prev)
			}
		}
	}
	d, r := fromAnywhere(path, ref)
	if !r.ok {
		return r
	}
	return point(d)
}
