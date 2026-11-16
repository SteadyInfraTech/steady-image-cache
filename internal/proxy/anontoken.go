package proxy

import (
	"net/http"
	"sync"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/regauth"
)

// Anonymous tokens for coordinators.
//
// A coordinator fetches for the nodes that ask it, and holds no one's
// credentials. Most public registries (Docker Hub, public.ecr.aws, ghcr.io)
// still answer a blob request with a token challenge, even for public content,
// so without more a coordinator is refused and every node fetches for itself.
// With SIC_COORDINATE_ANONYMOUS_TOKEN a coordinator answers the challenge as an
// anonymous client would: it asks the realm for a token with no credentials,
// for the service and scope the registry named, and asks again. Public content
// then comes through the coordinator. Private content does not: the realm
// grants an anonymous client nothing, the registry refuses it, and the asking
// node fetches with its own credentials as before.
//
// Only a coordinator does this, only for the registries SIC is configured to
// reach, only over https, without following redirects, and no credential of any
// client is involved. The token is kept for a short time, per registry, service
// and scope.

const (
	maxAnonTokens  = 256
	anonTokenWait  = 10 * time.Second
	minAnonTTL     = 10 * time.Second
	anonTTLPercent = 75 // of what the realm says
)

type anonToken struct {
	value   string
	expires time.Time
}

type anonTokens struct {
	now    func() time.Time
	client *http.Client
	mu     sync.Mutex
	tokens map[string]anonToken
}

// newAnonTokens makes the cache; rt is how the token service is reached (the
// proxy passes the transport it uses for registries, so compression, proxy
// settings and roots are the same), nil for the default.
func newAnonTokens(now func() time.Time, rt http.RoundTripper) *anonTokens {
	return &anonTokens{
		now:    now,
		client: &http.Client{Transport: rt, Timeout: anonTokenWait, CheckRedirect: noRedirect},
		tokens: map[string]anonToken{},
	}
}

// get returns a token for the challenge, from the cache or from the realm.
func (a *anonTokens) get(in info, host string, challenge string) (token, key string, ok bool) {
	pr, valid := regauth.ParseBearer(challenge)
	if !valid {
		return "", "", false
	}
	key = host + "\x00" + pr["service"] + "\x00" + pr["scope"]
	a.mu.Lock()
	t, hit := a.tokens[key]
	a.mu.Unlock()
	if hit && a.now().Before(t.expires) {
		return t.value, key, true
	}
	tok, ttl, err := regauth.AnonymousToken(in.ctx, a.client, challenge)
	if err != nil {
		return "", key, false
	}
	ttl = ttl * anonTTLPercent / 100
	if ttl < minAnonTTL {
		return tok, key, true // too short to be worth keeping: used for this request only
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.tokens) >= maxAnonTokens {
		for k, v := range a.tokens { // drop what has expired, else everything
			if !a.now().Before(v.expires) {
				delete(a.tokens, k)
			}
		}
		if len(a.tokens) >= maxAnonTokens {
			clear(a.tokens)
		}
	}
	a.tokens[key] = anonToken{value: tok, expires: a.now().Add(ttl)}
	return tok, key, true
}

// forget drops a token the registry refused.
func (a *anonTokens) forget(key string) {
	a.mu.Lock()
	delete(a.tokens, key)
	a.mu.Unlock()
}

// upstreamBlobGet asks the registry for a blob. For a coordinator whose request
// is refused with a Bearer challenge it gets an anonymous token and asks once
// more; in every other case it is one plain request.
func (p *Proxy) upstreamBlobGet(in info, host, path string) (resp *http.Response, ranged bool, err error) {
	hdr, ranged := resumeHeader(in.dl, map[string]string{"Authorization": in.auth})
	resp, err = p.get(in.ctx, p.upstreamClient, "https://"+host+path, hdr)
	if err != nil || p.anon == nil || !in.anon || in.auth != "" || resp.StatusCode != http.StatusUnauthorized {
		return resp, ranged, err
	}
	// One token request per miss, for the first Bearer challenge the registry
	// sent: a registry that sends several cannot multiply the calls.
	var tok, key string
	var ok, tried bool
	for _, ch := range challengeOf(resp) {
		if _, bearer := regauth.ParseBearer(ch); !bearer {
			continue
		}
		tok, key, ok = p.anon.get(in, host, ch)
		tried = true
		break
	}
	if !tried || !ok {
		return resp, ranged, nil
	}
	resp.Body.Close()
	hdr["Authorization"] = "Bearer " + tok
	resp, err = p.get(in.ctx, p.upstreamClient, "https://"+host+path, hdr)
	if err != nil || resp.StatusCode == http.StatusUnauthorized {
		p.anon.forget(key) // it did not work (or the registry never answered): do not keep it
		return resp, ranged, err
	}
	p.metrics.CoordTokens.Add(1) // counts the tokens that got a blob request through
	return resp, ranged, err
}
