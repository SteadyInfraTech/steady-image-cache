package proxy

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
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
// client is involved. The token service is a destination like any other: its
// host must itself be on SIC_UPSTREAM_REGISTRIES (SPEC 6.2), so a registry
// cannot send SIC to a host the operator did not name. For most registries that
// is the registry's own host (public.ecr.aws, ghcr.io); Docker Hub's is
// auth.docker.io. The token is kept for a short time, per registry, service and
// scope.

const (
	maxAnonTokens  = 256
	anonTokenWait  = 5 * time.Second  // the whole token request; a blob transfer is not held up longer than this per miss
	anonDenied     = 60 * time.Second // how long a refused (service, scope) is not asked for again
	anonBackoff    = 5 * time.Second  // after a failure that says nothing about the right to read: a blip, a 5xx, a 429
	minAnonTTL     = 10 * time.Second
	anonTTLPercent = 75 // of what the realm says
)

type anonToken struct {
	value   string
	expires time.Time
	denied  bool // the realm or the registry refused an anonymous client: do not ask again until expires
}

type anonTokens struct {
	now    func() time.Time
	client *http.Client
	mu     sync.Mutex
	tokens map[string]anonToken
	flying map[string]*tokenCall // token requests in progress, by key
}

// tokenCall is one request to a realm that every miss for the same key waits
// on: a coordinator that starts several downloads at once (the layers of one
// image share a scope) makes one request, not one per layer.
type tokenCall struct {
	done  chan struct{} // closed when token and err are final
	token string
	err   error
}

// newAnonTokens makes the cache; rt is how the token service is reached (the
// proxy passes the transport it uses for registries, so compression, proxy
// settings and roots are the same), nil for the default.
func newAnonTokens(now func() time.Time, rt http.RoundTripper) *anonTokens {
	return &anonTokens{
		now:    now,
		client: &http.Client{Transport: rt, Timeout: anonTokenWait, CheckRedirect: noRedirect},
		tokens: map[string]anonToken{},
		flying: map[string]*tokenCall{},
	}
}

// get returns a token for the challenge, from the cache or from the realm. A
// miss that finds a request for the same key in progress waits for it.
func (a *anonTokens) get(in info, host string, challenge string) (token, key string, ok bool) {
	pr, valid := regauth.ParseBearer(challenge)
	if !valid {
		return "", "", false
	}
	key = host + "\x00" + pr["service"] + "\x00" + pr["scope"]
	a.mu.Lock()
	if t, hit := a.tokens[key]; hit && a.now().Before(t.expires) {
		a.mu.Unlock()
		return t.value, key, !t.denied
	}
	if call, waiting := a.flying[key]; waiting {
		a.mu.Unlock()
		// The request is bounded by the client's timeout, and a blob fetch's
		// context is detached from its client (SPEC 5.3), so this wait is short
		// and nothing else can end it.
		<-call.done
		return call.token, key, call.err == nil
	}
	call := &tokenCall{done: make(chan struct{})}
	a.flying[key] = call
	a.mu.Unlock()
	a.request(in, key, challenge, call)
	return call.token, key, call.err == nil
}

// request asks the realm for call's key and records the outcome in the cache
// before it wakes the misses waiting for it.
func (a *anonTokens) request(in info, key, challenge string, call *tokenCall) {
	defer func() {
		a.mu.Lock()
		delete(a.flying, key)
		a.mu.Unlock()
		close(call.done)
	}()
	tok, ttl, err := regauth.AnonymousToken(in.ctx, a.client, challenge)
	if err != nil {
		call.err = err
		// A definite refusal (the service says no) is remembered for a minute; a
		// network error, a 5xx or a 429 only for a few seconds, so one hiccup
		// does not turn anonymous fetching off.
		wait := anonBackoff
		var se *regauth.StatusError
		if errors.As(err, &se) && se.Code >= 400 && se.Code < 500 && se.Code != http.StatusRequestTimeout && se.Code != http.StatusTooManyRequests {
			wait = anonDenied
		}
		a.deny(key, wait)
		return
	}
	call.token = tok
	ttl = ttl * anonTTLPercent / 100
	if ttl >= minAnonTTL { // shorter is not worth keeping: used by these requests only
		a.store(key, anonToken{value: tok, expires: a.now().Add(ttl)})
	}
}

func (a *anonTokens) store(key string, t anonToken) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.tokens) >= maxAnonTokens {
		for k, v := range a.tokens { // drop what has expired, else everything
			if !a.now().Before(v.expires) {
				delete(a.tokens, k)
			}
		}
		if len(a.tokens) >= maxAnonTokens { // none expired: drop the one that expires first
			var oldest string
			var oldestExp time.Time
			first := true
			for k, v := range a.tokens {
				if first || v.expires.Before(oldestExp) {
					oldest, oldestExp, first = k, v.expires, false
				}
			}
			delete(a.tokens, oldest)
		}
	}
	a.tokens[key] = t
}

// deny replaces a token the registry refused with a "denied" entry that lasts
// for wait, so that repeated misses on content an anonymous client may not read
// do not each ask the token service again (private content is the common case).
func (a *anonTokens) deny(key string, wait time.Duration) {
	a.store(key, anonToken{denied: true, expires: a.now().Add(wait)})
}

// anonRealmAllowed reports whether the host of a token realm is on the
// allowlist of upstream registries. A realm that is not is logged (once a
// minute per registry, which bounds the log's keys) and never contacted.
func (p *Proxy) anonRealmAllowed(registry, realm string) bool {
	u, err := url.Parse(realm)
	if err == nil && p.registries[strings.ToLower(u.Host)] {
		return true
	}
	shown := realm
	if err == nil {
		shown = u.Host
	}
	p.warn("anon-realm:"+registry, "%s names the token service %q, which is not in SIC_UPSTREAM_REGISTRIES: no anonymous token is requested, so a node that asked this coordinator fetches for itself", registry, shown)
	return false
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
		pr, bearer := regauth.ParseBearer(ch)
		if !bearer {
			continue
		}
		tried = true
		if !p.anonRealmAllowed(host, pr["realm"]) {
			break
		}
		tok, key, ok = p.anon.get(in, host, ch)
		break
	}
	if !tried || !ok {
		return resp, ranged, nil
	}
	resp.Body.Close()
	// A copy of what the first request sent (hdr may be the caller's own map)
	// with the token in place of the missing credential.
	authed := make(map[string]string, len(hdr))
	for k, v := range hdr {
		authed[k] = v
	}
	authed["Authorization"] = "Bearer " + tok
	resp, err = p.get(in.ctx, p.upstreamClient, "https://"+host+path, authed)
	if err != nil {
		return resp, ranged, err // the registry never answered: the token may be fine
	}
	if resp.StatusCode == http.StatusUnauthorized {
		// The registry refused the token: an anonymous client may not read this.
		// A new Bearer challenge instead says it wants something else (a
		// different scope, a throttled realm): that is not a verdict on the
		// content, so only a short pause.
		wait := anonDenied
		for _, ch := range challengeOf(resp) {
			if _, bearer := regauth.ParseBearer(ch); bearer {
				wait = anonBackoff
				break
			}
		}
		p.anon.deny(key, wait)
		return resp, ranged, nil
	}
	p.metrics.CoordAnonymousTokens.Add(1) // counts the tokens that got a blob request through
	return resp, ranged, err
}
