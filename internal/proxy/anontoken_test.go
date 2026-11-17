package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

// tokenService answers anonymous token requests and records them.
type tokenService struct {
	*httptest.Server
	asks  atomic.Int32
	mu    sync.Mutex
	query string
	auth  string
}

func newTokenService(t *testing.T, token string) *tokenService {
	t.Helper()
	ts := &tokenService{}
	ts.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.asks.Add(1)
		ts.mu.Lock()
		ts.query, ts.auth = r.URL.RawQuery, r.Header.Get("Authorization")
		ts.mu.Unlock()
		fmt.Fprintf(w, `{"token":%q,"expires_in":300}`, token)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func enableAnonymousTokens(nodes []*node, ts *tokenService) {
	for _, n := range nodes {
		n.p.registries[ts.Listener.Addr().String()] = true // the token service is on the allowlist
		n.p.anon = newAnonTokens(time.Now, nil)
		c := *ts.Client()
		c.CheckRedirect = noRedirect
		n.p.anon.client = &c
	}
}

func anonymousSetup(t *testing.T, token string) (nodes []*node, top, asker *node, reg *tokenRegistry, ts *tokenService, d string, body []byte) {
	t.Helper()
	body = []byte(strings.Repeat("public", 5000))
	d = digestOf(body)
	reg = newTokenRegistry(t, body, testManifest)
	ts = newTokenService(t, token)
	reg.challenges = []string{fmt.Sprintf(`Bearer realm="%s/token",service="reg",scope="repository:lib/app:pull"`, ts.URL)}
	host := reg.Listener.Addr().String()
	nodes = cluster(t, 3, host, reg.Client())
	enableAnonymousTokens(nodes, ts)
	top = coordinatorOf(nodes, d)
	for _, n := range nodes {
		if n != top {
			asker = n
		}
	}
	return
}

// The coordinator answers the registry's challenge as an anonymous client, so
// the asking node gets the blob from it and never goes to the registry.
func TestCoordinatorWithAnAnonymousTokenFetchesForTheAsker(t *testing.T) {
	_, top, asker, reg, ts, d, body := anonymousSetup(t, "tok")
	host := reg.Listener.Addr().String()
	resp, got := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	if asker.m.CoordHits.Load() != 1 || asker.m.CoordFallbacks.Load() != 0 {
		t.Fatalf("coordinator hits %d, fallbacks %d", asker.m.CoordHits.Load(), asker.m.CoordFallbacks.Load())
	}
	if asker.m.UpstreamHits.Load() != 0 || asker.m.BytesFromUpstream.Load() != 0 {
		t.Fatal("the asking node went to the registry")
	}
	if top.m.CoordAnonymousTokens.Load() != 1 || ts.asks.Load() != 1 {
		t.Fatalf("tokens used %d, token service asked %d times", top.m.CoordAnonymousTokens.Load(), ts.asks.Load())
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.auth != "" {
		t.Fatalf("a credential was sent to the token service: %q", ts.auth)
	}
	if !strings.Contains(ts.query, "service=reg") || !strings.Contains(ts.query, "scope=repository%3Alib%2Fapp%3Apull") {
		t.Fatalf("token request %q lacks the service or the scope", ts.query)
	}
}

// What an anonymous client may not read stays with the asking node and its own
// credentials: the registry refuses the anonymous token, the coordinator holds
// nothing, and the asker fetches as before.
func TestCoordinatorWhoseAnonymousTokenIsRefusedLeavesItToTheAsker(t *testing.T) {
	_, top, asker, reg, ts, d, body := anonymousSetup(t, "anonymous-but-not-allowed")
	host := reg.Listener.Addr().String()
	resp, got := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	waitCount(t, &asker.m.CoordFallbacks, 1)
	if asker.m.UpstreamHits.Load() != 1 {
		t.Fatalf("the asker fetched %d times itself, want 1", asker.m.UpstreamHits.Load())
	}
	if _, err := top.st.Stat(d); err == nil {
		t.Fatal("the coordinator holds content an anonymous client may not read")
	}
	if ts.asks.Load() != 1 {
		t.Fatalf("token service asked %d times", ts.asks.Load())
	}
	// A refusal is remembered for a while: the next miss on content an anonymous
	// client may not read does not ask the token service again.
	top.p.anon.mu.Lock()
	var denied int
	for _, v := range top.p.anon.tokens {
		if v.denied {
			denied++
		}
	}
	top.p.anon.mu.Unlock()
	if denied != 1 {
		t.Fatalf("%d denied entries, want 1", denied)
	}
	if _, _, ok := top.p.anon.get(info{ctx: t.Context()}, host, reg.challenges[0]); ok || ts.asks.Load() != 1 {
		t.Fatalf("a refused (service, scope) was asked for again: ok %v, %d asks", ok, ts.asks.Load())
	}
}

// Without the setting a coordinator does what it always did.
func TestCoordinatorWithoutTheSettingDoesNotAskATokenService(t *testing.T) {
	_, top, asker, reg, ts, d, _ := anonymousSetup(t, "tok")
	top.p.anon = nil
	host := reg.Listener.Addr().String()
	resp, _ := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ts.asks.Load() != 0 || top.m.CoordAnonymousTokens.Load() != 0 {
		t.Fatalf("token service asked %d times", ts.asks.Load())
	}
}

func TestAnonymousTokensAreCachedPerScopeAndExpire(t *testing.T) {
	ts := newTokenService(t, "T")
	now := time.Unix(1000, 0)
	a := newAnonTokens(func() time.Time { return now }, nil)
	c := *ts.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	ch := func(scope string) string {
		return fmt.Sprintf(`Bearer realm="%s/t",service="s",scope=%q`, ts.URL, scope)
	}
	in := info{ctx: t.Context()}
	for _, s := range []string{"r:a:pull", "r:a:pull", "r:b:pull"} {
		if tok, _, ok := a.get(in, "reg", ch(s)); !ok || tok != "T" {
			t.Fatalf("%s: %q, %v", s, tok, ok)
		}
	}
	if ts.asks.Load() != 2 {
		t.Fatalf("token service asked %d times for two scopes, want 2", ts.asks.Load())
	}
	// 300 s from the realm, kept for 75% of it.
	now = now.Add(200 * time.Second)
	a.get(in, "reg", ch("r:a:pull"))
	if ts.asks.Load() != 2 {
		t.Fatal("a token was asked for again before it expired")
	}
	now = now.Add(30 * time.Second)
	a.get(in, "reg", ch("r:a:pull"))
	if ts.asks.Load() != 3 {
		t.Fatalf("an expired token was kept (%d asks)", ts.asks.Load())
	}
	if _, _, ok := a.get(in, "reg", `Basic realm="x"`); ok {
		t.Fatal("a Basic challenge gave a token")
	}
}

func TestAnonymousTokenCacheIsBounded(t *testing.T) {
	ts := newTokenService(t, "T")
	a := newAnonTokens(time.Now, nil)
	c := *ts.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	in := info{ctx: t.Context()}
	for i := 0; i < maxAnonTokens+10; i++ {
		a.get(in, "reg", fmt.Sprintf(`Bearer realm="%s/t",scope="r:%d:pull"`, ts.URL, i))
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.tokens) > maxAnonTokens {
		t.Fatalf("%d tokens kept", len(a.tokens))
	}
}

// A token that lives for less than the time worth keeping is used once and not
// cached, so it is never reused past its real expiry.
func TestShortLivedAnonymousTokensAreNotCached(t *testing.T) {
	asks := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks++
		fmt.Fprint(w, `{"token":"T","expires_in":8}`)
	}))
	defer srv.Close()
	a := newAnonTokens(time.Now, nil)
	c := *srv.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	in := info{ctx: t.Context()}
	ch := fmt.Sprintf(`Bearer realm="%s/t",service="s",scope="r:a:pull"`, srv.URL)
	for range 2 {
		if tok, _, ok := a.get(in, "reg", ch); !ok || tok != "T" {
			t.Fatalf("%q, %v", tok, ok)
		}
	}
	if asks != 2 {
		t.Fatalf("token service asked %d times, want 2: an 8 s token must not be cached", asks)
	}
}

// Only the first Bearer challenge is acted on, so a registry that sends several
// cannot make a coordinator call several token services for one miss.
func TestOnlyTheFirstBearerChallengeIsTried(t *testing.T) {
	_, top, asker, reg, ts, d, _ := anonymousSetup(t, "denied")
	second := newTokenService(t, "tok")
	reg.challenges = append(reg.challenges, fmt.Sprintf(`Bearer realm="%s/token",service="reg",scope="repository:lib/app:pull"`, second.URL))
	enableAnonymousTokens([]*node{top, asker}, ts)
	host := reg.Listener.Addr().String()
	get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if second.asks.Load() != 0 {
		t.Fatalf("the second challenge's token service was asked %d times", second.asks.Load())
	}
	if top.m.CoordAnonymousTokens.Load() != 0 {
		t.Fatal("a token that was refused was counted as used")
	}
}

// A realm that errors is remembered too, so a flaky or refusing token service
// is not hit on every miss.
func TestARealmThatFailsIsNotAskedAgainForAWhile(t *testing.T) {
	asks := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	now := time.Unix(1000, 0)
	a := newAnonTokens(func() time.Time { return now }, nil)
	c := *srv.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	in := info{ctx: t.Context()}
	ch := fmt.Sprintf(`Bearer realm="%s/t",service="s",scope="r:a:pull"`, srv.URL)
	for range 3 {
		if _, _, ok := a.get(in, "reg", ch); ok {
			t.Fatal("a failing realm gave a token")
		}
	}
	if asks != 1 {
		t.Fatalf("the realm was asked %d times, want 1", asks)
	}
	now = now.Add(anonDenied + time.Second)
	a.get(in, "reg", ch)
	if asks != 2 {
		t.Fatalf("a denial outlived its time (%d asks)", asks)
	}
}

// A transport failure on the second request says nothing about the token.
func TestATransportErrorOnTheRetryKeepsTheToken(t *testing.T) {
	_, top, asker, reg, _, d, _ := anonymousSetup(t, "tok")
	host := reg.Listener.Addr().String()
	get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	top.p.anon.mu.Lock()
	defer top.p.anon.mu.Unlock()
	for k, v := range top.p.anon.tokens {
		if v.denied || v.value != "tok" {
			t.Fatalf("%q: %+v", k, v)
		}
	}
	if len(top.p.anon.tokens) != 1 {
		t.Fatalf("%d entries", len(top.p.anon.tokens))
	}
}

// A full cache loses one entry, the one that expires first, not all of them.
func TestAFullAnonymousTokenCacheEvictsOneEntry(t *testing.T) {
	now := time.Unix(1000, 0)
	a := newAnonTokens(func() time.Time { return now }, nil)
	for i := 0; i < maxAnonTokens; i++ {
		a.store(fmt.Sprintf("k%d", i), anonToken{value: "v", expires: now.Add(time.Duration(i+1) * time.Minute)})
	}
	a.store("new", anonToken{value: "v", expires: now.Add(time.Hour)})
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.tokens) != maxAnonTokens {
		t.Fatalf("%d entries, want %d", len(a.tokens), maxAnonTokens)
	}
	if _, ok := a.tokens["k0"]; ok {
		t.Fatal("the entry that expires first was kept")
	}
	if _, ok := a.tokens["k1"]; !ok {
		t.Fatal("a live entry was dropped with it")
	}
}

// A refusal and a hiccup are remembered for different times.
func TestADefiniteRefusalIsRememberedLongerThanAHiccup(t *testing.T) {
	for _, tc := range []struct {
		code int
		long bool
	}{{http.StatusForbidden, true}, {http.StatusUnauthorized, true}, {http.StatusInternalServerError, false}, {http.StatusTooManyRequests, false}} {
		asks := 0
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			asks++
			w.WriteHeader(tc.code)
		}))
		now := time.Unix(1000, 0)
		a := newAnonTokens(func() time.Time { return now }, nil)
		c := *srv.Client()
		c.CheckRedirect = noRedirect
		a.client = &c
		in := info{ctx: t.Context()}
		ch := fmt.Sprintf(`Bearer realm="%s/t",service="s",scope="r:a:pull"`, srv.URL)
		a.get(in, "reg", ch)
		now = now.Add(anonBackoff + time.Second)
		a.get(in, "reg", ch)
		srv.Close()
		if tc.long && asks != 1 || !tc.long && asks != 2 {
			t.Fatalf("%d: asked %d times after %v", tc.code, asks, anonBackoff+time.Second)
		}
	}
}

// A download that resumes asks the registry for the rest of the blob, and the
// anonymous token goes with that request, together with the Range: nothing of
// the first attempt's headers is reused.
func TestAnonymousTokenGoesWithTheRangeOfAResumedDownload(t *testing.T) {
	var mu sync.Mutex
	var seen []string // "Authorization|Range" of every request
	ts := newTokenService(t, "tok")
	reg := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization")+"|"+r.Header.Get("Range"))
		mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="reg",scope="repository:lib/app:pull"`, ts.URL))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer reg.Close()
	host := reg.Listener.Addr().String()
	nodes := cluster(t, 1, host, reg.Client())
	enableAnonymousTokens(nodes, ts)
	n := nodes[0]

	// A first attempt that failed after some bytes leaves a prefix to resume.
	part := []byte(strings.Repeat("x", 1000))
	var res store.Resume
	_, err := n.st.PutResumable(digestOf(append(part, part...)), io.MultiReader(bytes.NewReader(part), failingReader{}), 2000, nil, &res)
	if err == nil || res.Size() != 1000 {
		t.Fatalf("setup: err %v, resumable %d", err, res.Size())
	}
	dl := &download{resume: res}
	resp, ranged, err := n.p.upstreamBlobGet(info{ctx: t.Context(), dl: dl, anon: true}, host, "/v2/lib/app/blobs/sha256:00")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	want := []string{"|bytes=1000-", "Bearer tok|bytes=1000-"}
	if !ranged || resp.StatusCode != http.StatusPartialContent || !slices.Equal(seen, want) {
		t.Fatalf("ranged %v, status %d, requests %q, want %q", ranged, resp.StatusCode, seen, want)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// Misses that start together for one (service, scope) make one token request.
func TestConcurrentMissesShareOneTokenRequest(t *testing.T) {
	var asks atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks.Add(1)
		<-release
		fmt.Fprint(w, `{"token":"T","expires_in":300}`)
	}))
	defer srv.Close()
	a := newAnonTokens(time.Now, nil)
	c := *srv.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	ch := fmt.Sprintf(`Bearer realm="%s/t",service="s",scope="r:a:pull"`, srv.URL)
	const n = 8
	var wg sync.WaitGroup
	var got atomic.Int32
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tok, _, ok := a.get(info{ctx: t.Context()}, "reg", ch); ok && tok == "T" {
				got.Add(1)
			}
		}()
	}
	for asks.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // the others are waiting by now
	close(release)
	wg.Wait()
	if asks.Load() != 1 || got.Load() != n {
		t.Fatalf("%d token requests, %d of %d misses got the token", asks.Load(), got.Load(), n)
	}
}

// A client that gave up while the realm was asked is not a verdict on the realm.
func TestACancelledMissDoesNotDenyTheRealm(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	a := newAnonTokens(time.Now, nil)
	c := *srv.Client()
	c.CheckRedirect = noRedirect
	a.client = &c
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	ch := fmt.Sprintf(`Bearer realm="%s/t",service="s",scope="r:a:pull"`, srv.URL)
	if _, _, ok := a.get(info{ctx: ctx}, "reg", ch); ok {
		t.Fatal("a cancelled request gave a token")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.tokens) != 0 || len(a.flying) != 0 {
		t.Fatalf("%d cached, %d in flight after a cancelled request", len(a.tokens), len(a.flying))
	}
}

// The token service is a destination like any other: one that is not on
// SIC_UPSTREAM_REGISTRIES is never contacted, however the registry names it, and
// the operator is told what to add. The asking node fetches for itself.
func TestATokenServiceOffTheAllowlistIsNeverContacted(t *testing.T) {
	_, top, asker, reg, ts, d, body := anonymousSetup(t, "tok")
	sink := &logSink{}
	top.p.logf = sink.logf
	delete(top.p.registries, ts.Listener.Addr().String())
	host := reg.Listener.Addr().String()
	resp, got := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("the asking node did not get the blob itself: %d", resp.StatusCode)
	}
	if ts.asks.Load() != 0 || top.m.CoordAnonymousTokens.Load() != 0 {
		t.Fatalf("a token service off the allowlist was asked %d times", ts.asks.Load())
	}
	if !strings.Contains(sink.all(), "not in SIC_UPSTREAM_REGISTRIES") {
		t.Fatalf("the operator was not told why: %q", sink.all())
	}
}

// A registry the operator has marked protected is never read with an anonymous
// token, even if the registry would give one for public content.
func TestACoordinatorDoesNotReadAProtectedRegistryAnonymously(t *testing.T) {
	_, top, _, reg, ts, d, _ := anonymousSetup(t, "tok")
	host := reg.Listener.Addr().String()
	path := "/v2/lib/app/blobs/" + d + "?ns=" + host
	for _, protected := range []bool{true, false} {
		top.p.protected = map[string]bool{host: protected}
		asks := ts.asks.Load()
		top.p.coordinate(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil), "lib/app", d)
		if got := ts.asks.Load() - asks; (got != 0) == protected {
			t.Fatalf("protected %v: the token service was asked %d times", protected, got)
		}
	}
}
