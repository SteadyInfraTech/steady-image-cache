package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		n.p.anon = newAnonTokens(time.Now)
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
	if top.m.CoordTokens.Load() != 1 || ts.asks.Load() != 1 {
		t.Fatalf("tokens used %d, token service asked %d times", top.m.CoordTokens.Load(), ts.asks.Load())
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
	// The refused token is not kept: the next request asks the service again.
	top.p.anon.mu.Lock()
	n := len(top.p.anon.tokens)
	top.p.anon.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d refused tokens are still cached", n)
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
	if ts.asks.Load() != 0 || top.m.CoordTokens.Load() != 0 {
		t.Fatalf("token service asked %d times", ts.asks.Load())
	}
}

func TestAnonymousTokensAreCachedPerScopeAndExpire(t *testing.T) {
	ts := newTokenService(t, "T")
	now := time.Unix(1000, 0)
	a := newAnonTokens(func() time.Time { return now })
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
	a := newAnonTokens(time.Now)
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
