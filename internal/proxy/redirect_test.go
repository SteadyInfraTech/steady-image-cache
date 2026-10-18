package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// target is a server that records how often it is asked, and what Authorization
// it was given. It stands for "wherever a redirect points".
type target struct {
	*httptest.Server
	hits atomic.Int32
	auth atomic.Value // Authorization of the last request
	body []byte
}

func newTarget(t *testing.T, tls bool, body []byte) *target {
	t.Helper()
	tg := &target{body: body}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.hits.Add(1)
		tg.auth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body)
	})
	if tls {
		tg.Server = httptest.NewTLSServer(h)
	} else {
		tg.Server = httptest.NewServer(h)
	}
	t.Cleanup(tg.Close)
	return tg
}

// redirector answers every request with a redirect to where().
func redirector(t *testing.T, tls bool, where func() string) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, where(), http.StatusFound)
	})
	var s *httptest.Server
	if tls {
		s = httptest.NewTLSServer(h)
	} else {
		s = httptest.NewServer(h)
	}
	t.Cleanup(s.Close)
	return s
}

// A peer that redirects is not followed, wherever it points. It is a faulty
// node: it is skipped for a while, and the other peers are tried.
func TestPeerRedirectIsNotFollowed(t *testing.T) {
	body := []byte(strings.Repeat("the blob ", 50))
	d := digestOf(body)
	elsewhere := newTarget(t, false, body) // would happily serve the blob
	bad := redirector(t, false, func() string { return elsewhere.URL + "/anything" })
	sink := &logSink{}

	a := newNode(t, Options{PeerBackoff: time.Minute, Logf: sink.logf})
	a.fin.urls.Store(&[]string{bad.URL})
	getFailing(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if elsewhere.hits.Load() != 0 {
		t.Fatalf("the redirect was followed (%d requests)", elsewhere.hits.Load())
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("nothing may be stored from behind a peer's redirect")
	}
	if len(a.p.shuffledPeers()) != 0 {
		t.Fatal("a peer that redirects should be skipped for a while")
	}
	if !strings.Contains(sink.all(), "302") {
		t.Fatalf("the redirect went unlogged: %q", sink.all())
	}
}

func TestOtherPeersAreUsedWhenOneRedirects(t *testing.T) {
	body := []byte("a layer held by the good peer")
	d := digestOf(body)
	elsewhere := newTarget(t, false, []byte("not the layer"))
	bad := redirector(t, false, func() string { return elsewhere.URL })
	good := newNode(t, Options{})
	put(t, good, body)

	a := newNode(t, Options{PeerBackoff: time.Minute})
	a.fin.urls.Store(&[]string{bad.URL, good.srv.URL})
	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %q", resp.StatusCode, got)
	}
	if elsewhere.hits.Load() != 0 {
		t.Fatalf("the redirect was followed (%d requests)", elsewhere.hits.Load())
	}
}

// The same goes for the HEAD probe and for manifests.
func TestPeerRedirectIsNotFollowedForHeadAndManifests(t *testing.T) {
	elsewhere := newTarget(t, false, []byte("x"))
	bad := redirector(t, false, func() string { return elsewhere.URL })
	a := newNode(t, Options{PeerBackoff: time.Minute})
	a.fin.urls.Store(&[]string{bad.URL})

	if rec := request(a.p, "HEAD", "/v2/lib/app/blobs/"+digestOf([]byte("x"))); rec.Code != 404 {
		t.Fatalf("HEAD: status %d", rec.Code)
	}
	a.fin.urls.Store(&[]string{bad.URL}) // the first call put it in back-off; ask again
	a.p.dmu.Lock()
	a.p.down = map[string]time.Time{}
	a.p.dmu.Unlock()
	if rec := request(a.p, "GET", "/v2/lib/app/manifests/v1"); rec.Code != 404 {
		t.Fatalf("manifest: status %d", rec.Code)
	}
	if elsewhere.hits.Load() != 0 {
		t.Fatalf("a redirect was followed (%d requests)", elsewhere.hits.Load())
	}
}

// upstreamClientFor trusts the test servers' certificate, whatever host name a
// redirect uses, and has the proxy's own redirect rules.
func upstreamClientFor(srv *httptest.Server) *http.Client {
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.ServerName = "example.com" // the name in httptest's certificate
	return &http.Client{Transport: tr, CheckRedirect: upstreamRedirect}
}

// Registries send blobs to a CDN or an object store: that has to work, and the
// registry's credentials must not go along to another host.
func TestUpstreamRedirectToAnotherHostIsFollowedWithoutCredentials(t *testing.T) {
	body := []byte(strings.Repeat("a blob on the CDN ", 40))
	cdn := newTarget(t, true, body)
	var registryAuth atomic.Value
	registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryAuth.Store(r.Header.Get("Authorization"))
		// "localhost" is a different host from the registry's 127.0.0.1.
		http.Redirect(w, r, strings.Replace(cdn.URL, "127.0.0.1", "localhost", 1)+"/blob", http.StatusTemporaryRedirect)
	}))
	defer registry.Close()
	p, _, host := upstreamProxy(t, registry, &logSink{})
	p.upstreamClient = upstreamClientFor(registry)

	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(body)+"?ns="+host, "Authorization", "Bearer tok")
	if rec.Code != 200 || rec.Body.String() != string(body) {
		t.Fatalf("status %d, %d bytes", rec.Code, rec.Body.Len())
	}
	if registryAuth.Load() != "Bearer tok" {
		t.Fatalf("the registry saw Authorization %q", registryAuth.Load())
	}
	if cdn.hits.Load() != 1 || cdn.auth.Load() != "" {
		t.Fatalf("the CDN got %d requests, Authorization %q: credentials must stay with the registry", cdn.hits.Load(), cdn.auth.Load())
	}
	if _, err := p.store.Stat(digestOf(body)); err != nil {
		t.Fatal("the blob should be cached")
	}
}

// A redirect within the registry keeps the credentials: it is still the registry.
func TestUpstreamRedirectWithinTheRegistryKeepsCredentials(t *testing.T) {
	body := []byte("served from a second path")
	var seen []string
	var registry *httptest.Server
	registry = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path+" "+r.Header.Get("Authorization"))
		if r.URL.Path != "/storage" {
			http.Redirect(w, r, "/storage", http.StatusFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Write(body)
	}))
	defer registry.Close()
	p, _, host := upstreamProxy(t, registry, &logSink{})
	p.upstreamClient = upstreamClientFor(registry)
	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(body)+"?ns="+host, "Authorization", "Bearer tok")
	if rec.Code != 200 || len(seen) != 2 || !strings.HasSuffix(seen[1], "/storage Bearer tok") {
		t.Fatalf("status %d, requests %q", rec.Code, seen)
	}
}

// A registry cannot take the transfer to plain HTTP.
func TestUpstreamRedirectToPlainHTTPIsRefused(t *testing.T) {
	body := []byte("served over http")
	plain := newTarget(t, false, body)
	registry := redirector(t, true, func() string { return plain.URL })
	sink := &logSink{}
	p, _, host := upstreamProxy(t, registry, sink)
	p.upstreamClient = upstreamClientFor(registry)

	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(body)+"?ns="+host)
	if rec.Code != 404 {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	if plain.hits.Load() != 0 {
		t.Fatalf("the plain-HTTP address was contacted %d times", plain.hits.Load())
	}
	if !strings.Contains(sink.all(), "only https") {
		t.Fatalf("the refusal went unlogged: %q", sink.all())
	}
}

// A redirect loop ends, after a bounded number of requests.
func TestUpstreamRedirectLoopEnds(t *testing.T) {
	var n atomic.Int32
	registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		http.Redirect(w, r, r.URL.Path+"x", http.StatusFound)
	}))
	defer registry.Close()
	sink := &logSink{}
	p, _, host := upstreamProxy(t, registry, sink)
	p.upstreamClient = upstreamClientFor(registry)
	if rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf([]byte("loop"))+"?ns="+host); rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
	if got := int(n.Load()); got != maxRedirects+1 {
		t.Fatalf("the registry saw %d requests, want the first and %d redirects = %d", got, maxRedirects, maxRedirects+1)
	}
	if !strings.Contains(sink.all(), "redirects") {
		t.Fatalf("the failure went unlogged: %q", sink.all())
	}
}

// The clients New builds carry the policies; the tests above replace the
// upstream client's transport, so this checks the wiring itself.
func TestNewProxyClientsHaveTheRedirectPolicies(t *testing.T) {
	a := newNode(t, Options{})
	if err := a.p.peerHTTP().CheckRedirect(&http.Request{}, nil); err != http.ErrUseLastResponse {
		t.Fatalf("peer client: %v, want ErrUseLastResponse", err)
	}
	req := &http.Request{URL: mustURL(t, "http://x/")}
	if err := a.p.upstreamClient.CheckRedirect(req, nil); err == nil {
		t.Fatal("upstream client follows a redirect to http")
	}
	req = &http.Request{URL: mustURL(t, "https://x/")}
	if err := a.p.upstreamClient.CheckRedirect(req, nil); err != nil {
		t.Fatalf("upstream client refuses a redirect to https: %v", err)
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
