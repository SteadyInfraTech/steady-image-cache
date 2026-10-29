package proxy

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

func protectedProxy(t *testing.T, reg *tokenRegistry, protect bool) (*Proxy, *metrics.Metrics, string) {
	t.Helper()
	host := reg.Listener.Addr().String()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	o := Options{Upstream: true, UpstreamRegistries: []string{host}}
	if protect {
		o.ProtectedRegistries = []string{host}
	}
	p, err := New(st, &swapFinder{}, m, o)
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = reg.Client()
	return p, m, host
}

// What one client fetched with its credentials is not given to another that the
// registry does not know, however it got to the cache.
func TestProtectedRegistryChecksEveryClient(t *testing.T) {
	blob := []byte("a layer of a private image")
	reg := newTokenRegistry(t, blob, testManifest)
	p, m, host := protectedProxy(t, reg, true)
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host

	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 || rec.Body.String() != string(blob) {
		t.Fatalf("the entitled client: status %d", rec.Code)
	}
	waitCount(t, &m.UpstreamHits, 1)
	if _, err := p.store.Stat(digestOf(blob)); err != nil {
		t.Fatal("the blob should be cached now")
	}

	// It is cached. Nobody else gets it on the cache's word.
	for name, auth := range map[string]string{"no credentials": "", "wrong token": "Bearer evil"} {
		var hdr []string
		if auth != "" {
			hdr = []string{"Authorization", auth}
		}
		rec := request(p, "GET", path, hdr...)
		if rec.Code != 401 {
			t.Fatalf("%s: status %d, want 401", name, rec.Code)
		}
		if got := rec.Header().Values("WWW-Authenticate"); len(got) != 1 || got[0] != challenge {
			t.Fatalf("%s: challenge %q", name, got)
		}
		if rec.Body.String() == string(blob) {
			t.Fatalf("%s: got the blob", name)
		}
	}
	if m.AccessDenied.Load() != 2 {
		t.Fatalf("AccessDenied = %d, want 2", m.AccessDenied.Load())
	}
	// HEAD and manifests are guarded the same way.
	if rec := request(p, "HEAD", path); rec.Code != 401 {
		t.Fatalf("HEAD without credentials: %d", rec.Code)
	}
	if rec := request(p, "GET", "/v2/lib/app/manifests/"+digestOf(testManifest)+"?ns="+host); rec.Code != 401 {
		t.Fatalf("manifest without credentials: %d", rec.Code)
	}
}

// An answer is remembered briefly, so a pull of many layers asks the registry
// once, not once per layer.
func TestProtectedRegistryRemembersAYes(t *testing.T) {
	blob := []byte("layer")
	reg := newTokenRegistry(t, blob, testManifest)
	p, m, host := protectedProxy(t, reg, true)
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host
	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	before := reg.requests.Load()
	for i := 0; i < 5; i++ {
		if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
			t.Fatalf("status %d", rec.Code)
		}
	}
	if got := reg.requests.Load() - before; got != 0 {
		t.Fatalf("%d registry requests for answers that were remembered", got)
	}
	if m.AccessChecks.Load() != 1 {
		t.Fatalf("AccessChecks = %d, want 1", m.AccessChecks.Load())
	}
	// Past the time, the registry is asked again.
	p.now = func() time.Time { return time.Now().Add(2 * accessTTL) }
	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if m.AccessChecks.Load() != 2 {
		t.Fatalf("AccessChecks = %d, want 2 after the time", m.AccessChecks.Load())
	}
}

// The registry that cannot be asked cannot confirm anybody.
func TestProtectedRegistryFailsClosed(t *testing.T) {
	blob := []byte("layer")
	reg := newTokenRegistry(t, blob, testManifest)
	p, _, host := protectedProxy(t, reg, true)
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host
	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	reg.Close()
	p.access = accessCache{} // nothing remembered
	rec := request(p, "GET", path, "Authorization", "Bearer tok")
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() == string(blob) {
		t.Fatalf("status %d with the registry down", rec.Code)
	}
}

// The peer header is set by whoever sends it, so it opens nothing: a client
// that sets it is refused, and the cache is not read for it.
func TestProtectedRegistryIgnoresThePeerHeader(t *testing.T) {
	blob := []byte("layer")
	reg := newTokenRegistry(t, blob, testManifest)
	p, _, host := protectedProxy(t, reg, true)
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host
	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	for _, m := range []string{"GET", "HEAD"} {
		rec := request(p, m, path, PeerHeader, "1")
		if rec.Code != http.StatusForbidden || rec.Body.String() == string(blob) {
			t.Fatalf("%s with the peer header and no credentials: status %d", m, rec.Code)
		}
	}
	// No peer is asked for it, and none is believed.
	if got := p.askablePeers(host); len(got) != 0 {
		t.Fatalf("askablePeers for a protected registry = %v", got)
	}
}

// A yes is for the path that was asked, not for the repository: a digest that
// the registry would not give from this repository is not given from the cache
// either, even to a client that was just confirmed for another blob of it.
func TestProtectedRegistryGrantIsForThePath(t *testing.T) {
	blob := []byte("layer of lib/app")
	secret := []byte("layer of lib/secret, cached for someone else")
	reg := newTokenRegistry(t, blob, testManifest)
	// A registry that has only the first blob, in lib/app.
	only := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/lib/app/blobs/"+digestOf(blob)) {
			http.NotFound(w, r)
			return
		}
		reg.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(only.Close)
	host := only.Listener.Addr().String()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutVerified(digestOf(secret), bytes.NewReader(secret)); err != nil {
		t.Fatal(err)
	}
	p, err := New(st, &swapFinder{}, &metrics.Metrics{}, Options{Upstream: true, UpstreamRegistries: []string{host}, ProtectedRegistries: []string{host}})
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = only.Client()
	if rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("the blob the registry has: status %d", rec.Code)
	}
	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(secret)+"?ns="+host, "Authorization", "Bearer tok")
	if rec.Code != http.StatusNotFound || rec.Body.String() == string(secret) {
		t.Fatalf("a cached digest asked for in a repository that does not have it: status %d", rec.Code)
	}
}

// The namespace is the client's word, and the cache is kept by digest: content
// that came from a protected registry is not given out under another namespace
// on the cache's say-so either.
func TestProtectedBlobIsNotGivenOutUnderAnotherNamespace(t *testing.T) {
	blob := []byte("a layer of a private image")
	reg := newTokenRegistry(t, blob, testManifest)
	other := httptest.NewTLSServer(http.NotFoundHandler()) // an upstream that has nothing
	t.Cleanup(other.Close)
	host, otherHost := reg.Listener.Addr().String(), other.Listener.Addr().String()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(st, &swapFinder{}, &metrics.Metrics{}, Options{Upstream: true, UpstreamRegistries: []string{host, otherHost}, ProtectedRegistries: []string{host}})
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = reg.Client() // the test servers share one certificate
	d := digestOf(blob)
	if rec := request(p, "GET", "/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("the entitled client: status %d", rec.Code)
	}
	for name, ns := range map[string]string{"an upstream that does not have it": otherHost, "a host that is no upstream": "evil.example"} {
		for _, m := range []string{"GET", "HEAD"} {
			rec := request(p, m, "/v2/lib/app/blobs/"+d+"?ns="+ns, "Authorization", "Bearer tok")
			if rec.Code == 200 || rec.Body.String() == string(blob) {
				t.Fatalf("%s %s: status %d, the cache gave out a protected blob", m, name, rec.Code)
			}
		}
	}
	// Content that never came from a protected registry is as before.
	plain := []byte("a public layer")
	if _, err := st.PutVerified(digestOf(plain), bytes.NewReader(plain)); err != nil {
		t.Fatal(err)
	}
	if rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(plain)+"?ns="+otherHost); rec.Code != 200 {
		t.Fatalf("a blob that is not protected: status %d", rec.Code)
	}
}

// Nothing changes for a registry that is not listed.
func TestUnprotectedRegistryIsAsBefore(t *testing.T) {
	blob := []byte("layer")
	reg := newTokenRegistry(t, blob, testManifest)
	p, m, host := protectedProxy(t, reg, false)
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host
	if rec := request(p, "GET", path, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	if rec := request(p, "GET", path); rec.Code != 200 {
		t.Fatalf("cached, no credentials: status %d", rec.Code)
	}
	if m.AccessChecks.Load() != 0 {
		t.Fatal("a check was made")
	}
}

func TestProtectedRegistryMustBeUpstream(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for name, o := range map[string]Options{
		"upstream off":    {ProtectedRegistries: []string{"reg.example"}},
		"not in the list": {Upstream: true, UpstreamRegistries: []string{"other.example"}, ProtectedRegistries: []string{"reg.example"}},
	} {
		if _, err := New(st, &swapFinder{}, &metrics.Metrics{}, o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	o := Options{Upstream: true, UpstreamRegistries: []string{"Reg.Example"}, ProtectedRegistries: []string{"reg.example"}}
	if _, err := New(st, &swapFinder{}, &metrics.Metrics{}, o); err != nil {
		t.Errorf("same registry in another case: %v", err)
	}
}
