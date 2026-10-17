package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const challenge = `Bearer realm="https://auth.example/token",service="registry.example"`

// tokenRegistry is a registry that wants "Bearer tok" for everything, like
// Docker Hub or GHCR. Requests without it get a 401 and the challenge.
type tokenRegistry struct {
	*httptest.Server
	blob, manifest []byte
	requests       atomic.Int32 // all requests
	authorized     atomic.Int32 // those that were let in
	lastAuth       atomic.Value // Authorization of the last request
	challenges     []string     // what to send with a 401; defaults to challenge
}

func newTokenRegistry(t *testing.T, blob, manifest []byte) *tokenRegistry {
	t.Helper()
	r := &tokenRegistry{blob: blob, manifest: manifest, challenges: []string{challenge}}
	r.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.requests.Add(1)
		r.lastAuth.Store(req.Header.Get("Authorization"))
		if req.Header.Get("Authorization") != "Bearer tok" {
			for _, c := range r.challenges {
				w.Header().Add("WWW-Authenticate", c)
			}
			http.Error(w, "unauthorized", 401)
			return
		}
		r.authorized.Add(1)
		if strings.Contains(req.URL.Path, "/manifests/") {
			w.Header().Set("Docker-Content-Digest", digestOf(r.manifest))
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Write(r.manifest)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(r.blob)))
		if req.Method != http.MethodHead {
			w.Write(r.blob)
		}
	}))
	t.Cleanup(r.Close)
	return r
}

var testManifest = []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)

func request(p *Proxy, method, path string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

// The whole exchange containerd goes through with a registry that needs a token:
// it asks, is told where to get one, asks again with it, and from then on the
// content is cached.
func TestUpstreamChallengeIsRelayedAndTheTokenPassedOn(t *testing.T) {
	blob := []byte("layer behind a token")
	reg := newTokenRegistry(t, blob, testManifest)
	sink := &logSink{}
	p, m, host := upstreamProxy(t, reg.Server, sink)

	for _, tc := range []struct{ what, path string }{
		{"blob", "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host},
		{"manifest", "/v2/lib/app/manifests/v1?ns=" + host},
	} {
		rec := request(p, "GET", tc.path)
		if rec.Code != 401 {
			t.Fatalf("%s without a token: status %d, want 401", tc.what, rec.Code)
		}
		if got := rec.Header().Values("WWW-Authenticate"); len(got) != 1 || got[0] != challenge {
			t.Fatalf("%s: WWW-Authenticate = %q, want the registry's challenge", tc.what, got)
		}
		if rec.Body.Len() == 0 {
			t.Fatalf("%s: a 401 should say why", tc.what)
		}
	}
	if m.UpstreamChallenges.Load() != 2 {
		t.Fatalf("UpstreamChallenges = %d, want 2", m.UpstreamChallenges.Load())
	}
	if m.Misses.Load() != 0 {
		t.Fatalf("a challenge is not a miss: Misses = %d", m.Misses.Load())
	}
	if _, err := p.store.Stat(digestOf(blob)); err == nil {
		t.Fatal("nothing may be cached from a request that was turned away")
	}

	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host, "Authorization", "Bearer tok")
	if rec.Code != 200 || rec.Body.String() != string(blob) {
		t.Fatalf("blob with a token: status %d body %q", rec.Code, rec.Body.String())
	}
	rec = request(p, "GET", "/v2/lib/app/manifests/v1?ns="+host, "Authorization", "Bearer tok")
	if rec.Code != 200 || rec.Body.String() != string(testManifest) {
		t.Fatalf("manifest with a token: status %d body %q", rec.Code, rec.Body.String())
	}
	if reg.lastAuth.Load() != "Bearer tok" {
		t.Fatalf("registry saw Authorization %q", reg.lastAuth.Load())
	}
	if sink.all() != "" {
		t.Fatalf("a challenge is normal protocol and must not be logged: %q", sink.all())
	}

	// Cached now: the content is served without asking the registry again.
	before := reg.requests.Load()
	rec = request(p, "GET", "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host)
	if rec.Code != 200 || rec.Body.String() != string(blob) || reg.requests.Load() != before {
		t.Fatalf("cached blob: status %d, %d new registry requests", rec.Code, reg.requests.Load()-before)
	}
}

// A token that has expired or is wrong is turned away with the challenge again,
// so that the client can get a new one.
func TestRejectedTokenGetsTheChallengeAgain(t *testing.T) {
	blob := []byte("x")
	reg := newTokenRegistry(t, blob, testManifest)
	p, _, host := upstreamProxy(t, reg.Server, &logSink{})
	for _, path := range []string{"/v2/lib/app/blobs/" + digestOf(blob), "/v2/lib/app/manifests/v1"} {
		rec := request(p, "GET", path+"?ns="+host, "Authorization", "Bearer stale")
		if rec.Code != 401 || rec.Header().Get("WWW-Authenticate") != challenge {
			t.Fatalf("%s: status %d, challenge %q", path, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
	}
}

// A 401 with no challenge cannot be acted on; the client is better served by a
// 404, which sends containerd on to its next host, and the operator by a log line.
func TestUpstream401WithoutAChallengeIsNotFoundAndLogged(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", 401)
	}))
	defer srv.Close()
	sink := &logSink{}
	p, m, host := upstreamProxy(t, srv, sink)
	for _, path := range []string{"/v2/lib/app/blobs/" + digestOf([]byte("x")), "/v2/lib/app/manifests/v1"} {
		if rec := request(p, "GET", path+"?ns="+host); rec.Code != 404 {
			t.Errorf("%s: status %d, want 404", path, rec.Code)
		}
	}
	if !strings.Contains(sink.all(), "401") || !strings.Contains(sink.all(), "WWW-Authenticate") {
		t.Fatalf("an unusable 401 went unlogged: %q", sink.all())
	}
	if m.UpstreamChallenges.Load() != 0 {
		t.Fatalf("UpstreamChallenges = %d, want 0", m.UpstreamChallenges.Load())
	}
}

// An ordinary "not found" is neither relayed nor logged.
func TestUpstreamNotFoundIsStillQuiet(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", challenge) // irrelevant on a 404
		http.Error(w, "no", 404)
	}))
	defer srv.Close()
	sink := &logSink{}
	p, _, host := upstreamProxy(t, srv, sink)
	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf([]byte("y"))+"?ns="+host)
	if rec.Code != 404 || rec.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("status %d, WWW-Authenticate %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	if sink.all() != "" {
		t.Fatalf("an ordinary not-found must not be logged: %q", sink.all())
	}
}

// A registry may offer several ways to authenticate; all of them go to the
// client, in order, up to a limit.
func TestSeveralChallengesAreRelayedInOrderUpToALimit(t *testing.T) {
	blob := []byte("x")
	reg := newTokenRegistry(t, blob, testManifest)
	reg.challenges = []string{`Basic realm="r"`, `Bearer realm="https://a/t"`, "c3", "c4", "c5-dropped"}
	p, _, host := upstreamProxy(t, reg.Server, &logSink{})
	rec := request(p, "GET", "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host)
	got := rec.Header().Values("WWW-Authenticate")
	want := reg.challenges[:maxChallenges]
	if rec.Code != 401 || strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("status %d, challenges %q, want %q", rec.Code, got, want)
	}
}

func TestChallengeOfSkipsUnusableValues(t *testing.T) {
	h := http.Header{}
	h.Add("WWW-Authenticate", "")
	h.Add("WWW-Authenticate", "has\r\nInjected: header")
	h.Add("WWW-Authenticate", strings.Repeat("a", maxChallengeSize+1))
	h.Add("WWW-Authenticate", strings.Repeat("b", maxChallengeSize))
	h.Add("WWW-Authenticate", "Bearer ok")
	got := challengeOf(&http.Response{Header: h})
	if len(got) != 2 || got[0] != strings.Repeat("b", maxChallengeSize) || got[1] != "Bearer ok" {
		t.Fatalf("got %d values: %q", len(got), got)
	}
	if challengeOf(&http.Response{Header: http.Header{}}) != nil {
		t.Fatal("no header, no challenge")
	}
}

// A HEAD for a blob that is not cached is put to the registry as a HEAD, and a
// challenge in the answer reaches the client as well.
func TestHeadRelaysTheChallengeAndTheSize(t *testing.T) {
	blob := []byte("a layer of known size")
	reg := newTokenRegistry(t, blob, testManifest)
	p, _, host := upstreamProxy(t, reg.Server, &logSink{})
	path := "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host

	rec := request(p, "HEAD", path)
	if rec.Code != 401 || rec.Header().Get("WWW-Authenticate") != challenge {
		t.Fatalf("HEAD without a token: status %d, challenge %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	rec = request(p, "HEAD", path, "Authorization", "Bearer tok")
	if rec.Code != 200 || rec.Header().Get("Content-Length") != strconv.Itoa(len(blob)) {
		t.Fatalf("HEAD with a token: status %d, length %q", rec.Code, rec.Header().Get("Content-Length"))
	}
}

// Requests from other SIC nodes are answered from the cache alone, so they never
// reach the registry and never get a challenge.
func TestPeerRequestsNeverReachTheRegistry(t *testing.T) {
	blob := []byte("x")
	reg := newTokenRegistry(t, blob, testManifest)
	p, _, host := upstreamProxy(t, reg.Server, &logSink{})
	for _, path := range []string{"/v2/lib/app/blobs/" + digestOf(blob), "/v2/lib/app/manifests/v1"} {
		rec := request(p, "GET", path+"?ns="+host, PeerHeader, "1")
		if rec.Code != 404 || rec.Header().Get("WWW-Authenticate") != "" {
			t.Fatalf("%s: status %d, challenge %q", path, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
	}
	if reg.requests.Load() != 0 {
		t.Fatalf("the registry saw %d requests", reg.requests.Load())
	}
}

// Blob downloads are shared per digest. A client without credentials must not
// pull a client that has them into its 401: while the anonymous request is out,
// the one with a token joins it, and when the 401 comes back it asks again.
func TestClientWithATokenIsNotGivenAnotherClientsRefusal(t *testing.T) {
	blob := []byte(strings.Repeat("private layer ", 100))
	gate := make(chan struct{})
	anonymous := make(chan struct{}, 4)
	var authorized atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			anonymous <- struct{}{}
			<-gate
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, "unauthorized", 401)
			return
		}
		authorized.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		w.Write(blob)
	}))
	defer srv.Close()
	p, _, host := upstreamProxy(t, srv, &logSink{})
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()
	url := proxySrv.URL + "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host

	type result struct {
		status    int
		body      string
		challenge string
	}
	ask := func(auth string) <-chan result {
		out := make(chan result, 1)
		go func() {
			req, _ := http.NewRequest("GET", url, nil)
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				out <- result{status: -1, body: err.Error()}
				return
			}
			defer resp.Body.Close()
			b := new(strings.Builder)
			buf := make([]byte, 4096)
			for {
				n, err := resp.Body.Read(buf)
				b.Write(buf[:n])
				if err != nil {
					break
				}
			}
			out <- result{resp.StatusCode, b.String(), resp.Header.Get("WWW-Authenticate")}
		}()
		return out
	}

	anon := ask("")
	<-anonymous // the anonymous request is at the registry
	withToken := ask("Bearer tok")
	for i := 0; i < 400 && !p.downloading(digestOf(blob)); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // let the second client join the download
	close(gate)

	if r := <-anon; r.status != 401 || r.challenge != challenge {
		t.Fatalf("anonymous client: %+v", r)
	}
	if r := <-withToken; r.status != 200 || r.body != string(blob) {
		t.Fatalf("client with a token: status %d, %d bytes (%q)", r.status, len(r.body), r.body[:min(len(r.body), 60)])
	}
	if authorized.Load() != 1 {
		t.Fatalf("registry served %d authorized requests, want 1", authorized.Load())
	}
}
