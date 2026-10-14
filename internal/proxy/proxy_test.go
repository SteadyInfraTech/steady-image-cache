package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

func digestOf(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

// node is one SIC instance behind a real HTTP server, so that peers can talk to
// each other the way they do in a cluster.
type node struct {
	p   *Proxy
	st  *store.Store
	m   *metrics.Metrics
	fin *swapFinder
	srv *httptest.Server
}

// newNode starts a node. Its peers are filled in later with setPeers because
// nodes need each other's URLs.
func newNode(t *testing.T, o Options) *node {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	n := &node{st: st, m: m, fin: &swapFinder{}}
	p, err := New(st, n.fin, m, o)
	if err != nil {
		t.Fatal(err)
	}
	n.p = p
	n.srv = httptest.NewServer(p)
	t.Cleanup(n.srv.Close)
	return n
}

type swapFinder struct{ urls atomic.Pointer[[]string] }

func (s *swapFinder) Candidates() []string {
	if u := s.urls.Load(); u != nil {
		return append([]string(nil), *u...)
	}
	return nil
}

func (n *node) setPeers(peers ...*node) {
	var u []string
	for _, p := range peers {
		u = append(u, p.srv.URL)
	}
	n.fin.urls.Store(&u)
}

var _ peer.Finder = (*swapFinder)(nil)

func put(t *testing.T, n *node, body []byte) string {
	t.Helper()
	d := digestOf(body)
	if _, err := n.st.PutVerified(d, strings.NewReader(string(body))); err != nil {
		t.Fatal(err)
	}
	return d
}

func get(t *testing.T, url string, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func TestLocalBlobHitSupportsRange(t *testing.T) {
	a := newNode(t, Options{})
	body := []byte("0123456789")
	d := put(t, a, body)
	resp, b := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d, "Range", "bytes=2-4")
	if resp.StatusCode != 206 || string(b) != "234" {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
	if resp.Header.Get("Docker-Content-Digest") != d {
		t.Fatal("missing digest header")
	}
	if a.m.LocalHits.Load() != 1 {
		t.Fatalf("LocalHits = %d", a.m.LocalHits.Load())
	}
}

func TestBlobFetchedFromPeerAndCached(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	body := []byte("layer bytes")
	d := put(t, b, body)
	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d body %q", resp.StatusCode, got)
	}
	if _, err := a.st.Stat(d); err != nil {
		t.Fatal("blob should be cached locally after a peer fetch")
	}
	if a.m.PeerHits.Load() != 1 {
		t.Fatalf("PeerHits = %d", a.m.PeerHits.Load())
	}
}

// Two nodes that both miss used to ask each other forever: A's single-flight
// entry stayed open while B's request to A waited on it.
func TestPeersDoNotLoopOnMiss(t *testing.T) {
	a, b := newNode(t, Options{Timeout: 5 * time.Second}), newNode(t, Options{Timeout: 5 * time.Second})
	a.setPeers(b)
	b.setPeers(a)
	d := digestOf([]byte("nobody has this"))

	start := time.Now()
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("miss took %v: peers are waiting on each other", el)
	}
}

func TestPeerRequestIsAnsweredFromLocalCacheOnly(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	var hits atomic.Int32
	counted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.NotFound(w, r)
	}))
	defer counted.Close()
	a.fin.urls.Store(&[]string{b.srv.URL, counted.URL})

	d := digestOf([]byte("absent"))
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d, PeerHeader, "1")
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatal("a peer-originated request must not be forwarded to other peers")
	}
}

func TestManifestByDigestIsFetchedFromPeer(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	d := put(t, b, doc)
	resp, got := get(t, a.srv.URL+"/v2/lib/app/manifests/"+d)
	if resp.StatusCode != 200 || string(got) != string(doc) {
		t.Fatalf("status %d body %q", resp.StatusCode, got)
	}
	if _, err := a.st.Stat(d); err != nil {
		t.Fatal("manifest should be cached after the peer fetch")
	}
}

func TestManifestByTagIsFetchedFromPeerAndAliased(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	d := put(t, b, doc)
	if err := b.st.SetAlias("reg.example", "lib/app", "v1", d); err != nil {
		t.Fatal(err)
	}
	resp, got := get(t, a.srv.URL+"/v2/lib/app/manifests/v1?ns=reg.example")
	if resp.StatusCode != 200 || string(got) != string(doc) {
		t.Fatalf("status %d body %q", resp.StatusCode, got)
	}
	if got, ok := a.st.Resolve("reg.example", "lib/app", "v1"); !ok || got != d {
		t.Fatalf("tag not aliased locally: %q %v", got, ok)
	}
}

func TestManifestContentTypeFollowsDocument(t *testing.T) {
	a := newNode(t, Options{})
	cases := []struct {
		name, body, want string
	}{
		{"oci index", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`, "application/vnd.oci.image.index.v1+json"},
		{"docker list", `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[]}`, "application/vnd.docker.distribution.manifest.list.v2+json"},
		{"docker v2", `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[]}`, "application/vnd.docker.distribution.manifest.v2+json"},
		{"index without mediaType", `{"schemaVersion":2,"manifests":[{}]}`, "application/vnd.oci.image.index.v1+json"},
		{"manifest without mediaType", `{"schemaVersion":2,"layers":[]}`, "application/vnd.oci.image.manifest.v1+json"},
		{"not json", `garbage`, "application/vnd.oci.image.manifest.v1+json"},
	}
	for _, c := range cases {
		d := put(t, a, []byte(c.body))
		resp, got := get(t, a.srv.URL+"/v2/lib/app/manifests/"+d)
		if resp.StatusCode != 200 || string(got) != c.body {
			t.Errorf("%s: status %d body %q", c.name, resp.StatusCode, got)
		}
		if ct := resp.Header.Get("Content-Type"); ct != c.want {
			t.Errorf("%s: Content-Type %q, want %q", c.name, ct, c.want)
		}
	}
}

func TestInvalidReferencesAreRejected(t *testing.T) {
	a := newNode(t, Options{})
	for _, path := range []string{
		"/v2/lib/app/blobs/sha256:short",
		"/v2/lib/app/blobs/sha256:" + strings.Repeat("A", 64),
		"/v2/lib/app/blobs/sha256:" + strings.Repeat("a", 60) + "%2F..",
		"/v2/../etc/blobs/" + digestOf([]byte("x")),
		"/v2/lib/./app/blobs/" + digestOf([]byte("x")),
		"/v2/lib//app/blobs/" + digestOf([]byte("x")),
		"/v2/lib/app/manifests/bad%20tag",
		"/v2/lib/app/manifests/sha256:nothex",
	} {
		req := httptest.NewRequest("GET", path, nil)
		rec := httptest.NewRecorder()
		a.p.ServeHTTP(rec, req)
		if rec.Code != 400 && rec.Code != 404 {
			t.Errorf("%s: status %d, want 400/404", path, rec.Code)
		}
	}
}

func TestPeerDigestMismatchIsRejected(t *testing.T) {
	a := newNode(t, Options{})
	lying := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not the blob you asked for"))
	}))
	defer lying.Close()
	a.fin.urls.Store(&[]string{lying.URL})
	d := digestOf([]byte("the real blob"))
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if a.m.VerifyErrors.Load() != 1 {
		t.Fatalf("VerifyErrors = %d", a.m.VerifyErrors.Load())
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("a mismatching blob must not be stored")
	}
}

func TestPeerCannotAnswerDigestManifestWithDifferentDigest(t *testing.T) {
	a := newNode(t, Options{})
	other := []byte(`{"schemaVersion":2,"layers":[]}`)
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", digestOf(other))
		w.Write(other)
	}))
	defer liar.Close()
	a.fin.urls.Store(&[]string{liar.URL})
	want := digestOf([]byte(`{"schemaVersion":2,"layers":[1]}`))
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/manifests/"+want)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
}

func respWith(body, header string) *http.Response {
	r := &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}
	if header != "" {
		r.Header.Set("Docker-Content-Digest", header)
	}
	return r
}

func TestReadManifest(t *testing.T) {
	body := `{"schemaVersion":2}`
	d := digestOf([]byte(body))
	other := digestOf([]byte("other"))
	cases := []struct {
		name, ref, header string
		ok                bool
	}{
		{"tag, header matches", "v1", d, true},
		{"tag, no header (it is optional upstream)", "v1", "", true},
		{"tag, header names other content", "v1", other, false},
		{"tag, header with another algorithm is not checked", "v1", "sha512:abc", true},
		{"digest ref matches", d, "", true},
		{"digest ref, content differs", other, "", false},
		{"digest ref, header lies", d, other, false},
	}
	for _, c := range cases {
		got, b, err := readManifest(respWith(body, c.header), c.ref)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v", c.name, err)
			continue
		}
		if c.ok && (got != d || string(b) != body) {
			t.Errorf("%s: digest %q body %q", c.name, got, b)
		}
	}
}

func TestReadManifestSizeCap(t *testing.T) {
	atMax := strings.Repeat("x", maxManifestSize)
	if _, _, err := readManifest(respWith(atMax, ""), "v1"); err != nil {
		t.Fatalf("a manifest of exactly the maximum size must be accepted: %v", err)
	}
	over := strings.Repeat("x", maxManifestSize+1)
	if _, _, err := readManifest(respWith(over, ""), "v1"); err == nil {
		t.Fatal("an oversize manifest must be refused")
	}
}

func TestUpstreamHostAllowlist(t *testing.T) {
	mk := func(upstream bool, regs ...string) *Proxy {
		p, err := New(nil, nil, &metrics.Metrics{}, Options{Upstream: upstream, UpstreamRegistries: regs})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		name string
		p    *Proxy
		ns   string
		host string
		ok   bool
	}{
		{"disabled", mk(false, "ghcr.io"), "ghcr.io", "", false},
		{"empty allowlist", mk(true), "ghcr.io", "", false},
		{"listed", mk(true, "ghcr.io"), "ghcr.io", "ghcr.io", true},
		{"case-insensitive", mk(true, "GHCR.io"), "ghcr.IO", "ghcr.io", true},
		{"not listed", mk(true, "ghcr.io"), "evil.example", "", false},
		{"docker hub alias", mk(true, "docker.io"), "docker.io", "registry-1.docker.io", true},
		{"internal address", mk(true, "ghcr.io"), "169.254.169.254", "", false},
		{"userinfo trick", mk(true, "ghcr.io"), "ghcr.io@evil.example", "", false},
		{"path trick", mk(true, "ghcr.io"), "evil.example/ghcr.io", "", false},
		{"port", mk(true, "registry.local:5000"), "registry.local:5000", "registry.local:5000", true},
	}
	for _, c := range cases {
		host, ok := c.p.upstreamHost(c.ns)
		if host != c.host || ok != c.ok {
			t.Errorf("%s: upstreamHost(%q) = %q,%v want %q,%v", c.name, c.ns, host, ok, c.host, c.ok)
		}
	}
}

func TestUpstreamDisabledNeverDialsNamespaceHost(t *testing.T) {
	var hit atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Add(1) }))
	defer target.Close()
	a := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{"ghcr.io"}})
	host := strings.TrimPrefix(target.URL, "http://")
	d := digestOf([]byte("x"))
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer secret")
	if resp.StatusCode != 404 || hit.Load() != 0 {
		t.Fatalf("status %d, unlisted registry contacted %d times", resp.StatusCode, hit.Load())
	}
}

// A layer that takes longer than the timeout in total must still arrive as long
// as data keeps flowing.
func TestSlowButSteadyTransferSucceeds(t *testing.T) {
	body := []byte(strings.Repeat("x", 10*1024))
	d := digestOf(body)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		fl := w.(http.Flusher)
		chunk := len(body) / 10
		for i := 0; i < 10; i++ {
			w.Write(body[i*chunk : (i+1)*chunk])
			fl.Flush()
			time.Sleep(60 * time.Millisecond)
		}
	}))
	defer slow.Close()
	a := newNode(t, Options{Timeout: 250 * time.Millisecond})
	a.fin.urls.Store(&[]string{slow.URL})
	start := time.Now()
	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || len(got) != len(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	if time.Since(start) < 400*time.Millisecond {
		t.Fatal("test is not exercising a transfer longer than the timeout")
	}
}

func TestStalledTransferTimesOut(t *testing.T) {
	d := digestOf([]byte("never completes"))
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100") // promises more than it sends
		w.Write([]byte("start"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer stall.Close()
	a := newNode(t, Options{Timeout: 200 * time.Millisecond})
	a.fin.urls.Store(&[]string{stall.URL})
	start := time.Now()
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("stalled peer was not cut off")
	}
}

// One fetch serves every waiting client: it must go ahead even if the client
// that started it disconnects.
func TestFetchSurvivesInitiatorDisconnect(t *testing.T) {
	body := []byte("shared layer")
	d := digestOf(body)
	release := make(chan struct{})
	var calls atomic.Int32
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		w.Write(body)
	}))
	defer peerSrv.Close()
	a := newNode(t, Options{Timeout: 5 * time.Second})
	a.fin.urls.Store(&[]string{peerSrv.URL})

	// First client gives up almost immediately.
	req, _ := http.NewRequest("GET", a.srv.URL+"/v2/lib/app/blobs/"+d, nil)
	c := &http.Client{Timeout: 100 * time.Millisecond}
	if _, err := c.Do(req); err == nil {
		t.Fatal("expected the first client to time out")
	}
	// Second client joins the same flight.
	done := make(chan string, 1)
	go func() {
		_, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
		done <- string(got)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	select {
	case got := <-done:
		if got != string(body) {
			t.Fatalf("second client got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second client never got the blob")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("peer was asked %d times, want 1", n)
	}
}

func TestPeerOrderIsShuffled(t *testing.T) {
	p, _ := New(nil, &swapFinder{}, &metrics.Metrics{}, Options{})
	sf := &swapFinder{}
	var urls []string
	for i := 0; i < 8; i++ {
		urls = append(urls, fmt.Sprintf("http://10.0.0.%d:5000", i))
	}
	sf.urls.Store(&urls)
	p.peers = sf
	first := map[string]bool{}
	for i := 0; i < 200; i++ {
		first[p.shuffledPeers()[0]] = true
	}
	if len(first) < 2 {
		t.Fatal("every miss would hit the same first peer")
	}
}

// A peer that does not say how long its answer is could stream data until the
// disk is full, since the digest is only checked at the end.
func TestPeerWithoutContentLengthIsRejected(t *testing.T) {
	body := []byte("a perfectly good blob")
	d := digestOf(body)
	chunked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
		w.(http.Flusher).Flush() // sending headers early forces chunked encoding
	}))
	defer chunked.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{chunked.URL})
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if a.m.VerifyErrors.Load() != 1 {
		t.Fatalf("VerifyErrors = %d", a.m.VerifyErrors.Load())
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("content of unknown length must not be stored")
	}
}

func TestOversizeManifestFromPeerIsRejected(t *testing.T) {
	big := strings.Repeat("x", maxManifestSize+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(big)))
		io.WriteString(w, big)
	}))
	defer srv.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{srv.URL})
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/manifests/v1")
	if resp.StatusCode != 404 || a.m.VerifyErrors.Load() != 1 {
		t.Fatalf("status %d, VerifyErrors %d", resp.StatusCode, a.m.VerifyErrors.Load())
	}
}

// Registries are allowed to omit Docker-Content-Digest, so the digest of a
// manifest fetched by tag comes from its content.
func TestManifestWithoutDigestHeaderIsAccepted(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(doc) }))
	defer srv.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{srv.URL})
	resp, got := get(t, a.srv.URL+"/v2/lib/app/manifests/v1?ns=reg.example")
	if resp.StatusCode != 200 || string(got) != string(doc) {
		t.Fatalf("status %d body %q", resp.StatusCode, got)
	}
	if d, ok := a.st.Resolve("reg.example", "lib/app", "v1"); !ok || d != digestOf(doc) {
		t.Fatalf("alias = %q %v", d, ok)
	}
}

func TestPeerManifestHeaderMustMatchContent(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"layers":[]}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", digestOf([]byte("something else")))
		w.Write(doc)
	}))
	defer srv.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{srv.URL})
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/manifests/v1")
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if _, ok := a.st.Resolve("", "lib/app", "v1"); ok {
		t.Fatal("no alias may be created for refused content")
	}
}

// Go would decompress a gzip-encoded response by itself, which changes the
// bytes that are hashed. Layers are already compressed and must pass untouched.
func TestPeerRequestsDoNotAskForCompression(t *testing.T) {
	var enc atomic.Value
	enc.Store("unset")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enc.Store(r.Header.Get("Accept-Encoding"))
		http.NotFound(w, r)
	}))
	defer srv.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{srv.URL})
	get(t, a.srv.URL+"/v2/lib/app/blobs/"+digestOf([]byte("x")))
	if v := enc.Load().(string); v != "" {
		t.Fatalf("Accept-Encoding = %q, want none", v)
	}
}

func TestNoSpaceStopsTheFetchWithoutWriting(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("free disk space is only measured on Linux")
	}
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	d := put(t, b, []byte("some layer"))
	a.st.SetMinFree(1 << 60) // more than any disk has
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("blob stored despite the reserve")
	}
	if len(a.p.shuffledPeers()) != 1 {
		t.Fatal("a full disk is not the peer's fault; it must not be skipped")
	}
}

// A peer that never answers must cost one slow request, not one per miss.
func TestHungPeerIsSkippedAfterFirstFailure(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int32
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer hung.Close()
	defer close(release)

	a := newNode(t, Options{Timeout: 30 * time.Second, PeerTimeout: 200 * time.Millisecond, PeerBackoff: time.Minute})
	good := newNode(t, Options{})
	a.fin.urls.Store(&[]string{hung.URL, good.srv.URL})

	start := time.Now()
	for i := 0; i < 6; i++ {
		d := put(t, good, []byte(fmt.Sprintf("layer %d", i)))
		resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status %d", i, resp.StatusCode)
		}
	}
	// The hung peer can be tried at most once (when it happened to come first).
	if n := hits.Load(); n > 1 {
		t.Fatalf("hung peer was asked %d times", n)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("six fetches took %v; a dead peer is stalling every miss", el)
	}
}

func TestPeerErrorStatusBacksOffButNotFoundDoesNot(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "boom", 503) }))
	defer failing.Close()
	missing := httptest.NewServer(http.HandlerFunc(http.NotFound))
	defer missing.Close()
	a := newNode(t, Options{PeerBackoff: time.Minute})
	a.fin.urls.Store(&[]string{failing.URL, missing.URL})
	get(t, a.srv.URL+"/v2/lib/app/blobs/"+digestOf([]byte("x")))
	got := a.p.shuffledPeers()
	if len(got) != 1 || got[0] != missing.URL {
		t.Fatalf("peers after one miss = %v; only the failing one should be skipped", got)
	}
}

func TestBackoffExpires(t *testing.T) {
	a := newNode(t, Options{PeerBackoff: 30 * time.Second})
	a.fin.urls.Store(&[]string{"http://10.0.0.1:5000", "http://10.0.0.2:5000"})
	clock := time.Now()
	a.p.now = func() time.Time { return clock }

	a.p.markDown("http://10.0.0.1:5000")
	if got := a.p.shuffledPeers(); len(got) != 1 || got[0] != "http://10.0.0.2:5000" {
		t.Fatalf("down peer still offered: %v", got)
	}
	clock = clock.Add(29 * time.Second)
	if len(a.p.shuffledPeers()) != 1 {
		t.Fatal("peer came back before its back-off passed")
	}
	clock = clock.Add(2 * time.Second)
	if len(a.p.shuffledPeers()) != 2 {
		t.Fatal("peer was not offered again after its back-off")
	}
}

func TestNoBackoffWhenDisabled(t *testing.T) {
	a := newNode(t, Options{PeerBackoff: 0})
	a.fin.urls.Store(&[]string{"http://10.0.0.1:5000"})
	a.p.markDown("http://10.0.0.1:5000")
	if len(a.p.shuffledPeers()) != 1 {
		t.Fatal("PeerBackoff=0 must disable skipping")
	}
}

// Peers talk mutual TLS. After the certificates and the CA were rotated on disk,
// a running node must go on reaching its peers without a restart.
func TestPeerTrafficSurvivesCertificateRotation(t *testing.T) {
	f := tlstest.NewFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, tlstest.New(t, "one"), t0)
	rl, err := tlsutil.New(f.Cert, f.Key, f.CA, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	rl.SetCheckInterval(0)

	// Peer B serves over mutual TLS.
	bst, _ := store.New(t.TempDir())
	bp, _ := New(bst, &swapFinder{}, &metrics.Metrics{}, Options{})
	bsrv := httptest.NewUnstartedServer(bp)
	bsrv.TLS = rl.ServerConfig()
	bsrv.StartTLS()
	defer bsrv.Close()

	// Node A reaches B with a client certificate.
	ast, _ := store.New(t.TempDir())
	ap, err := New(ast, &swapFinder{}, &metrics.Metrics{}, Options{TLS: rl, ServerName: tlstest.ServerName})
	if err != nil {
		t.Fatal(err)
	}
	urls := []string{bsrv.URL}
	ap.peers = fixed(urls)

	fetch := func(body string) int {
		d := digestOf([]byte(body))
		if _, err := bst.PutVerified(d, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		ap.ServeHTTP(rec, httptest.NewRequest("GET", "/v2/lib/app/blobs/"+d, nil))
		return rec.Code
	}
	if code := fetch("before rotation"); code != 200 {
		t.Fatalf("before rotation: status %d", code)
	}
	// Rotate to a new CA and certificate, as a new issuer would.
	f.Write(t, tlstest.New(t, "two"), t0.Add(time.Minute/2))
	// Drop the kept-alive connection: in production it would end sooner or
	// later, and the next one needs a new handshake with the new material.
	bsrv.CloseClientConnections()
	if code := fetch("after rotation"); code != 200 {
		t.Fatalf("after rotation: status %d (the peer client kept the old certificates)", code)
	}
}

type fixed []string

func (f fixed) Candidates() []string { return append([]string(nil), f...) }
