package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
	p    *Proxy
	st   *store.Store
	root string
	m    *metrics.Metrics
	fin  *swapFinder
	srv  *httptest.Server
}

// newNode starts a node. Its peers are filled in later with setPeers because
// nodes need each other's URLs.
func newNode(t *testing.T, o Options) *node {
	t.Helper()
	root := t.TempDir()
	st, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	n := &node{st: st, m: m, fin: &swapFinder{}, root: root}
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

// A full disk is this node's problem: the client must hear "insufficient
// storage", not "not found", the operator must see why, and the peer that
// would have served the blob must not be blamed.
func TestFullDiskIsReportedAs507(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("free disk space is only measured on Linux")
	}
	sink := &logSink{}
	a, b := newNode(t, Options{Logf: sink.logf, PeerBackoff: time.Minute}), newNode(t, Options{})
	a.setPeers(b)
	d := put(t, b, []byte("some layer"))
	a.st.SetMinFree(1 << 60) // more than any disk has
	resp, body := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != http.StatusInsufficientStorage {
		t.Fatalf("status %d, want 507", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Insufficient Storage") {
		t.Fatalf("body %q", body)
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("blob stored despite the reserve")
	}
	if a.m.DiskFull.Load() != 1 || a.m.Misses.Load() != 0 || a.m.PeerErrors.Load() != 0 {
		t.Fatalf("DiskFull=%d Misses=%d PeerErrors=%d", a.m.DiskFull.Load(), a.m.Misses.Load(), a.m.PeerErrors.Load())
	}
	if !strings.Contains(sink.all(), "SIC_MIN_FREE_BYTES") {
		t.Fatalf("the operator was not told why; log: %q", sink.all())
	}
	if len(a.p.shuffledPeers()) != 1 {
		t.Fatal("a full disk is not the peer's fault; it must not be skipped")
	}
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logSink) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// breakCache replaces the blob directory with a file, so that every write to
// the cache fails. (File permissions would not do: tests may run as root.)
func breakCache(t *testing.T, dir string) {
	t.Helper()
	blobs := filepath.Join(dir, "blobs", "sha256")
	if err := os.RemoveAll(blobs); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobs, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestUnwritableCacheIs500AndNotThePeersFault(t *testing.T) {
	good := newNode(t, Options{})
	blob := put(t, good, []byte("a layer"))
	doc := []byte(`{"schemaVersion":2,"layers":[]}`)
	man := put(t, good, doc)

	dir := t.TempDir()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	breakCache(t, dir)
	sink := &logSink{}
	m := &metrics.Metrics{}
	fin := &swapFinder{}
	fin.urls.Store(&[]string{good.srv.URL})
	p, err := New(st, fin, m, Options{Logf: sink.logf, PeerBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v2/lib/app/blobs/" + blob, "/v2/lib/app/manifests/" + man} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 500 {
			t.Errorf("%s: status %d, want 500", path, rec.Code)
		}
	}
	if m.StoreErrors.Load() == 0 || m.PeerErrors.Load() != 0 || m.Misses.Load() != 0 {
		t.Errorf("StoreErrors=%d PeerErrors=%d Misses=%d", m.StoreErrors.Load(), m.PeerErrors.Load(), m.Misses.Load())
	}
	if len(p.shuffledPeers()) != 1 {
		t.Error("a peer was skipped for a failure of this node's own disk")
	}
	if !strings.Contains(sink.all(), "cannot write to the cache directory") {
		t.Errorf("log: %q", sink.all())
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

	a.p.markDown("http://10.0.0.1:5000", errors.New("boom"))
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
	a.p.markDown("http://10.0.0.1:5000", errors.New("boom"))
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

func TestValidRepo(t *testing.T) {
	for _, ok := range []string{
		"alpine", "library/alpine", "team/api", "a/b/c/d", "My_App.v2", "Lib/App", "a--b", "a__b", "a.b", "x1/y2",
	} {
		if !validRepo(ok) {
			t.Errorf("validRepo(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{
		"", "a b", "a\nb", "a\x7fb", "a\x00b", "a%b", "a?b", "a#b", "a\\b", "/a", "a/", "a//b", ".", "..", "a/../b", "a/./b",
		"-a", "a-", "_a", "a_", "a..b", "a._b", "a___b", "é", strings.Repeat("a", 256),
	} {
		if validRepo(bad) {
			t.Errorf("validRepo(%q) = true, want false", bad)
		}
	}
}

// One odd request used to take every peer out of rotation: building the peer
// URL failed, and that was counted as the peer failing.
func TestMalformedRequestsDoNotTakePeersOffline(t *testing.T) {
	a, good := newNode(t, Options{PeerBackoff: time.Minute}), newNode(t, Options{})
	a.setPeers(good)
	d := digestOf([]byte("x"))
	for _, repo := range []string{"a%0Ab", "a%7Fb", "a%20b", "a%00b", "a%09b", "a%2F..%2Fb", "a%3Fb"} {
		rec := httptest.NewRecorder()
		a.p.ServeHTTP(rec, httptest.NewRequest("GET", "/v2/"+repo+"/blobs/"+d, nil))
		if rec.Code != 400 {
			t.Errorf("%s: status %d, want 400", repo, rec.Code)
		}
	}
	if len(a.p.shuffledPeers()) != 1 || a.m.PeerErrors.Load() != 0 {
		t.Fatalf("peers offered: %d, PeerErrors: %d", len(a.p.shuffledPeers()), a.m.PeerErrors.Load())
	}
	// A legitimate mixed-case name still goes through to the peers.
	rec := httptest.NewRecorder()
	a.p.ServeHTTP(rec, httptest.NewRequest("GET", "/v2/Team/My_App.v2/blobs/"+d, nil))
	if rec.Code != 404 {
		t.Fatalf("mixed-case repository: status %d, want 404 (not found, not invalid)", rec.Code)
	}
}

// A peer address that cannot be turned into a request is a configuration
// problem: it is logged, and it does not count as the peer failing.
func TestUnbuildablePeerURLIsLoggedNotBlamedOnThePeer(t *testing.T) {
	sink := &logSink{}
	a := newNode(t, Options{PeerBackoff: time.Minute, Logf: sink.logf})
	a.fin.urls.Store(&[]string{"http://bad host:5000"})
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/blobs/"+digestOf([]byte("x")))
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if len(a.p.shuffledPeers()) != 1 || a.m.PeerErrors.Load() != 0 {
		t.Fatalf("peers offered: %d, PeerErrors: %d", len(a.p.shuffledPeers()), a.m.PeerErrors.Load())
	}
	if !strings.Contains(sink.all(), "cannot build a request") {
		t.Fatalf("log: %q", sink.all())
	}
}

func TestWarningsAreRateLimited(t *testing.T) {
	sink := &logSink{}
	a := newNode(t, Options{Logf: sink.logf})
	clock := time.Now()
	a.p.now = func() time.Time { return clock }

	a.p.warn("k", "problem %d", 1)
	a.p.warn("k", "problem %d", 2)
	a.p.warn("k", "problem %d", 3)
	a.p.warn("other", "different problem")
	if n := len(sink.lines); n != 2 {
		t.Fatalf("%d lines, want 2 (one per key): %q", n, sink.all())
	}
	clock = clock.Add(59 * time.Second)
	a.p.warn("k", "problem %d", 4)
	if len(sink.lines) != 2 {
		t.Fatal("logged again before a minute had passed")
	}
	clock = clock.Add(2 * time.Second)
	a.p.warn("k", "problem %d", 5)
	last := sink.lines[len(sink.lines)-1]
	if !strings.Contains(last, "problem 5") || !strings.Contains(last, "3 similar messages suppressed") {
		t.Fatalf("last line %q", last)
	}
}

func TestWarnWithoutALoggerIsHarmless(t *testing.T) {
	a := newNode(t, Options{})
	a.p.warn("k", "nobody is listening")
}

// Refused content is logged with the peer's address, so that a misbehaving
// node can be found.
func TestRefusedContentNamesThePeerInTheLog(t *testing.T) {
	sink := &logSink{}
	lying := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("not it")) }))
	defer lying.Close()
	a := newNode(t, Options{Logf: sink.logf})
	a.fin.urls.Store(&[]string{lying.URL})
	get(t, a.srv.URL+"/v2/lib/app/blobs/"+digestOf([]byte("the real one")))
	log := sink.all()
	if !strings.Contains(log, lying.URL) || !strings.Contains(log, "digest mismatch") {
		t.Fatalf("log: %q", log)
	}
}

// upstreamProxy builds a proxy that may contact the given TLS test server as a
// registry, trusting its certificate.
func upstreamProxy(t *testing.T, srv *httptest.Server, sink *logSink) (*Proxy, *metrics.Metrics, string) {
	t.Helper()
	host := srv.Listener.Addr().String() // 127.0.0.1:port, a valid registry name
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	p, err := New(st, &swapFinder{}, m, Options{Upstream: true, UpstreamRegistries: []string{host}, Logf: sink.logf})
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = srv.Client()
	return p, m, host
}

func serve(p *Proxy, path string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

// The upstream path had no end-to-end test: this one covers a blob and a
// manifest fetched from a registry, and that the caller's credentials are
// passed on.
func TestUpstreamFetchEndToEnd(t *testing.T) {
	blob := []byte("a layer from the registry")
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	var auth atomic.Value
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/blobs/"+digestOf(blob)):
			w.Write(blob)
		case strings.HasSuffix(r.URL.Path, "/manifests/v1"):
			w.Header().Set("Docker-Content-Digest", digestOf(doc))
			w.Write(doc)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	sink := &logSink{}
	p, m, host := upstreamProxy(t, srv, sink)

	rec := serve(p, "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host, "Authorization", "Bearer s3cret")
	if rec.Code != 200 || rec.Body.String() != string(blob) {
		t.Fatalf("blob: status %d body %q", rec.Code, rec.Body.String())
	}
	if auth.Load() != "Bearer s3cret" {
		t.Fatalf("upstream saw Authorization %q", auth.Load())
	}
	rec = serve(p, "/v2/lib/app/manifests/v1?ns="+host)
	if rec.Code != 200 || rec.Body.String() != string(doc) {
		t.Fatalf("manifest: status %d body %q", rec.Code, rec.Body.String())
	}
	if m.UpstreamHits.Load() != 2 {
		t.Fatalf("UpstreamHits = %d", m.UpstreamHits.Load())
	}
	if sink.all() != "" {
		t.Fatalf("a successful fetch must not log: %q", sink.all())
	}
	// A second request is served from the cache.
	if rec = serve(p, "/v2/lib/app/blobs/"+digestOf(blob)+"?ns="+host); rec.Code != 200 || m.LocalHits.Load() != 1 {
		t.Fatalf("not cached: status %d LocalHits %d", rec.Code, m.LocalHits.Load())
	}
}

// A registry that asks the client to authenticate is told apart from one that
// simply does not have the content: the former is logged, the latter is not.
func TestUpstreamAuthChallengeIsLoggedNotSilent(t *testing.T) {
	status := atomic.Int32{}
	status.Store(401)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status.Load() == 401 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="https://auth.example/token"`)
		}
		http.Error(w, "no", int(status.Load()))
	}))
	defer srv.Close()
	sink := &logSink{}
	p, _, host := upstreamProxy(t, srv, sink)

	for _, path := range []string{"/v2/lib/app/blobs/" + digestOf([]byte("x")), "/v2/lib/app/manifests/v1"} {
		if rec := serve(p, path+"?ns="+host); rec.Code != 404 {
			t.Errorf("%s: status %d", path, rec.Code)
		}
	}
	log := sink.all()
	if !strings.Contains(log, "401") || !strings.Contains(log, "authentication challenges") {
		t.Fatalf("a 401 from upstream went unlogged: %q", log)
	}

	status.Store(404)
	sink2 := &logSink{}
	p2, _, host2 := upstreamProxy(t, srv, sink2)
	if rec := serve(p2, "/v2/lib/app/blobs/"+digestOf([]byte("y"))+"?ns="+host2); rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
	if sink2.all() != "" {
		t.Fatalf("an ordinary not-found must not be logged: %q", sink2.all())
	}
}

func TestManifestRequestsWithDifferentAcceptHeadersDoNotShareFlight(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	seen := make(chan string, 2)
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Accept")
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Docker-Content-Digest", digestOf(doc))
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = w.Write(doc)
	}))
	defer srv.Close()

	p, _, host := upstreamProxy(t, srv, &logSink{})
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()
	url := proxySrv.URL + "/v2/lib/app/manifests/v1?ns=" + host
	accepts := []string{
		"application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	}
	type result struct {
		status int
		body   string
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, len(accepts))
	client := &http.Client{Timeout: 5 * time.Second}
	for _, accept := range accepts {
		accept := accept
		go func() {
			<-start
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			req.Header.Set("Accept", accept)
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			results <- result{status: resp.StatusCode, body: string(body), err: err}
		}()
	}
	close(start)

	got := map[string]bool{}
	for i := 0; i < len(accepts); i++ {
		select {
		case accept := <-seen:
			got[accept] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("upstream did not receive both Accept variants; got %v", got)
		}
	}
	for _, accept := range accepts {
		if !got[accept] {
			t.Errorf("upstream did not receive Accept %q; saw %v", accept, got)
		}
	}
	close(release)
	for range accepts {
		res := <-results
		if res.err != nil || res.status != http.StatusOK || res.body != string(doc) {
			t.Errorf("manifest response = status %d, body %q, err %v", res.status, res.body, res.err)
		}
	}
}

func TestManifestRequestsWithDifferentAuthorizationHeadersDoNotShareFlight(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	seen := make(chan string, 2)
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Authorization")
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Docker-Content-Digest", digestOf(doc))
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = w.Write(doc)
	}))
	defer srv.Close()

	p, _, host := upstreamProxy(t, srv, &logSink{})
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()
	url := proxySrv.URL + "/v2/lib/app/manifests/v1?ns=" + host
	authHeaders := []string{"Bearer credential-one", "Bearer credential-two"}

	type result struct {
		status int
		body   string
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, len(authHeaders))
	client := &http.Client{Timeout: 5 * time.Second}
	for _, auth := range authHeaders {
		auth := auth
		go func() {
			<-start
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			req.Header.Set("Authorization", auth)
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			results <- result{status: resp.StatusCode, body: string(body), err: err}
		}()
	}
	close(start)

	got := map[string]bool{}
	for range authHeaders {
		select {
		case auth := <-seen:
			got[auth] = true
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatalf("upstream did not receive both Authorization variants; got %v", got)
		}
	}
	for _, auth := range authHeaders {
		if !got[auth] {
			close(release)
			t.Fatalf("upstream did not receive Authorization %q; saw %v", auth, got)
		}
	}
	close(release)
	for range authHeaders {
		res := <-results
		if res.err != nil || res.status != http.StatusOK || res.body != string(doc) {
			t.Errorf("manifest response = status %d, body %q, err %v", res.status, res.body, res.err)
		}
	}
}

func TestManifestAliasPersistenceFailureReturnsServerError(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(doc)
	}))
	defer srv.Close()
	host := srv.Listener.Addr().String()
	root := t.TempDir()
	st, err := store.New(root)
	if err != nil {
		t.Fatal(err)
	}
	// Force the atomic aliases.json rename to fail, without relying on
	// permissions (which are ineffective when tests run as root).
	if err := os.Mkdir(filepath.Join(root, "aliases.json"), 0755); err != nil {
		t.Fatal(err)
	}
	p, err := New(st, &swapFinder{}, &metrics.Metrics{}, Options{Upstream: true, UpstreamRegistries: []string{host}})
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = srv.Client()
	req := httptest.NewRequest(http.MethodGet, "/v2/lib/app/manifests/v1?ns="+host, nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500; body %q", rec.Code, rec.Body.String())
	}
	if _, ok := st.Resolve(host, "lib/app", "v1"); ok {
		t.Fatal("manifest alias became visible despite persistence failure")
	}
}

// This end-to-end recovery test protects the integrity boundary.
// A corrupt local hit must not be served or turn into a permanent miss: the
// proxy should discard it, fetch a verified copy from a healthy peer, and
// replace the damaged file before responding.
func TestCorruptedLocalBlobRecoversFromPeer(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)

	body := []byte("healthy layer bytes")
	d := put(t, b, body)
	corruptPath := filepath.Join(a.root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:"))
	if err := os.MkdirAll(filepath.Dir(corruptPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(corruptPath, []byte("corrupted cached bytes"), 0644); err != nil {
		t.Fatal(err)
	}

	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != http.StatusOK || string(got) != string(body) {
		t.Fatalf("status %d body %q, want healthy peer bytes %q", resp.StatusCode, got, body)
	}
	stored, err := os.ReadFile(corruptPath)
	if err != nil || string(stored) != string(body) {
		t.Fatalf("repaired cache contains %q, err %v", stored, err)
	}
	if a.m.PeerHits.Load() != 1 {
		t.Fatalf("PeerHits = %d, want 1 after recovering corrupted cache", a.m.PeerHits.Load())
	}
}

// slowSource serves a blob whose body does not arrive until release is closed.
func slowSource(t *testing.T, body []byte) (url string, headersSent <-chan struct{}, release func()) {
	t.Helper()
	sent := make(chan struct{})
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(sent)
		<-gate
		w.Write(body)
	}))
	var once sync.Once
	rel := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { rel(); srv.Close() })
	return srv.URL, sent, rel
}

// A node that is still downloading a layer answers a peer's request for it
// straight away ("not here yet"). It used to keep the peer waiting until the
// download was done, so the peer hit its timeout and skipped a healthy node.
func TestPeerAskingForABlobThatIsStillArrivingIsNotSkipped(t *testing.T) {
	body := []byte(strings.Repeat("a big layer ", 100))
	d := digestOf(body)
	src, headersSent, release := slowSource(t, body)

	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})
	b := newNode(t, Options{PeerTimeout: 500 * time.Millisecond, PeerBackoff: time.Minute})
	b.setPeers(a)

	done := make(chan struct{})
	go func() { get(t, a.srv.URL+"/v2/lib/app/blobs/"+d); close(done) }()
	<-headersSent
	time.Sleep(200 * time.Millisecond) // let A start storing the body

	start := time.Now()
	resp, _ := get(t, b.srv.URL+"/v2/lib/app/blobs/"+d)
	if took := time.Since(start); took > 400*time.Millisecond {
		t.Fatalf("B waited %v for A; A should have answered immediately", took)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("status %d, want 404 (B has no other source)", resp.StatusCode)
	}
	if b.m.PeerErrors.Load() != 0 || len(b.p.shuffledPeers()) != 1 {
		t.Fatalf("A was marked down: PeerErrors=%d, peers left=%d", b.m.PeerErrors.Load(), len(b.p.shuffledPeers()))
	}

	release()
	<-done
	resp, got := get(t, b.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("once A has the blob, B must get it: status %d", resp.StatusCode)
	}
}
