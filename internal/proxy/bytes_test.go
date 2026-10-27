package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Traffic is counted in bytes by where it came from and where it went, so the
// registry traffic a cluster saves can be measured: bytes sent to clients
// against bytes fetched from registries.
func TestBytesFollowABlobFromAPeerToAClient(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 5000))
	n := uint64(len(body))
	a := newNode(t, Options{})
	d := put(t, a, body)
	b := newNode(t, Options{})
	b.setPeers(a)

	if resp, got := get(t, b.srv.URL+"/v2/lib/app/blobs/"+d); resp.StatusCode != 200 || len(got) != len(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	waitCount(t, &a.m.BytesToPeers, n)
	waitCount(t, &b.m.BytesFromPeers, n)
	waitCount(t, &b.m.BytesToClients, n)
	if a.m.BytesToClients.Load() != 0 || b.m.BytesToPeers.Load() != 0 || b.m.BytesFromUpstream.Load() != 0 {
		t.Fatalf("bytes counted in the wrong place: a to clients %d, b to peers %d, b from upstream %d",
			a.m.BytesToClients.Load(), b.m.BytesToPeers.Load(), b.m.BytesFromUpstream.Load())
	}

	// The second time it is B's own cache: more bytes to the client, none fetched.
	get(t, b.srv.URL+"/v2/lib/app/blobs/"+d)
	waitCount(t, &b.m.BytesToClients, 2*n)
	if b.m.BytesFromPeers.Load() != n {
		t.Fatalf("bytes from peers = %d after a local hit, want %d", b.m.BytesFromPeers.Load(), n)
	}
}

func TestBytesOfARangeRequestAreOnlyTheRange(t *testing.T) {
	a := newNode(t, Options{})
	d := put(t, a, []byte(strings.Repeat("x", 4000)))
	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d, "Range", "bytes=0-99")
	if resp.StatusCode != http.StatusPartialContent || len(got) != 100 {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	waitCount(t, &a.m.BytesToClients, 100)
}

// Content that is refused cost the traffic anyway, and is not stored or served
// in full.
func TestBytesOfRefusedContentAreStillCounted(t *testing.T) {
	d := digestOf([]byte("the real content"))
	lying := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.Write([]byte(strings.Repeat("X", 4096)))
	}))
	defer lying.Close()
	b := newNode(t, Options{})
	b.fin.urls.Store(&[]string{lying.URL})
	getFailing(t, b.srv.URL+"/v2/lib/app/blobs/"+d)
	waitCount(t, &b.m.BytesFromPeers, 4096)
	if _, err := b.st.Stat(d); err == nil {
		t.Fatal("refused content was stored")
	}
	// A client that was already waiting got what had arrived, all but the last
	// byte, which is held back until the digest is checked; it never gets the
	// whole of content that fails.
	if got := b.m.BytesToClients.Load(); got >= 4096 {
		t.Fatalf("%d bytes counted as sent to clients: the whole of refused content", got)
	}
}

func TestBytesFromTheRegistryAreCounted(t *testing.T) {
	body := []byte(strings.Repeat("registry", 4000))
	d := digestOf(body)
	reg := newSlowRegistry(t, body)
	reg.release()
	a := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{reg.host}})
	a.p.upstreamClient = reg.Client()
	resp, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+reg.host)
	if resp.StatusCode != 200 || len(got) != len(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	waitCount(t, &a.m.BytesFromUpstream, uint64(len(body)))
	waitCount(t, &a.m.BytesToClients, uint64(len(body)))
	if a.m.BytesFromPeers.Load() != 0 {
		t.Fatalf("bytes from peers = %d", a.m.BytesFromPeers.Load())
	}
}
