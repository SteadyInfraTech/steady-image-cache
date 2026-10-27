package proxy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
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

// Manifests count like blobs: what a miss reads from a peer or a registry.
func TestBytesOfAManifestFromAPeerAreCounted(t *testing.T) {
	a := newNode(t, Options{})
	d := put(t, a, testManifest)
	b := newNode(t, Options{})
	b.setPeers(a)
	resp, got := get(t, b.srv.URL+"/v2/lib/app/manifests/"+d)
	if resp.StatusCode != 200 || string(got) != string(testManifest) {
		t.Fatalf("status %d, %q", resp.StatusCode, got)
	}
	waitCount(t, &b.m.BytesFromPeers, uint64(len(testManifest)))
	waitCount(t, &a.m.BytesToPeers, uint64(len(testManifest)))
	waitCount(t, &b.m.BytesToClients, uint64(len(testManifest)))
}

func TestBytesOfAManifestFromTheRegistryAreCounted(t *testing.T) {
	reg := newTokenRegistry(t, []byte("unused"), testManifest)
	host := reg.Listener.Addr().String()
	a := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{host}})
	a.p.upstreamClient = reg.Client()
	resp, _ := get(t, a.srv.URL+"/v2/lib/app/manifests/"+digestOf(testManifest)+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	waitCount(t, &a.m.BytesFromUpstream, uint64(len(testManifest)))
}

// The cache's file is sent by the response writer's own copy (sendfile on a
// real connection), so the counting wrapper must not hide it.
type readerFromRecorder struct {
	*httptest.ResponseRecorder
	readFrom bool
}

func (r *readerFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	r.readFrom = true
	return io.Copy(struct{ io.Writer }{r.ResponseRecorder}, src)
}

func TestCountingWriterKeepsTheFastCopyPath(t *testing.T) {
	var n atomic.Uint64
	rec := &readerFromRecorder{ResponseRecorder: httptest.NewRecorder()}
	cw := &countingWriter{ResponseWriter: rec, n: &n}
	if _, ok := any(cw).(io.ReaderFrom); !ok {
		t.Fatal("the wrapper hides ReadFrom")
	}
	if m, err := io.CopyN(cw, strings.NewReader(strings.Repeat("z", 1000)), 700); err != nil || m != 700 {
		t.Fatalf("copied %d, %v", m, err)
	}
	if !rec.readFrom {
		t.Fatal("the copy did not go through the writer's own ReadFrom")
	}
	if n.Load() != 700 {
		t.Fatalf("counted %d bytes, want 700", n.Load())
	}
	// A writer without ReadFrom still gets the bytes, and they are counted.
	plain := httptest.NewRecorder()
	var n2 atomic.Uint64
	cw2 := &countingWriter{ResponseWriter: plain, n: &n2}
	if _, err := cw2.ReadFrom(strings.NewReader("abcde")); err != nil || n2.Load() != 5 || plain.Body.String() != "abcde" {
		t.Fatalf("fallback: %v, %d, %q", err, n2.Load(), plain.Body.String())
	}
}

// failingWriter accepts some of a write and then fails, as a connection does
// when the client goes away in the middle of a response.
type failingWriter struct {
	*httptest.ResponseRecorder
	accept int
}

func (f *failingWriter) Write(b []byte) (int, error) {
	n := f.accept
	if n > len(b) {
		n = len(b)
	}
	return n, errors.New("broken pipe")
}

func TestBytesAcceptedBeforeAWriteErrorAreCounted(t *testing.T) {
	body := []byte(strings.Repeat("abcdefghij", 1000))
	f := t.TempDir() + "/part"
	if err := os.WriteFile(f, body, 0o644); err != nil {
		t.Fatal(err)
	}
	a := newNode(t, Options{})
	dl := newDownload("")
	dl.Begin(f, int64(len(body)))
	dl.Wrote(int64(len(body)))
	req := httptest.NewRequest("GET", "/v2/lib/app/blobs/"+digestOf(body), nil)
	a.p.stream(&failingWriter{ResponseRecorder: httptest.NewRecorder(), accept: 7}, req, digestOf(body), dl, false, false)
	if got := a.m.BytesToClients.Load(); got != 7 {
		t.Fatalf("counted %d bytes, want the 7 that were accepted", got)
	}
}
