package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A cached blob that cannot be read, as opposed to one that is missing, is a
// fault of this node. It is answered as a miss but must be counted and logged.
func TestUnreadableCachedBlobIsCountedAndLogged(t *testing.T) {
	sink := &logSink{}
	n := newNode(t, Options{Logf: sink.logf})
	d := digestOf([]byte("a layer"))
	// A directory where the blob should be: it opens, but cannot be read.
	if err := os.MkdirAll(filepath.Join(n.root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:")), 0o755); err != nil {
		t.Fatal(err)
	}

	resp, _ := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	if n.m.StoreErrors.Load() == 0 {
		t.Fatal("an unreadable cache entry was not counted in StoreErrors")
	}
	if !strings.Contains(sink.all(), "cannot read "+d) {
		t.Fatalf("no warning about the unreadable entry; log:\n%s", sink.all())
	}
}

// An ordinary miss must stay quiet.
func TestMissingCachedBlobIsNotAStoreError(t *testing.T) {
	n := newNode(t, Options{})
	get(t, n.srv.URL+"/v2/lib/app/blobs/"+digestOf([]byte("never stored")))
	if got := n.m.StoreErrors.Load(); got != 0 {
		t.Fatalf("StoreErrors = %d for a plain miss", got)
	}
}

// The cause of a request that could not be built must stay reachable.
func TestBuildRequestErrorKeepsItsCause(t *testing.T) {
	n := newNode(t, Options{})
	_, err := n.p.do(context.Background(), http.MethodGet, http.DefaultClient, "http://[::1", nil)
	if !errors.Is(err, errBuildRequest) {
		t.Fatalf("error %v is not errBuildRequest", err)
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Fatalf("error %v lost its *url.Error cause", err)
	}
}

// A registry that sends a manifest one byte at a time never goes idle, so only
// the overall bound stops it.
func TestManifestFetchHasAnOverallDeadline(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		for r.Context().Err() == nil {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer srv.Close()
	p, _, host := upstreamProxy(t, srv, &logSink{})
	p.manifestTimeout = 300 * time.Millisecond

	start := time.Now()
	rec := serve(p, "/v2/lib/app/manifests/"+digestOf([]byte("x"))+"?ns="+host)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("request took %s, the deadline did not apply", took)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
}

// A client that joined a manifest fetch under way leaves when it goes away; the
// fetch carries on for the client that started it.
func TestManifestWaiterLeavesWhenClientGoesAway(t *testing.T) {
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	gate := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-gate
		w.Write(doc)
	}))
	var release sync.Once
	open := func() { release.Do(func() { close(gate) }) }
	t.Cleanup(func() { open(); srv.Close() })
	p, _, host := upstreamProxy(t, srv, &logSink{})
	path := "/v2/lib/app/manifests/" + digestOf(doc) + "?ns=" + host

	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- serve(p, path) }()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan struct{})
	go func() {
		defer close(second)
		req := httptest.NewRequest("GET", path, nil).WithContext(ctx)
		p.ServeHTTP(httptest.NewRecorder(), req)
	}()
	time.Sleep(50 * time.Millisecond) // let it join
	cancel()
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting request did not return after its client went away")
	}

	open()
	if rec := <-first; rec.Code != 200 || rec.Body.String() != string(doc) {
		t.Fatalf("first request: status %d body %q", rec.Code, rec.Body.String())
	}
}
