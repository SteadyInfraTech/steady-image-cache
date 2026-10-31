package proxy

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

// twoUpstreams is a proxy with a protected registry (reg) and another upstream
// that has nothing, over verified peers. It holds blob, fetched through the
// protected registry.
func twoUpstreams(t *testing.T, blob []byte) (p *Proxy, host, otherHost string) {
	t.Helper()
	reg := newTokenRegistry(t, blob, testManifest)
	other := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(other.Close)
	host, otherHost = reg.Listener.Addr().String(), other.Listener.Addr().String()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pki := tlstest.New(t, "nodes")
	f := tlstest.NewFiles(t)
	f.Write(t, pki, time.Now().Add(-time.Minute))
	rl, err := tlsutil.New(f.Cert, f.Key, f.CA, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	p, err = New(st, &swapFinder{}, &metrics.Metrics{}, Options{Upstream: true, UpstreamRegistries: []string{host, otherHost}, ProtectedRegistries: []string{host}, TLS: rl, ServerName: tlstest.ServerName})
	if err != nil {
		t.Fatal(err)
	}
	p.upstreamClient = reg.Client()
	return p, host, otherHost
}

// asNode sends a request as a verified peer that protects the registry too.
func asNode(p *Proxy, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	req.Header.Set(PeerHeader, "1")
	req.Header.Set(ProtectsHeader, "1")
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{DNSNames: []string{tlstest.ServerName}}}}
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	return rec
}

// A namespace that is no registry name names nothing to ask, and the blob
// handler serves by digest: it must not be a way round the check.
func TestMarkedBlobIsNotGivenOutUnderAnInvalidNamespace(t *testing.T) {
	blob := []byte("a layer of a private image")
	p, host, _ := twoUpstreams(t, blob)
	d := digestOf(blob)
	if rec := request(p, "GET", "/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("the entitled client: status %d", rec.Code)
	}
	for _, ns := range []string{"a/b", "bad%20name", "x:y:z", "-", "evil.example/"} {
		for _, m := range []string{"GET", "HEAD"} {
			rec := request(p, m, "/v2/lib/app/blobs/"+d+"?ns="+ns)
			if rec.Code != http.StatusBadRequest || rec.Body.String() == string(blob) {
				t.Fatalf("%s under ns=%q: status %d, want 400", m, ns, rec.Code)
			}
		}
	}
}

// A node that is asked for a protected registry's content marks it before it
// goes to fetch it: the copy it makes must not be readable under another
// namespace, whoever supplies the bytes.
func TestNodeRequestUnderProtectedNamespaceIsMarked(t *testing.T) {
	blob := []byte("a layer another node has")
	p, host, _ := twoUpstreams(t, blob)
	d := digestOf(blob)
	if p.store.IsProtected(d) {
		t.Fatal("marked before it was asked for")
	}
	asNode(p, "/v2/lib/app/blobs/"+d+"?ns="+host) // this node has nothing; the answer does not matter
	if !p.store.IsProtected(d) {
		t.Fatal("a node's request under a protected namespace left the digest unmarked")
	}
}

// A tag of an unprotected namespace may lead to a digest that came from a
// protected registry; the tag cannot be told apart from any other until it is
// resolved, and then the digest is not the client's to read on the cache's word.
func TestTagOfAnotherNamespaceDoesNotLeadToProtectedContent(t *testing.T) {
	p, host, otherHost := twoUpstreams(t, []byte("x"))
	d := digestOf(testManifest)
	if rec := request(p, "GET", "/v2/lib/app/manifests/v1?ns="+host, "Authorization", "Bearer tok"); rec.Code != 200 {
		t.Fatalf("the entitled client: status %d", rec.Code)
	}
	if !p.store.IsProtected(d) {
		t.Fatal("setup: the manifest should be marked")
	}
	// A cached tag of the other namespace that resolves to it (fresh, so that
	// it would be answered from the cache).
	if err := p.store.SetAlias(otherHost, "lib/app", "v1", d); err != nil {
		t.Fatal(err)
	}
	tag := "/v2/lib/app/manifests/v1?ns=" + otherHost
	if rec := request(p, "GET", tag); rec.Code == 200 || bytes.Contains(rec.Body.Bytes(), testManifest) {
		t.Fatalf("a client the registry does not know: status %d", rec.Code)
	}
	if rec := asNode(p, tag); rec.Code != http.StatusForbidden || bytes.Contains(rec.Body.Bytes(), testManifest) {
		t.Fatalf("a node: status %d, want 403", rec.Code)
	}
}

// Admission looked at the digest before the download was chosen, and downloads
// are shared by digest. A download that was marked in between is not the
// admitted client's to read.
func TestDownloadMarkedAfterAdmissionIsNotJoinedUnchecked(t *testing.T) {
	blob := []byte("a layer of a private image")
	p, _, otherHost := twoUpstreams(t, blob)
	d := digestOf(blob)

	dl := newDownload("")
	p.dlmu.Lock()
	p.downloads[d] = dl
	p.dlmu.Unlock()
	// The entitled client's request has marked the digest and started this
	// download; the request below was admitted before that.
	if err := p.store.MarkProtected(d); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder)
	go func() {
		req := httptest.NewRequest("GET", "/v2/lib/app/blobs/"+d+"?ns="+otherHost, nil)
		rec := httptest.NewRecorder()
		p.blob(rec, req, "lib/app", d)
		done <- rec
	}()
	time.Sleep(100 * time.Millisecond) // let it join
	if _, err := p.store.PutVerified(d, bytes.NewReader(blob)); err != nil {
		t.Fatal(err)
	}
	dl.finish(fetched)
	rec := <-done
	if rec.Code == 200 || bytes.Equal(rec.Body.Bytes(), blob) {
		t.Fatalf("status %d: the client got a protected blob it was never confirmed for", rec.Code)
	}
}
