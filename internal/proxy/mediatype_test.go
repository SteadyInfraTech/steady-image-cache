package proxy

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

// A stored JSON document is served with a media type of the registry formats or
// not at all: its own "mediaType" is the publisher's word, and an image config
// asked for by its digest is as good a document as any.
func TestManifestMediaTypeIsNeverAPage(t *testing.T) {
	for _, mt := range []string{
		"text/html", "text/html; charset=utf-8", "application/xhtml+xml", "image/svg+xml",
		"application/javascript", "application/vnd.oci.image.manifest.v1+json; x=y",
		"application/vnd.docker.x", "TEXT/HTML", "application/vnd.oci.evil",
	} {
		body := `{"mediaType":"` + mt + `","x":"<script>alert(1)</script>"}`
		n := newNode(t, Options{})
		d := digestOf([]byte(body))
		if _, err := n.st.PutVerified(d, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		resp, got := get(t, n.srv.URL+"/v2/lib/app/manifests/"+d)
		if resp.StatusCode != 200 || string(got) != body {
			t.Fatalf("%q: status %d", mt, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != defaultManifestType {
			t.Errorf("%q was served as %q", mt, ct)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%q: no nosniff", mt)
		}
		if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'none'") {
			t.Errorf("%q: no content security policy", mt)
		}
	}
}

func TestRegistryMediaTypesAreServedAsTheyAre(t *testing.T) {
	for _, mt := range []string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.docker.distribution.manifest.v1+prettyjws",
	} {
		body := `{"schemaVersion":2,"mediaType":"` + mt + `"}`
		n := newNode(t, Options{})
		d := digestOf([]byte(body))
		if _, err := n.st.PutVerified(d, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		resp, _ := get(t, n.srv.URL+"/v2/lib/app/manifests/"+d)
		if ct := resp.Header.Get("Content-Type"); ct != mt {
			t.Errorf("%q was served as %q", mt, ct)
		}
	}
}

// With mutual TLS on, a peer that is only reachable over plain HTTP is not asked:
// nothing authenticates the answer.
func TestPlainHTTPPeersAreNotAskedWhenTLSIsOn(t *testing.T) {
	n := newNode(t, Options{})
	n.fin.urls.Store(&[]string{"http://10.0.0.1:5000", "https://10.0.0.2:5000"})
	if got := n.p.livePeers(); len(got) != 2 {
		t.Fatalf("without TLS both are peers: %v", got)
	}
	n.p.tls = &tlsutil.Reloader{}
	got := n.p.livePeers()
	if len(got) != 1 || got[0] != "https://10.0.0.2:5000" {
		t.Fatalf("with TLS only the https peer is asked: %v", got)
	}
}

// SSL_CERT_FILE makes the upstream client trust that bundle and nothing else.
func TestUpstreamClientTrustsOnlyWhatSSLCertFileNames(t *testing.T) {
	secured := func(name string) (*httptest.Server, []byte) {
		pki := tlstest.New(t, name)
		cert, err := tls.X509KeyPair(pki.CertPEM, pki.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewUnstartedServer(http.NotFoundHandler())
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		srv.StartTLS()
		t.Cleanup(srv.Close)
		return srv, pki.CAPEM
	}
	a, caA := secured("a")
	b, _ := secured("b")
	file := filepath.Join(t.TempDir(), "roots.crt")
	if err := os.WriteFile(file, caA, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", file)
	n := newNode(t, Options{Upstream: true})
	if resp, err := n.p.upstreamClient.Get(a.URL); err != nil {
		t.Fatalf("a server in the bundle: %v", err)
	} else {
		resp.Body.Close()
	}
	if resp, err := n.p.upstreamClient.Get(b.URL); err == nil {
		resp.Body.Close()
		t.Fatal("a server outside the bundle was trusted")
	}
}

func TestStartupFailsOnAnUnusableSSLCertFile(t *testing.T) {
	t.Setenv("SSL_CERT_FILE", filepath.Join(t.TempDir(), "missing.crt"))
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(st, &swapFinder{}, &metrics.Metrics{}, Options{}); err == nil {
		t.Fatal("started with a trust setting that names nothing")
	}
}
