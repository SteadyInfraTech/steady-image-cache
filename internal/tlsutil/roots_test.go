package tlsutil

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

// server is an HTTPS server with a certificate from a CA of its own; ca is that
// CA's certificate.
func server(t *testing.T) (srv *httptest.Server, ca []byte) {
	t.Helper()
	pki := tlstest.New(t, t.Name())
	cert, err := tls.X509KeyPair(pki.CertPEM, pki.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	srv = httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, pki.CAPEM
}

func reaches(t *testing.T, srv *httptest.Server, file, dir string) error {
	t.Helper()
	pool, err := UpstreamRoots(file, dir)
	if err != nil {
		t.Fatal(err)
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	resp, err := c.Get(srv.URL)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

// A bundle named by SSL_CERT_FILE is the whole of the trust: a server whose
// certificate is not in it is refused, public CAs or not.
func TestUpstreamRootsAreExclusive(t *testing.T) {
	a, caA := server(t)
	b, caB := server(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "bundle.crt")
	if err := os.WriteFile(file, caA, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reaches(t, a, file, ""); err != nil {
		t.Fatalf("the server in the bundle: %v", err)
	}
	if err := reaches(t, b, file, ""); err == nil {
		t.Fatal("a server that is not in the bundle was trusted")
	}
	// A directory works the same way, and adds to the file.
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "b.pem"), caB, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reaches(t, b, "", d); err != nil {
		t.Fatalf("the server in the directory: %v", err)
	}
	if err := reaches(t, a, "", d); err == nil {
		t.Fatal("a server that is not in the directory was trusted")
	}
	if err := reaches(t, a, file, d); err != nil {
		t.Fatalf("file and directory together: %v", err)
	}
}

func TestUpstreamRootsFailClosed(t *testing.T) {
	if pool, err := UpstreamRoots("", ""); pool != nil || err != nil {
		t.Fatalf("nothing set means the system default: %v %v", pool, err)
	}
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.crt")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][2]string{
		"missing file": {filepath.Join(dir, "none.crt"), ""},
		"no cert":      {empty, ""},
		"missing dir":  {"", filepath.Join(dir, "none")},
		"dir without":  {"", dir},
	} {
		if pool, err := UpstreamRoots(args[0], args[1]); err == nil || pool != nil {
			t.Errorf("%s: pool %v, err %v", name, pool, err)
		}
	}
}
