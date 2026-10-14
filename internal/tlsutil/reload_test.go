package tlsutil

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

const serverName = tlstest.ServerName

type pki = tlstest.PKI
type files = tlstest.Files

func newPKI(t *testing.T, cn string) pki { return tlstest.New(t, cn) }
func newFiles(t *testing.T) files        { return tlstest.NewFiles(t) }

func newReloader(t *testing.T, f files) *Reloader {
	t.Helper()
	r, err := New(f.Cert, f.Key, f.CA, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	r.SetCheckInterval(0) // check on every call
	return r
}

func TestNewFailsOnBadFiles(t *testing.T) {
	f := newFiles(t)
	if _, err := New(f.Cert, f.Key, f.CA, nil); err == nil {
		t.Fatal("missing files must be an error")
	}
	p := newPKI(t, "a")
	f.Write(t, p, time.Now())
	os.WriteFile(f.CA, []byte("not a certificate"), 0600)
	if _, err := New(f.Cert, f.Key, f.CA, nil); err == nil || !strings.Contains(err.Error(), "no valid certificates") {
		t.Fatalf("garbage CA must be an error, got %v", err)
	}
}

func TestCurrentReloadsOnChangeOnly(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, newPKI(t, "one"), t0)
	r := newReloader(t, f)

	s1 := r.Current()
	if s1.Gen != 1 {
		t.Fatalf("gen = %d", s1.Gen)
	}
	if s := r.Current(); s.Gen != 1 {
		t.Fatalf("unchanged files must not bump the generation, got %d", s.Gen)
	}
	f.Write(t, newPKI(t, "two"), t0.Add(30*time.Second))
	s2 := r.Current()
	if s2.Gen != 2 {
		t.Fatalf("gen = %d after rotation", s2.Gen)
	}
	if string(s2.Cert.Certificate[0]) == string(s1.Cert.Certificate[0]) {
		t.Fatal("certificate did not change")
	}
}

func TestBrokenRotationKeepsPreviousMaterial(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, newPKI(t, "one"), t0)
	r := newReloader(t, f)
	good := r.Current()

	// New certificate arrived but the matching key has not yet.
	other := newPKI(t, "two")
	os.WriteFile(f.Cert, other.CertPEM, 0600)
	os.Chtimes(f.Cert, t0.Add(time.Second), t0.Add(time.Second))
	if s := r.Current(); s.Gen != good.Gen || string(s.Cert.Certificate[0]) != string(good.Cert.Certificate[0]) {
		t.Fatal("a mismatched pair must not replace working material")
	}
	// Once the key lands the pair is valid and is picked up.
	os.WriteFile(f.Key, other.KeyPEM, 0600)
	os.Chtimes(f.Key, t0.Add(2*time.Second), t0.Add(2*time.Second))
	os.WriteFile(f.CA, other.CAPEM, 0600)
	os.Chtimes(f.CA, t0.Add(2*time.Second), t0.Add(2*time.Second))
	if s := r.Current(); s.Gen != good.Gen+1 {
		t.Fatalf("completed rotation not picked up, gen = %d", s.Gen)
	}
}

func TestChecksAreRateLimited(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, newPKI(t, "one"), t0)
	r, err := New(f.Cert, f.Key, f.CA, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Now()
	r.now = func() time.Time { return clock }
	r.checked = clock
	f.Write(t, newPKI(t, "two"), t0.Add(time.Second))
	if r.Current().Gen != 1 {
		t.Fatal("files were checked before the interval elapsed")
	}
	clock = clock.Add(r.interval + time.Second)
	if r.Current().Gen != 2 {
		t.Fatal("files were not checked after the interval elapsed")
	}
}

// mtls starts a server that requires client certificates, using reloaders for
// its material, and returns a function that makes a request with the given
// client config.
func mtls(t *testing.T, server *Reloader) (get func(*Snapshot) (int, error)) {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	srv.TLS = server.ServerConfig()
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return func(client *Snapshot) (int, error) {
		cfg := client.ClientConfig(serverName)
		tr := &http.Transport{TLSClientConfig: cfg}
		defer tr.CloseIdleConnections()
		resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get(srv.URL)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		return resp.StatusCode, nil
	}
}

func TestMutualTLSIsEnforced(t *testing.T) {
	f := newFiles(t)
	p := newPKI(t, "one")
	f.Write(t, p, time.Now().Add(-time.Minute))
	server := newReloader(t, f)
	get := mtls(t, server)

	good := server.Current()
	if code, err := get(&good); err != nil || code != 200 {
		t.Fatalf("valid client: %d %v", code, err)
	}

	// A client with a certificate from a different CA is refused.
	rogue := newFiles(t)
	rogue.Write(t, newPKI(t, "rogue"), time.Now().Add(-time.Minute))
	rs := newReloader(t, rogue).Current()
	rs.Pool = good.Pool // trusts the server, but is not trusted by it
	if _, err := get(&rs); err == nil {
		t.Fatal("a client certificate from an unknown CA must be rejected")
	}

	// A client without any certificate is refused.
	noCert := good
	noCert.Cert.Certificate = nil
	tr := &http.Transport{TLSClientConfig: noCert.ClientConfig(serverName)}
	tr.TLSClientConfig.Certificates = nil
	defer tr.CloseIdleConnections()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.TLS = server.ServerConfig()
	srv.StartTLS()
	defer srv.Close()
	if resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get(srv.URL); err == nil {
		resp.Body.Close()
		t.Fatal("a client without a certificate must be rejected")
	}
}

// Rotating the certificate and CA on disk must take effect for new connections
// of a running server, without a restart.
func TestServerPicksUpRotation(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, newPKI(t, "one"), t0)
	server := newReloader(t, f)
	get := mtls(t, server)

	oldClient := server.Current()
	if code, err := get(&oldClient); err != nil || code != 200 {
		t.Fatalf("before rotation: %d %v", code, err)
	}

	// Rotate to an entirely new CA and certificate, as a new cluster issuer would.
	f.Write(t, newPKI(t, "two"), t0.Add(30*time.Second))

	// The old material no longer works against the server...
	if _, err := get(&oldClient); err == nil {
		t.Fatal("the server is still using the old certificates")
	}
	// ...and the new material does.
	newClient := server.Current()
	if newClient.Gen != 2 {
		t.Fatalf("gen = %d", newClient.Gen)
	}
	if code, err := get(&newClient); err != nil || code != 200 {
		t.Fatalf("after rotation: %d %v", code, err)
	}
}

func TestConcurrentUse(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, newPKI(t, "one"), t0)
	r := newReloader(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = r.Current()
				_ = r.ServerConfig()
			}
		}()
	}
	f.Write(t, newPKI(t, "two"), t0.Add(time.Second))
	wg.Wait()
}
