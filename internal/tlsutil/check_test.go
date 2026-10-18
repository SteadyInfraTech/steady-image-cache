package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil/tlstest"
)

var bothRoles = Server | Client

func snapshotOf(t *testing.T, p pki) Snapshot {
	t.Helper()
	f := newFiles(t)
	f.Write(t, p, time.Now())
	return newReloader(t, f).Current()
}

func problemsOf(t *testing.T, p pki, roles Role) []string {
	t.Helper()
	return snapshotOf(t, p).Problems(serverName, roles, time.Now())
}

func TestCorrectCertificateHasNoProblems(t *testing.T) {
	if got := problemsOf(t, newPKI(t, "good"), bothRoles); len(got) != 0 {
		t.Fatalf("problems: %q", got)
	}
}

// Each mistake is reported, in terms of what to change, and only for the role
// it breaks.
func TestEachCertificateMistakeIsReported(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  tlstest.Opts
		roles Role
		want  string // part of the one expected problem
	}{
		{"no DNS name", tlstest.Opts{NoDNS: true}, Server, `DNS SAN "steady-image-cache"`},
		{"wrong DNS name", tlstest.Opts{DNSNames: []string{"sic.example.com"}}, Server, `DNS SAN "steady-image-cache"`},
		{"no server auth", tlstest.Opts{EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, Server, `"server auth"`},
		{"no client auth", tlstest.Opts{EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, Client, `"client auth"`},
		{"unrelated usage only", tlstest.Opts{EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning}}, Client, `"client auth"`},
		{"no IP SAN for containerd", tlstest.Opts{NoIP: true}, Server, "IP SAN 127.0.0.1"},
		{"expired", tlstest.Opts{NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(-time.Hour)}, Server, "validity period"},
		{"not valid yet", tlstest.Opts{NotBefore: time.Now().Add(time.Hour)}, Client, "validity period"},
		{"expires soon", tlstest.Opts{NotAfter: time.Now().Add(3 * 24 * time.Hour)}, Server, "expires on"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := problemsOf(t, tlstest.NewWith(t, "x", tc.opts), tc.roles)
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("problems = %q, want one mentioning %q", got, tc.want)
			}
		})
	}
}

// A mistake only matters for the role that needs the thing.
func TestMistakesAreOnlyReportedForTheRoleTheyBreak(t *testing.T) {
	serverOnly := tlstest.NewWith(t, "s", tlstest.Opts{EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	if got := problemsOf(t, serverOnly, Server); len(got) != 0 {
		t.Fatalf("a server certificate needs no client auth: %q", got)
	}
	clientOnly := tlstest.NewWith(t, "c", tlstest.Opts{NoDNS: true, NoIP: true, EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if got := problemsOf(t, clientOnly, Client); len(got) != 0 {
		t.Fatalf("a client certificate needs no names: %q", got)
	}
	// ... but both roles together need both.
	if got := problemsOf(t, serverOnly, bothRoles); len(got) != 1 || !strings.Contains(got[0], "client") {
		t.Fatalf("problems = %q", got)
	}
}

// A certificate that is not signed by the configured CA is refused by peers.
func TestCertificateFromAnotherCAIsReported(t *testing.T) {
	a, b := newPKI(t, "a"), newPKI(t, "b")
	mixed := pki{CertPEM: a.CertPEM, KeyPEM: a.KeyPEM, CAPEM: b.CAPEM}
	got := problemsOf(t, mixed, bothRoles)
	if len(got) != 2 || !strings.Contains(got[0], "CA in ca.crt") || !strings.Contains(got[1], "CA in ca.crt") {
		t.Fatalf("problems = %q", got)
	}
}

// Several mistakes at once are all listed, so that one rotation fixes them.
func TestSeveralMistakesAreAllListed(t *testing.T) {
	bad := tlstest.NewWith(t, "x", tlstest.Opts{NoDNS: true, NoIP: true, EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	if got := problemsOf(t, bad, bothRoles); len(got) != 3 {
		t.Fatalf("problems = %q, want the name, the IP and the client usage", got)
	}
}

// The audit speaks up at start-up and after each rotation that brings a problem,
// not on every check; and a reloader that was not asked to audit stays quiet.
func TestAuditLogsAtStartAndAfterRotation(t *testing.T) {
	f := newFiles(t)
	t0 := time.Now().Add(-time.Minute)
	f.Write(t, tlstest.NewWith(t, "bad", tlstest.Opts{NoDNS: true}), t0)
	var got []string
	r, err := New(f.Cert, f.Key, f.CA, func(format string, args ...any) { got = append(got, fmt.Sprintf(format, args...)) })
	if err != nil {
		t.Fatal(err)
	}
	r.SetCheckInterval(0)
	problems := func() int {
		n := 0
		for _, l := range got {
			if strings.Contains(l, "certificate problem") {
				n++
			}
		}
		return n
	}

	r.Audit(serverName, bothRoles)
	if problems() != 1 || !strings.Contains(got[0], `DNS SAN "steady-image-cache"`) {
		t.Fatalf("start-up: %q", got)
	}
	r.Current()
	r.Current()
	if problems() != 1 {
		t.Fatalf("an unchanged certificate was reported again: %q", got)
	}

	f.Write(t, newPKI(t, "good"), t0.Add(30*time.Second))
	r.Current()
	if problems() != 1 {
		t.Fatalf("a good rotation must add no problem line: %q", got)
	}

	f.Write(t, tlstest.NewWith(t, "bad2", tlstest.Opts{EKU: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}), t0.Add(60*time.Second))
	r.Current()
	if problems() != 2 || !strings.Contains(got[len(got)-1], `"client auth"`) {
		t.Fatalf("a bad rotation must be reported: %q", got)
	}

	quiet := newFiles(t)
	quiet.Write(t, tlstest.NewWith(t, "bad3", tlstest.Opts{NoDNS: true}), t0)
	var qlog []string
	if _, err := New(quiet.Cert, quiet.Key, quiet.CA, func(f string, a ...any) { qlog = append(qlog, fmt.Sprintf(f, a...)) }); err != nil {
		t.Fatal(err)
	}
	if len(qlog) != 0 {
		t.Fatalf("no Audit, no problem lines: %q", qlog)
	}
}

// What the README says about names, tested against real handshakes with the
// configs SIC uses.

// serveAs runs a server on 127.0.0.1 with the given certificate and returns what
// a client with the given snapshot gets. dial chooses how the client is set up.
func handshake(t *testing.T, p pki, client func(Snapshot) *tls.Config) error {
	t.Helper()
	f := newFiles(t)
	f.Write(t, p, time.Now().Add(-time.Minute))
	r := newReloader(t, f)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }))
	srv.TLS = r.ServerConfig()
	srv.StartTLS()
	defer srv.Close()
	tr := &http.Transport{TLSClientConfig: client(r.Current())}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get(srv.URL) // https://127.0.0.1:port
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// SIC reaches a peer by IP address but checks the configured name.
func peerClient(s Snapshot) *tls.Config { return s.ClientConfig(serverName) }

// containerd reaches SIC at https://127.0.0.1:5000 and checks that address.
func containerdClient(s Snapshot) *tls.Config {
	return &tls.Config{RootCAs: s.Pool, Certificates: []tls.Certificate{s.Cert}}
}

func TestPeersNeedTheDNSNameAndNotTheIP(t *testing.T) {
	if err := handshake(t, tlstest.NewWith(t, "x", tlstest.Opts{NoIP: true}), peerClient); err != nil {
		t.Fatalf("a peer is reached by IP but checked by name, so no IP SAN is needed: %v", err)
	}
	if err := handshake(t, tlstest.NewWith(t, "x", tlstest.Opts{NoDNS: true}), peerClient); err == nil {
		t.Fatal("a certificate without the server name must be refused by peers")
	}
}

func TestContainerdNeedsTheIPSAN(t *testing.T) {
	if err := handshake(t, newPKI(t, "x"), containerdClient); err != nil {
		t.Fatalf("the correct certificate works for containerd: %v", err)
	}
	err := handshake(t, tlstest.NewWith(t, "x", tlstest.Opts{NoIP: true}), containerdClient)
	if err == nil || !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("without the IP SAN containerd must refuse the certificate, naming the address: %v", err)
	}
	if err := handshake(t, tlstest.NewWith(t, "x", tlstest.Opts{NoDNS: true}), containerdClient); err != nil {
		t.Fatalf("containerd checks the IP, not the DNS name: %v", err)
	}
}

// The audit and the handshakes agree: whatever it calls a problem for peers is
// refused by peers, and what it does not is accepted. (The IP SAN and the
// approaching expiry concern containerd and the future, not peers.)
func TestAuditAgreesWithPeerHandshakes(t *testing.T) {
	both := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	for _, tc := range []struct {
		name   string
		opts   tlstest.Opts
		peerOK bool
	}{
		{"correct", tlstest.Opts{}, true},
		{"no IP SAN", tlstest.Opts{NoIP: true}, true},
		{"no DNS name", tlstest.Opts{NoDNS: true}, false},
		{"wrong DNS name", tlstest.Opts{DNSNames: []string{"other"}}, false},
		{"server auth only", tlstest.Opts{EKU: both[:1]}, false},
		{"client auth only", tlstest.Opts{EKU: both[1:]}, false},
		{"no usages listed (any)", tlstest.Opts{EKU: []x509.ExtKeyUsage{}}, true},
		{"expired", tlstest.Opts{NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(-time.Hour)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := tlstest.NewWith(t, "x", tc.opts)
			hsErr := handshake(t, p, peerClient)
			if (hsErr == nil) != tc.peerOK {
				t.Fatalf("handshake error = %v, want ok=%v", hsErr, tc.peerOK)
			}
			var forPeers []string
			for _, pr := range problemsOf(t, p, bothRoles) {
				if !strings.Contains(pr, "IP SAN") && !strings.Contains(pr, "expires on") {
					forPeers = append(forPeers, pr)
				}
			}
			if (len(forPeers) == 0) != tc.peerOK {
				t.Fatalf("audit says %q but the handshake gave %v", forPeers, hsErr)
			}
		})
	}
}

// Issuers such as cert-manager write the intermediate CA after the certificate
// and give only the root as the CA. That is a working setup, so it must not be
// reported as a certificate "not signed by the CA".
func TestCertificateIssuedByAnIntermediateCAIsAccepted(t *testing.T) {
	p := tlstest.NewWith(t, "chain", tlstest.Opts{Intermediate: true})
	if got := problemsOf(t, p, bothRoles); len(got) != 0 {
		t.Fatalf("problems: %q", got)
	}
	if err := handshake(t, p, peerClient); err != nil {
		t.Fatalf("the chain should work between peers: %v", err)
	}
}

// The README tells operators to give containerd a certificate of its own, with
// only the client auth usage, instead of a copy of the node's key. That has to
// work, and the usage has to matter.
func TestContainerdMayUseItsOwnClientOnlyCertificate(t *testing.T) {
	node := newPKI(t, "node")
	asContainerd := func(eku []x509.ExtKeyUsage) func(Snapshot) *tls.Config {
		return func(s Snapshot) *tls.Config {
			certPEM, keyPEM := node.IssueClient(t, eku)
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			return &tls.Config{RootCAs: s.Pool, Certificates: []tls.Certificate{cert}}
		}
	}
	if err := handshake(t, node, asContainerd(nil)); err != nil {
		t.Fatalf("a client-only certificate from the same CA should be accepted: %v", err)
	}
	if err := handshake(t, node, asContainerd([]x509.ExtKeyUsage{})); err != nil {
		t.Fatalf("a certificate with no usages listed means any, and should be accepted: %v", err)
	}
	if err := handshake(t, node, asContainerd([]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})); err == nil {
		t.Fatal("a certificate that only allows server auth must not be accepted from a client")
	}
}
