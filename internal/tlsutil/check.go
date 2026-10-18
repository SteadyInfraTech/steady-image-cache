package tlsutil

import (
	"crypto/x509"
	"errors"
	"fmt"
	"time"
)

// Role says what a certificate is used for on a node.
type Role int

const (
	// Server: the listener presents it, to peers (who reach the node by IP and
	// check the configured server name) and to containerd (which reaches it at
	// 127.0.0.1).
	Server Role = 1 << iota
	// Client: SIC presents it to peers when it asks them for content.
	Client
)

// expiresSoon is how far ahead an approaching expiry is reported.
const expiresSoon = 14 * 24 * time.Hour

// Problems lists what is wrong with the certificate for the roles it has, in
// terms an operator can act on. Each of these makes connections fail (peers
// are skipped, containerd falls back or cannot pull), usually with a handshake
// error on the other side that says little about the cause. An empty result
// means none was found.
func (s Snapshot) Problems(serverName string, roles Role, now time.Time) []string {
	if len(s.Cert.Certificate) == 0 {
		return []string{"the certificate file holds no certificate"}
	}
	leaf, err := x509.ParseCertificate(s.Cert.Certificate[0])
	if err != nil {
		return []string{fmt.Sprintf("cannot parse the certificate: %v", err)}
	}
	inter := x509.NewCertPool()
	for _, der := range s.Cert.Certificate[1:] {
		if c, err := x509.ParseCertificate(der); err == nil {
			inter.AddCert(c)
		}
	}
	verify := func(usage x509.ExtKeyUsage, dns string) error {
		_, err := leaf.Verify(x509.VerifyOptions{
			Roots: s.Pool, Intermediates: inter, CurrentTime: now,
			DNSName: dns, KeyUsages: []x509.ExtKeyUsage{usage},
		})
		return err
	}
	var out []string
	if roles&Server != 0 {
		if err := verify(x509.ExtKeyUsageServerAuth, serverName); err != nil {
			out = append(out, "peers will not accept it as a server certificate: "+explain(err, "server auth", serverName))
		}
		if err := leaf.VerifyHostname("127.0.0.1"); err != nil {
			out = append(out, "it has no IP SAN 127.0.0.1, so containerd, which reaches SIC at https://127.0.0.1:5000, will refuse it "+
				"(ignore this if your hosts.toml names SIC differently)")
		}
	}
	if roles&Client != 0 {
		if err := verify(x509.ExtKeyUsageClientAuth, ""); err != nil {
			out = append(out, "peers will not accept it as a client certificate: "+explain(err, "client auth", ""))
		}
	}
	if left := leaf.NotAfter.Sub(now); left > 0 && left < expiresSoon {
		out = append(out, fmt.Sprintf("it expires on %s (in %s)", leaf.NotAfter.UTC().Format("2006-01-02"), left.Round(time.Hour)))
	}
	return out
}

// explain turns a verification error into the thing to change. usage is the
// extended key usage that was being checked; name the DNS name, if one was.
func explain(err error, usage, name string) string {
	var (
		invalid x509.CertificateInvalidError
		host    x509.HostnameError
		unknown x509.UnknownAuthorityError
	)
	switch {
	case errors.As(err, &invalid) && invalid.Reason == x509.Expired:
		return "it is outside its validity period (expired, or not valid yet)"
	case errors.As(err, &invalid) && invalid.Reason == x509.IncompatibleUsage:
		return fmt.Sprintf("it lacks the %q extended key usage (one certificate used both as server and as client needs \"server auth\" and \"client auth\")", usage)
	case errors.As(err, &host):
		return fmt.Sprintf("it has no DNS SAN %q (the name SIC_TLS_SERVER_NAME checks; peers are reached by IP)", name)
	case errors.As(err, &unknown):
		return "it is not signed by the CA in ca.crt"
	}
	return err.Error()
}

// Audit logs the problems Problems finds in the current certificate, and in
// every certificate loaded later, so that a bad rotation is reported at once
// and not when the first handshake fails. It never stops anything: the
// certificate is still used.
func (r *Reloader) Audit(serverName string, roles Role) {
	r.mu.Lock()
	r.audit = func(s Snapshot) {
		for _, p := range s.Problems(serverName, roles, r.now()) {
			r.logf("tls: certificate problem: %s", p)
		}
	}
	r.audit(r.cur)
	r.mu.Unlock()
}
