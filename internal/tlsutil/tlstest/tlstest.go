// Package tlstest generates throwaway certificates for tests.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ServerName is the DNS name every generated certificate is valid for.
const ServerName = "steady-image-cache"

// PKI is a CA and one certificate signed by it that is valid for both server
// and client use, on 127.0.0.1 and for ServerName.
type PKI struct {
	CAPEM, CertPEM, KeyPEM []byte

	ca    *x509.Certificate // what signed the certificate, for IssueClient
	caKey *ecdsa.PrivateKey
}

// Opts changes what a generated certificate is valid for, so that tests can
// make the kind of certificate an operator might get wrong. The zero value is
// the correct one.
type Opts struct {
	// DNSNames replaces ServerName as the certificate's DNS names; NoDNS leaves
	// it with none.
	DNSNames []string
	NoDNS    bool
	// NoIP leaves out the 127.0.0.1 IP address.
	NoIP bool
	// EKU replaces the extended key usages (server and client authentication).
	// A non-nil empty list leaves the certificate with none, which means "any".
	EKU []x509.ExtKeyUsage
	// Intermediate has the certificate issued by an intermediate CA instead of
	// by the root. CertPEM then holds the certificate and the intermediate, as
	// issuers like cert-manager write it, and CAPEM only the root.
	Intermediate bool
	// NotBefore and NotAfter replace the validity period (an hour ago, in a year).
	NotBefore, NotAfter time.Time
}

// New creates a fresh CA and certificate. cn only distinguishes them.
func New(t testing.TB, cn string) PKI { return NewWith(t, cn, Opts{}) }

// NewWith is New with a certificate that may differ from the correct one.
func NewWith(t testing.TB, cn string, o Opts) PKI {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn + " CA"},
		NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(2 * 365 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	dns := o.DNSNames
	if dns == nil && !o.NoDNS {
		dns = []string{ServerName}
	}
	if o.NoDNS {
		dns = nil
	}
	var ips []net.IP
	if !o.NoIP {
		ips = []net.IP{net.ParseIP("127.0.0.1")}
	}
	eku := o.EKU
	if eku == nil {
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	}
	notBefore, notAfter := o.NotBefore, o.NotAfter
	if notBefore.IsZero() {
		notBefore = time.Now().Add(-time.Hour)
	}
	if notAfter.IsZero() {
		notAfter = time.Now().Add(365 * 24 * time.Hour)
	}

	issuer, issuerKey := caCert, caKey
	var chain []byte
	if o.Intermediate {
		iKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		iTpl := &x509.Certificate{
			SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: cn + " intermediate CA"},
			NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		}
		iDER, err := x509.CreateCertificate(rand.Reader, iTpl, caCert, &iKey.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		issuer, _ = x509.ParseCertificate(iDER)
		issuerKey = iKey
		chain = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: iDER})
	}

	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: cn},
		NotBefore: notBefore, NotAfter: notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: eku,
		DNSNames:    dns, IPAddresses: ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, issuer, &key.PublicKey, issuerKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return PKI{
		ca: caCert, caKey: caKey,
		CAPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		CertPEM: append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), chain...),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

// Files are the three paths a node reads its TLS material from.
type Files struct{ Dir, Cert, Key, CA string }

func NewFiles(t testing.TB) Files {
	d := t.TempDir()
	return Files{d, filepath.Join(d, "tls.crt"), filepath.Join(d, "tls.key"), filepath.Join(d, "ca.crt")}
}

// Write replaces the files and sets their modification time to at, as a real
// rotation changes it.
func (f Files) Write(t testing.TB, p PKI, at time.Time) {
	t.Helper()
	for path, b := range map[string][]byte{f.Cert: p.CertPEM, f.Key: p.KeyPEM, f.CA: p.CAPEM} {
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
}

// IssueClient issues another certificate from the same CA, for a caller that is
// only a client, as containerd is: no names, and the given extended key usages
// (nil: client authentication only; a non-nil empty list: none, meaning any).
func (p PKI) IssueClient(t testing.TB, eku []x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	if eku == nil {
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(4), Subject: pkix.Name{CommonName: "containerd"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(365 * 24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: eku,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.ca, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
