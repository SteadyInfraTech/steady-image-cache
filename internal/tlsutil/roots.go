package tlsutil

import (
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// UpstreamRoots returns the certificate pool for connections to upstream
// registries, or nil to use the system default.
//
// Go's own loading of SSL_CERT_FILE adds the default certificate directories
// to it, and the image keeps its public bundle in one of them, so a deployment
// that sets SSL_CERT_FILE to trust only its private CA would still trust every
// public CA. When either variable is set, the pool is built from what they name
// and nothing else, and a setting that yields no certificate is an error rather
// than a silent fallback to a wider trust.
func UpstreamRoots(file, dir string) (*x509.CertPool, error) {
	if file == "" && dir == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	n := 0
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("SSL_CERT_FILE: %w", err)
		}
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("SSL_CERT_FILE %s holds no certificate", file)
		}
		n++
	}
	for _, d := range strings.Split(dir, string(os.PathListSeparator)) {
		if d == "" {
			continue
		}
		entries, err := os.ReadDir(d)
		if err != nil {
			return nil, fmt.Errorf("SSL_CERT_DIR: %w", err)
		}
		for _, e := range entries {
			b, err := os.ReadFile(filepath.Join(d, e.Name())) // follows the links a CA directory is made of
			if err != nil {
				continue // a directory, or a dangling link
			}
			if pool.AppendCertsFromPEM(b) {
				n++
			}
		}
	}
	if n == 0 {
		return nil, errors.New("SSL_CERT_FILE/SSL_CERT_DIR name no certificate")
	}
	return pool, nil
}
