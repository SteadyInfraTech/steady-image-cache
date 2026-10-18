// Package tlsutil loads TLS material from files and picks up changes without a
// restart. Kubernetes Secrets are rotated in place (by cert-manager, say), so a
// certificate read once at startup eventually expires under a running process.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"sync"
	"time"
)

// Snapshot is one consistent set of certificate, key and CA. Gen starts at 1
// and increases every time new files were loaded, so callers can tell when to
// rebuild anything derived from it.
type Snapshot struct {
	Cert tls.Certificate
	Pool *x509.CertPool
	Gen  uint64
}

// ClientConfig returns a TLS config that presents this certificate and verifies
// the server against the CA. serverName is checked instead of the dialled
// address, because peers are reached by IP.
func (s Snapshot) ClientConfig(serverName string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      s.Pool,
		ServerName:   serverName,
		Certificates: []tls.Certificate{s.Cert},
	}
}

type stamp struct{ mod, size int64 }

// Reloader serves the current Snapshot and reloads the files when they change.
type Reloader struct {
	certFile, keyFile, caFile string
	interval                  time.Duration
	logf                      func(string, ...any)
	now                       func() time.Time

	mu      sync.Mutex
	cur     Snapshot
	stamps  [3]stamp
	checked time.Time
	audit   func(Snapshot) // set by Audit; called with each newly loaded set
}

// New loads the files once and fails if they are unusable. logf may be nil.
func New(certFile, keyFile, caFile string, logf func(string, ...any)) (*Reloader, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	r := &Reloader{certFile: certFile, keyFile: keyFile, caFile: caFile, interval: 10 * time.Second, logf: logf, now: time.Now}
	snap, st, err := r.load()
	if err != nil {
		return nil, err
	}
	snap.Gen = 1
	r.cur, r.stamps, r.checked = snap, st, r.now()
	return r, nil
}

func (r *Reloader) statAll() ([3]stamp, error) {
	var out [3]stamp
	for i, f := range []string{r.certFile, r.keyFile, r.caFile} {
		fi, err := os.Stat(f) // follows symlinks, as Secret volumes use them
		if err != nil {
			return out, err
		}
		out[i] = stamp{fi.ModTime().UnixNano(), fi.Size()}
	}
	return out, nil
}

// load reads all three files. The files are stat'ed first so that a change that
// lands while they are being read is noticed by the next check.
func (r *Reloader) load() (Snapshot, [3]stamp, error) {
	st, err := r.statAll()
	if err != nil {
		return Snapshot{}, st, err
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return Snapshot{}, st, err
	}
	pem, err := os.ReadFile(r.caFile)
	if err != nil {
		return Snapshot{}, st, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return Snapshot{}, st, errors.New("no valid certificates in " + r.caFile)
	}
	return Snapshot{Cert: cert, Pool: pool}, st, nil
}

// SetCheckInterval changes how often the files are checked for changes (10s by
// default). Zero checks on every call.
func (r *Reloader) SetCheckInterval(d time.Duration) {
	r.mu.Lock()
	r.interval = d
	r.mu.Unlock()
}

// Current returns the newest valid material. The files are checked at most once
// per interval. If new files are broken (a half-finished rotation, say) the
// previous material is kept and the problem is logged once.
func (r *Reloader) Current() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Sub(r.checked) < r.interval {
		return r.cur
	}
	r.checked = now
	st, err := r.statAll()
	if err != nil {
		r.logf("tls: cannot check certificate files, keeping the loaded ones: %v", err)
		return r.cur
	}
	if st == r.stamps {
		return r.cur
	}
	snap, st, err := r.load()
	r.stamps = st // do not retry (or log) until the files change again
	if err != nil {
		r.logf("tls: reload failed, keeping the previous certificates: %v", err)
		return r.cur
	}
	snap.Gen = r.cur.Gen + 1
	r.cur = snap
	r.logf("tls: reloaded certificates (generation %d)", snap.Gen)
	if r.audit != nil {
		r.audit(snap)
	}
	return r.cur
}

// ServerConfig returns a config for a server that requires and verifies client
// certificates. Each connection gets the material that is current at that
// moment.
func (r *Reloader) ServerConfig() *tls.Config {
	conn := func() *tls.Config {
		s := r.Current()
		return &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{s.Cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    s.Pool,
			NextProtos:   []string{"h2", "http/1.1"},
		}
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		// net/http insists on some certificate source being configured.
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			s := r.Current()
			return &s.Cert, nil
		},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return conn(), nil },
	}
}
