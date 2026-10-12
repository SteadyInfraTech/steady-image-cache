package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/SteadyInfraTech/steady-image-cache/internal/flight"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Proxy struct {
	store    *store.Store
	peers    peer.Finder
	client   *http.Client
	metrics  *metrics.Metrics
	upstream bool
	flights  flight.Group
}

func New(s *store.Store, p peer.Finder, t time.Duration, m *metrics.Metrics, upstream bool, ca, cert, key, serverName string) (*Proxy, error) {
	tr := &http.Transport{}
	if ca != "" {
		b, e := os.ReadFile(ca)
		if e != nil {
			return nil, e
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("invalid peer CA")
		}
		tc := &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
		if cert != "" && key != "" {
			c, e := tls.LoadX509KeyPair(cert, key)
			if e != nil {
				return nil, e
			}
			tc.Certificates = []tls.Certificate{c}
		}
		tr.TLSClientConfig = tc
	}
	return &Proxy{store: s, peers: p, client: &http.Client{Timeout: t, Transport: tr}, metrics: m, upstream: upstream}, nil
}
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(200)
		return
	}
	if repo, d, ok := blobPath(r.URL.Path); ok && (r.Method == "GET" || r.Method == "HEAD") {
		p.blob(w, r, repo, d)
		return
	}
	if repo, ref, ok := manifestPath(r.URL.Path); ok && (r.Method == "GET" || r.Method == "HEAD") {
		p.manifest(w, r, repo, ref)
		return
	}
	http.Error(w, "endpoint not implemented", 404)
}
func splitPath(path, marker string) (string, string, bool) {
	if !strings.HasPrefix(path, "/v2/") {
		return "", "", false
	}
	x := strings.TrimPrefix(path, "/v2/")
	i := strings.LastIndex(x, marker)
	if i < 1 {
		return "", "", false
	}
	return x[:i], x[i+len(marker):], true
}
func blobPath(path string) (string, string, bool)     { return splitPath(path, "/blobs/") }
func manifestPath(path string) (string, string, bool) { return splitPath(path, "/manifests/") }
func ns(r *http.Request) string {
	if v := r.URL.Query().Get("ns"); v != "" {
		return v
	}
	return r.Host
}
func (p *Proxy) serveDigest(w http.ResponseWriter, r *http.Request, d, contentType string) bool {
	f, e := p.store.Open(d)
	if e != nil {
		return false
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return false
	}
	w.Header().Set("Docker-Content-Digest", d)
	w.Header().Set("Accept-Ranges", "bytes")
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	// ServeContent implements GET/HEAD, RFC 7233 byte ranges, If-Modified-Since and 416 responses.
	http.ServeContent(w, r, "", st.ModTime(), f)
	return true
}
func (p *Proxy) blob(w http.ResponseWriter, r *http.Request, repo, d string) {
	if p.serveDigest(w, r, d, "application/octet-stream") {
		p.metrics.LocalHits.Add(1)
		return
	}
	path := "/v2/" + repo + "/blobs/" + url.PathEscape(d)
	ok := p.flights.Do("blob:"+d, func() bool {
		if _, err := p.store.Stat(d); err == nil {
			return true
		}
		if p.fetchPeers(r, path, d) {
			p.metrics.PeerHits.Add(1)
			return true
		}
		if p.upstream && p.fetchUpstream(r, path, d) {
			p.metrics.UpstreamHits.Add(1)
			return true
		}
		return false
	})
	if ok && p.serveDigest(w, r, d, "application/octet-stream") {
		return
	}
	p.metrics.Misses.Add(1)
	http.Error(w, "blob unavailable", 404)
}
func (p *Proxy) manifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	namespace := ns(r)
	if d, ok := p.store.Resolve(namespace, repo, ref); ok && p.serveDigest(w, r, d, "application/vnd.oci.image.manifest.v1+json") {
		p.metrics.LocalHits.Add(1)
		return
	}
	path := "/v2/" + repo + "/manifests/" + url.PathEscape(ref)
	var d, ct string
	ok := p.flights.Do("manifest:"+namespace+"/"+repo+"@"+ref, func() bool {
		if cached, found := p.store.Resolve(namespace, repo, ref); found {
			d = cached
			return true
		}
		var found bool
		d, ct, found = p.fetchManifestPeers(r, path)
		if !found && p.upstream {
			d, ct, found = p.fetchManifestUpstream(r, path)
		}
		if found {
			_ = p.store.SetAlias(namespace, repo, ref, d)
		}
		return found
	})
	if ok {
		if d == "" {
			d, _ = p.store.Resolve(namespace, repo, ref)
		}
		if ct == "" {
			ct = "application/vnd.oci.image.manifest.v1+json"
		}
		if p.serveDigest(w, r, d, ct) {
			return
		}
	}
	p.metrics.Misses.Add(1)
	http.Error(w, "manifest unavailable", 404)
}
func (p *Proxy) peerURL(base, path string, r *http.Request) string {
	u := strings.TrimRight(base, "/") + path
	q := url.Values{}
	if n := ns(r); n != "" {
		q.Set("ns", n)
	}
	return u + "?" + q.Encode()
}
func (p *Proxy) fetchPeers(r *http.Request, path, d string) bool {
	for _, base := range p.peers.Candidates() {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", p.peerURL(base, path, r), nil)
		resp, e := p.client.Do(req)
		if e != nil {
			p.metrics.PeerErrors.Add(1)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		_, e = p.store.PutVerified(d, resp.Body)
		resp.Body.Close()
		if e == nil {
			return true
		}
		p.metrics.VerifyErrors.Add(1)
	}
	return false
}
func (p *Proxy) fetchManifestPeers(r *http.Request, path string) (string, string, bool) {
	for _, base := range p.peers.Candidates() {
		req, _ := http.NewRequestWithContext(r.Context(), "GET", p.peerURL(base, path, r), nil)
		req.Header.Set("Accept", r.Header.Get("Accept"))
		resp, e := p.client.Do(req)
		if e != nil {
			p.metrics.PeerErrors.Add(1)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		d := resp.Header.Get("Docker-Content-Digest")
		if d == "" {
			resp.Body.Close()
			continue
		}
		_, e = p.store.PutVerified(d, resp.Body)
		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()
		if e == nil {
			p.metrics.PeerHits.Add(1)
			return d, ct, true
		}
		p.metrics.VerifyErrors.Add(1)
	}
	return "", "", false
}
func (p *Proxy) upstreamURL(r *http.Request, path string) string { return "https://" + ns(r) + path }
func (p *Proxy) fetchUpstream(r *http.Request, path, d string) bool {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", p.upstreamURL(r, path), nil)
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	resp, e := p.client.Do(req)
	if e != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false
	}
	_, e = p.store.PutVerified(d, resp.Body)
	return e == nil
}
func (p *Proxy) fetchManifestUpstream(r *http.Request, path string) (string, string, bool) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", p.upstreamURL(r, path), nil)
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	req.Header.Set("Accept", r.Header.Get("Accept"))
	resp, e := p.client.Do(req)
	if e != nil {
		return "", "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", false
	}
	d := resp.Header.Get("Docker-Content-Digest")
	if d == "" {
		return "", "", false
	}
	_, e = p.store.PutVerified(d, resp.Body)
	if e != nil {
		return "", "", false
	}
	p.metrics.UpstreamHits.Add(1)
	return d, resp.Header.Get("Content-Type"), true
}
