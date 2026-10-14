package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/flight"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

const (
	// PeerHeader marks a request that comes from another SIC instance. Such
	// requests are answered from the local cache only, which stops two nodes
	// that both miss from asking each other (and waiting on themselves).
	PeerHeader = "X-SIC-Peer"

	defaultManifestType = "application/vnd.oci.image.manifest.v1+json"
	maxManifestSize     = 4 << 20
)

var (
	tagRE      = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$`)
	registryRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[0-9]{1,5})?$`)
)

// Options configures a Proxy.
type Options struct {
	// Timeout bounds connection setup and the time a peer or upstream may stay
	// silent. It is not a limit on the total transfer time, so large layers on
	// slow links still complete.
	Timeout time.Duration
	// Upstream enables fetching from the registry named in ?ns=, but only for
	// registries listed in UpstreamRegistries.
	Upstream           bool
	UpstreamRegistries []string
	// TLS settings for SIC-to-SIC traffic.
	CA, Cert, Key, ServerName string
}

type Proxy struct {
	store          *store.Store
	peers          peer.Finder
	peerClient     *http.Client
	upstreamClient *http.Client
	metrics        *metrics.Metrics
	upstream       bool
	registries     map[string]bool
	idle           time.Duration
	flights        flight.Group
}

func New(s *store.Store, p peer.Finder, m *metrics.Metrics, o Options) (*Proxy, error) {
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	peerTr := newTransport(o.Timeout)
	if o.CA != "" {
		b, e := os.ReadFile(o.CA)
		if e != nil {
			return nil, e
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("invalid peer CA")
		}
		tc := &tls.Config{RootCAs: pool, ServerName: o.ServerName, MinVersion: tls.VersionTLS12}
		if o.Cert != "" && o.Key != "" {
			c, e := tls.LoadX509KeyPair(o.Cert, o.Key)
			if e != nil {
				return nil, e
			}
			tc.Certificates = []tls.Certificate{c}
		}
		peerTr.TLSClientConfig = tc
	}
	regs := map[string]bool{}
	for _, r := range o.UpstreamRegistries {
		if r = strings.ToLower(strings.TrimSpace(r)); r != "" {
			regs[r] = true
		}
	}
	return &Proxy{
		store:      s,
		peers:      p,
		peerClient: &http.Client{Transport: peerTr},
		// Upstream registries are public services: verify them against the
		// system roots, not against the private CA used between SIC nodes.
		upstreamClient: &http.Client{Transport: newTransport(o.Timeout)},
		metrics:        m,
		upstream:       o.Upstream,
		registries:     regs,
		idle:           o.Timeout,
	}, nil
}

func newTransport(t time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: t, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   t,
		ResponseHeaderTimeout: t,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   4,
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2/" || r.URL.Path == "/v2" {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		w.WriteHeader(200)
		return
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if repo, d, ok := blobPath(r.URL.Path); ok {
			p.blob(w, r, repo, d)
			return
		}
		if repo, ref, ok := manifestPath(r.URL.Path); ok {
			p.manifest(w, r, repo, ref)
			return
		}
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

func isPeer(r *http.Request) bool { return r.Header.Get(PeerHeader) != "" }

// validRepo accepts slash-separated names and rejects anything that could
// escape the /v2/<repo>/... path when it is forwarded.
func validRepo(repo string) bool {
	if repo == "" || len(repo) > 255 || strings.ContainsAny(repo, "\\?#%\x00") {
		return false
	}
	for _, seg := range strings.Split(repo, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func validRef(ref string) bool {
	if strings.HasPrefix(ref, "sha256:") {
		return store.ValidDigest(ref)
	}
	return tagRE.MatchString(ref)
}

// info carries what the fetchers need from the client request. It deliberately
// holds a context that is detached from the client: a single-flight fetch
// serves every waiting client, so it must not be cancelled because the one
// client that happened to start it went away.
type info struct {
	ctx    context.Context
	ns     string
	accept string
	auth   string
}

func newInfo(r *http.Request) info {
	return info{
		ctx:    context.WithoutCancel(r.Context()),
		ns:     ns(r),
		accept: r.Header.Get("Accept"),
		auth:   r.Header.Get("Authorization"),
	}
}

// serveDigest serves a stored object. ctype may inspect the content to choose a
// Content-Type; nil means application/octet-stream.
func (p *Proxy) serveDigest(w http.ResponseWriter, r *http.Request, d string, ctype func(io.ReadSeeker) string) bool {
	f, e := p.store.Open(d)
	if e != nil {
		return false
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return false
	}
	ct := "application/octet-stream"
	if ctype != nil {
		ct = ctype(f)
		if _, e := f.Seek(0, io.SeekStart); e != nil {
			return false
		}
	}
	w.Header().Set("Docker-Content-Digest", d)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", ct)
	// ServeContent implements GET/HEAD, RFC 7233 byte ranges, If-Modified-Since and 416 responses.
	http.ServeContent(w, r, "", st.ModTime(), f)
	return true
}

// manifestType reports the media type of a stored manifest. It uses the
// document's own mediaType and falls back to telling an index from a manifest
// by its shape, because mediaType is optional in OCI documents.
func manifestType(f io.ReadSeeker) string {
	b, err := io.ReadAll(io.LimitReader(f, maxManifestSize))
	if err != nil {
		return defaultManifestType
	}
	var doc struct {
		MediaType string          `json:"mediaType"`
		Manifests json.RawMessage `json:"manifests"`
		Layers    json.RawMessage `json:"layers"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return defaultManifestType
	}
	switch {
	case doc.MediaType != "":
		return doc.MediaType
	case len(doc.Manifests) > 0:
		return "application/vnd.oci.image.index.v1+json"
	default:
		return defaultManifestType
	}
}

func (p *Proxy) blob(w http.ResponseWriter, r *http.Request, repo, d string) {
	if !validRepo(repo) || !store.ValidDigest(d) {
		http.Error(w, "invalid blob reference", 400)
		return
	}
	if p.serveDigest(w, r, d, nil) {
		p.metrics.LocalHits.Add(1)
		return
	}
	if isPeer(r) {
		http.Error(w, "blob unavailable", 404)
		return
	}
	in := newInfo(r)
	path := "/v2/" + repo + "/blobs/" + d
	ok := p.flights.Do("blob:"+d, func() bool {
		if _, err := p.store.Stat(d); err == nil {
			return true
		}
		if p.fetchPeers(in, path, d) {
			p.metrics.PeerHits.Add(1)
			return true
		}
		if p.fetchUpstream(in, path, d) {
			p.metrics.UpstreamHits.Add(1)
			return true
		}
		return false
	})
	if ok && p.serveDigest(w, r, d, nil) {
		return
	}
	p.metrics.Misses.Add(1)
	http.Error(w, "blob unavailable", 404)
}

func (p *Proxy) manifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	if !validRepo(repo) || !validRef(ref) {
		http.Error(w, "invalid manifest reference", 400)
		return
	}
	namespace := ns(r)
	if d, ok := p.store.Resolve(namespace, repo, ref); ok && p.serveDigest(w, r, d, manifestType) {
		p.metrics.LocalHits.Add(1)
		return
	}
	if isPeer(r) {
		http.Error(w, "manifest unavailable", 404)
		return
	}
	in := newInfo(r)
	byDigest := strings.HasPrefix(ref, "sha256:")
	path := "/v2/" + repo + "/manifests/" + ref
	ok := p.flights.Do("manifest:"+namespace+"/"+repo+"@"+ref, func() bool {
		if d, found := p.store.Resolve(namespace, repo, ref); found {
			if _, err := p.store.Stat(d); err == nil {
				return true
			}
		}
		d, found := p.fetchManifestPeers(in, path, ref)
		if found {
			p.metrics.PeerHits.Add(1)
		} else if d, found = p.fetchManifestUpstream(in, path, ref); found {
			p.metrics.UpstreamHits.Add(1)
		}
		if found && !byDigest {
			_ = p.store.SetAlias(namespace, repo, ref, d)
		}
		return found
	})
	if ok {
		if d, found := p.store.Resolve(namespace, repo, ref); found && p.serveDigest(w, r, d, manifestType) {
			return
		}
	}
	p.metrics.Misses.Add(1)
	http.Error(w, "manifest unavailable", 404)
}

func (p *Proxy) peerURL(base, path, namespace string) string {
	u := strings.TrimRight(base, "/") + path
	if namespace == "" {
		return u
	}
	return u + "?" + url.Values{"ns": {namespace}}.Encode()
}

// shuffledPeers spreads misses over the cluster instead of sending every node to
// the same first peer.
func (p *Proxy) shuffledPeers() []string {
	c := p.peers.Candidates()
	rand.Shuffle(len(c), func(i, j int) { c[i], c[j] = c[j], c[i] })
	return c
}

// idleBody cancels the request when no data has arrived for d. It replaces a
// total-transfer timeout, which would abort any layer that is merely big.
type idleBody struct {
	io.ReadCloser
	t      *time.Timer
	d      time.Duration
	cancel context.CancelFunc
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.t.Reset(b.d)
	}
	return n, err
}

func (b *idleBody) Close() error {
	b.t.Stop()
	b.cancel()
	return b.ReadCloser.Close()
}

// get performs a GET whose body must be read within the idle limit.
func (p *Proxy) get(ctx context.Context, c *http.Client, u string, hdr map[string]string) (*http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	t := time.AfterFunc(p.idle, cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Stop()
		cancel()
		return nil, err
	}
	for k, v := range hdr {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Stop()
		cancel()
		return nil, err
	}
	t.Reset(p.idle)
	resp.Body = &idleBody{ReadCloser: resp.Body, t: t, d: p.idle, cancel: cancel}
	return resp, nil
}

func (p *Proxy) fetchPeers(in info, path, d string) bool {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerClient, p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1"})
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

// fetchManifestPeers returns the digest of the manifest it stored. For a digest
// reference the peer must answer with exactly that digest.
func (p *Proxy) fetchManifestPeers(in info, path, ref string) (string, bool) {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerClient, p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1", "Accept": in.accept})
		if e != nil {
			p.metrics.PeerErrors.Add(1)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		d, ok := manifestDigest(resp.Header.Get("Docker-Content-Digest"), ref)
		if !ok {
			resp.Body.Close()
			continue
		}
		_, e = p.store.PutVerified(d, resp.Body)
		resp.Body.Close()
		if e == nil {
			return d, true
		}
		p.metrics.VerifyErrors.Add(1)
	}
	return "", false
}

// manifestDigest picks the digest a fetched manifest must hash to. A digest
// reference is authoritative and a mismatching header is rejected; for a tag the
// registry's Docker-Content-Digest header names the content.
func manifestDigest(header, ref string) (string, bool) {
	if strings.HasPrefix(ref, "sha256:") {
		if header != "" && header != ref {
			return "", false
		}
		return ref, true
	}
	if !store.ValidDigest(header) {
		return "", false
	}
	return header, true
}

// upstreamHost returns the registry host to contact for a namespace, or false if
// upstream access is disabled or the registry is not on the allowlist. The
// namespace comes from the client request, so without the allowlist any client
// could make SIC connect to arbitrary hosts.
func (p *Proxy) upstreamHost(namespace string) (string, bool) {
	if !p.upstream {
		return "", false
	}
	namespace = strings.ToLower(namespace)
	if !registryRE.MatchString(namespace) || !p.registries[namespace] {
		return "", false
	}
	if namespace == "docker.io" {
		return "registry-1.docker.io", true
	}
	return namespace, true
}

func (p *Proxy) fetchUpstream(in info, path, d string) bool {
	host, ok := p.upstreamHost(in.ns)
	if !ok {
		return false
	}
	resp, e := p.get(in.ctx, p.upstreamClient, "https://"+host+path, map[string]string{"Authorization": in.auth})
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

func (p *Proxy) fetchManifestUpstream(in info, path, ref string) (string, bool) {
	host, ok := p.upstreamHost(in.ns)
	if !ok {
		return "", false
	}
	resp, e := p.get(in.ctx, p.upstreamClient, "https://"+host+path, map[string]string{"Authorization": in.auth, "Accept": in.accept})
	if e != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", false
	}
	d, ok := manifestDigest(resp.Header.Get("Docker-Content-Digest"), ref)
	if !ok {
		return "", false
	}
	if _, e = p.store.PutVerified(d, resp.Body); e != nil {
		p.metrics.VerifyErrors.Add(1)
		return "", false
	}
	return d, true
}
