package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/flight"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil"
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
	// Timeout bounds connection setup for upstream registries and the time a
	// peer or upstream may stay silent while sending. It is not a limit on the
	// total transfer time, so large layers on slow links still complete.
	Timeout time.Duration
	// PeerTimeout is how long a peer may take to accept a connection and start
	// answering. A peer that exceeds it is skipped for PeerBackoff, so a dead
	// node costs one slow request rather than one per miss. Peers answer from
	// their local cache only, so a healthy one responds immediately.
	PeerTimeout time.Duration
	PeerBackoff time.Duration
	// Upstream enables fetching from the registry named in ?ns=, but only for
	// registries listed in UpstreamRegistries.
	Upstream           bool
	UpstreamRegistries []string
	// TLS supplies the client certificate and CA for SIC-to-SIC traffic and is
	// re-checked for changes. Nil means plain HTTP between peers.
	TLS        *tlsutil.Reloader
	ServerName string
}

type Proxy struct {
	store          *store.Store
	peers          peer.Finder
	upstreamClient *http.Client
	metrics        *metrics.Metrics
	upstream       bool
	registries     map[string]bool
	idle           time.Duration
	flights        flight.Group

	peerTimeout, backoff time.Duration
	tls                  *tlsutil.Reloader
	serverName           string
	cmu                  sync.Mutex // guards peerClient and peerGen
	peerClient           *http.Client
	peerGen              uint64
	dmu                  sync.Mutex // guards down
	down                 map[string]time.Time
	now                  func() time.Time
}

func New(s *store.Store, p peer.Finder, m *metrics.Metrics, o Options) (*Proxy, error) {
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Second
	}
	if o.PeerTimeout <= 0 {
		o.PeerTimeout = 5 * time.Second
	}
	regs := map[string]bool{}
	for _, r := range o.UpstreamRegistries {
		if r = strings.ToLower(strings.TrimSpace(r)); r != "" {
			regs[r] = true
		}
	}
	px := &Proxy{
		store: s,
		peers: p,
		// Upstream registries are public services: verify them against the
		// system roots, not against the private CA used between SIC nodes.
		upstreamClient: &http.Client{Transport: newTransport(o.Timeout, o.Timeout, true)},
		metrics:        m,
		upstream:       o.Upstream,
		registries:     regs,
		idle:           o.Timeout,
		peerTimeout:    o.PeerTimeout,
		backoff:        o.PeerBackoff,
		tls:            o.TLS,
		serverName:     o.ServerName,
		down:           map[string]time.Time{},
		now:            time.Now,
	}
	if o.TLS != nil {
		snap := o.TLS.Current()
		px.peerClient, px.peerGen = px.newPeerClient(&snap), snap.Gen
	} else {
		px.peerClient = px.newPeerClient(nil)
	}
	return px, nil
}

// newTransport builds a transport with bounded connection setup. Compression is
// left alone: a registry proxy must pass bytes through untouched, and Go would
// otherwise decompress a response and so change the content that is hashed.
func newTransport(dial, header time.Duration, envProxy bool) *http.Transport {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: dial, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   dial,
		ResponseHeaderTimeout: header,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   4,
		DisableCompression:    true,
	}
	if envProxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	return tr
}

func (p *Proxy) newPeerClient(snap *tlsutil.Snapshot) *http.Client {
	// Peer traffic never goes through an HTTP proxy, whatever the environment says.
	tr := newTransport(p.peerTimeout, p.peerTimeout, false)
	if snap != nil {
		tr.TLSClientConfig = snap.ClientConfig(p.serverName)
	}
	return &http.Client{Transport: tr}
}

// peerHTTP returns the client for peer requests, rebuilt when the certificates
// on disk have been rotated.
func (p *Proxy) peerHTTP() *http.Client {
	if p.tls == nil {
		return p.peerClient
	}
	snap := p.tls.Current()
	p.cmu.Lock()
	defer p.cmu.Unlock()
	if snap.Gen != p.peerGen {
		old := p.peerClient
		p.peerClient, p.peerGen = p.newPeerClient(&snap), snap.Gen
		old.CloseIdleConnections()
	}
	return p.peerClient
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

// markDown leaves a peer alone for the back-off period after it failed.
func (p *Proxy) markDown(base string) {
	if p.backoff <= 0 {
		return
	}
	p.dmu.Lock()
	p.down[base] = p.now().Add(p.backoff)
	p.dmu.Unlock()
}

// shuffledPeers returns the peers worth asking, in random order so misses are
// spread over the cluster instead of all going to the same first peer. Peers
// that failed recently are left out until their back-off has passed.
func (p *Proxy) shuffledPeers() []string {
	all := p.peers.Candidates()
	now := p.now()
	out := make([]string, 0, len(all))
	p.dmu.Lock()
	for k, until := range p.down {
		if !now.Before(until) {
			delete(p.down, k)
		}
	}
	for _, base := range all {
		if _, down := p.down[base]; !down {
			out = append(out, base)
		}
	}
	p.dmu.Unlock()
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
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

// errBadContent marks content that was refused rather than lost in transit.
var errBadContent = errors.New("refused content")

// putBlob stores a response body as the blob d. The server must declare how
// long the body is: that length is the bound on what gets written, and it has
// to fit on the disk. Without a declared length a peer could stream data until
// the disk is full, because the digest can only be checked at the end.
func (p *Proxy) putBlob(d string, resp *http.Response) error {
	if resp.ContentLength < 0 {
		return errBadContent
	}
	_, err := p.store.PutVerifiedLimit(d, resp.Body, resp.ContentLength)
	if errors.Is(err, store.ErrTooLarge) {
		return errBadContent
	}
	return err
}

func (p *Proxy) fetchPeers(in info, path, d string) bool {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1"})
		if e != nil {
			p.metrics.PeerErrors.Add(1)
			p.markDown(base)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				p.markDown(base)
			}
			continue
		}
		e = p.putBlob(d, resp)
		resp.Body.Close()
		switch {
		case e == nil:
			return true
		case errors.Is(e, store.ErrNoSpace):
			return false // no other peer can help; the disk is the problem
		case errors.Is(e, errBadContent) || errors.Is(e, store.ErrDigestMismatch):
			p.metrics.VerifyErrors.Add(1)
		default:
			p.metrics.PeerErrors.Add(1)
		}
		p.markDown(base)
	}
	return false
}

// errBadManifest marks a manifest that was refused.
var errBadManifest = errors.New("refused manifest")

// readManifest reads a manifest response (at most maxManifestSize bytes) and
// works out its digest from the content itself, so the registry's
// Docker-Content-Digest header, which is optional, is a cross-check and not a
// requirement. A digest reference must match exactly.
func readManifest(resp *http.Response, ref string) (string, []byte, error) {
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestSize+1))
	if err != nil {
		return "", nil, err
	}
	if len(b) > maxManifestSize {
		return "", nil, errBadManifest
	}
	sum := sha256.Sum256(b)
	d := "sha256:" + hex.EncodeToString(sum[:])
	if strings.HasPrefix(ref, "sha256:") && d != ref {
		return "", nil, errBadManifest
	}
	if h := resp.Header.Get("Docker-Content-Digest"); strings.HasPrefix(h, "sha256:") && h != d {
		return "", nil, errBadManifest
	}
	return d, b, nil
}

// fetchManifestPeers returns the digest of the manifest it stored.
func (p *Proxy) fetchManifestPeers(in info, path, ref string) (string, bool) {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1", "Accept": in.accept})
		if e != nil {
			p.metrics.PeerErrors.Add(1)
			p.markDown(base)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				p.markDown(base)
			}
			continue
		}
		d, b, e := readManifest(resp, ref)
		resp.Body.Close()
		if e == nil {
			_, e = p.store.PutVerified(d, bytes.NewReader(b))
		}
		switch {
		case e == nil:
			return d, true
		case errors.Is(e, errBadManifest):
			p.metrics.VerifyErrors.Add(1)
		default:
			p.metrics.PeerErrors.Add(1)
		}
		p.markDown(base)
	}
	return "", false
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
	if e = p.putBlob(d, resp); e != nil {
		if errors.Is(e, errBadContent) || errors.Is(e, store.ErrDigestMismatch) {
			p.metrics.VerifyErrors.Add(1)
		}
		return false
	}
	return true
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
	d, b, e := readManifest(resp, ref)
	if e == nil {
		_, e = p.store.PutVerified(d, bytes.NewReader(b))
	}
	if e != nil {
		if errors.Is(e, errBadManifest) {
			p.metrics.VerifyErrors.Add(1)
		}
		return "", false
	}
	return d, true
}
