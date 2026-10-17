package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	// Logf receives warnings about refused or failed transfers, at most one
	// per kind and peer per minute. Nil discards them.
	Logf func(string, ...any)
}

type Proxy struct {
	store          *store.Store
	peers          peer.Finder
	upstreamClient *http.Client
	metrics        *metrics.Metrics
	upstream       bool
	registries     map[string]bool
	idle           time.Duration
	flights        flight.Group[fetchResult] // manifests
	dlmu           sync.Mutex                // guards downloads
	downloads      map[string]*download      // blobs being fetched, by digest

	peerTimeout, backoff time.Duration
	tls                  *tlsutil.Reloader
	serverName           string
	cmu                  sync.Mutex // guards peerClient and peerGen
	peerClient           *http.Client
	peerGen              uint64
	dmu                  sync.Mutex // guards down
	down                 map[string]time.Time
	now                  func() time.Time

	logf    func(string, ...any)
	lmu     sync.Mutex // guards lastLog and skipped
	lastLog map[string]time.Time
	skipped map[string]int
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
		downloads:      map[string]*download{},
		now:            time.Now,
		logf:           o.Logf,
		lastLog:        map[string]time.Time{},
		skipped:        map[string]int{},
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

// repoRE is the repository name grammar of the OCI distribution spec, with
// upper case letters also allowed because some registries do. Anything else,
// notably control characters, spaces and '%', is refused before it is used to
// build a URL for a peer.
var repoRE = regexp.MustCompile(`^[a-zA-Z0-9]+(?:(?:[._]|__|-+)[a-zA-Z0-9]+)*(?:/[a-zA-Z0-9]+(?:(?:[._]|__|-+)[a-zA-Z0-9]+)*)*$`)

func validRepo(repo string) bool { return len(repo) <= 255 && repoRE.MatchString(repo) }

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
	dl     *download // the blob download these requests belong to, if any
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
	p.store.Touch(d)
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

// fetchResult is the outcome of getting content from peers or upstream. When
// the failure is on this node (full disk, unwritable cache) status is the HTTP
// status to give the client instead of "not found", and no other source is
// worth trying.
type fetchResult struct {
	ok     bool
	status int
}

var fetched = fetchResult{ok: true}

// refuse answers a request that failed because of this node's cache.
func refuse(w http.ResponseWriter, what string, status int) {
	http.Error(w, "cannot cache "+what+": "+http.StatusText(status), status)
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
	dl := p.join(d, func(dl *download) fetchResult {
		in := in
		in.dl = dl
		if _, err := p.store.Stat(d); err == nil {
			return fetched
		}
		if r := p.fetchPeers(in, path, d); r.ok {
			p.metrics.PeerHits.Add(1)
			return r
		} else if r.status != 0 {
			return r
		}
		r := p.fetchUpstream(in, path, d)
		if r.ok {
			p.metrics.UpstreamHits.Add(1)
		}
		return r
	})
	// A plain GET is answered while the blob is still arriving. Anything else
	// (HEAD, a byte range) needs the whole blob, so it waits for it.
	if streamable(r) && p.stream(w, r, d, dl) {
		return
	}
	res, ok := dl.wait(r.Context())
	if !ok {
		return // the client gave up
	}
	if res.ok && p.serveDigest(w, r, d, nil) {
		return
	}
	if res.status != 0 {
		refuse(w, "blob", res.status)
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
	res := p.flights.Do("manifest:"+namespace+"/"+repo+"@"+ref+"|accept:"+in.accept+"|auth:"+in.auth, func() fetchResult {
		if d, found := p.store.Resolve(namespace, repo, ref); found {
			if _, err := p.store.Stat(d); err == nil {
				return fetched
			}
		}
		d, r := p.fetchManifestPeers(in, path, ref)
		if r.ok {
			p.metrics.PeerHits.Add(1)
		} else if r.status == 0 {
			if d, r = p.fetchManifestUpstream(in, path, ref); r.ok {
				p.metrics.UpstreamHits.Add(1)
			}
		}
		if r.ok && !byDigest {
			if err := p.store.SetAlias(namespace, repo, ref, d); err != nil {
				p.metrics.StoreErrors.Add(1)
				p.warn("alias:"+namespace+"/"+repo+"@"+ref, "cannot persist manifest alias: %v", err)
				return fetchResult{status: http.StatusInternalServerError}
			}
		}
		return r
	})
	if res.ok {
		if d, found := p.store.Resolve(namespace, repo, ref); found && p.serveDigest(w, r, d, manifestType) {
			return
		}
	}
	if res.status != 0 {
		refuse(w, "manifest", res.status)
		return
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

// warn logs a warning, but not more than once a minute for the same key; the
// messages that were held back are counted in the next one. A node that cannot
// reach a peer, or has no disk space, would otherwise log on every request.
func (p *Proxy) warn(key, format string, args ...any) {
	if p.logf == nil {
		return
	}
	now := p.now()
	p.lmu.Lock()
	if last, ok := p.lastLog[key]; ok && now.Sub(last) < time.Minute {
		p.skipped[key]++
		p.lmu.Unlock()
		return
	}
	held := p.skipped[key]
	delete(p.skipped, key)
	p.lastLog[key] = now
	p.lmu.Unlock()
	msg := fmt.Sprintf(format, args...)
	if held > 0 {
		msg += fmt.Sprintf(" (%d similar messages suppressed)", held)
	}
	p.logf("%s", msg)
}

// markDown leaves a peer alone for the back-off period after it failed.
func (p *Proxy) markDown(base string, why error) {
	if p.backoff <= 0 {
		return
	}
	p.dmu.Lock()
	p.down[base] = p.now().Add(p.backoff)
	p.dmu.Unlock()
	p.warn("down:"+base, "peer %s failed (%v); skipping it for %s", base, why, p.backoff)
}

// peerFailed records that a request to a peer could not be completed. A request
// that could not even be built is not the peer's doing and does not count.
func (p *Proxy) peerFailed(base string, e error) {
	if errors.Is(e, errBuildRequest) {
		p.warn("build:"+base, "cannot build a request for peer %q: %v", base, e)
		return
	}
	p.metrics.PeerErrors.Add(1)
	p.markDown(base, e)
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
		return nil, fmt.Errorf("%w: %v", errBuildRequest, err)
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

// errBadContent marks content that was refused rather than lost in transit;
// errBuildRequest a request that could not be built from what the client sent.
var (
	errBadContent   = errors.New("refused content")
	errBuildRequest = errors.New("cannot build request")
)

// putBlob stores a response body as the blob d. The server must declare how
// long the body is: that length is the bound on what gets written, and it has
// to fit on the disk. Without a declared length a peer could stream data until
// the disk is full, because the digest can only be checked at the end.
func (p *Proxy) putBlob(d string, resp *http.Response, dl *download) error {
	if resp.ContentLength < 0 {
		return fmt.Errorf("%w: no Content-Length", errBadContent)
	}
	var err error
	if dl != nil {
		_, err = p.store.PutStream(d, resp.Body, resp.ContentLength, dl)
	} else {
		_, err = p.store.PutVerifiedLimit(d, resp.Body, resp.ContentLength)
	}
	if errors.Is(err, store.ErrTooLarge) {
		return fmt.Errorf("%w: longer than the declared %d bytes", errBadContent, resp.ContentLength)
	}
	return err
}

// localFailure recognises an error that is this node's own problem, counts and
// logs it, and returns what to tell the client. size is what was being stored.
func (p *Proxy) localFailure(e error, d string, size int64) (fetchResult, bool) {
	switch {
	case errors.Is(e, store.ErrNoSpace):
		p.metrics.DiskFull.Add(1)
		p.warn("space", "not caching %s (%d bytes): free disk space would drop below SIC_MIN_FREE_BYTES=%d, or the disk is full",
			d, size, p.store.MinFree())
		return fetchResult{status: http.StatusInsufficientStorage}, true
	case errors.Is(e, store.ErrLocal):
		p.metrics.StoreErrors.Add(1)
		p.warn("local", "cannot write to the cache directory: %v", e)
		return fetchResult{status: http.StatusInternalServerError}, true
	}
	return fetchResult{}, false
}

// refused reports whether e means the content itself was unacceptable.
func refused(e error) bool {
	return errors.Is(e, errBadContent) || errors.Is(e, errBadManifest) || errors.Is(e, store.ErrDigestMismatch)
}

func (p *Proxy) fetchPeers(in info, path, d string) fetchResult {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1"})
		if e != nil {
			p.peerFailed(base, e)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				p.markDown(base, fmt.Errorf("answered %s", resp.Status))
			}
			continue
		}
		size := resp.ContentLength
		e = p.putBlob(d, resp, in.dl)
		resp.Body.Close()
		if e == nil {
			return fetched
		}
		if r, ours := p.localFailure(e, d, size); ours {
			return r // no other peer can help, and this one did nothing wrong
		}
		if refused(e) {
			p.metrics.VerifyErrors.Add(1)
			p.warn("verify:"+base, "refused blob %s from peer %s: %v", d, base, e)
			p.markDown(base, e)
		} else {
			p.peerFailed(base, e)
		}
	}
	return fetchResult{}
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
		return "", nil, fmt.Errorf("%w: larger than %d bytes", errBadManifest, maxManifestSize)
	}
	sum := sha256.Sum256(b)
	d := "sha256:" + hex.EncodeToString(sum[:])
	if strings.HasPrefix(ref, "sha256:") && d != ref {
		return "", nil, fmt.Errorf("%w: content hashes to %s, not the requested digest", errBadManifest, d)
	}
	if h := resp.Header.Get("Docker-Content-Digest"); strings.HasPrefix(h, "sha256:") && h != d {
		return "", nil, fmt.Errorf("%w: Docker-Content-Digest %s does not match the content (%s)", errBadManifest, h, d)
	}
	return d, b, nil
}

// storeManifest reads a manifest from resp and stores it.
func (p *Proxy) storeManifest(resp *http.Response, ref string) (string, int64, error) {
	d, b, e := readManifest(resp, ref)
	if e != nil {
		return "", int64(len(b)), e
	}
	_, e = p.store.PutVerified(d, bytes.NewReader(b))
	return d, int64(len(b)), e
}

// fetchManifestPeers returns the digest of the manifest it stored.
func (p *Proxy) fetchManifestPeers(in info, path, ref string) (string, fetchResult) {
	for _, base := range p.shuffledPeers() {
		resp, e := p.get(in.ctx, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1", "Accept": in.accept})
		if e != nil {
			p.peerFailed(base, e)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				p.markDown(base, fmt.Errorf("answered %s", resp.Status))
			}
			continue
		}
		d, size, e := p.storeManifest(resp, ref)
		resp.Body.Close()
		if e == nil {
			return d, fetched
		}
		if r, ours := p.localFailure(e, d, size); ours {
			return "", r
		}
		if refused(e) {
			p.metrics.VerifyErrors.Add(1)
			p.warn("verify:"+base, "refused manifest %s from peer %s: %v", ref, base, e)
			p.markDown(base, e)
		} else {
			p.peerFailed(base, e)
		}
	}
	return "", fetchResult{}
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

// upstreamFailed logs a failed request to an upstream registry.
func (p *Proxy) upstreamFailed(host string, e error) {
	if errors.Is(e, errBuildRequest) {
		p.warn("build:"+host, "cannot build a request for upstream %q: %v", host, e)
		return
	}
	p.warn("upstream:"+host, "upstream %s: %v", host, e)
}

// upstreamAnswered logs an upstream reply that is neither success nor "not
// found". Most important is 401: the registry wants the client to authenticate
// (Docker Hub and GHCR do even for public images), and SIC does not pass that
// challenge on, so the client only ever sees "not found".
func (p *Proxy) upstreamAnswered(host string, resp *http.Response) {
	if resp.StatusCode == http.StatusNotFound {
		return
	}
	hint := ""
	if resp.StatusCode == http.StatusUnauthorized {
		hint = "; SIC does not relay authentication challenges, so registries that require a token cannot be used for upstream fallback"
	}
	p.warn(fmt.Sprintf("upstream:%s:%d", host, resp.StatusCode), "upstream %s answered %s%s", host, resp.Status, hint)
}

func (p *Proxy) fetchUpstream(in info, path, d string) fetchResult {
	host, ok := p.upstreamHost(in.ns)
	if !ok {
		return fetchResult{}
	}
	resp, e := p.get(in.ctx, p.upstreamClient, "https://"+host+path, map[string]string{"Authorization": in.auth})
	if e != nil {
		p.upstreamFailed(host, e)
		return fetchResult{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		p.upstreamAnswered(host, resp)
		return fetchResult{}
	}
	size := resp.ContentLength
	if e = p.putBlob(d, resp, in.dl); e != nil {
		if r, ours := p.localFailure(e, d, size); ours {
			return r
		}
		if refused(e) {
			p.metrics.VerifyErrors.Add(1)
			p.warn("verify:"+host, "refused blob %s from upstream %s: %v", d, host, e)
		}
		return fetchResult{}
	}
	return fetched
}

func (p *Proxy) fetchManifestUpstream(in info, path, ref string) (string, fetchResult) {
	host, ok := p.upstreamHost(in.ns)
	if !ok {
		return "", fetchResult{}
	}
	resp, e := p.get(in.ctx, p.upstreamClient, "https://"+host+path, map[string]string{"Authorization": in.auth, "Accept": in.accept})
	if e != nil {
		p.upstreamFailed(host, e)
		return "", fetchResult{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		p.upstreamAnswered(host, resp)
		return "", fetchResult{}
	}
	d, size, e := p.storeManifest(resp, ref)
	if e != nil {
		if r, ours := p.localFailure(e, d, size); ours {
			return "", r
		}
		if refused(e) {
			p.metrics.VerifyErrors.Add(1)
			p.warn("verify:"+host, "refused manifest %s from upstream %s: %v", ref, host, e)
		}
		return "", fetchResult{}
	}
	return d, fetched
}
