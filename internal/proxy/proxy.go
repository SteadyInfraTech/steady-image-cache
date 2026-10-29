package proxy

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	// CoordHeader marks a peer request that asks the receiver to fetch the
	// blob, if it does not have it and it is the blob's coordinator, and to
	// stream it while it does.
	CoordHeader = "X-SIC-Coordinate"

	defaultManifestType = "application/vnd.oci.image.manifest.v1+json"
	maxManifestSize     = 4 << 20

	// maxManifestFetch bounds everything one manifest request may do: asking
	// peers, asking the registry and reading the answers. The idle timeout only
	// catches a source that goes quiet, so without this one that sends a byte
	// every few seconds could keep the request, and every client that joined
	// it, waiting for as long as maxManifestSize takes to arrive.
	maxManifestFetch = 2 * time.Minute
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
	// WriteIdle is how long a response may make no progress before the
	// connection is dropped; zero means two minutes.
	WriteIdle time.Duration
	// PeerTimeout is how long a peer may take to accept a connection and start
	// answering. A peer that exceeds it is skipped for PeerBackoff, so a dead
	// node costs one slow request rather than one per miss. Peers answer from
	// their local cache only, so a healthy one responds immediately.
	PeerTimeout time.Duration
	PeerBackoff time.Duration
	// PeerMaxTries is how many peers one miss may ask. Asking them one after
	// another costs a round trip each, so on a large cluster a blob nobody has
	// would otherwise take that many round trips to reach the registry. Zero:
	// all of them.
	PeerMaxTries int
	// Self is this node's own peer address, spelled as the other nodes spell it
	// (the finder's candidates). Coordinate needs it; without it each node
	// fetches on its own.
	Self string
	// Coordinate makes a miss ask the digest's coordinator first, so nodes that
	// miss at the same moment share one fetch instead of each going upstream.
	// CoordWait is how long the coordinator may take to start delivering.
	Coordinate bool
	CoordWait  time.Duration
	// TagTTL is how long a tag keeps pointing at the digest it was last
	// confirmed to have, when its registry can be asked. Zero: never ask again.
	TagTTL time.Duration
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
	store           *store.Store
	peers           peer.Finder
	upstreamClient  *http.Client
	metrics         *metrics.Metrics
	upstream        bool
	registries      map[string]bool
	idle            time.Duration
	manifestTimeout time.Duration             // bound on one manifest request, see maxManifestFetch
	flights         flight.Group[fetchResult] // manifests
	dlmu            sync.Mutex                // guards downloads
	downloads       map[string]*download      // blobs being fetched, by digest

	peerTimeout, backoff time.Duration
	maxTries             int
	self                 string
	coordWait            time.Duration
	writeIdle            time.Duration // see writeIdle; zero means no limit
	coordClient          *http.Client
	tagTTL               time.Duration
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
	if o.CoordWait <= 0 {
		o.CoordWait = 20 * time.Second
	}
	regs := map[string]bool{}
	for _, r := range o.UpstreamRegistries {
		if r = strings.ToLower(strings.TrimSpace(r)); r != "" {
			regs[r] = true
		}
	}
	roots, err := tlsutil.UpstreamRoots(os.Getenv("SSL_CERT_FILE"), os.Getenv("SSL_CERT_DIR"))
	if err != nil {
		return nil, err
	}
	upstreamTransport := newTransport(o.Timeout, o.Timeout, true)
	if roots != nil {
		upstreamTransport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	px := &Proxy{
		store: s,
		peers: p,
		// Upstream registries are public services: verify them against the
		// system roots, not against the private CA used between SIC nodes.
		upstreamClient:  &http.Client{Transport: upstreamTransport, CheckRedirect: upstreamRedirect},
		metrics:         m,
		upstream:        o.Upstream,
		registries:      regs,
		idle:            o.Timeout,
		manifestTimeout: maxManifestFetch,
		peerTimeout:     o.PeerTimeout,
		backoff:         o.PeerBackoff,
		maxTries:        o.PeerMaxTries,
		coordWait:       o.CoordWait,
		writeIdle:       cmp.Or(o.WriteIdle, writeIdle),
		tagTTL:          o.TagTTL,
		tls:             o.TLS,
		serverName:      o.ServerName,
		down:            map[string]time.Time{},
		downloads:       map[string]*download{},
		now:             time.Now,
		logf:            o.Logf,
		lastLog:         map[string]time.Time{},
		skipped:         map[string]int{},
	}
	if o.Coordinate {
		px.self = o.Self
	}
	if o.TLS != nil {
		snap := o.TLS.Current()
		px.peerClient, px.peerGen = px.newPeerClient(&snap, px.peerTimeout), snap.Gen
		px.coordClient = px.newPeerClient(&snap, px.coordWait)
	} else {
		px.peerClient = px.newPeerClient(nil, px.peerTimeout)
		px.coordClient = px.newPeerClient(nil, px.coordWait)
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

// Peers never redirect: they answer from their own cache or say they cannot.
// Following a redirect would let a node with a valid certificate send this one
// to any address, over plain HTTP too, with this node's peer headers.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// maxRedirects bounds the redirects followed for an upstream registry. They
// are real: registries send blobs to a CDN or object store.
const maxRedirects = 5

// upstreamRedirect follows a registry's redirects, over https only, so that a
// registry cannot move the transfer to plain HTTP. (net/http does not send the
// Authorization header to a different host, which is what a CDN needs.)
func upstreamRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	if req.URL.Scheme != "https" {
		return fmt.Errorf("refusing a redirect to %s: only https is followed", req.URL.Scheme)
	}
	return nil
}

// peerAnswered notes a peer's reply that is neither success nor "not here". A
// server error or a redirect means the node is faulty, and it is left alone for
// a while.
func (p *Proxy) peerAnswered(base string, resp *http.Response) {
	if resp.StatusCode >= 500 || resp.StatusCode >= 300 && resp.StatusCode < 400 {
		p.markDown(base, fmt.Errorf("answered %s", resp.Status))
	}
}

// newPeerClient builds a client for peer requests; header is how long a peer
// may take to start answering.
func (p *Proxy) newPeerClient(snap *tlsutil.Snapshot, header time.Duration) *http.Client {
	// Peer traffic never goes through an HTTP proxy, whatever the environment says.
	tr := newTransport(p.peerTimeout, header, false)
	if snap != nil {
		tr.TLSClientConfig = snap.ClientConfig(p.serverName)
	}
	return &http.Client{Transport: tr, CheckRedirect: noRedirect}
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
		old, oldCoord := p.peerClient, p.coordClient
		p.peerClient, p.peerGen = p.newPeerClient(&snap, p.peerTimeout), snap.Gen
		p.coordClient = p.newPeerClient(&snap, p.coordWait)
		old.CloseIdleConnections()
		oldCoord.CloseIdleConnections()
	}
	return p.peerClient
}

// coordHTTP is peerHTTP for requests to a coordinator, which may take longer
// to start answering because it fetches the blob first.
func (p *Proxy) coordHTTP() *http.Client {
	p.peerHTTP() // rebuilds both after a certificate rotation
	p.cmu.Lock()
	defer p.cmu.Unlock()
	return p.coordClient
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
		p.cacheReadFailed(d, e)
		return false
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		p.cacheReadFailed(d, e)
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
	// Whatever the bytes are, a browser must not take them for a page.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	// ServeContent implements GET/HEAD, RFC 7233 byte ranges, If-Modified-Since and 416 responses.
	cw := &countingWriter{ResponseWriter: w, n: p.servedCounter(r), idle: p.writeIdle}
	defer func() { _ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) }() // the connection is reused
	http.ServeContent(cw, r, "", st.ModTime(), f)
	return true
}

// cacheReadFailed handles a stored object that could not be opened. A missing
// one, or one that failed verification (the store has already counted and
// logged that), is an ordinary miss. Anything else is a fault of this node's
// cache, such as running out of file descriptors or an unreadable directory. It
// is still answered as a miss, so the client gets its content from elsewhere,
// but it is counted and logged instead of passing unseen.
func (p *Proxy) cacheReadFailed(d string, e error) {
	if errors.Is(e, os.ErrNotExist) || errors.Is(e, store.ErrDigestMismatch) {
		return
	}
	p.metrics.StoreErrors.Add(1)
	p.warn("read", "cannot read %s from the cache: %v", d, e)
}

// registryMediaType reports whether t is a media type of the OCI or Docker
// registry formats: a JSON document that is not rendered by a browser.
func registryMediaType(t string) bool {
	m, params, err := mime.ParseMediaType(t)
	if err != nil || len(params) > 0 || m != strings.ToLower(t) {
		return false
	}
	if !strings.HasPrefix(m, "application/vnd.oci.") && !strings.HasPrefix(m, "application/vnd.docker.") {
		return false
	}
	return strings.HasSuffix(m, "+json") || strings.HasSuffix(m, "+prettyjws")
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
		// The field is the publisher's. Only a registry document type goes into
		// Content-Type: anything else (text/html in an image config that is
		// asked for by its digest, say) would make a browser run the cached
		// bytes as a page on this node's origin.
		if registryMediaType(doc.MediaType) {
			return doc.MediaType
		}
		return defaultManifestType
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
	// challenge holds the WWW-Authenticate values of an upstream 401, to be
	// passed on to the client (status is then 401).
	challenge []string
	// retryAfter is the Retry-After value of an upstream 429 (status is then
	// 429), already checked to be a plain number of seconds or an HTTP date.
	retryAfter string
}

var fetched = fetchResult{ok: true}

// refuse answers a request that failed because of this node's cache.
func refuse(w http.ResponseWriter, what string, status int) {
	http.Error(w, "cannot cache "+what+": "+http.StatusText(status), status)
}

// Limits on the authentication challenge that is relayed from a registry.
const (
	maxChallenges    = 4
	maxChallengeSize = 4096
)

// challengeOf returns the WWW-Authenticate values of a 401 response, or nil
// when it has none that can be relayed. A 401 without a challenge tells the
// client nothing it could act on, so it is treated like any other failure.
func challengeOf(resp *http.Response) []string {
	var out []string
	for _, v := range resp.Header.Values("WWW-Authenticate") {
		if v == "" || len(v) > maxChallengeSize || strings.ContainsAny(v, "\r\n") {
			continue
		}
		if out = append(out, v); len(out) == maxChallenges {
			break
		}
	}
	return out
}

// reply answers a request whose fetch ended with res. An upstream 401 is passed
// to the client together with its challenge, so that the client can get a token
// from the registry's own authentication service and ask again.
func reply(w http.ResponseWriter, what string, res fetchResult) {
	if res.status == http.StatusUnauthorized && len(res.challenge) > 0 {
		relayChallenge(w, res.challenge)
		return
	}
	if res.status == http.StatusTooManyRequests {
		relayThrottle(w, res.retryAfter)
		return
	}
	refuse(w, what, res.status)
}

// relayThrottle tells the client that the registry is rate limiting, so that
// it backs off instead of taking a "not found" for the answer.
func relayThrottle(w http.ResponseWriter, retryAfter string) {
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	http.Error(w, "the upstream registry is rate limiting requests", http.StatusTooManyRequests)
}

// retryAfterOf returns the Retry-After header of resp if it is something safe
// to pass on: a number of seconds of at most six digits, or an HTTP date.
func retryAfterOf(resp *http.Response) string {
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return ""
	}
	if len(v) <= 6 {
		if _, err := strconv.ParseUint(v, 10, 32); err == nil {
			return v
		}
		return ""
	}
	if t, err := http.ParseTime(v); err == nil {
		return t.UTC().Format(http.TimeFormat)
	}
	return ""
}

// upstreamThrottle turns an upstream 429 into a result that passes it on.
func (p *Proxy) upstreamThrottle(host string, resp *http.Response) (fetchResult, bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return fetchResult{}, false
	}
	p.upstreamAnswered(host, resp)
	return fetchResult{status: http.StatusTooManyRequests, retryAfter: retryAfterOf(resp)}, true
}

func relayChallenge(w http.ResponseWriter, challenge []string) {
	for _, v := range challenge {
		w.Header().Add("WWW-Authenticate", v)
	}
	http.Error(w, "authentication required by the upstream registry", http.StatusUnauthorized)
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
		// A peer never starts a download here, but it may share one that is
		// under way: with a rollout the first node's pull is then passed down
		// the line while it runs, instead of every other node waiting for it
		// to finish and then going to the registry.
		if r.Header.Get(CoordHeader) != "" && streamable(r) && p.coordinates(d) {
			// This node is the digest's coordinator, and the asking node
			// is waiting for it to fetch the blob once for all of them.
			if p.coordinate(w, r, repo, d) {
				return
			}
		}
		if dl := p.activeDownload(d); dl != nil && streamable(r) && p.stream(w, r, d, dl, true, true) {
			p.metrics.PeerStreams.Add(1)
			return
		}
		if p.serveDigest(w, r, d, nil) { // finished while we looked
			p.metrics.LocalHits.Add(1)
			return
		}
		http.Error(w, "blob unavailable", 404)
		return
	}
	in := newInfo(r)
	path := "/v2/" + repo + "/blobs/" + d
	if r.Method == http.MethodHead && !p.downloading(d) {
		// A HEAD asks whether the blob exists and how big it is. It does not
		// pull a possibly multi-gigabyte layer onto this node; whoever wants
		// the content will GET it.
		p.headBlob(w, in, path, d)
		return
	}
	var res fetchResult
	for attempt := 0; ; attempt++ {
		dl := p.join(d, in.auth, p.missFetch(in, path, d, true))
		// A plain GET is answered while the blob is still arriving. Anything
		// else (a byte range) needs the whole blob, so it waits for it.
		if streamable(r) && p.stream(w, r, d, dl, false, false) {
			return
		}
		var ok bool
		if res, ok = dl.wait(r.Context()); !ok {
			return // the client gave up
		}
		// The download that was joined asked the registry with someone else's
		// credentials, or none, and was turned away. That says nothing about
		// this client's: ask again with them, once.
		if res.status == http.StatusUnauthorized && dl.auth != in.auth && attempt == 0 {
			continue
		}
		break
	}
	if res.ok && p.serveDigest(w, r, d, nil) {
		return
	}
	if res.status != 0 {
		reply(w, "blob", res)
		return
	}
	p.metrics.Misses.Add(1)
	http.Error(w, "blob unavailable", 404)
}

// missFetch returns the function that fetches the blob d when this node does
// not have it: from the digest's coordinator if there is another one and asked
// is set, then from the peers, then from the registry.
func (p *Proxy) missFetch(in info, path, d string, viaCoordinator bool) func(*download) fetchResult {
	return func(dl *download) fetchResult {
		in := in
		in.dl = dl
		if _, err := p.store.Stat(d); err == nil {
			return fetched
		}
		if viaCoordinator {
			if c, ok := p.otherCoordinator(d); ok {
				cr := p.fetchCoordinator(in, c, path, d)
				if cr.ok {
					p.metrics.PeerHits.Add(1)
					p.metrics.CoordHits.Add(1)
					return cr
				}
				if cr.status != 0 && cr.status != http.StatusInsufficientStorage {
					return cr
				}
				p.metrics.CoordFallbacks.Add(1)
			}
		}
		pr := p.fetchPeers(in, path, d)
		if pr.ok {
			p.metrics.PeerHits.Add(1)
			return pr
		}
		if pr.status != 0 && pr.status != http.StatusInsufficientStorage {
			return pr
		}
		// A peer's claim that the blob does not fit is not the last word
		// (see fetchPeers): the registry gets its turn, and the client
		// hears "insufficient storage" only if nothing else delivered.
		r := p.fetchUpstream(in, path, d)
		if r.ok {
			p.metrics.UpstreamHits.Add(1)
		} else if r.status == 0 {
			return pr
		}
		return r
	}
}

// downloading reports whether d is being fetched right now.
func (p *Proxy) downloading(d string) bool { return p.activeDownload(d) != nil }

// activeDownload returns the download of d that is under way, or nil.
func (p *Proxy) activeDownload(d string) *download {
	p.dlmu.Lock()
	defer p.dlmu.Unlock()
	return p.downloads[d]
}

// headBlob answers a HEAD for a blob that is not cached by asking the peers and
// then the upstream registry with a HEAD of its own, and relaying the answer.
// Nothing is downloaded or stored.
func (p *Proxy) headBlob(w http.ResponseWriter, in info, path, d string) {
	size, ok := int64(-1), false
	var challenge []string
	var throttle *fetchResult
	for _, base := range p.askablePeers() {
		resp, e := p.do(in.ctx, http.MethodHead, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1"})
		if e != nil {
			p.peerFailed(base, e)
			continue
		}
		resp.Body.Close()
		p.peerAnswered(base, resp)
		if resp.StatusCode == 200 && resp.ContentLength >= 0 {
			size, ok = resp.ContentLength, true
			break
		}
	}
	if !ok {
		if host, up := p.upstreamHost(in.ns); up {
			resp, e := p.do(in.ctx, http.MethodHead, p.upstreamClient, "https://"+host+path, map[string]string{"Authorization": in.auth})
			if e != nil {
				p.upstreamFailed(host, e)
			} else {
				resp.Body.Close()
				if resp.StatusCode == 200 && resp.ContentLength >= 0 {
					size, ok = resp.ContentLength, true
				} else if r, ch := p.upstreamChallenge(resp); ch {
					challenge = r.challenge
				} else if r, th := p.upstreamThrottle(host, resp); th {
					throttle = &r
				} else if resp.StatusCode != 200 {
					p.upstreamAnswered(host, resp)
				}
			}
		}
	}
	if !ok && challenge != nil {
		relayChallenge(w, challenge)
		return
	}
	if !ok && throttle != nil {
		relayThrottle(w, throttle.retryAfter)
		return
	}
	if !ok {
		p.metrics.Misses.Add(1)
		http.Error(w, "blob unavailable", 404)
		return
	}
	h := w.Header()
	h.Set("Docker-Content-Digest", d)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
}

func (p *Proxy) manifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	if !validRepo(repo) || !validRef(ref) {
		http.Error(w, "invalid manifest reference", 400)
		return
	}
	if v := r.URL.Query().Get("ns"); v != "" && !registryRE.MatchString(v) {
		// The namespace is part of the key under which tags are remembered
		// ("ns/repo@tag"); one with a slash in it could collide with another
		// namespace's repository.
		http.Error(w, "invalid namespace", 400)
		return
	}
	namespace := ns(r)
	// A node that asks us for a tag is answered with what we have, however old:
	// it cannot be sent anywhere better. A client is answered from the cache
	// only while the tag is known to be current (see tagFresh).
	if d, ok := p.store.Resolve(namespace, repo, ref); ok && (isPeer(r) || p.tagFresh(namespace, repo, ref)) && p.serveDigest(w, r, d, manifestType) {
		p.metrics.LocalHits.Add(1)
		return
	}
	if isPeer(r) {
		http.Error(w, "manifest unavailable", 404)
		return
	}
	in := newInfo(r)
	path := "/v2/" + repo + "/manifests/" + ref
	res, err := p.flights.DoContext(r.Context(), "manifest:"+namespace+"/"+repo+"@"+ref+"|accept:"+in.accept+"|auth:"+in.auth, func() fetchResult {
		// The fetch serves every client waiting on it, so it does not stop when
		// the one that started it leaves; it stops when it has run for too long.
		ctx, cancel := context.WithTimeout(in.ctx, p.manifestTimeout)
		defer cancel()
		in.ctx = ctx
		return p.fetchManifest(in, namespace, repo, ref, path)
	})
	if err != nil {
		return // the client gave up
	}
	if res.ok {
		if d, found := p.store.Resolve(namespace, repo, ref); found && p.serveDigest(w, r, d, manifestType) {
			return
		}
	}
	if res.status != 0 {
		reply(w, "manifest", res)
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
	out := p.livePeers()
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// livePeers returns the known peers that have not failed recently, in no
// particular order.
func (p *Proxy) livePeers() []string {
	all := p.peers.Candidates()
	if len(all) == 0 {
		// Nobody is known. That is also what a node sees while its neighbours
		// are starting, so ask the finder to look again; this miss goes on
		// without them, the next ones may not.
		if pk, ok := p.peers.(peer.Poker); ok {
			pk.Poke()
		}
	}
	now := p.now()
	out := make([]string, 0, len(all))
	p.dmu.Lock()
	for k, until := range p.down {
		if !now.Before(until) {
			delete(p.down, k)
		}
	}
	for _, base := range all {
		if p.tls != nil && !strings.HasPrefix(strings.ToLower(base), "https://") {
			// Mutual TLS is what authenticates a peer, and a plain HTTP request
			// has none: whoever sits on the path could answer it, and what it
			// answers would be stored (a tag pointing at its choice of content).
			continue
		}
		if _, down := p.down[base]; !down {
			out = append(out, base)
		}
	}
	p.dmu.Unlock()
	return out
}

// askablePeers is shuffledPeers cut to the number one miss may ask. The peers
// are in random order, so the ones left out differ from miss to miss.
func (p *Proxy) askablePeers() []string {
	peers := p.shuffledPeers()
	if p.maxTries > 0 && len(peers) > p.maxTries {
		p.metrics.PeerCapped.Add(1)
		peers = peers[:p.maxTries]
	}
	return peers
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
	return p.do(ctx, http.MethodGet, c, u, hdr)
}

func (p *Proxy) do(ctx context.Context, method string, c *http.Client, u string, hdr map[string]string) (*http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	t := time.AfterFunc(p.idle, cancel)
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		t.Stop()
		cancel()
		return nil, fmt.Errorf("%w: %w", errBuildRequest, err)
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
func (p *Proxy) putBlob(d string, resp *http.Response, dl *download, received *atomic.Uint64) error {
	if resp.ContentLength < 0 {
		return fmt.Errorf("%w: no Content-Length", errBadContent)
	}
	// Counted as it is read, so that content that fails verification or is cut
	// short still shows as the traffic it cost.
	body := &countingReader{r: resp.Body, n: received}
	var err error
	if dl != nil {
		_, err = p.store.PutStream(d, body, resp.ContentLength, dl)
	} else {
		_, err = p.store.PutVerifiedLimit(d, body, resp.ContentLength)
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
	var noSpace fetchResult
	for _, base := range p.askablePeers() {
		res, done := p.tryPeer(in, p.peerHTTP(), base, path, d, map[string]string{PeerHeader: "1"}, false)
		if done {
			return res
		}
		if res.status == http.StatusInsufficientStorage {
			// The size is whatever this peer declared, and nothing has
			// been checked yet: a peer that claims more than the disk
			// holds must not end the miss for the peers (and the registry)
			// that would tell the truth. If the blob really is too big, the
			// next one fails the same way, quickly, before any byte is read.
			noSpace = res
		}
	}
	return noSpace
}

// tryPeer asks one peer for the blob and stores what it sends. slowOK says the
// peer may take long to start answering, so that is not held against it. done is false
// when the next peer should be asked; the result then carries only a possible
// "insufficient storage".
func (p *Proxy) tryPeer(in info, c *http.Client, base, path, d string, hdr map[string]string, slowOK bool) (res fetchResult, done bool) {
	resp, e := p.get(in.ctx, c, p.peerURL(base, path, in.ns), hdr)
	if e != nil {
		if slowOK && isSlowStart(e) {
			return fetchResult{}, false // slow to start is not down
		}
		p.peerFailed(base, e)
		return fetchResult{}, false
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		p.peerAnswered(base, resp)
		return fetchResult{}, false
	}
	size := resp.ContentLength
	e = p.putBlob(d, resp, in.dl, &p.metrics.BytesFromPeers)
	resp.Body.Close()
	if e == nil {
		return fetched, true
	}
	if r, ours := p.localFailure(e, d, size); ours {
		if r.status == http.StatusInsufficientStorage {
			return r, false
		}
		return r, true // no other peer can help, and this one did nothing wrong
	}
	if refused(e) {
		p.metrics.VerifyErrors.Add(1)
		p.warn("verify:"+base, "refused blob %s from peer %s: %v", d, base, e)
		p.markDown(base, e)
	} else {
		p.peerFailed(base, e)
	}
	return fetchResult{}, false
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
func (p *Proxy) storeManifest(resp *http.Response, ref string, received *atomic.Uint64) (string, int64, error) {
	resp.Body = &countedBody{ReadCloser: resp.Body, n: received}
	d, b, e := readManifest(resp, ref)
	if e != nil {
		return "", int64(len(b)), e
	}
	_, e = p.store.PutVerified(d, bytes.NewReader(b))
	return d, int64(len(b)), e
}

// fetchManifestPeers returns the digest of the manifest it stored.
func (p *Proxy) fetchManifestPeers(in info, path, ref string) (string, fetchResult) {
	for _, base := range p.askablePeers() {
		resp, e := p.get(in.ctx, p.peerHTTP(), p.peerURL(base, path, in.ns), map[string]string{PeerHeader: "1", "Accept": in.accept})
		if e != nil {
			p.peerFailed(base, e)
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			p.peerAnswered(base, resp)
			continue
		}
		d, size, e := p.storeManifest(resp, ref, &p.metrics.BytesFromPeers)
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
// found", or a 401 that carries no challenge to relay. (A 401 with a challenge
// is not logged: it is how every anonymous pull from Docker Hub or GHCR starts,
// and the client is told to authenticate.)
func (p *Proxy) upstreamAnswered(host string, resp *http.Response) {
	if resp.StatusCode == http.StatusNotFound {
		return
	}
	hint := ""
	if resp.StatusCode == http.StatusUnauthorized {
		hint = " without a WWW-Authenticate challenge, so the client cannot be told how to authenticate"
	}
	p.warn(fmt.Sprintf("upstream:%s:%d", host, resp.StatusCode), "upstream %s answered %s%s", host, resp.Status, hint)
}

// upstreamChallenge turns an upstream 401 that carries a challenge into a
// result that passes it on to the client.
func (p *Proxy) upstreamChallenge(resp *http.Response) (fetchResult, bool) {
	if resp.StatusCode != http.StatusUnauthorized {
		return fetchResult{}, false
	}
	ch := challengeOf(resp)
	if ch == nil {
		return fetchResult{}, false
	}
	p.metrics.UpstreamChallenges.Add(1)
	return fetchResult{status: http.StatusUnauthorized, challenge: ch}, true
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
		if r, ok := p.upstreamChallenge(resp); ok {
			return r
		}
		if r, ok := p.upstreamThrottle(host, resp); ok {
			return r
		}
		p.upstreamAnswered(host, resp)
		return fetchResult{}
	}
	size := resp.ContentLength
	if e = p.putBlob(d, resp, in.dl, &p.metrics.BytesFromUpstream); e != nil {
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
		if r, ok := p.upstreamChallenge(resp); ok {
			return "", r
		}
		if r, ok := p.upstreamThrottle(host, resp); ok {
			return "", r
		}
		p.upstreamAnswered(host, resp)
		return "", fetchResult{}
	}
	d, size, e := p.storeManifest(resp, ref, &p.metrics.BytesFromUpstream)
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

// servedCounter is the byte counter for what a response to r sends: another
// node's pull of a blob is counted apart from a client's.
func (p *Proxy) servedCounter(r *http.Request) *atomic.Uint64 {
	if isPeer(r) {
		return &p.metrics.BytesToPeers
	}
	return &p.metrics.BytesToClients
}

// countingReader adds the bytes read to n.
type countingReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (c *countingReader) Read(b []byte) (int, error) {
	m, err := c.r.Read(b)
	if m > 0 {
		c.n.Add(uint64(m))
	}
	return m, err
}

// countedBody is a response body that adds the bytes read to n.
type countedBody struct {
	io.ReadCloser
	n *atomic.Uint64
}

func (c *countedBody) Read(b []byte) (int, error) {
	m, err := c.ReadCloser.Read(b)
	if m > 0 {
		c.n.Add(uint64(m))
	}
	return m, err
}

// writeIdle is how long a response write may make no progress before the
// connection is dropped. A client that takes a blob and then stops reading holds
// a file descriptor, a goroutine and, once the blob has been evicted, the disk
// blocks of a file that is already gone, for as long as the connection stays up.
// The limit is on progress, not on the whole transfer: a layer of any size may
// take as long as it likes while it keeps moving. A file is sent in copyChunk
// pieces with the deadline renewed for each, so a client must take at least
// copyChunk per writeIdle (about 9 KiB/s) to be kept; the streamed path renews it
// for every 64 KiB.
const writeIdle = 2 * time.Minute

// copyChunk is how much of a file is handed to the kernel between two renewals
// of the write deadline.
const copyChunk = 1 << 20

// countingWriter adds the bytes written to n and, when idle is set, keeps a
// write deadline of idle from the last write that made progress.
type countingWriter struct {
	http.ResponseWriter
	n    *atomic.Uint64
	idle time.Duration
}

func (c *countingWriter) renew() {
	if c.idle > 0 {
		_ = http.NewResponseController(c.ResponseWriter).SetWriteDeadline(time.Now().Add(c.idle))
	}
}

func (c *countingWriter) Write(b []byte) (int, error) {
	c.renew()
	m, err := c.ResponseWriter.Write(b)
	if m > 0 {
		c.n.Add(uint64(m))
	}
	return m, err
}

// ReadFrom passes the copy on to the response writer, which sends a file
// straight from the kernel (sendfile) when it is handed one; hiding the method
// would make every cached layer go through a buffer in user space.
func (c *countingWriter) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	for {
		c.renew()
		var n int64
		var err error
		src := r
		var outer *io.LimitedReader
		if c.idle > 0 {
			// The kernel copy is only used for a file inside a LimitedReader, so
			// the chunk is cut from that one, not wrapped around it.
			if lr, ok := r.(*io.LimitedReader); ok {
				outer = lr
				src = &io.LimitedReader{R: lr.R, N: min(lr.N, copyChunk)}
			} else {
				src = io.LimitReader(r, copyChunk)
			}
		}
		if rf, ok := c.ResponseWriter.(io.ReaderFrom); ok {
			n, err = rf.ReadFrom(src)
		} else {
			n, err = io.Copy(struct{ io.Writer }{c.ResponseWriter}, src)
		}
		if n > 0 {
			c.n.Add(uint64(n))
		}
		total += n
		if outer != nil {
			outer.N -= n
			if outer.N <= 0 {
				return total, err
			}
		}
		if err != nil || c.idle <= 0 || n < copyChunk {
			return total, err
		}
	}
}

func (c *countingWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }
