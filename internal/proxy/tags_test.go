package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

// tagRegistry is a registry whose tags can move and which can fail or ask for
// credentials. It counts what it is asked.
type tagRegistry struct {
	*httptest.Server
	mu    sync.Mutex
	docs  map[string][]byte // digest -> manifest
	tags  map[string]string // tag -> digest
	heads atomic.Int32
	// gets counts GETs of a tag and of a digest separately.
	tagGets, digestGets atomic.Int32
	down                atomic.Bool // answers 503
	noDigestHeader      atomic.Bool // leaves Docker-Content-Digest out of a HEAD
	needToken           atomic.Bool // wants "Bearer tok"
	throttle            atomic.Bool // answers 429 with Retry-After: 7
	throttleGets        atomic.Bool // the same, but only to requests that are not a HEAD
}

func newTagRegistry(t *testing.T) *tagRegistry {
	t.Helper()
	r := &tagRegistry{docs: map[string][]byte{}, tags: map[string]string{}}
	r.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		i := strings.LastIndex(req.URL.Path, "/manifests/")
		if i < 0 {
			http.NotFound(w, req)
			return
		}
		ref := req.URL.Path[i+len("/manifests/"):]
		isDigest := strings.HasPrefix(ref, "sha256:")
		switch {
		case req.Method == http.MethodHead:
			r.heads.Add(1)
		case isDigest:
			r.digestGets.Add(1)
		default:
			r.tagGets.Add(1)
		}
		if r.down.Load() {
			http.Error(w, "down", 503)
			return
		}
		if r.throttle.Load() || (r.throttleGets.Load() && req.Method != http.MethodHead) {
			w.Header().Set("Retry-After", "7")
			http.Error(w, "slow down", 429)
			return
		}
		if r.needToken.Load() && req.Header.Get("Authorization") != "Bearer tok" {
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, "unauthorized", 401)
			return
		}
		r.mu.Lock()
		d := ref
		if !isDigest {
			d = r.tags[ref]
		}
		doc, ok := r.docs[d]
		r.mu.Unlock()
		if !ok {
			http.NotFound(w, req)
			return
		}
		if !(req.Method == http.MethodHead && r.noDigestHeader.Load()) {
			w.Header().Set("Docker-Content-Digest", d)
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		if req.Method != http.MethodHead {
			w.Write(doc)
		}
	}))
	t.Cleanup(r.Close)
	return r
}

// publish makes tag point at a new manifest and returns its digest.
func (r *tagRegistry) publish(tag, version string) (digest string, doc []byte) {
	doc = []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[],"annotations":{"version":%q}}`, version))
	digest = digestOf(doc)
	r.mu.Lock()
	r.docs[digest] = doc
	r.tags[tag] = digest
	r.mu.Unlock()
	return digest, doc
}

func (r *tagRegistry) unpublish(tag string) {
	r.mu.Lock()
	delete(r.tags, tag)
	r.mu.Unlock()
}

func (r *tagRegistry) asked() (heads, tagGets, digestGets int) {
	return int(r.heads.Load()), int(r.tagGets.Load()), int(r.digestGets.Load())
}

// tagEnv is a node that may use a registry and peers, with a clock the test
// controls.
type tagEnv struct {
	p    *Proxy
	st   *store.Store
	m    *metrics.Metrics
	fin  *swapFinder
	host string
	sink *logSink
	cmu  sync.Mutex
	at   time.Time
}

func (e *tagEnv) now() time.Time { e.cmu.Lock(); defer e.cmu.Unlock(); return e.at }
func (e *tagEnv) advance(d time.Duration) {
	e.cmu.Lock()
	e.at = e.at.Add(d)
	e.cmu.Unlock()
}

const testTTL = 10 * time.Minute

func newTagEnv(t *testing.T, reg *tagRegistry, ttl time.Duration, upstream bool) *tagEnv {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &tagEnv{st: st, m: &metrics.Metrics{}, fin: &swapFinder{}, host: reg.Listener.Addr().String(), sink: &logSink{}, at: time.Now()}
	o := Options{Upstream: upstream, UpstreamRegistries: []string{e.host}, TagTTL: ttl, Logf: e.sink.logf}
	if e.p, err = New(st, e.fin, e.m, o); err != nil {
		t.Fatal(err)
	}
	e.p.upstreamClient = reg.Client()
	e.p.now = e.now
	return e
}

func (e *tagEnv) tag(tag string, hdr ...string) *httptest.ResponseRecorder {
	return request(e.p, "GET", "/v2/lib/app/manifests/"+tag+"?ns="+e.host, hdr...)
}

func wantManifest(t *testing.T, rec *httptest.ResponseRecorder, doc []byte) {
	t.Helper()
	if rec.Code != 200 || rec.Body.String() != string(doc) {
		t.Fatalf("status %d body %q, want 200 and %q", rec.Code, rec.Body.String(), doc)
	}
}

func wantAsked(t *testing.T, reg *tagRegistry, heads, tagGets, digestGets int) {
	t.Helper()
	h, tg, dg := reg.asked()
	if h != heads || tg != tagGets || dg != digestGets {
		t.Fatalf("registry was asked %d HEADs, %d tag GETs, %d digest GETs; want %d, %d, %d", h, tg, dg, heads, tagGets, digestGets)
	}
}

// peerWith returns a node that holds a manifest, and, if tag is set, answers
// for that tag with it.
func peerWith(t *testing.T, e *tagEnv, doc []byte, tag string) *node {
	t.Helper()
	b := newNode(t, Options{})
	d := put(t, b, doc)
	if tag != "" {
		if err := b.st.SetAlias(e.host, "lib/app", tag, d); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// --- freshness ---

// A tag that was confirmed recently is answered from the cache, without asking
// anyone.
func TestFreshTagIsServedWithoutAskingTheRegistry(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)

	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 1, 0, 1) // the tag is resolved by a HEAD, the manifest fetched by digest
	e.advance(testTTL - time.Second)
	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 1, 0, 1)
}

// An old tag is confirmed with a HEAD; if it has not moved, nothing is fetched,
// and it counts as fresh again for another TTL.
func TestStaleTagThatHasNotMovedCostsOneHead(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)

	e.advance(testTTL + time.Second)
	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 2, 0, 1)
	if e.m.TagChanges.Load() != 0 || e.m.StaleTags.Load() != 0 {
		t.Fatalf("changes %d, stale %d", e.m.TagChanges.Load(), e.m.StaleTags.Load())
	}
	wantManifest(t, e.tag("latest"), doc) // fresh again
	wantAsked(t, reg, 2, 0, 1)
	if e.m.TagChecks.Load() != 2 {
		t.Fatalf("TagChecks = %d", e.m.TagChecks.Load())
	}
}

// The case the whole change is for: "latest" moves, and a node that held the
// old one gets the new one instead of serving the old one for ever.
func TestMovedTagIsPickedUpAfterTheTTL(t *testing.T) {
	reg := newTagRegistry(t)
	d1, doc1 := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc1)

	d2, doc2 := reg.publish("latest", "2")
	e.advance(time.Minute)
	wantManifest(t, e.tag("latest"), doc1) // not due yet: still the old one

	e.advance(testTTL)
	wantManifest(t, e.tag("latest"), doc2)
	if got, _ := e.st.Resolve(e.host, "lib/app", "latest"); got != d2 {
		t.Fatalf("alias is %s, want %s", got, d2)
	}
	if _, err := e.st.Stat(d1); err != nil {
		t.Fatal("the old manifest should stay: eviction decides when it goes")
	}
	if e.m.TagChanges.Load() != 1 {
		t.Fatalf("TagChanges = %d", e.m.TagChanges.Load())
	}
	wantManifest(t, e.tag("latest"), doc2)
	wantAsked(t, reg, 2, 0, 2)
}

// A tag that cannot be confirmed because the registry is down is served as it
// is, rather than breaking a pull that the cache can answer.
func TestStaleTagIsServedWhenTheRegistryIsDown(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)

	reg.down.Store(true)
	e.advance(2 * testTTL)
	wantManifest(t, e.tag("latest"), doc)
	if e.m.StaleTags.Load() != 1 {
		t.Fatalf("StaleTags = %d", e.m.StaleTags.Load())
	}
	if !strings.Contains(e.sink.all(), "without confirming") {
		t.Fatalf("serving a stale tag went unlogged: %q", e.sink.all())
	}
	// It is tried again on the next request, since it was not confirmed.
	reg.down.Store(false)
	d2, doc2 := reg.publish("latest", "2")
	wantManifest(t, e.tag("latest"), doc2)
	if got, _ := e.st.Resolve(e.host, "lib/app", "latest"); got != d2 {
		t.Fatalf("alias is %s", got)
	}
}

// A registry that says it does not have the tag any more does not make the
// cache forget it; it is not asked again until the TTL is up.
func TestTagRemovedFromTheRegistryStaysCached(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)
	reg.unpublish("latest")

	e.advance(2 * testTTL)
	wantManifest(t, e.tag("latest"), doc)
	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 2, 0, 1) // one more HEAD, not one per request
}

// A zero TTL means tags are never confirmed again; and with no way to ask the
// registry (an air-gapped node) the age of a tag does not matter.
func TestTagsAreNotCheckedWhenThereIsNothingToCheck(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ttl      time.Duration
		upstream bool
	}{
		{"zero TTL", 0, true},
		{"upstream fallback off", testTTL, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTagRegistry(t)
			_, doc := reg.publish("latest", "1")
			e := newTagEnv(t, reg, tc.ttl, tc.upstream)
			if !tc.upstream {
				b := peerWith(t, e, doc, "latest")
				e.fin.urls.Store(&[]string{b.srv.URL})
			}
			wantManifest(t, e.tag("latest"), doc)
			before, _, _ := reg.asked()
			e.advance(365 * 24 * time.Hour)
			wantManifest(t, e.tag("latest"), doc)
			if after, _, _ := reg.asked(); after != before {
				t.Fatalf("the registry was asked %d more times", after-before)
			}
		})
	}
}

// Digests never change, so they are never confirmed.
func TestDigestReferencesAreNeverChecked(t *testing.T) {
	reg := newTagRegistry(t)
	d, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag(d), doc)
	wantAsked(t, reg, 0, 0, 1)
	e.advance(100 * testTTL)
	wantManifest(t, e.tag(d), doc)
	wantAsked(t, reg, 0, 0, 1)
}

// A tag that containerd knew but the cache never confirmed is confirmed when it
// is first asked for.
func TestUnconfirmedAliasIsCheckedAtFirstUse(t *testing.T) {
	reg := newTagRegistry(t)
	oldD, oldDoc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	put2 := func(doc []byte) { _, _ = e.st.PutVerified(digestOf(doc), strings.NewReader(string(doc))) }
	put2(oldDoc)
	if added, err := e.st.SetAliasIfAbsent(e.host, "lib/app", "latest", oldD); err != nil || !added {
		t.Fatalf("added=%v err=%v", added, err)
	}
	_, doc2 := reg.publish("latest", "2") // the registry moved on before the first request

	wantManifest(t, e.tag("latest"), doc2)
}

// Many clients asking for one stale tag at once cause one check.
func TestConcurrentRequestsForAStaleTagCheckOnce(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)
	e.advance(2 * testTTL)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wantManifest(t, e.tag("latest"), doc)
		}()
	}
	wg.Wait()
	if h, _, _ := reg.asked(); h > 3 { // the first fetch's HEAD, one check, and slack for a late joiner
		t.Fatalf("the registry was asked %d HEADs", h)
	}
}

// --- who says what a tag is ---

// A peer that answers for a tag with a different manifest than the registry's is
// not believed, when the registry can be asked.
func TestRegistryDecidesWhatATagIsNotAPeer(t *testing.T) {
	reg := newTagRegistry(t)
	_, good := reg.publish("latest", "good")
	evil := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[],"annotations":{"version":"evil"}}`)
	e := newTagEnv(t, reg, testTTL, true)
	b := peerWith(t, e, evil, "latest") // a peer that has "latest" pointing at its own manifest
	e.fin.urls.Store(&[]string{b.srv.URL})

	wantManifest(t, e.tag("latest"), good)
	if d, _ := e.st.Resolve(e.host, "lib/app", "latest"); d != digestOf(good) {
		t.Fatalf("the tag points at %s", d)
	}
}

// The manifest itself still comes from a peer when one has it, which keeps the
// registry's load to the one HEAD, and is checked against the registry's digest.
func TestManifestComesFromAPeerByDigestWhenOneHasIt(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	b := peerWith(t, e, doc, "") // has the manifest, knows no tag
	e.fin.urls.Store(&[]string{b.srv.URL})

	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 1, 0, 0)
	if e.m.PeerHits.Load() != 1 {
		t.Fatalf("PeerHits = %d", e.m.PeerHits.Load())
	}
}

// A peer cannot make the registry's digest mean something else: content that
// does not hash to it is refused, and the registry is asked.
func TestPeerManifestThatDoesNotMatchTheRegistrysDigestIsRefused(t *testing.T) {
	reg := newTagRegistry(t)
	d, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", d) // claims the right digest
		w.Write([]byte(`{"schemaVersion":2,"layers":[],"annotations":{"version":"evil"}}`))
	}))
	defer liar.Close()
	e.fin.urls.Store(&[]string{liar.URL})

	wantManifest(t, e.tag("latest"), doc)
	if e.m.VerifyErrors.Load() != 1 {
		t.Fatalf("VerifyErrors = %d", e.m.VerifyErrors.Load())
	}
}

// With no registry to ask, a peer is the only source for a tag, as before.
func TestPeersAnswerForTagsWhenTheRegistryCannotBeAsked(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, false)
	b := peerWith(t, e, doc, "latest")
	e.fin.urls.Store(&[]string{b.srv.URL})
	wantManifest(t, e.tag("latest"), doc)
	wantAsked(t, reg, 0, 0, 0)
}

// ... and when the registry is down and nothing is cached, too.
func TestPeersAnswerForTagsWhenTheRegistryIsDownAndNothingIsCached(t *testing.T) {
	reg := newTagRegistry(t)
	reg.down.Store(true)
	e := newTagEnv(t, reg, testTTL, true)
	_, doc := reg.publish("latest", "1")
	b := peerWith(t, e, doc, "latest")
	e.fin.urls.Store(&[]string{b.srv.URL})
	wantManifest(t, e.tag("latest"), doc)
}

// A tag the registry does not have may exist only on the nodes: peers can answer.
func TestPeersAnswerForTagsTheRegistryDoesNotHave(t *testing.T) {
	reg := newTagRegistry(t)
	e := newTagEnv(t, reg, testTTL, true)
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[],"annotations":{"local":"yes"}}`)
	b := peerWith(t, e, doc, "dev-build")
	e.fin.urls.Store(&[]string{b.srv.URL})
	wantManifest(t, e.tag("dev-build"), doc)
}

// A registry that has the tag but does not say which manifest it is can only be
// believed itself: the peers are not asked.
func TestRegistryThatOmitsTheDigestIsBelievedAlone(t *testing.T) {
	reg := newTagRegistry(t)
	_, good := reg.publish("latest", "good")
	reg.noDigestHeader.Store(true)
	e := newTagEnv(t, reg, testTTL, true)
	evil := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[],"annotations":{"version":"evil"}}`)
	b := peerWith(t, e, evil, "latest")
	e.fin.urls.Store(&[]string{b.srv.URL})
	wantManifest(t, e.tag("latest"), good)
	if _, tg, _ := reg.asked(); tg != 1 {
		t.Fatalf("tag GETs = %d", tg)
	}
}

// Peers asking us for a tag are answered with what we have, however old, and
// never cause a call to the registry.
func TestPeerRequestsForATagAreAnsweredFromTheCacheWhateverItsAge(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)
	e.advance(100 * testTTL)
	before, _, _ := reg.asked()
	rec := request(e.p, "GET", "/v2/lib/app/manifests/latest?ns="+e.host, PeerHeader, "1")
	wantManifest(t, rec, doc)
	if after, _, _ := reg.asked(); after != before {
		t.Fatal("a peer's request reached the registry")
	}
	rec = request(e.p, "GET", "/v2/lib/app/manifests/other?ns="+e.host, PeerHeader, "1")
	if rec.Code != 404 {
		t.Fatalf("status %d", rec.Code)
	}
}

// --- credentials ---

// A registry that wants credentials to say what a tag is gets them from the
// client, which is sent the challenge, before any peer's word is taken.
func TestChallengeComesBeforeAnyPeersAnswerForATag(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	reg.needToken.Store(true)
	e := newTagEnv(t, reg, testTTL, true)
	b := peerWith(t, e, doc, "latest") // would answer without any credentials
	e.fin.urls.Store(&[]string{b.srv.URL})

	rec := e.tag("latest")
	if rec.Code != 401 || rec.Header().Get("WWW-Authenticate") != challenge {
		t.Fatalf("status %d, challenge %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	wantManifest(t, e.tag("latest", "Authorization", "Bearer tok"), doc)
}

// A stale tag is not held up by a client that cannot show credentials: the cache
// serves it, as it serves a cached tag to any client.
func TestStaleTagIsServedWhenTheRegistryWantsCredentialsTheClientLacks(t *testing.T) {
	reg := newTagRegistry(t)
	_, doc := reg.publish("latest", "1")
	e := newTagEnv(t, reg, testTTL, true)
	wantManifest(t, e.tag("latest"), doc)

	reg.needToken.Store(true)
	e.advance(2 * testTTL)
	wantManifest(t, e.tag("latest"), doc)
	if e.m.StaleTags.Load() != 1 {
		t.Fatalf("StaleTags = %d", e.m.StaleTags.Load())
	}
}

// --- what is kept ---

func TestSIC_TagTTLReachesTheProxy(t *testing.T) {
	st, _ := store.New(t.TempDir())
	p, err := New(st, &swapFinder{}, &metrics.Metrics{}, Options{TagTTL: 3 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if p.tagTTL != 3*time.Minute {
		t.Fatalf("tagTTL = %v", p.tagTTL)
	}
}
