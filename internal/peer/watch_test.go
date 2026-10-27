package peer

import (
	"context"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAPI is an API server that lists pods and streams watch events that the
// test sends.
type fakeAPI struct {
	*httptest.Server
	lists, watches atomic.Int32
	listRV         atomic.Value // the resourceVersion asked for by the last listing
	watchURL       atomic.Value // the last watch request
	list           atomic.Value // body of the listing
	watchStatus    atomic.Int32 // 0 means stream
	mu             sync.Mutex
	events         chan string // one JSON event per item; closing it ends the watch
}

func podJSON(name, ip string, ready bool, rv string) string {
	st := "False"
	if ready {
		st = "True"
	}
	return fmt.Sprintf(`{"metadata":{"name":%q,"resourceVersion":%q},"status":{"hostIP":%q,"phase":"Running","conditions":[{"type":"Ready","status":%q}]}}`, name, rv, ip, st)
}

func listJSON(rv string, pods ...string) string {
	return fmt.Sprintf(`{"metadata":{"resourceVersion":%q},"items":[%s]}`, rv, strings.Join(pods, ","))
}

func newFakeAPI(t *testing.T) (*fakeAPI, *Kubernetes) {
	t.Helper()
	f := &fakeAPI{events: make(chan string, 16)}
	f.list.Store(listJSON("10", podJSON("b", "10.0.0.2", true, "9")))
	f.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") != "1" {
			f.lists.Add(1)
			f.listRV.Store(r.URL.Query().Get("resourceVersion"))
			w.Write([]byte(f.list.Load().(string)))
			return
		}
		// Counting the watch and taking its event channel are one step, so a
		// test that waits for the count can swap the channel afterwards.
		f.mu.Lock()
		f.watches.Add(1)
		ev := f.events
		f.mu.Unlock()
		f.watchURL.Store(r.URL.String())
		if st := int(f.watchStatus.Load()); st != 0 {
			w.WriteHeader(st)
			w.Write([]byte(`{"message":"no"}`))
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		for {
			select {
			case e, ok := <-ev:
				if !ok {
					return
				}
				w.Write([]byte(e + "\n"))
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	}))
	t.Cleanup(f.Close)

	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	tok := filepath.Join(dir, "token")
	if err := os.WriteFile(tok, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldTok, oldCA := tokenFile, caFile
	tokenFile, caFile = tok, ca
	t.Cleanup(func() { tokenFile, caFile = oldTok, oldCA })
	host, port, _ := net.SplitHostPort(f.Listener.Addr().String())
	t.Setenv("KUBERNETES_SERVICE_HOST", host)
	t.Setenv("KUBERNETES_SERVICE_PORT_HTTPS", port)
	k, err := NewKubernetes("kube-system", "app=sic", "10.0.0.1", 5000, "https")
	if err != nil {
		t.Fatal(err)
	}
	k.EnableWatch()
	k.minGap = 20 * time.Millisecond
	return f, k
}

func (f *fakeAPI) send(typ, obj string) {
	f.events <- fmt.Sprintf(`{"type":%q,"object":%s}`, typ, obj)
}

func startRun(t *testing.T, k *Kubernetes, interval time.Duration) *logSink {
	t.Helper()
	ctx, cancel := contextWithCancel(t)
	l := &logSink{}
	done := make(chan struct{})
	go func() { defer close(done); k.Run(ctx, interval, l.logf) }()
	t.Cleanup(func() { cancel(); <-done })
	return l
}

type logSink struct {
	mu  sync.Mutex
	buf []string
}

func (l *logSink) logf(f string, a ...any) {
	l.mu.Lock()
	l.buf = append(l.buf, fmt.Sprintf(f, a...))
	l.mu.Unlock()
}

func (l *logSink) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.buf, "\n")
}

func waitPeers(t *testing.T, k *Kubernetes, want ...string) {
	t.Helper()
	for i := 0; i < 400; i++ {
		got := k.Candidates()
		if len(got) == 0 && len(want) == 0 || reflect.DeepEqual(got, want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("peers %v, want %v", k.Candidates(), want)
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A neighbour that becomes Ready, goes unready or goes away is known to the
// node as it happens, with one listing and one watch for all of it, however
// long the interval.
func TestWatchFollowsPodsAsTheyChange(t *testing.T) {
	f, k := newFakeAPI(t)
	startRun(t, k, time.Hour)
	waitPeers(t, k, "https://10.0.0.2:5000")

	f.send("ADDED", podJSON("c", "10.0.0.3", true, "11"))
	waitPeers(t, k, "https://10.0.0.2:5000", "https://10.0.0.3:5000")
	f.send("MODIFIED", podJSON("b", "10.0.0.2", false, "12"))
	waitPeers(t, k, "https://10.0.0.3:5000")
	f.send("DELETED", podJSON("c", "10.0.0.3", true, "13"))
	waitPeers(t, k)
	f.send("ADDED", podJSON("d", "10.0.0.4", true, "14"))
	waitPeers(t, k, "https://10.0.0.4:5000")

	if f.lists.Load() != 1 || f.watches.Load() != 1 {
		t.Fatalf("%d listings and %d watches, want 1 and 1", f.lists.Load(), f.watches.Load())
	}
	// The listing came from the server's cache, and the watch resumes from it.
	if got := f.listRV.Load(); got != "0" {
		t.Fatalf("the listing asked for resourceVersion %q, want 0", got)
	}
	u := f.watchURL.Load().(string)
	for _, want := range []string{"watch=1", "resourceVersion=10", "allowWatchBookmarks=true", "labelSelector=app%3Dsic", "timeoutSeconds=36000"} {
		if !strings.Contains(u, want) {
			t.Fatalf("watch request %q lacks %q", u, want)
		}
	}
}

// A resource version the server no longer has means events were missed: the
// node lists again and takes what the list says.
func TestWatchGoneListsAgain(t *testing.T) {
	f, k := newFakeAPI(t)
	startRun(t, k, time.Hour)
	waitPeers(t, k, "https://10.0.0.2:5000")

	f.list.Store(listJSON("50", podJSON("e", "10.0.0.5", true, "49")))
	f.send("ERROR", `{"kind":"Status","code":410,"message":"too old resource version"}`)
	waitPeers(t, k, "https://10.0.0.5:5000")
	waitFor(t, "a second watch", func() bool { return f.watches.Load() == 2 })
	if f.lists.Load() != 2 {
		t.Fatalf("%d listings, want 2", f.lists.Load())
	}
	if !strings.Contains(f.watchURL.Load().(string), "resourceVersion=50") {
		t.Fatalf("the new watch did not start from the new listing: %v", f.watchURL.Load())
	}
}

// The server ends a watch after its timeout. The node lists again, which also
// repairs anything a lost event left wrong.
func TestWatchEndingIsAFreshListing(t *testing.T) {
	f, k := newFakeAPI(t)
	startRun(t, k, time.Hour)
	waitPeers(t, k, "https://10.0.0.2:5000")
	waitFor(t, "the first watch", func() bool { return f.watches.Load() == 1 })
	f.list.Store(listJSON("20", podJSON("f", "10.0.0.6", true, "19")))
	f.mu.Lock()
	old := f.events
	f.events = make(chan string, 16)
	f.mu.Unlock()
	close(old) // the server ends the watch
	waitPeers(t, k, "https://10.0.0.6:5000")
	waitFor(t, "a second watch", func() bool { return f.watches.Load() == 2 })
}

// A bookmark moves the version a later watch resumes from.
func TestBookmarkAdvancesTheVersion(t *testing.T) {
	f, k := newFakeAPI(t)
	startRun(t, k, time.Hour)
	waitPeers(t, k, "https://10.0.0.2:5000")
	f.send("BOOKMARK", `{"metadata":{"resourceVersion":"77"}}`)
	waitFor(t, "the bookmark", func() bool {
		k.mu.RLock()
		defer k.mu.RUnlock()
		return k.rv == "77"
	})
	if got := k.Candidates(); !reflect.DeepEqual(got, []string{"https://10.0.0.2:5000"}) {
		t.Fatalf("a bookmark changed the peers: %v", got)
	}
}

// A service account that may list but not watch still gets peers, listed on
// the interval, and the log says what to grant.
func TestWithoutPermissionToWatchItListsOnTheInterval(t *testing.T) {
	f, k := newFakeAPI(t)
	f.watchStatus.Store(403)
	l := startRun(t, k, 60*time.Millisecond)
	waitPeers(t, k, "https://10.0.0.2:5000")
	waitFor(t, "repeated listings", func() bool { return f.lists.Load() >= 3 })
	if f.watches.Load() != 1 {
		t.Fatalf("%d watches: a refused watch must not be retried", f.watches.Load())
	}
	if log := l.all(); !strings.Contains(log, "watch") || strings.Count(log, "grant") != 1 {
		t.Fatalf("log: %q", log)
	}
}

// A node that finds no peers pokes discovery. With a watch that means "list
// again", and it is not held up for the watch to end.
func TestPokeListsAgainWhileWatching(t *testing.T) {
	f, k := newFakeAPI(t)
	startRun(t, k, time.Hour)
	waitPeers(t, k, "https://10.0.0.2:5000")
	f.list.Store(listJSON("30", podJSON("g", "10.0.0.7", true, "29")))
	k.Poke()
	waitPeers(t, k, "https://10.0.0.7:5000")
}

// A broken event is an error, not a silent loss: the node lists again.
func TestUnreadableEventEndsTheWatchWithAnError(t *testing.T) {
	f, k := newFakeAPI(t)
	l := startRun(t, k, 50*time.Millisecond)
	waitPeers(t, k, "https://10.0.0.2:5000")
	f.events <- `{"type":"ADDED","object":"not a pod"}`
	waitFor(t, "the error to be logged", func() bool { return strings.Contains(l.all(), "watch") })
	waitFor(t, "a second listing", func() bool { return f.lists.Load() >= 2 })
}

func contextWithCancel(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
