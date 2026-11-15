package peer

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The EndpointSlice client against the fake API server of watch_test.go: the
// same server answers listings and streams watch events, whatever the path.

func sliceJSON(name, rv string, port int, zone string, addrs ...string) string {
	var eps []string
	for _, a := range addrs {
		ready := "true"
		if strings.HasPrefix(a, "!") {
			a, ready = a[1:], "false"
		}
		eps = append(eps, fmt.Sprintf(`{"addresses":[%q],"conditions":{"ready":%s},"zone":%q}`, a, ready, zone))
	}
	return fmt.Sprintf(`{"metadata":{"name":%q,"resourceVersion":%q},"endpoints":[%s],"ports":[{"port":%d}]}`,
		name, rv, strings.Join(eps, ","), port)
}

func newFakeSlices(t *testing.T) (*fakeAPI, *EndpointSliceKubernetes) {
	t.Helper()
	f, _ := newFakeAPI(t) // sets up the server, the token and CA files and the environment
	f.list.Store(listJSON("10", sliceJSON("s1", "9", 5000, "z1", "10.0.0.2")))
	e, err := NewEndpointSliceKubernetes("kube-system", "app=sic", "10.0.0.1", 5000, "https", NewZoneResolver())
	if err != nil {
		t.Fatal(err)
	}
	e.EnableWatch()
	e.minGap = 20 * time.Millisecond
	return f, e
}

func runSlices(t *testing.T, e *EndpointSliceKubernetes, interval time.Duration) *logSink {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := &logSink{}
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx, interval, l.logf) }()
	t.Cleanup(func() { cancel(); <-done })
	return l
}

func waitSlicePeers(t *testing.T, e *EndpointSliceKubernetes, want ...string) {
	t.Helper()
	waitFor(t, fmt.Sprintf("peers %v", want), func() bool {
		got := e.Candidates()
		return len(got) == 0 && len(want) == 0 || reflect.DeepEqual(got, want)
	})
}

func TestEndpointSliceWatchFollowsSlicesAsTheyChange(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")

	f.send("ADDED", sliceJSON("s2", "11", 5000, "z2", "10.0.0.3"))
	waitSlicePeers(t, e, "https://10.0.0.2:5000", "https://10.0.0.3:5000")
	f.send("MODIFIED", sliceJSON("s1", "12", 5000, "z1", "!10.0.0.2")) // goes unready
	waitSlicePeers(t, e, "https://10.0.0.3:5000")
	f.send("DELETED", sliceJSON("s2", "13", 5000, "z2", "10.0.0.3"))
	waitSlicePeers(t, e)
	f.send("ADDED", sliceJSON("s3", "14", 6000, "z1", "10.0.0.4", "10.0.0.1")) // the slice's port; this node is left out
	waitSlicePeers(t, e, "https://10.0.0.4:6000")

	if f.lists.Load() != 1 || f.watches.Load() != 1 {
		t.Fatalf("%d listings and %d watches, want 1 and 1", f.lists.Load(), f.watches.Load())
	}
	if got := f.listRV.Load(); got != "0" {
		t.Fatalf("the listing asked for resourceVersion %q, want 0 (the server's cache)", got)
	}
	u := f.watchURL.Load().(string)
	for _, want := range []string{"/apis/discovery.k8s.io/v1/namespaces/kube-system/endpointslices?", "watch=1", "resourceVersion=10",
		"allowWatchBookmarks=true", "labelSelector=app%3Dsic", "timeoutSeconds=36000"} {
		if !strings.Contains(u, want) {
			t.Fatalf("watch request %q lacks %q", u, want)
		}
	}
	e.mu.RLock()
	rv := e.rv
	e.mu.RUnlock()
	if rv != "14" {
		t.Fatalf("resourceVersion %q, want 14: a later watch must resume from the last event", rv)
	}
}

func TestEndpointSliceZonesFollowTheSlices(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	if z := e.resolver.ZoneOf("https://10.0.0.2:5000"); z != "z1" {
		t.Fatalf("zone %q, want z1", z)
	}
	f.send("DELETED", sliceJSON("s1", "12", 5000, "z1", "10.0.0.2"))
	waitSlicePeers(t, e)
	if z := e.resolver.ZoneOf("https://10.0.0.2:5000"); z != "" {
		t.Fatalf("a peer that is gone keeps its zone %q", z)
	}
}

func TestEndpointSliceGoneListsAgain(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	f.list.Store(listJSON("50", sliceJSON("s9", "49", 5000, "z1", "10.0.0.5")))
	f.send("ERROR", `{"kind":"Status","code":410,"message":"too old resource version"}`)
	waitSlicePeers(t, e, "https://10.0.0.5:5000")
	waitFor(t, "a second watch", func() bool { return f.watches.Load() == 2 })
	if f.lists.Load() != 2 || !strings.Contains(f.watchURL.Load().(string), "resourceVersion=50") {
		t.Fatalf("%d listings; watch %v", f.lists.Load(), f.watchURL.Load())
	}
}

func TestEndpointSliceWatchEndingIsAFreshListing(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	waitFor(t, "the first watch", func() bool { return f.watches.Load() == 1 })
	f.list.Store(listJSON("20", sliceJSON("s6", "19", 5000, "z1", "10.0.0.6")))
	f.mu.Lock()
	old := f.events
	f.events = make(chan string, 16)
	f.mu.Unlock()
	close(old)
	waitSlicePeers(t, e, "https://10.0.0.6:5000")
	waitFor(t, "a second watch", func() bool { return f.watches.Load() == 2 })
}

func TestEndpointSliceBookmarkAdvancesTheVersionOnly(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	f.send("BOOKMARK", `{"metadata":{"resourceVersion":"77"}}`)
	waitFor(t, "the bookmark", func() bool {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return e.rv == "77"
	})
	if got := e.Candidates(); !reflect.DeepEqual(got, []string{"https://10.0.0.2:5000"}) {
		t.Fatalf("a bookmark changed the peers: %v", got)
	}
}

func TestEndpointSliceWithoutPermissionToWatchItListsOnTheInterval(t *testing.T) {
	f, e := newFakeSlices(t)
	f.watchStatus.Store(403)
	l := runSlices(t, e, 60*time.Millisecond)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	waitFor(t, "repeated listings", func() bool { return f.lists.Load() >= 3 })
	if f.watches.Load() != 1 {
		t.Fatalf("%d watches: a refused watch must not be retried", f.watches.Load())
	}
	if !strings.Contains(l.all(), "listing every") {
		t.Fatalf("log: %q", l.all())
	}
}

func TestEndpointSlicePokeListsAgainWhileWatching(t *testing.T) {
	f, e := newFakeSlices(t)
	runSlices(t, e, time.Hour)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	f.list.Store(listJSON("30", sliceJSON("s7", "29", 5000, "z1", "10.0.0.7")))
	e.Poke()
	e.Poke() // a second poke while one is pending must not block
	waitSlicePeers(t, e, "https://10.0.0.7:5000")
}

func TestEndpointSliceUnreadableEventEndsTheWatchWithAnError(t *testing.T) {
	f, e := newFakeSlices(t)
	l := runSlices(t, e, 50*time.Millisecond)
	waitSlicePeers(t, e, "https://10.0.0.2:5000")
	f.events <- `{"type":"ADDED","object":"not a slice"}`
	waitFor(t, "the error to be logged", func() bool { return strings.Contains(l.all(), "watch") })
	waitFor(t, "a second listing", func() bool { return f.lists.Load() >= 2 })
}

func TestEndpointSliceErrorEventsAndBookmarksThatAreBroken(t *testing.T) {
	e := &EndpointSliceKubernetes{port: 5000, scheme: "https", slices: map[string]endpointSlice{}}
	if err := e.applySliceEvent(watchEvent{Type: "ERROR", Object: []byte(`{"code":500,"message":"boom"}`)}); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("an ERROR event gave %v", err)
	}
	if err := e.applySliceEvent(watchEvent{Type: "BOOKMARK", Object: []byte(`7`)}); err == nil {
		t.Fatal("a bookmark that is not an object was accepted")
	}
	if err := e.applySliceEvent(watchEvent{Type: "SOMETHING", Object: []byte(`{}`)}); err != nil {
		t.Fatalf("an unknown event type must be ignored: %v", err)
	}
}

func TestEndpointSliceListingErrors(t *testing.T) {
	f, e := newFakeSlices(t)
	f.list.Store(`not json`)
	if err := e.Refresh(context.Background()); err == nil {
		t.Fatal("an unreadable listing was accepted")
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	if err := e.Refresh(context.Background()); err == nil || !strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST") {
		t.Fatalf("without an API address: %v", err)
	}
}

func TestEndpointSliceNeedsTheServiceAccountFilesAndAValidCA(t *testing.T) {
	dir := t.TempDir()
	oldTok, oldCA := tokenFile, caFile
	t.Cleanup(func() { tokenFile, caFile = oldTok, oldCA })
	tokenFile, caFile = dir+"/none-token", dir+"/none-ca"
	if _, err := NewEndpointSliceKubernetes("ns", "a=b", "10.0.0.1", 5000, "https", nil); err == nil {
		t.Fatal("a missing token file was accepted")
	}
	if err := os.WriteFile(dir+"/token", []byte("t"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokenFile = dir + "/token"
	if _, err := NewEndpointSliceKubernetes("ns", "a=b", "10.0.0.1", 5000, "https", nil); err == nil {
		t.Fatal("a missing CA file was accepted")
	}
	if err := os.WriteFile(dir+"/ca", []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	caFile = dir + "/ca"
	if _, err := NewEndpointSliceKubernetes("ns", "a=b", "10.0.0.1", 5000, "https", nil); err == nil || !strings.Contains(err.Error(), "endpointslice") {
		t.Fatalf("a CA file without certificates: %v", err)
	}
	if _, err := certPoolFromPEM([]byte("x")); err == nil {
		t.Fatal("certPoolFromPEM accepted garbage")
	}
}
