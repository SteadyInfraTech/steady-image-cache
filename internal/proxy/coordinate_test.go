package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowRegistry serves one blob over TLS and holds the second half of every
// response until released, counting the requests for the blob.
type slowRegistry struct {
	*httptest.Server
	gets    atomic.Int32
	release func()
	host    string
}

func newSlowRegistry(t *testing.T, body []byte) *slowRegistry {
	t.Helper()
	gate := make(chan struct{})
	r := &slowRegistry{}
	r.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.gets.Add(1)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(200)
		w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		select {
		case <-gate:
		case <-req.Context().Done():
			return
		}
		w.Write(body[len(body)/2:])
	}))
	var once sync.Once
	r.release = func() { once.Do(func() { close(gate) }) }
	r.host = r.Listener.Addr().String()
	t.Cleanup(func() { r.release(); r.Close() })
	return r
}

// cluster starts n nodes that know each other and know themselves, with the
// registry as the only upstream.
func cluster(t *testing.T, n int, host string, client *http.Client) []*node {
	t.Helper()
	var nodes []*node
	for i := 0; i < n; i++ {
		nd := newNode(t, Options{Coordinate: true, CoordWait: 5 * time.Second, Upstream: true, UpstreamRegistries: []string{host}})
		nd.p.self = nd.srv.URL
		nd.p.upstreamClient = client
		nodes = append(nodes, nd)
	}
	for _, nd := range nodes {
		var others []*node
		for _, o := range nodes {
			if o != nd {
				others = append(others, o)
			}
		}
		nd.setPeers(others...)
	}
	return nodes
}

func coordinatorOf(nodes []*node, d string) *node {
	top := nodes[0].p.topNode(d)
	for _, n := range nodes {
		if n.srv.URL == top {
			return n
		}
	}
	return nil
}

// Every node views the same ranking, and over many digests every node gets
// its share of the coordinating.
func TestCoordinatorIsAgreedAndSpread(t *testing.T) {
	var nodes []*node
	for i := 0; i < 5; i++ {
		nd := newNode(t, Options{Coordinate: true})
		nd.p.self = nd.srv.URL
		nodes = append(nodes, nd)
	}
	for _, nd := range nodes {
		var others []*node
		for _, o := range nodes {
			if o != nd {
				others = append(others, o)
			}
		}
		nd.setPeers(others...)
	}
	count := map[string]int{}
	for i := 0; i < 500; i++ {
		d := digestOf([]byte(fmt.Sprint(i)))
		top := nodes[0].p.topNode(d)
		for _, n := range nodes[1:] {
			if got := n.p.topNode(d); got != top {
				t.Fatalf("%s: nodes disagree: %s vs %s", d, got, top)
			}
		}
		count[top]++
	}
	for _, n := range nodes {
		if c := count[n.srv.URL]; c < 50 || c > 150 {
			t.Fatalf("node coordinates %d of 500 digests, want about 100: %v", c, count)
		}
	}
}

// The point: nodes that miss on the same layer at the same moment make one
// request to the registry between them.
func TestSimultaneousMissesMakeOneRegistryRequest(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 6000))
	d := digestOf(body)
	reg := newSlowRegistry(t, body)
	nodes := cluster(t, 4, reg.host, reg.Client())

	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, got := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+reg.host)
			if resp.StatusCode != 200 || string(got) != string(body) {
				t.Errorf("status %d, %d bytes", resp.StatusCode, len(got))
			}
		}()
	}
	time.Sleep(300 * time.Millisecond) // everyone is asking, nothing has finished
	reg.release()
	wg.Wait()

	if g := reg.gets.Load(); g != 1 {
		t.Fatalf("the registry was asked %d times, want 1", g)
	}
	var coordHits, served uint64
	for _, n := range nodes {
		if _, err := n.st.Stat(d); err != nil {
			t.Fatal("a node did not keep the blob")
		}
		coordHits += n.m.CoordHits.Load()
		served += n.m.CoordServed.Load()
	}
	if coordHits != 3 || served != 3 {
		t.Fatalf("coordinator hits %d, served %d, want 3 and 3", coordHits, served)
	}
}

// A coordinator that is gone costs the asking node nothing but the attempt.
func TestDeadCoordinatorFallsBackToTheRegistry(t *testing.T) {
	body := []byte(strings.Repeat("dead-coordinator", 3000))
	d := digestOf(body)
	reg := newSlowRegistry(t, body)
	reg.release()
	nodes := cluster(t, 3, reg.host, reg.Client())
	dead := coordinatorOf(nodes, d)
	dead.srv.Close()
	var asker *node
	for _, n := range nodes {
		if n != dead {
			asker = n
		}
	}
	resp, got := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+reg.host)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	if asker.m.CoordFallbacks.Load() == 0 || asker.m.UpstreamHits.Load() != 1 {
		t.Fatalf("fallbacks %d, upstream hits %d", asker.m.CoordFallbacks.Load(), asker.m.UpstreamHits.Load())
	}
}

// A node that is not the coordinator in its own view does not fetch for a
// node that thinks it is: it answers like any peer, at once.
func TestNonCoordinatorDoesNotFetchForAPeer(t *testing.T) {
	body := []byte(strings.Repeat("abc", 9000))
	d := digestOf(body)
	reg := newSlowRegistry(t, body)
	reg.release()
	nodes := cluster(t, 3, reg.host, reg.Client())
	top := coordinatorOf(nodes, d)
	var other *node
	for _, n := range nodes {
		if n != top {
			other = n
		}
	}
	req, _ := http.NewRequest("GET", other.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+reg.host, nil)
	req.Header.Set(PeerHeader, "1")
	req.Header.Set(CoordHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 || reg.gets.Load() != 0 {
		t.Fatalf("status %d, registry asked %d times", resp.StatusCode, reg.gets.Load())
	}
}

// The coordinator fetches for everyone and has nobody's credentials, so a
// registry that wants them turns it away. The asking node then asks with its
// own, and the coordinator's attempt leaves nothing behind.
func TestCoordinatorWithoutCredentialsFallsBackToTheAskersOwn(t *testing.T) {
	body := []byte(strings.Repeat("private", 5000))
	d := digestOf(body)
	reg := newTokenRegistry(t, body, testManifest)
	host := reg.Listener.Addr().String()
	nodes := cluster(t, 3, host, reg.Client())
	top := coordinatorOf(nodes, d)
	var asker *node
	for _, n := range nodes {
		if n != top {
			asker = n
		}
	}
	resp, got := get(t, asker.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host, "Authorization", "Bearer tok")
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	if _, err := asker.st.Stat(d); err != nil {
		t.Fatal("the asking node did not keep the blob")
	}
	if _, err := top.st.Stat(d); err == nil {
		t.Fatal("the coordinator has content it had no credentials for")
	}
}
