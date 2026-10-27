package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitBytes waits until n's download of d has received some of the blob.
func waitBytes(t *testing.T, n *node, d string) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if dl := n.p.activeDownload(d); dl != nil {
			dl.mu.Lock()
			started := dl.cur != nil && dl.cur.written > 0
			dl.mu.Unlock()
			if started {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the download never received any data")
}

// During a rollout the first node to pull a layer is still downloading it when
// the others ask. They get it from the download, as it arrives.
func TestPeerStreamsFromADownloadThatIsStillRunning(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 5000))
	d := digestOf(body)
	src, release, calls := halfSource(t, body, 20000, false)

	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})
	b := newNode(t, Options{})
	b.setPeers(a)

	go func() { get(t, a.srv.URL+"/v2/lib/app/blobs/"+d) }()
	waitBytes(t, a, d)

	resp, err := http.Get(b.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 19000)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("B got nothing while A was still downloading: %v", err)
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("A should still be downloading")
	}
	release()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(first)+string(rest) != string(body) {
		t.Fatalf("body differs (%d bytes), err %v", len(first)+len(rest), err)
	}
	if _, err := b.st.Stat(d); err != nil {
		t.Fatal("B should have cached the blob")
	}
	if calls.Load() != 1 {
		t.Fatalf("the source was asked %d times, want 1: B must not go around A", calls.Load())
	}
	waitCount(t, &a.m.PeerStreams, 1)
	waitCount(t, &b.m.PeerHits, 1)
}

// What B gets from a download is verified by B like anything else. A download
// that turns out to be wrong ends short, and B keeps nothing.
func TestBlobStreamedFromAFailingDownloadIsNotKept(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 5000))
	d := digestOf(body)
	src, release, _ := halfSource(t, body, 20000, true)

	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})
	b := newNode(t, Options{})
	b.setPeers(a)

	go func() { getFailing(t, a.srv.URL+"/v2/lib/app/blobs/"+d) }()
	waitBytes(t, a, d)

	done := make(chan struct{})
	go func() { getFailing(t, b.srv.URL+"/v2/lib/app/blobs/"+d); close(done) }()
	time.Sleep(100 * time.Millisecond) // B is attached to A's download
	release()
	<-done
	if _, err := b.st.Stat(d); err == nil {
		t.Fatal("B stored a blob that failed verification")
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("A stored a blob that failed verification")
	}
}

// Two nodes that are both downloading a blob, and each list the other as a
// peer, must not end up waiting for each other. Whichever order things happen
// in, both clients get the blob once the source delivers it.
func TestNodesDownloadingTheSameBlobDoNotWaitForEachOther(t *testing.T) {
	for i := 0; i < 20; i++ {
		body := []byte(strings.Repeat("0123456789", 5000))
		d := digestOf(body)
		src, release, _ := halfSource(t, body, 20000, false)

		a := newNode(t, Options{PeerTimeout: 10 * time.Second})
		b := newNode(t, Options{PeerTimeout: 10 * time.Second})
		a.fin.urls.Store(&[]string{src, b.srv.URL})
		b.fin.urls.Store(&[]string{src, a.srv.URL})

		var wg sync.WaitGroup
		results := make([]string, 2)
		for j, n := range []*node{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, got := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d)
				if resp.StatusCode == 200 {
					results[j] = string(got)
				}
			}()
		}
		time.Sleep(50 * time.Millisecond)
		release()
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the nodes are waiting for each other", i)
		}
		for j, r := range results {
			if r != string(body) {
				t.Fatalf("round %d: client %d got %d bytes", i, j, len(r))
			}
		}
	}
}

// countingPeers starts n peers that answer 404 and count what they were asked.
func countingPeers(t *testing.T, n int) (urls []string, asked *atomic.Int32) {
	t.Helper()
	asked = new(atomic.Int32)
	for i := 0; i < n; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			asked.Add(1)
			http.NotFound(w, r)
		}))
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL)
	}
	return urls, asked
}

func TestMissAsksNoMorePeersThanTheCap(t *testing.T) {
	body := []byte("a layer nobody has")
	d := digestOf(body)
	for _, tc := range []struct {
		name, method, path string
	}{
		{"blob", "GET", "/v2/lib/app/blobs/" + d},
		{"head", "HEAD", "/v2/lib/app/blobs/" + d},
		{"manifest", "GET", "/v2/lib/app/manifests/" + d},
	} {
		for _, c := range []struct{ tries, want int }{{2, 2}, {0, 5}, {9, 5}} {
			urls, asked := countingPeers(t, 5)
			n := newNode(t, Options{PeerMaxTries: c.tries})
			n.fin.urls.Store(&urls)
			req, _ := http.NewRequest(tc.method, n.srv.URL+tc.path, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 404 {
				t.Fatalf("%s tries=%d: status %d, want 404", tc.name, c.tries, resp.StatusCode)
			}
			if int(asked.Load()) != c.want {
				t.Fatalf("%s tries=%d: %d peers asked, want %d", tc.name, c.tries, asked.Load(), c.want)
			}
			wantCapped := uint64(0)
			if c.tries > 0 && c.tries < 5 {
				wantCapped = 1
			}
			if n.m.PeerCapped.Load() != wantCapped {
				t.Fatalf("%s tries=%d: PeerCapped = %d, want %d", tc.name, c.tries, n.m.PeerCapped.Load(), wantCapped)
			}
		}
	}
}

// The peers left out change from miss to miss, so a blob that one particular
// node has is still found by some misses.
func TestCapDoesNotAlwaysLeaveOutTheSamePeers(t *testing.T) {
	body := []byte("a layer one node has")
	d := digestOf(body)
	urls, _ := countingPeers(t, 4)
	holder := newNode(t, Options{})
	put(t, holder, body)
	urls = append(urls, holder.srv.URL)

	found := 0
	for i := 0; i < 60; i++ {
		n := newNode(t, Options{PeerMaxTries: 2})
		n.fin.urls.Store(&urls)
		if resp, _ := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d); resp.StatusCode == 200 {
			found++
		}
	}
	// Two of five asked, so about 40% (24 of 60) should be found.
	if found < 8 || found > 45 {
		t.Fatalf("found %d of 60, expected about 24", found)
	}
}
