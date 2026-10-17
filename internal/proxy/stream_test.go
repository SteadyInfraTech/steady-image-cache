package proxy

import (
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// halfSource sends the first part of body, then holds the rest back until
// release is called. calls counts the requests it got.
func halfSource(t *testing.T, body []byte, first int, wrong bool) (url string, release func(), calls *atomic.Int32) {
	t.Helper()
	gate := make(chan struct{})
	calls = new(atomic.Int32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(200)
		w.Write(body[:first])
		w.(http.Flusher).Flush()
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		rest := body[first:]
		if wrong {
			rest = []byte(strings.Repeat("X", len(rest)))
		}
		w.Write(rest)
	}))
	var once sync.Once
	rel := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { rel(); srv.Close() })
	return srv.URL, rel, calls
}

// The point of streaming: the client gets the start of a layer while the rest
// is still being downloaded.
func TestClientReceivesABlobWhileItIsStillDownloading(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 5000))
	d := digestOf(body)
	src, release, _ := halfSource(t, body, 20000, false)
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})

	resp, err := http.Get(a.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.ContentLength != int64(len(body)) {
		t.Fatalf("status %d, length %d", resp.StatusCode, resp.ContentLength)
	}
	first := make([]byte, 19000)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("no data before the download finished: %v", err)
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("the blob must not be visible in the cache before it is verified")
	}
	release()
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(first)+string(rest) != string(body) {
		t.Fatalf("body differs (%d bytes), err %v", len(first)+len(rest), err)
	}
	if _, err := a.st.Stat(d); err != nil {
		t.Fatal("blob should be cached after the download")
	}
	if a.m.Streamed.Load() != 1 {
		t.Fatalf("Streamed = %d", a.m.Streamed.Load())
	}
}

// Clients that ask while a download runs all get the whole blob from it, and
// the source is asked once.
func TestConcurrentClientsShareOneStreamingDownload(t *testing.T) {
	body := []byte(strings.Repeat("shared layer ", 4000))
	d := digestOf(body)
	src, release, calls := halfSource(t, body, 10000, false)
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})

	const clients = 4
	type result struct {
		got []byte
		err error
	}
	out := make(chan result, clients)
	started := make(chan struct{}, clients)
	for i := 0; i < clients; i++ {
		go func() {
			resp, err := http.Get(a.srv.URL + "/v2/lib/app/blobs/" + d)
			if err != nil {
				out <- result{err: err}
				return
			}
			defer resp.Body.Close()
			first := make([]byte, 5000)
			if _, err := io.ReadFull(resp.Body, first); err != nil {
				out <- result{err: err}
				return
			}
			started <- struct{}{}
			rest, err := io.ReadAll(resp.Body)
			out <- result{got: append(first, rest...), err: err}
		}()
	}
	for i := 0; i < clients; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("a client got nothing while the download was running")
		}
	}
	release()
	for i := 0; i < clients; i++ {
		r := <-out
		if r.err != nil || string(r.got) != string(body) {
			t.Fatalf("client %d: %d bytes, err %v", i, len(r.got), r.err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("source was asked %d times", calls.Load())
	}
}

// Content that fails verification must not reach the client as a complete
// response, even though most of it was passed on before the check.
func TestStreamedBlobThatFailsVerificationEndsShort(t *testing.T) {
	body := []byte(strings.Repeat("good bytes ", 3000))
	d := digestOf(body)
	src, release, _ := halfSource(t, body, 10000, true)
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})

	resp, err := http.Get(a.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 9000)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	release()
	rest, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("response completed with %d bytes although verification failed", len(first)+len(rest))
	}
	if len(first)+len(rest) >= len(body) {
		t.Fatalf("all %d bytes were sent", len(first)+len(rest))
	}
	waitCount(t, &a.m.StreamAborts, 1)
	waitCount(t, &a.m.VerifyErrors, 1)
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("a mismatching blob must not be stored")
	}
}

// Range and HEAD requests need the whole blob, so they wait for it and are
// then answered from the store.
func TestRangeAndHeadWaitForTheCompleteBlob(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 3000))
	d := digestOf(body)
	src, release, calls := halfSource(t, body, 10000, false)
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})
	url := a.srv.URL + "/v2/lib/app/blobs/" + d

	type result struct {
		status int
		n      int
		body   string
		err    error
	}
	out := make(chan result, 2)
	go func() {
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Range", "bytes=5-14")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		out <- result{status: resp.StatusCode, body: string(b), err: err}
	}()
	// The HEAD comes once the download is under way: a HEAD that finds no
	// download asks the source with a HEAD of its own instead of waiting.
	for i := 0; i < 400 && calls.Load() == 0; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	go func() {
		resp, err := http.Head(url)
		if err != nil {
			out <- result{err: err}
			return
		}
		resp.Body.Close()
		out <- result{status: resp.StatusCode, n: int(resp.ContentLength)}
	}()
	select {
	case r := <-out:
		t.Fatalf("answered before the download finished: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	release()
	for i := 0; i < 2; i++ {
		r := <-out
		switch {
		case r.err != nil:
			t.Fatal(r.err)
		case r.status == 206 && r.body == "5678901234":
		case r.status == 200 && r.n == len(body):
		default:
			t.Fatalf("unexpected answer %+v", r)
		}
	}
}

// A client that goes away does not stop the download, which others share and
// which fills the cache.
func TestStreamingDownloadSurvivesTheClientLeaving(t *testing.T) {
	body := []byte(strings.Repeat("persistent ", 4000))
	d := digestOf(body)
	src, release, calls := halfSource(t, body, 10000, false)
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src})

	resp, err := http.Get(a.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 100)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := a.st.Stat(d); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("download stopped when its client left")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatalf("source asked %d times", calls.Load())
	}
	resp2, got := get(t, a.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp2.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp2.StatusCode, len(got))
	}
}

// The last byte of a streamed blob is only sent once the store has verified the
// digest, so a blob that fails the check can never be a complete response.
// This drives a download by hand to hold the moment between "everything is
// written" and "verified".
func TestLastByteWaitsForVerification(t *testing.T) {
	for _, ok := range []bool{true, false} {
		body := []byte(strings.Repeat("abcdefghij", 1000))
		f := t.TempDir() + "/part"
		if err := os.WriteFile(f, body, 0o644); err != nil {
			t.Fatal(err)
		}
		a := newNode(t, Options{})
		dl := newDownload("")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			a.p.stream(w, r, digestOf(body), dl)
		}))
		defer srv.Close()
		dl.Begin(f, int64(len(body)))
		dl.Wrote(int64(len(body))) // all of it is on disk, none of it verified

		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(body)-1)
		if _, err := io.ReadFull(resp.Body, got); err != nil {
			t.Fatalf("the unverified part should already be sent: %v", err)
		}
		type res struct {
			n   int
			err error
		}
		tail := make(chan res, 1)
		go func() {
			n, err := io.Copy(io.Discard, resp.Body)
			tail <- res{int(n), err}
		}()
		select {
		case r := <-tail:
			t.Fatalf("response ended before verification: %+v", r)
		case <-time.After(200 * time.Millisecond):
		}
		if ok {
			dl.End(nil)
		} else {
			dl.End(store.ErrDigestMismatch)
		}
		r := <-tail
		switch {
		case ok && (r.err != nil || r.n != 1):
			t.Fatalf("verified blob: last byte %+v", r)
		case !ok && r.err == nil:
			t.Fatalf("unverified blob completed: %+v", r)
		}
		resp.Body.Close()
	}
}

// A HEAD for a blob this node does not have asks the other nodes with a HEAD and
// reports the size; it does not fetch the blob.
func TestHeadForAMissingBlobDoesNotDownloadIt(t *testing.T) {
	body := []byte(strings.Repeat("a layer nobody asked for yet ", 500))
	d := digestOf(body)
	var gets, heads atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			heads.Add(1)
		} else {
			gets.Add(1)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodGet {
			w.Write(body)
		}
	}))
	defer src.Close()
	a := newNode(t, Options{})
	a.fin.urls.Store(&[]string{src.URL})

	resp, err := http.Head(a.srv.URL + "/v2/lib/app/blobs/" + d)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.ContentLength != int64(len(body)) || resp.Header.Get("Docker-Content-Digest") != d {
		t.Fatalf("status %d, length %d, digest %q", resp.StatusCode, resp.ContentLength, resp.Header.Get("Docker-Content-Digest"))
	}
	if gets.Load() != 0 || heads.Load() != 1 {
		t.Fatalf("source saw %d GETs and %d HEADs, want 0 and 1", gets.Load(), heads.Load())
	}
	if _, err := a.st.Stat(d); err == nil {
		t.Fatal("a HEAD must not store the blob")
	}
}

func TestHeadForABlobNobodyHasIs404(t *testing.T) {
	a := newNode(t, Options{})
	resp, err := http.Head(a.srv.URL + "/v2/lib/app/blobs/" + digestOf([]byte("nowhere")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
