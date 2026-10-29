package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// flakySource serves body, but the first request it gets breaks off after cut
// bytes. Later requests are answered in full, or from the byte asked for when
// they carry a Range, unless ignoreRange or badRange say otherwise.
type flakySource struct {
	body        []byte
	cut         int
	ignoreRange bool
	badRange    bool
	alwaysCut   bool // every request breaks off, the later ones at the same place plus extra
	extra       int
	mu          sync.Mutex
	ranges      []string // the Range header of each GET, in order
	gets        atomic.Int32
}

func (f *flakySource) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)))
		return
	}
	n := f.gets.Add(1)
	rng := r.Header.Get("Range")
	f.mu.Lock()
	f.ranges = append(f.ranges, rng)
	f.mu.Unlock()
	start := 0
	if rng != "" && !f.ignoreRange {
		fmt.Sscanf(rng, "bytes=%d-", &start)
		if f.badRange {
			start++ // a continuation that does not continue
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(f.body)-1, len(f.body)))
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)-start))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)))
		w.WriteHeader(http.StatusOK)
	}
	out := f.body[start:]
	if n == 1 || f.alwaysCut {
		cut := f.cut
		if n > 1 {
			cut = f.extra
		}
		if cut > len(out) {
			cut = len(out)
		}
		w.Write(out[:cut])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // the connection drops mid-body
	}
	w.Write(out)
}

func (f *flakySource) rangesSeen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ranges...)
}

func partialFiles(t *testing.T, n *node) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(n.root, "blobs", "sha256", ".part-*"))
	return m
}

func resumeBody() []byte { return []byte(strings.Repeat("0123456789abcdef", 8192)) } // 128 KiB

// The first peer breaks off partway; the node asks the next for the rest, and a
// client that was already receiving the blob gets all of it, unaware.
func TestResumeContinuesFromAnotherPeer(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 70000}
	a := httptest.NewServer(http.HandlerFunc(src.handler))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(src.handler))
	defer b.Close()
	n := newNode(t, Options{})
	n.fin.urls.Store(&[]string{a.URL, b.URL})

	resp, got := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes (want %d)", resp.StatusCode, len(got), len(body))
	}
	if _, err := n.st.Stat(d); err != nil {
		t.Fatal("the blob was not kept")
	}
	ranges := src.rangesSeen()
	if len(ranges) != 2 || ranges[0] != "" || !strings.HasPrefix(ranges[1], "bytes=") || ranges[1] == "bytes=0-" {
		t.Fatalf("requests %q: want a whole one and then one for the rest", ranges)
	}
	var start int
	fmt.Sscanf(ranges[1], "bytes=%d-", &start)
	if start <= 0 || start > 70000 {
		t.Fatalf("the rest was asked from byte %d, want a prefix of what the first source sent", start)
	}
	waitCount(t, &n.m.Resumed, 1)
	if got := n.m.ResumedBytes.Load(); got != uint64(start) {
		t.Fatalf("ResumedBytes = %d, want %d", got, start)
	}
	if left := partialFiles(t, n); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
}

// A source that ignores the Range and sends everything again is fine for the
// download: it replaces the start, and the blob is stored once it verifies. A
// client that was already reading the first source's bytes is cut off, as when
// a source is replaced, because those bytes were never checked against what
// replaced them; its retry is answered from the cache.
func TestResumeWhenTheSourceIgnoresTheRange(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 50000, ignoreRange: true}
	a := httptest.NewServer(http.HandlerFunc(src.handler))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(src.handler))
	defer b.Close()
	n := newNode(t, Options{})
	n.fin.urls.Store(&[]string{a.URL, b.URL})

	if resp, err := http.Get(n.srv.URL + "/v2/lib/app/blobs/" + d); err == nil {
		resp.Body.Close() // whether this one is cut short is not the point
	}
	eventually(t, "the blob to be stored", func() bool { _, err := n.st.Stat(d); return err == nil })
	resp, got := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes", resp.StatusCode, len(got))
	}
	if n.m.Resumed.Load() != 0 {
		t.Fatalf("Resumed = %d, want 0: nothing was continued", n.m.Resumed.Load())
	}
	if left := partialFiles(t, n); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
}

// A 206 that does not start where the kept bytes end is refused, nothing is
// stored, and nothing is left on disk.
func TestResumeWithARangeThatDoesNotContinueIsRefused(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 40000, badRange: true}
	a := httptest.NewServer(http.HandlerFunc(src.handler))
	defer a.Close()
	n := newNode(t, Options{})
	// The same peer twice: the second ask carries the Range.
	n.fin.urls.Store(&[]string{a.URL, a.URL})

	getFailing(t, n.srv.URL+"/v2/lib/app/blobs/"+d)
	if _, err := n.st.Stat(d); err == nil {
		t.Fatal("a blob that was assembled from a wrong range was stored")
	}
	eventually(t, "the partial file to be removed", func() bool { return len(partialFiles(t, n)) == 0 })
}

// The registry breaks off partway, and the node asks it for the rest.
func TestResumeFromTheRegistryAfterItBreaksOff(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 90000}
	reg := httptest.NewTLSServer(http.HandlerFunc(src.handler))
	defer reg.Close()
	host := reg.Listener.Addr().String()
	n := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{host}})
	n.p.upstreamClient = reg.Client()

	resp, got := get(t, n.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host)
	if resp.StatusCode != 200 || string(got) != string(body) {
		t.Fatalf("status %d, %d bytes (want %d)", resp.StatusCode, len(got), len(body))
	}
	ranges := src.rangesSeen()
	if len(ranges) != 2 || ranges[0] != "" || ranges[1] == "" {
		t.Fatalf("requests %q", ranges)
	}
	waitCount(t, &n.m.Resumed, 1)
	waitCount(t, &n.m.UpstreamHits, 1)
	if left := partialFiles(t, n); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
}

// A registry that keeps dying is not asked for ever: a try that gets no further
// than the last ends it.
func TestResumeFromTheRegistryGivesUpWhenThereIsNoProgress(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 30000, alwaysCut: true, extra: 0}
	reg := httptest.NewTLSServer(http.HandlerFunc(src.handler))
	defer reg.Close()
	host := reg.Listener.Addr().String()
	n := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{host}})
	n.p.upstreamClient = reg.Client()

	getFailing(t, n.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host)
	eventually(t, "the download to end", func() bool { return !n.p.downloading(d) })
	if g := src.gets.Load(); g != 2 {
		t.Fatalf("the registry was asked %d times, want 2 (the first, and one try that went nowhere)", g)
	}
	if _, err := n.st.Stat(d); err == nil {
		t.Fatal("an unfinished blob was stored")
	}
	if left := partialFiles(t, n); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
}

// A registry that makes progress every time, but never finishes, is asked at
// most maxResumes more times.
func TestResumeFromTheRegistryIsBounded(t *testing.T) {
	body := resumeBody()
	d := digestOf(body)
	src := &flakySource{body: body, cut: 10000, alwaysCut: true, extra: 10000}
	reg := httptest.NewTLSServer(http.HandlerFunc(src.handler))
	defer reg.Close()
	host := reg.Listener.Addr().String()
	n := newNode(t, Options{Upstream: true, UpstreamRegistries: []string{host}})
	n.p.upstreamClient = reg.Client()

	getFailing(t, n.srv.URL+"/v2/lib/app/blobs/"+d+"?ns="+host)
	eventually(t, "the download to end", func() bool { return !n.p.downloading(d) })
	if g := src.gets.Load(); g != int32(1+maxResumes) {
		t.Fatalf("the registry was asked %d times, want %d", g, 1+maxResumes)
	}
	if left := partialFiles(t, n); len(left) != 0 {
		t.Fatalf("partial files left: %v", left)
	}
}

func TestParseContentRange(t *testing.T) {
	for _, tc := range []struct {
		in         string
		start, end int64
		total      int64
		ok         bool
	}{
		{"bytes 100-199/200", 100, 199, 200, true},
		{"bytes 0-0/1", 0, 0, 1, true},
		{"bytes 100-199/*", 0, 0, 0, false},
		{"bytes 200-100/300", 0, 0, 0, false},
		{"bytes 0-300/300", 0, 0, 0, false},
		{"items 0-1/2", 0, 0, 0, false},
		{"", 0, 0, 0, false},
		{"bytes x-y/z", 0, 0, 0, false},
	} {
		s, e, tot, ok := parseContentRange(tc.in)
		if ok != tc.ok || ok && (s != tc.start || e != tc.end || tot != tc.total) {
			t.Errorf("%q: got %d %d %d %v, want %d %d %d %v", tc.in, s, e, tot, ok, tc.start, tc.end, tc.total, tc.ok)
		}
	}
}
