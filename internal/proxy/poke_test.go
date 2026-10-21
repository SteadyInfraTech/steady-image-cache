package proxy

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

// pokable is a finder that counts how often it is told to look again.
type pokable struct {
	urls  []string
	pokes atomic.Int32
}

func (f *pokable) Candidates() []string { return append([]string(nil), f.urls...) }
func (f *pokable) Poke()                { f.pokes.Add(1) }

func missWith(t *testing.T, f *pokable, method, path string) {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(st, f, &metrics.Metrics{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	req, _ := http.NewRequest(method, srv.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("%s %s: status %d, want 404", method, path, resp.StatusCode)
	}
}

func TestMissWithNoKnownPeersAsksTheFinderToLookAgain(t *testing.T) {
	d := digestOf([]byte("nobody has this"))
	for _, tc := range []struct{ name, method, path string }{
		{"blob", "GET", "/v2/lib/app/blobs/" + d},
		{"head", "HEAD", "/v2/lib/app/blobs/" + d},
		{"manifest", "GET", "/v2/lib/app/manifests/" + d},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &pokable{}
			missWith(t, f, tc.method, tc.path)
			if f.pokes.Load() == 0 {
				t.Fatal("a miss with no known peers did not poke the finder")
			}
		})
	}
}

func TestMissWithKnownPeersDoesNotPoke(t *testing.T) {
	urls, _ := countingPeers(t, 2)
	f := &pokable{urls: urls}
	missWith(t, f, "GET", "/v2/lib/app/blobs/"+digestOf([]byte("nobody has this")))
	if n := f.pokes.Load(); n != 0 {
		t.Fatalf("poked %d times although peers were known", n)
	}
}

// A finder that cannot be poked (a fixed list) is simply left alone.
func TestMissWithAFinderThatCannotBePokedStillWorks(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(st, fixed{}, &metrics.Metrics{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/v2/lib/app/blobs/" + digestOf([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
