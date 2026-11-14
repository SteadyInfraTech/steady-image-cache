package preload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func dg(b []byte) string { s := sha256.Sum256(b); return "sha256:" + hex.EncodeToString(s[:]) }

// fakeSIC serves an index with two platforms, one image each, over the registry
// API, optionally behind a Bearer challenge answered by its own token service.
type fakeSIC struct {
	srv      *httptest.Server
	mu       sync.Mutex
	gets     []string
	needAuth bool
	wantUser string
	blobs    map[string][]byte
	bad      string // a blob digest to corrupt
	tokenKey string // the JSON field of the token answer; default "token"
}

func newFake(t *testing.T, needAuth bool) (*fakeSIC, string) {
	f := &fakeSIC{needAuth: needAuth, wantUser: "bot", blobs: map[string][]byte{}}
	mk := func(tag string) (idx descriptor, layers []string) {
		cfg := []byte(`{"c":"` + tag + `"}`)
		l1, l2 := []byte("layer-one-"+tag), []byte("layer-two-"+tag)
		for _, b := range [][]byte{cfg, l1, l2} {
			f.blobs[dg(b)] = b
		}
		m, _ := json.Marshal(manifest{
			Config: &descriptor{Digest: dg(cfg), Size: int64(len(cfg))},
			Layers: []descriptor{{Digest: dg(l1)}, {Digest: dg(l2)}},
		})
		f.blobs[dg(m)] = m
		return descriptor{MediaType: "application/vnd.oci.image.manifest.v1+json", Digest: dg(m), Size: int64(len(m))}, nil
	}
	amd, _ := mk("amd64")
	arm, _ := mk("arm64")
	amd.Platform = &struct{ OS, Architecture, Variant string }{"linux", "amd64", ""}
	arm.Platform = &struct{ OS, Architecture, Variant string }{"linux", "arm64", ""}
	index, _ := json.Marshal(manifest{Manifests: []descriptor{amd, arm}})
	indexDigest := dg(index)

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		u, _, _ := r.BasicAuth()
		if u != f.wantUser {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		if f.tokenKey == "access_token" {
			fmt.Fprint(w, `{"access_token":"T"}`)
			return
		}
		fmt.Fprint(w, `{"token":"T"}`)
	})
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if f.needAuth && r.Header.Get("Authorization") != "Bearer T" {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="reg",scope="repository:team/app:pull"`, f.srv.URL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Query().Get("ns") != "reg.example" {
			http.Error(w, "ns", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.gets = append(f.gets, r.URL.Path)
		f.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/manifests/v1"):
			w.Write(index)
		case strings.Contains(r.URL.Path, "/blobs/") || strings.Contains(r.URL.Path, "/manifests/sha256:"):
			d := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			b, ok := f.blobs[d]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if d == f.bad {
				b = []byte("not what the digest says")
			}
			w.Write(b)
		default:
			http.NotFound(w, r)
		}
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f, indexDigest
}

func TestPreloadPullsOnlyThePlatformsBlobs(t *testing.T) {
	f, _ := newFake(t, false)
	res, _, err := Run(context.Background(), Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/arm64"},
		[]string{"reg.example/team/app:v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Blobs != 3 {
		t.Fatalf("result = %+v, want one image of 3 blobs (config and two layers)", res)
	}
	for _, p := range f.gets {
		if strings.Contains(p, dg([]byte("layer-one-amd64"))) {
			t.Fatalf("pulled a blob of the other platform: %s", p)
		}
	}
	want := int64(len(`{"c":"arm64"}`) + len("layer-one-arm64") + len("layer-two-arm64"))
	if res[0].Bytes != want {
		t.Fatalf("bytes = %d, want %d", res[0].Bytes, want)
	}
}

func TestPreloadAnswersATokenChallengeWithTheCallersCredentials(t *testing.T) {
	f, _ := newFake(t, true)
	o := Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/amd64", Username: "bot", Password: "pw", AllowPlainHTTPRealm: true}
	if _, _, err := Run(context.Background(), o, []string{"reg.example/team/app:v1"}); err != nil {
		t.Fatal(err)
	}
	o.Username = "someone-else"
	if _, _, err := Run(context.Background(), o, []string{"reg.example/team/app:v1"}); err == nil {
		t.Fatal("a refused token was not an error")
	}
}

func TestPreloadFailsOnContentThatIsNotTheDigest(t *testing.T) {
	f, _ := newFake(t, false)
	f.bad = dg([]byte("layer-two-amd64"))
	_, _, err := Run(context.Background(), Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/amd64"},
		[]string{"reg.example/team/app:v1"})
	if err == nil || !strings.Contains(err.Error(), "content is") {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
}

func TestPreloadNamesAPlatformTheIndexLacks(t *testing.T) {
	f, _ := newFake(t, false)
	_, _, err := Run(context.Background(), Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/s390x"},
		[]string{"reg.example/team/app:v1"})
	if err == nil || !strings.Contains(err.Error(), "linux/s390x") {
		t.Fatalf("err = %v, want it to name the missing platform", err)
	}
}

func TestPreloadReportsWhereTheBytesCameFrom(t *testing.T) {
	f, _ := newFake(t, false)
	var mu sync.Mutex
	calls := 0
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		peers, up := 100, 1000
		if calls > 1 {
			peers, up = 600, 1000
		}
		fmt.Fprintf(w, "sic_bytes_from_peers_total %d\nsic_bytes_from_upstream_total %d\n", peers, up)
	}))
	defer admin.Close()
	_, src, err := Run(context.Background(), Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/amd64", AdminURL: admin.URL},
		[]string{"reg.example/team/app:v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !src.Known || src.FromPeers != 500 || src.FromRegistries != 0 {
		t.Fatalf("sources = %+v, want 500 from peers and none from registries", src)
	}
}

func TestParseRef(t *testing.T) {
	good := map[string]Ref{
		"reg.example/team/app:v1": {"reg.example", "team/app", "v1"},
		"reg.example:5000/app":    {"reg.example:5000", "app", "latest"},
		"ghcr.io/o/r@sha256:abc":  {"ghcr.io", "o/r", "sha256:abc"},
		"docker.io/nginx:1.27":    {"docker.io", "library/nginx", "1.27"},
		"localhost/app:dev":       {"localhost", "app", "dev"},
	}
	for in, want := range good {
		got, err := ParseRef(in)
		if err != nil || got != want {
			t.Errorf("ParseRef(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, in := range []string{"nginx", "nginx:1", "team/app:v1", "reg.example/", ""} {
		if _, err := ParseRef(in); err == nil {
			t.Errorf("ParseRef(%q) did not fail", in)
		}
	}
}

func TestPreloadAcceptsAnAccessTokenAnswer(t *testing.T) {
	f, _ := newFake(t, true)
	f.tokenKey = "access_token"
	o := Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/amd64", Username: "bot", Password: "pw", AllowPlainHTTPRealm: true}
	if _, _, err := Run(context.Background(), o, []string{"reg.example/team/app:v1"}); err != nil {
		t.Fatalf("a token service that answers access_token was refused: %v", err)
	}
}

func TestPreloadSendsNoCredentialsToAPlainHTTPRealm(t *testing.T) {
	f, _ := newFake(t, true)
	o := Options{Addr: f.srv.URL, Client: f.srv.Client(), Platform: "linux/amd64", Username: "bot", Password: "pw"}
	_, _, err := Run(context.Background(), o, []string{"reg.example/team/app:v1"})
	if err == nil || !strings.Contains(err.Error(), "not https") {
		t.Fatalf("err = %v, want a refusal to send credentials to an http realm", err)
	}
}

func TestPreloadFollowsNestedIndexesAndRefusesAnEmptyResult(t *testing.T) {
	blobs := map[string][]byte{}
	put := func(v any) ([]byte, string) {
		b, _ := json.Marshal(v)
		blobs[dg(b)] = b
		return b, dg(b)
	}
	layer := []byte("only-layer")
	blobs[dg(layer)] = layer
	cfg := []byte("{}")
	blobs[dg(cfg)] = cfg
	_, img := put(manifest{Config: &descriptor{Digest: dg(cfg)}, Layers: []descriptor{{Digest: dg(layer)}}})
	plat := &struct{ OS, Architecture, Variant string }{"linux", "amd64", ""}
	_, inner := put(manifest{Manifests: []descriptor{{Digest: img, Platform: plat}}})
	outer, _ := put(manifest{Manifests: []descriptor{{Digest: inner, Platform: plat}}})
	_, empty := put(manifest{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		switch {
		case last == "nested":
			w.Write(outer)
		case last == "nothing":
			w.Write(blobs[empty])
		case blobs[last] != nil:
			w.Write(blobs[last])
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	o := Options{Addr: srv.URL, Client: srv.Client(), Platform: "linux/amd64"}
	res, _, err := Run(context.Background(), o, []string{"reg.example/team/app:nested"})
	if err != nil || len(res) != 1 || res[0].Blobs != 2 {
		t.Fatalf("nested index: %+v, %v; want the image's config and layer", res, err)
	}
	if _, _, err := Run(context.Background(), o, []string{"reg.example/team/app:nothing"}); err == nil {
		t.Fatal("a manifest with no config and no layers was reported as a success")
	}
}
