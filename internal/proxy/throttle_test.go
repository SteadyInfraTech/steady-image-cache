package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A registry that is rate limiting must be heard as such. Reported as "not
// found", the client takes it for an answer and does not back off, and the
// image pull fails for a reason that is gone a minute later.
func TestUpstream429IsRelayedWithRetryAfter(t *testing.T) {
	blob := []byte("a layer")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	p, m, host := upstreamProxy(t, srv, &logSink{})

	for _, tc := range []struct{ what, method, path string }{
		{"blob GET", "GET", "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host},
		{"blob HEAD", "HEAD", "/v2/lib/app/blobs/" + digestOf(blob) + "?ns=" + host},
		{"manifest by tag", "GET", "/v2/lib/app/manifests/v1?ns=" + host},
		{"manifest by digest", "GET", "/v2/lib/app/manifests/" + digestOf(testManifest) + "?ns=" + host},
	} {
		rec := request(p, tc.method, tc.path)
		if rec.Code != http.StatusTooManyRequests {
			t.Errorf("%s: status %d, want 429", tc.what, rec.Code)
		}
		if got := rec.Header().Get("Retry-After"); got != "7" {
			t.Errorf("%s: Retry-After = %q, want 7", tc.what, got)
		}
	}
	if m.Misses.Load() != 0 {
		t.Errorf("a throttled request is not a miss: Misses = %d", m.Misses.Load())
	}
	if _, err := p.store.Stat(digestOf(blob)); err == nil {
		t.Error("nothing may be cached from a throttled request")
	}
}

func TestRetryAfterIsPassedOnOnlyWhenItIsSafe(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"30", "30"},
		{" 120 ", "120"},
		{"Wed, 21 Oct 2026 07:28:00 GMT", "Wed, 21 Oct 2026 07:28:00 GMT"},
		{"", ""},
		{"abc", ""},
		{"-5", ""},
		{"1234567", ""},
		{"30\r\nSet-Cookie: x=1", ""},
		{strings.Repeat("9", 4000), ""},
	} {
		resp := &http.Response{Header: http.Header{}}
		if tc.in != "" {
			resp.Header["Retry-After"] = []string{tc.in}
		}
		if got := retryAfterOf(resp); got != tc.want {
			t.Errorf("Retry-After %q -> %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Throttling is not a reason to fail a pull the cache can answer: the tag that
// is cached is served, as it is when the registry is down.
func TestStaleTagIsServedWhenTheRegistryThrottles(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*tagRegistry)
	}{
		{"HEAD and GET throttled", func(r *tagRegistry) { r.throttle.Store(true) }},
		{"registry gives no digest and throttles the GET", func(r *tagRegistry) {
			r.noDigestHeader.Store(true)
			r.throttleGets.Store(true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTagRegistry(t)
			_, doc := reg.publish("latest", "1")
			e := newTagEnv(t, reg, testTTL, true)
			wantManifest(t, e.tag("latest"), doc)

			tc.setup(reg)
			e.advance(2 * testTTL)
			wantManifest(t, e.tag("latest"), doc)
			if e.m.StaleTags.Load() != 1 {
				t.Fatalf("StaleTags = %d, want 1", e.m.StaleTags.Load())
			}
		})
	}
}

// With nothing cached the client has to be told to wait.
func TestThrottledTagWithNothingCachedIs429(t *testing.T) {
	reg := newTagRegistry(t)
	reg.publish("latest", "1")
	reg.throttle.Store(true)
	e := newTagEnv(t, reg, testTTL, true)
	rec := e.tag("latest")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "7" {
		t.Fatalf("status %d Retry-After %q, want 429 and 7", rec.Code, rec.Header().Get("Retry-After"))
	}
}
