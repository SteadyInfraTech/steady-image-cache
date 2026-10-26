package health

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveIsAlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	Live(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("got %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
}

func TestReady(t *testing.T) {
	failing := errors.New("cache write failed: read-only file system")
	cases := []struct {
		name     string
		check    func() error
		wantCode int
		wantBody string
	}{
		{"passes", func() error { return nil }, http.StatusOK, "ok"},
		{"fails with the reason", func() error { return failing }, http.StatusServiceUnavailable, "not ready: " + failing.Error()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Ready(c.check).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != c.wantCode {
				t.Fatalf("status %d, want %d", rec.Code, c.wantCode)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != c.wantBody {
				t.Fatalf("body %q, want %q", got, c.wantBody)
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("a probe answer must not be cached")
			}
		})
	}
}

func TestReadyAsksEveryTime(t *testing.T) {
	var calls int
	h := Ready(func() error { calls++; return nil })
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/readyz", nil))
	}
	if calls != 3 {
		t.Fatalf("check ran %d times for 3 probes", calls)
	}
}
