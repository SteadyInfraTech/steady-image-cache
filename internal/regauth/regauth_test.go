package regauth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func client(srv *httptest.Server) *http.Client {
	c := *srv.Client()
	c.CheckRedirect = noRedirect
	return &c
}

func challengeFor(srv *httptest.Server) string {
	return fmt.Sprintf(`Bearer realm="%s/token?x=1",service="reg, one",scope="repository:a/b:pull"`, srv.URL)
}

func TestAnonymousTokenAsksTheRealmWithoutCredentials(t *testing.T) {
	var got *http.Request
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		fmt.Fprint(w, `{"token":"T","expires_in":300}`)
	}))
	defer srv.Close()
	tok, ttl, err := AnonymousToken(context.Background(), client(srv), challengeFor(srv))
	if err != nil || tok != "T" || ttl != 300*time.Second {
		t.Fatalf("%q, %v, %v", tok, ttl, err)
	}
	if got.Header.Get("Authorization") != "" {
		t.Fatalf("a credential was sent: %q", got.Header.Get("Authorization"))
	}
	q := got.URL.Query()
	if q.Get("x") != "1" || q.Get("service") != "reg, one" || q.Get("scope") != "repository:a/b:pull" {
		t.Fatalf("query %v: the realm's own query, the service and the scope must all be sent", q)
	}
}

func TestAnonymousTokenFormsAndLimits(t *testing.T) {
	body := ""
	status := 200
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()
	ch := challengeFor(srv)
	for _, c := range []struct {
		name, body string
		status     int
		tok        string
		ttl        time.Duration
		fail       bool
	}{
		{"access_token", `{"access_token":"A"}`, 200, "A", DefaultTTL, false},
		{"token wins", `{"token":"T","access_token":"A","expires_in":5}`, 200, "T", 5 * time.Second, false},
		{"ttl is capped", `{"token":"T","expires_in":999999}`, 200, "T", maxTTL, false},
		{"no token", `{}`, 200, "", 0, true},
		{"not json", `nope`, 200, "", 0, true},
		{"a token with a space", `{"token":"a b"}`, 200, "", 0, true},
		{"denied", `{"errors":[]}`, 401, "", 0, true},
	} {
		body, status = c.body, c.status
		tok, ttl, err := AnonymousToken(context.Background(), client(srv), ch)
		if (err != nil) != c.fail || tok != c.tok || ttl != c.ttl {
			t.Errorf("%s: %q, %v, %v", c.name, tok, ttl, err)
		}
	}
}

func TestAnonymousTokenRefusesWhatItShouldNot(t *testing.T) {
	var asked atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		http.Redirect(w, r, "https://elsewhere.example/token", http.StatusFound)
	}))
	defer srv.Close()
	for _, ch := range []string{
		`Basic realm="x"`,
		`Bearer service="s"`,
		`Bearer realm="http://auth.example/token"`, // plain http
		`Bearer realm="/token"`,                    // no host
		`Bearer realm="ftp://auth.example/token"`,
	} {
		if _, _, err := AnonymousToken(context.Background(), client(srv), ch); err == nil {
			t.Errorf("accepted %q", ch)
		}
	}
	if asked.Load() != 0 {
		t.Fatalf("%d requests were made for a challenge that must be refused", asked.Load())
	}
	// A redirect from the realm is not followed: the answer is the redirect.
	if _, _, err := AnonymousToken(context.Background(), client(srv), challengeFor(srv)); err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("a redirect from the realm: %v", err)
	}
}
