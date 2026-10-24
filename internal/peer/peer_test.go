package peer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func pods(t *testing.T, doc string) podList {
	t.Helper()
	var pl podList
	if err := json.Unmarshal([]byte(doc), &pl); err != nil {
		t.Fatal(err)
	}
	return pl
}

func TestParsePeersKeepsOnlyReadyPods(t *testing.T) {
	pl := pods(t, `{"items":[
	 {"metadata":{"name":"ready"},          "status":{"hostIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"self"},           "status":{"hostIP":"10.0.0.1","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"not-ready"},      "status":{"hostIP":"10.0.0.3","phase":"Running","conditions":[{"type":"Ready","status":"False"}]}},
	 {"metadata":{"name":"no-conditions"},  "status":{"hostIP":"10.0.0.4","phase":"Running"}},
	 {"metadata":{"name":"pending"},        "status":{"hostIP":"10.0.0.5","phase":"Pending","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"terminating","deletionTimestamp":"2026-10-14T00:00:00Z"},
	                                        "status":{"hostIP":"10.0.0.6","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"no-ip"},          "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"dup"},            "status":{"hostIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"not-an-ip"},      "status":{"hostIP":"evil.example/x?","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
	]}`)
	got := parsePeers(pl, "10.0.0.1", "https", 5000)
	want := []string{"https://10.0.0.2:5000"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParsePeersBracketsIPv6(t *testing.T) {
	pl := pods(t, `{"items":[{"metadata":{"name":"a"},"status":{"hostIP":"fd00::2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}`)
	got := parsePeers(pl, "", "https", 5000)
	if want := []string{"https://[fd00::2]:5000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParsePeersIsSorted(t *testing.T) {
	pl := pods(t, `{"items":[
	 {"metadata":{"name":"b"},"status":{"hostIP":"10.0.0.9","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}},
	 {"metadata":{"name":"a"},"status":{"hostIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}`)
	got := parsePeers(pl, "", "http", 80)
	if want := []string{"http://10.0.0.2:80", "http://10.0.0.9:80"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

// The kubelet rotates the service account token; a copy read once at startup
// stops working when it expires.
func TestAuthTransportReadsTheCurrentToken(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	f := filepath.Join(t.TempDir(), "token")
	tr := &authTransport{tokenFile: f, base: http.DefaultTransport}
	c := &http.Client{Transport: tr}

	do := func() {
		resp, err := c.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	os.WriteFile(f, []byte("first\n"), 0600)
	do()
	if seen != "Bearer first" {
		t.Fatalf("Authorization = %q", seen)
	}
	os.WriteFile(f, []byte("second\n"), 0600)
	do()
	if seen != "Bearer second" {
		t.Fatalf("token was not re-read: %q", seen)
	}
}

func TestAuthTransportFailsWithoutToken(t *testing.T) {
	c := &http.Client{Transport: &authTransport{tokenFile: filepath.Join(t.TempDir(), "missing"), base: http.DefaultTransport}}
	if _, err := c.Get("http://127.0.0.1:1/"); err == nil {
		t.Fatal("expected an error when the token file is gone")
	}
}
