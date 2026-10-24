package peer

import (
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// apiServer stands in for the Kubernetes API server: a TLS server whose
// certificate is what the pod's service account CA file holds.
type apiServer struct {
	*httptest.Server
	status  atomic.Int32
	body    atomic.Value // string
	lastURL atomic.Value
	lastTok atomic.Value
}

func newAPIServer(t *testing.T) (*apiServer, *Kubernetes) {
	t.Helper()
	a := &apiServer{}
	a.status.Store(200)
	a.body.Store(`{"items":[]}`)
	a.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.lastURL.Store(r.URL.String())
		a.lastTok.Store(r.Header.Get("Authorization"))
		w.WriteHeader(int(a.status.Load()))
		w.Write([]byte(a.body.Load().(string)))
	}))
	t.Cleanup(a.Close)

	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw})
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	tok := filepath.Join(dir, "token")
	if err := os.WriteFile(tok, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldTok, oldCA := tokenFile, caFile
	tokenFile, caFile = tok, ca
	t.Cleanup(func() { tokenFile, caFile = oldTok, oldCA })

	host, port, _ := net.SplitHostPort(a.Listener.Addr().String())
	t.Setenv("KUBERNETES_SERVICE_HOST", host)
	t.Setenv("KUBERNETES_SERVICE_PORT_HTTPS", port)
	k, err := NewKubernetes("kube-system", "app=steady-image-cache,tier=cache", "10.0.0.1", 5000, "https")
	if err != nil {
		t.Fatal(err)
	}
	return a, k
}

const readyPod = `{"items":[{"metadata":{"name":"b"},"status":{"hostIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}`

// The listing has to ask for the right pods, with the pod's own token, and a
// good answer becomes the peers.
func TestRefreshListsTheLabelledPodsWithTheServiceAccountToken(t *testing.T) {
	a, k := newAPIServer(t)
	a.body.Store(readyPod)
	if err := k.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, want := k.Candidates(), []string{"https://10.0.0.2:5000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("peers %v, want %v", got, want)
	}
	u := a.lastURL.Load().(string)
	if !strings.HasPrefix(u, "/api/v1/namespaces/kube-system/pods?") {
		t.Fatalf("asked %q", u)
	}
	if !strings.Contains(u, "labelSelector=app%3Dsteady-image-cache%2Ctier%3Dcache") {
		t.Fatalf("the label selector is missing or not encoded: %q", u)
	}
	if got := a.lastTok.Load(); got != "Bearer s3cret" {
		t.Fatalf("Authorization %q", got)
	}
}

// A listing that fails must not make the node forget the peers it knew: an API
// server that is down for a minute would otherwise turn every miss into a
// registry request.
func TestFailedListingKeepsTheKnownPeers(t *testing.T) {
	a, k := newAPIServer(t)
	a.body.Store(readyPod)
	if err := k.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := k.Candidates()
	if len(want) != 1 {
		t.Fatalf("setup: peers %v", want)
	}

	a.status.Store(403)
	a.body.Store(`{"message":"pods is forbidden"}`)
	err := k.Refresh(t.Context())
	if err == nil || !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("a refused listing must say why: %v", err)
	}
	a.status.Store(200)
	a.body.Store(`{"items": [`) // cut off
	if err := k.Refresh(t.Context()); err == nil {
		t.Fatal("a broken answer was accepted")
	}
	if got := k.Candidates(); !reflect.DeepEqual(got, want) {
		t.Fatalf("peers after failed listings: %v, want %v", got, want)
	}

	// An empty answer is an answer: the neighbours really are gone.
	a.body.Store(`{"items":[]}`)
	if err := k.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := k.Candidates(); len(got) != 0 {
		t.Fatalf("peers after an empty listing: %v", got)
	}
}
