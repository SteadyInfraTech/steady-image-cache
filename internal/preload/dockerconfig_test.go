package preload

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDockerCredentialsReadsAuthsPerRegistry(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString([]byte("alice:s3:cret"))
	p := writeConfig(t, `{"auths":{
		"https://index.docker.io/v1/":{"auth":"`+enc+`"},
		"ghcr.io":{"username":"bob","password":"pw"},
		"registry.example.com:5000":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("carol:x"))+`"},
		"empty.example":{},
		"bad.example":{"auth":"!!not base64!!"},
		"nocolon.example":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("nocolon"))+`"}}}`)
	lookup, helpers, err := DockerCredentials(p)
	if err != nil || helpers {
		t.Fatalf("DockerCredentials = %v, helpers %v", err, helpers)
	}
	for _, c := range []struct{ reg, user, pass string }{
		{"docker.io", "alice", "s3:cret"}, // the password may contain a colon
		{"registry-1.docker.io", "alice", "s3:cret"},
		{"GHCR.io", "bob", "pw"},
		{"registry.example.com:5000", "carol", "x"},
		{"registry.example.com", "", ""}, // another port is another registry
		{"empty.example", "", ""},
		{"bad.example", "", ""},
		{"nocolon.example", "", ""},
		{"other.example", "", ""},
	} {
		if u, pw := lookup(c.reg); u != c.user || pw != c.pass {
			t.Errorf("lookup(%q) = %q, %q; want %q, %q", c.reg, u, pw, c.user, c.pass)
		}
	}
}

func TestDockerCredentialsMissingMalformedAndHelpers(t *testing.T) {
	if l, _, err := DockerCredentials(filepath.Join(t.TempDir(), "none.json")); err != nil || l != nil {
		t.Fatalf("a missing file is not an error and gives no lookup: %v, %v", l != nil, err)
	}
	if l, _, err := DockerCredentials(""); err != nil || l != nil {
		t.Fatalf("no path: %v, %v", l != nil, err)
	}
	if _, _, err := DockerCredentials(writeConfig(t, `{not json`)); err == nil {
		t.Fatal("a malformed file was accepted")
	}
	_, helpers, err := DockerCredentials(writeConfig(t, `{"credsStore":"desktop"}`))
	if err != nil || !helpers {
		t.Fatalf("credsStore: helpers %v, %v", helpers, err)
	}
	_, helpers, err = DockerCredentials(writeConfig(t, `{"credHelpers":{"ecr.example":"ecr-login"}}`))
	if err != nil || !helpers {
		t.Fatalf("credHelpers: helpers %v, %v", helpers, err)
	}
}

func TestDockerConfigPath(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", "/x/dc")
	if got := DockerConfigPath(); got != filepath.Join("/x/dc", "config.json") {
		t.Fatalf("DOCKER_CONFIG: %q", got)
	}
	t.Setenv("DOCKER_CONFIG", "")
	t.Setenv("HOME", "/home/u")
	if got := DockerConfigPath(); !strings.HasSuffix(got, filepath.Join(".docker", "config.json")) {
		t.Fatalf("default: %q", got)
	}
}

// The lookup is used only for the registry it names, and the environment
// credentials win for theirs.
func TestPreloadLookupCredentialsAreForTheirRegistryOnly(t *testing.T) {
	var auth string
	tok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"token":"T"}`)
	}))
	defer tok.Close()
	ch := `Bearer realm="` + tok.URL + `/t",service="s"`
	cfg := writeConfig(t, `{"auths":{"ghcr.io":{"username":"bob","password":"pw"}}}`)
	lookup, _, err := DockerCredentials(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{o: Options{Lookup: lookup, AllowPlainHTTPRealm: true}}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("bob:pw"))
	if _, err := r.token(context.Background(), ch, "repository:a/b:pull", "ghcr.io"); err != nil || auth != want {
		t.Fatalf("ghcr.io got %q (%v), want %q", auth, err, want)
	}
	auth = ""
	if _, err := r.token(context.Background(), ch, "repository:a/b:pull", "evil.example"); err != nil || auth != "" {
		t.Fatalf("credentials for ghcr.io were sent for another registry: %q (%v)", auth, err)
	}
	// Environment credentials for a registry take precedence over the file.
	r.o.Username, r.o.Password, r.o.CredentialRegistry = "env", "ep", "ghcr.io"
	want = "Basic " + base64.StdEncoding.EncodeToString([]byte("env:ep"))
	if _, err := r.token(context.Background(), ch, "repository:a/b:pull", "ghcr.io"); err != nil || auth != want {
		t.Fatalf("environment credentials did not win: %q (%v)", auth, err)
	}
}
