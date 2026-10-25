package proxy

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

// The request path, the Retry-After header and manifests are all text that comes
// from clients, peers and registries. These targets check that no input makes
// the parsers panic or accept what they promise to refuse.

func FuzzSplitPath(f *testing.F) {
	f.Add("/v2/library/app/blobs/sha256:"+strings.Repeat("a", 64), "/blobs/")
	f.Add("/v2/a/b/manifests/latest", "/manifests/")
	f.Add("/v2/", "/blobs/")
	f.Add("/v2//blobs/", "/blobs/")
	f.Fuzz(func(t *testing.T, path, marker string) {
		repo, ref, ok := splitPath(path, marker)
		if !ok {
			return
		}
		if !strings.HasPrefix(path, "/v2/") || repo == "" {
			t.Fatalf("splitPath(%q, %q) = %q, %q accepted a path it should refuse", path, marker, repo, ref)
		}
	})
}

func FuzzValidRepoAndRef(f *testing.F) {
	f.Add("library/app", "latest")
	f.Add("a/b/c", "sha256:"+strings.Repeat("0", 64))
	f.Add("../etc", "..")
	f.Add("a%2fb", "x y")
	f.Fuzz(func(t *testing.T, repo, ref string) {
		// Both end up in URLs and cache keys: whatever is accepted must hold
		// nothing that could change what a path or a key means.
		if validRepo(repo) && strings.ContainsAny(repo, " \t\r\n%?#\\") {
			t.Fatalf("validRepo accepted %q", repo)
		}
		if validRef(ref) && strings.ContainsAny(ref, "/ \t\r\n%?#\\") {
			t.Fatalf("validRef accepted %q", ref)
		}
	})
}

func FuzzRetryAfterOf(f *testing.F) {
	f.Add("120")
	f.Add("Wed, 21 Oct 2026 07:28:00 GMT")
	f.Add("-1")
	f.Add("99999999999999999999")
	f.Add("1\r\nSet-Cookie: x")
	f.Fuzz(func(t *testing.T, v string) {
		resp := &http.Response{Header: http.Header{"Retry-After": {v}}}
		got := retryAfterOf(resp)
		// What is relayed to a client must not be able to add a header line.
		if strings.ContainsAny(got, "\r\n") {
			t.Fatalf("retryAfterOf(%q) = %q contains a line break", v, got)
		}
	})
}

func FuzzManifestType(f *testing.F) {
	f.Add([]byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`))
	f.Add([]byte(`{"manifests":[]}`))
	f.Add([]byte(`null`))
	f.Add([]byte(strings.Repeat("[", 20000)))
	f.Fuzz(func(t *testing.T, data []byte) {
		if got := manifestType(bytes.NewReader(data)); got == "" {
			t.Fatal("manifestType returned an empty media type")
		}
	})
}
