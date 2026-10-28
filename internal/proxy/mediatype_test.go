package proxy

import (
	"strings"
	"testing"
)

// A stored JSON document is served with a media type of the registry formats or
// not at all: its own "mediaType" is the publisher's word, and an image config
// asked for by its digest is as good a document as any.
func TestManifestMediaTypeIsNeverAPage(t *testing.T) {
	for _, mt := range []string{
		"text/html", "text/html; charset=utf-8", "application/xhtml+xml", "image/svg+xml",
		"application/javascript", "application/vnd.oci.image.manifest.v1+json; x=y",
		"application/vnd.docker.x", "TEXT/HTML", "application/vnd.oci.evil",
	} {
		body := `{"mediaType":"` + mt + `","x":"<script>alert(1)</script>"}`
		n := newNode(t, Options{})
		d := digestOf([]byte(body))
		if _, err := n.st.PutVerified(d, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		resp, got := get(t, n.srv.URL+"/v2/lib/app/manifests/"+d)
		if resp.StatusCode != 200 || string(got) != body {
			t.Fatalf("%q: status %d", mt, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != defaultManifestType {
			t.Errorf("%q was served as %q", mt, ct)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%q: no nosniff", mt)
		}
		if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'none'") {
			t.Errorf("%q: no content security policy", mt)
		}
	}
}

func TestRegistryMediaTypesAreServedAsTheyAre(t *testing.T) {
	for _, mt := range []string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.docker.distribution.manifest.v1+prettyjws",
	} {
		body := `{"schemaVersion":2,"mediaType":"` + mt + `"}`
		n := newNode(t, Options{})
		d := digestOf([]byte(body))
		if _, err := n.st.PutVerified(d, strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
		resp, _ := get(t, n.srv.URL+"/v2/lib/app/manifests/"+d)
		if ct := resp.Header.Get("Content-Type"); ct != mt {
			t.Errorf("%q was served as %q", mt, ct)
		}
	}
}
