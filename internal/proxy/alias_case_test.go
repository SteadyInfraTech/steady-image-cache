package proxy

import "testing"

// Every spelling of a registry name is one registry: the tag is remembered once.
func TestNamespaceCaseDoesNotMakeNewAliases(t *testing.T) {
	a, b := newNode(t, Options{}), newNode(t, Options{})
	a.setPeers(b)
	doc := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`)
	d := put(t, b, doc)
	if err := b.st.SetAlias("reg.example", "lib/app", "v1", d); err != nil {
		t.Fatal(err)
	}
	for _, spelling := range []string{"reg.example", "REG.example", "Reg.Example"} {
		resp, _ := get(t, a.srv.URL+"/v2/lib/app/manifests/v1?ns="+spelling)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: status %d", spelling, resp.StatusCode)
		}
	}
	if _, ok := a.st.Resolve("reg.example", "lib/app", "v1"); !ok {
		t.Fatal("the lower-case alias is missing")
	}
	for _, spelling := range []string{"REG.example", "Reg.Example"} {
		if _, ok := a.st.Resolve(spelling, "lib/app", "v1"); ok {
			t.Errorf("an alias exists for the spelling %s", spelling)
		}
	}
}
