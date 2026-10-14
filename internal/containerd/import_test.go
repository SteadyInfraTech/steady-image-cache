package containerd

import "testing"

func TestSplitImageRef(t *testing.T) {
	d := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := []struct {
		in, a, b, c string
		ok          bool
	}{
		{"registry.example.com/team/api:v42", "registry.example.com", "team/api", "v42", true},
		{"docker.io/library/alpine", "docker.io", "library/alpine", "latest", true},
		{"registry.local:5000/team/api:v1", "registry.local:5000", "team/api", "v1", true},
		{"registry.local:5000/team/api", "registry.local:5000", "team/api", "latest", true},
		// name:tag@digest: the tag is the alias, and the repository keeps no ":tag".
		{"docker.io/library/nginx:1.25@" + d, "docker.io", "library/nginx", "1.25", true},
		// A digest-only reference resolves to itself and needs no alias.
		{"registry.example.com/team/api@" + d, "", "", "", false},
		{"no-slash", "", "", "", false},
		{"/leading", "", "", "", false},
	}
	for _, x := range cases {
		a, b, c, ok := splitImageRef(x.in)
		if ok != x.ok || a != x.a || b != x.b || c != x.c {
			t.Errorf("%s => %q %q %q %v, want %q %q %q %v", x.in, a, b, c, ok, x.a, x.b, x.c, x.ok)
		}
	}
}
