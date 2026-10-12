package containerd

import "testing"

func TestSplitImageRef(t *testing.T) {
	cases := []struct{ in, a, b, c string }{
		{"registry.example.com/team/api:v42", "registry.example.com", "team/api", "v42"},
		{"registry.example.com/team/api@sha256:abc", "registry.example.com", "team/api", "sha256:abc"},
		{"docker.io/library/alpine", "docker.io", "library/alpine", "latest"},
	}
	for _, x := range cases {
		a, b, c, ok := splitImageRef(x.in)
		if !ok || a != x.a || b != x.b || c != x.c {
			t.Fatalf("%s => %q %q %q %v", x.in, a, b, c, ok)
		}
	}
}
