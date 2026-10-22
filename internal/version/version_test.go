package version

import (
	"strings"
	"testing"
)

func TestRequested(t *testing.T) {
	for _, c := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"--version"}, true},
		{[]string{"-version"}, true},
		{[]string{"version"}, true},
		{[]string{"--versio"}, false},
		{[]string{"--version", "extra"}, false},
		{[]string{"serve"}, false},
	} {
		if got := Requested(c.args); got != c.want {
			t.Errorf("Requested(%q) = %v, want %v", c.args, got, c.want)
		}
	}
}

func TestStringNamesTheVersionAndPlatform(t *testing.T) {
	old := Version
	defer func() { Version = old }()
	Version = "v1.2.3"
	s := String()
	for _, want := range []string{"sic v1.2.3", "go1.", "/"} {
		if !strings.Contains(s, want) {
			t.Errorf("%q does not contain %q", s, want)
		}
	}
	if strings.Contains(s, "\n") {
		t.Errorf("%q is not one line", s)
	}
}
