// Package version identifies the build. The release build sets Version with
// -ldflags "-X github.com/SteadyInfraTech/steady-image-cache/internal/version.Version=v1.0.0".
package version

import (
	"fmt"
	"runtime"
)

// Version is the release the binary was built from, or "dev" for a build made
// without it.
var Version = "dev"

// String is the one-line description printed by --version and at startup.
func String() string {
	return fmt.Sprintf("sic %s (%s %s/%s)", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// Requested reports whether the command line asks for the version. SIC takes
// all its settings from the environment, so this is the only argument it knows.
func Requested(args []string) bool {
	if len(args) != 1 {
		return false
	}
	switch args[0] {
	case "--version", "-version", "version":
		return true
	}
	return false
}
