// Package health holds the handlers behind the probe endpoints.
package health

import (
	"net/http"
)

// Live answers 200 for as long as the HTTP server can answer at all. It checks
// nothing else on purpose: a liveness probe that fails restarts the process, and
// a restart interrupts every transfer under way, so only a hung server should
// cause one.
func Live(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// Ready returns a handler that answers 200 when check passes and 503, with the
// reason, when it does not. Readiness only decides whether the other nodes see
// this one as a peer: the registry itself keeps answering containerd on the node
// either way.
func Ready(check func() error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if err := check(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}
