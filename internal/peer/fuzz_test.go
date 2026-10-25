package peer

import (
	"encoding/json"
	"net"
	"net/url"
	"strings"
	"testing"
)

// The pod list comes from the API server and its host addresses become URLs this
// node connects to, so whatever the document holds, only plain addresses may
// come out.
func FuzzParsePeers(f *testing.F) {
	f.Add([]byte(`{"items":[{"metadata":{"name":"a"},"status":{"hostIP":"10.0.0.2","phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}]}`), "10.0.0.1")
	f.Add([]byte(`{"items":[{"status":{"hostIP":"evil.example/x?","phase":"Running"}}]}`), "")
	f.Add([]byte(`{"items":null}`), "fd00::1")
	f.Fuzz(func(t *testing.T, doc []byte, self string) {
		var pl podList
		if json.Unmarshal(doc, &pl) != nil {
			return
		}
		for _, base := range parsePeers(pl, self, "https", 5000) {
			u, err := url.Parse(base)
			if err != nil {
				t.Fatalf("peer %q is not a URL: %v", base, err)
			}
			host, port, err := net.SplitHostPort(u.Host)
			if err != nil || net.ParseIP(host) == nil || port != "5000" || u.Path != "" || u.RawQuery != "" || u.User != nil || !strings.HasPrefix(base, "https://") {
				t.Fatalf("peer %q is not a bare https://ip:port", base)
			}
		}
	})
}
