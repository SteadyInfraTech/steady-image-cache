package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExposesEveryCounter(t *testing.T) {
	m := &Metrics{}
	m.DiskFull.Add(3)
	m.StoreErrors.Add(2)
	m.ImportErrors.Add(4)
	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"sic_local_hits_total 0", "sic_peer_errors_total 0", "sic_verify_errors_total 0",
		"sic_disk_full_total 3", "sic_store_errors_total 2", "sic_containerd_import_errors_total 4",
		"# TYPE sic_disk_full_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q:\n%s", want, body)
		}
	}
}
