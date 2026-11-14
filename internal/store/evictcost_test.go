package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestEvictScanCost measures what one eviction costs as the cache grows: it reads
// the blob directory, stats every file and sorts, then removes about 5%. It is
// a measurement, not a check, and runs only with SIC_EVICT_COST=1 (a table is
// printed). The numbers decide whether eviction needs an index.
func TestEvictScanCost(t *testing.T) {
	if os.Getenv("SIC_EVICT_COST") != "1" {
		t.Skip("set SIC_EVICT_COST=1 to measure the cost of an eviction")
	}
	for _, n := range []int{1000, 10000, 50000, 100000} {
		root := t.TempDir()
		dir := filepath.Join(root, "blobs", "sha256")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-time.Hour)
		for i := 0; i < n; i++ {
			sum := sha256.Sum256([]byte(fmt.Sprint(i)))
			p := filepath.Join(dir, hex.EncodeToString(sum[:]))
			if err := os.WriteFile(p, make([]byte, 1024), 0o644); err != nil {
				t.Fatal(err)
			}
			_ = os.Chtimes(p, old, old)
		}
		s, err := New(root)
		if err != nil {
			t.Fatal(err)
		}
		s.SetEvictMinAge(0)
		before := s.Used()
		start := time.Now()
		s.evict(1)
		took := time.Since(start)
		blobs, _ := s.Evicted()
		t.Logf("EVICT blobs=%d took=%v evicted=%d (%.1f%% of the used bytes)", n, took.Round(time.Millisecond), blobs, 100*float64(before-s.Used())/float64(before))
	}
}
