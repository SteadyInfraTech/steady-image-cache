package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBoolenvKeepsDefaultOnGarbage(t *testing.T) {
	t.Setenv("SIC_CONTAINERD_IMPORT", "treu")
	if !FromEnv().ContainerdImport {
		t.Fatal("a typo must not turn a default-on setting off")
	}
	t.Setenv("SIC_CONTAINERD_IMPORT", "false")
	if FromEnv().ContainerdImport {
		t.Fatal("explicit false must be honoured")
	}
}

func TestLists(t *testing.T) {
	t.Setenv("SIC_UPSTREAM_REGISTRIES", " docker.io, ghcr.io ,,")
	t.Setenv("SIC_PEERS", "https://10.0.0.1:5000")
	c := FromEnv()
	if want := []string{"docker.io", "ghcr.io"}; !reflect.DeepEqual(c.UpstreamRegistries, want) {
		t.Fatalf("registries = %v", c.UpstreamRegistries)
	}
	if want := []string{"https://10.0.0.1:5000"}; !reflect.DeepEqual(c.Peers, want) {
		t.Fatalf("peers = %v", c.Peers)
	}
}

func TestNonPositiveDurationsIgnored(t *testing.T) {
	t.Setenv("SIC_IMPORT_INTERVAL", "0s")
	t.Setenv("SIC_UPSTREAM_TIMEOUT", "-5s")
	c := FromEnv()
	if c.ImportInterval != 5*time.Minute || c.UpstreamTimeout != 30*time.Second {
		t.Fatalf("got %v %v", c.ImportInterval, c.UpstreamTimeout)
	}
}

func full() Config {
	return Config{TLSCert: "c", TLSKey: "k", TLSCA: "ca", ClientCert: "cc", ClientKey: "ck"}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring; empty means valid
	}{
		{"complete mTLS", func(c *Config) {}, ""},
		{"no TLS at all", func(c *Config) { *c = Config{} }, "SIC_TLS_CERT"},
		{"TLS without a CA would not authenticate clients", func(c *Config) { c.TLSCA = "" }, "SIC_TLS_CA is not set"},
		{"cert without key", func(c *Config) { c.TLSKey = "" }, "must be set together"},
		{"key without cert", func(c *Config) { c.TLSCert = "" }, "must be set together"},
		{"no client certificate for peer calls", func(c *Config) { c.ClientCert, c.ClientKey = "", "" }, "SIC_TLS_CLIENT_CERT, SIC_TLS_CLIENT_KEY are not set"},
		{"explicitly insecure", func(c *Config) { *c = Config{Insecure: true} }, ""},
	}
	for _, tc := range cases {
		c := full()
		tc.mutate(&c)
		err := c.Validate()
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", tc.name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: error %v, want one containing %q", tc.name, err, tc.wantErr)
		}
	}
}

func TestClientCertDefaultsToServerCert(t *testing.T) {
	t.Setenv("SIC_TLS_CERT", "/tls/tls.crt")
	t.Setenv("SIC_TLS_KEY", "/tls/tls.key")
	c := FromEnv()
	if c.ClientCert != "/tls/tls.crt" || c.ClientKey != "/tls/tls.key" {
		t.Fatalf("client cert = %q %q", c.ClientCert, c.ClientKey)
	}
	t.Setenv("SIC_TLS_CLIENT_CERT", "/other.crt")
	t.Setenv("SIC_TLS_CLIENT_KEY", "/other.key")
	if c := FromEnv(); c.ClientCert != "/other.crt" || c.ClientKey != "/other.key" {
		t.Fatalf("explicit client cert ignored: %q %q", c.ClientCert, c.ClientKey)
	}
}

func TestPeerAndDiskSettings(t *testing.T) {
	c := FromEnv()
	if c.PeerTimeout != 5*time.Second || c.PeerBackoff != 30*time.Second || c.MinFreeBytes != 1<<30 {
		t.Fatalf("defaults: %v %v %d", c.PeerTimeout, c.PeerBackoff, c.MinFreeBytes)
	}
	t.Setenv("SIC_PEER_TIMEOUT", "2s")
	t.Setenv("SIC_PEER_BACKOFF", "0s")
	t.Setenv("SIC_MIN_FREE_BYTES", "0")
	c = FromEnv()
	if c.PeerTimeout != 2*time.Second || c.PeerBackoff != 0 || c.MinFreeBytes != 0 {
		t.Fatalf("overrides: %v %v %d", c.PeerTimeout, c.PeerBackoff, c.MinFreeBytes)
	}
	t.Setenv("SIC_PEER_TIMEOUT", "0s")
	t.Setenv("SIC_MIN_FREE_BYTES", "-1")
	c = FromEnv()
	if c.PeerTimeout != 5*time.Second || c.MinFreeBytes != 1<<30 {
		t.Fatalf("invalid values must fall back to defaults: %v %d", c.PeerTimeout, c.MinFreeBytes)
	}
}

func TestScrubSettings(t *testing.T) {
	c := FromEnv()
	if c.ScrubInterval != 24*time.Hour || c.ScrubRate != 32<<20 {
		t.Fatalf("defaults: interval %v, rate %d", c.ScrubInterval, c.ScrubRate)
	}
	t.Setenv("SIC_SCRUB_INTERVAL", "0")
	t.Setenv("SIC_SCRUB_RATE", "1048576")
	c = FromEnv()
	if c.ScrubInterval != 0 || c.ScrubRate != 1<<20 {
		t.Fatalf("interval %v, rate %d: 0 must turn the scrub off, and the rate must be read", c.ScrubInterval, c.ScrubRate)
	}
	t.Setenv("SIC_SCRUB_INTERVAL", "-1h")
	t.Setenv("SIC_SCRUB_RATE", "0")
	c = FromEnv()
	if c.ScrubInterval != 24*time.Hour || c.ScrubRate != 32<<20 {
		t.Fatalf("invalid values must keep the defaults: interval %v, rate %d", c.ScrubInterval, c.ScrubRate)
	}
}

func TestCacheLimitSettings(t *testing.T) {
	c := FromEnv()
	if c.MinFreePercent != 20 || c.MaxCacheBytes != 0 {
		t.Fatalf("defaults: percent %d, max %d", c.MinFreePercent, c.MaxCacheBytes)
	}
	t.Setenv("SIC_MIN_FREE_PERCENT", "35")
	t.Setenv("SIC_MAX_CACHE_BYTES", "5368709120")
	c = FromEnv()
	if c.MinFreePercent != 35 || c.MaxCacheBytes != 5<<30 {
		t.Fatalf("percent %d, max %d", c.MinFreePercent, c.MaxCacheBytes)
	}
	t.Setenv("SIC_MIN_FREE_PERCENT", "0")
	if c = FromEnv(); c.MinFreePercent != 0 {
		t.Fatalf("0 must switch the percentage reserve off, got %d", c.MinFreePercent)
	}
	t.Setenv("SIC_MAX_CACHE_BYTES", "")
	for _, bad := range []string{"-1", "91", "lots"} {
		t.Setenv("SIC_MIN_FREE_PERCENT", bad)
		if c = FromEnv(); c.MinFreePercent != 20 {
			t.Fatalf("percent %q must keep the default, got %d", bad, c.MinFreePercent)
		}
	}
	t.Setenv("SIC_MIN_FREE_PERCENT", "")
	for _, bad := range []string{"-1", "lots"} {
		t.Setenv("SIC_MAX_CACHE_BYTES", bad)
		if c = FromEnv(); c.MaxCacheBytes != 0 {
			t.Fatalf("size %q must keep the default, got %d", bad, c.MaxCacheBytes)
		}
	}
}

func TestTagTTL(t *testing.T) {
	if got := FromEnv().TagTTL; got != 10*time.Minute {
		t.Fatalf("default = %v", got)
	}
	for _, tc := range []struct {
		env  string
		want time.Duration
	}{
		{"30s", 30 * time.Second},
		{"24h", 24 * time.Hour},
		{"0", 0}, // never confirm again
		{"-5m", 10 * time.Minute},
		{"soon", 10 * time.Minute},
	} {
		t.Setenv("SIC_TAG_TTL", tc.env)
		if got := FromEnv().TagTTL; got != tc.want {
			t.Errorf("SIC_TAG_TTL=%q: %v, want %v", tc.env, got, tc.want)
		}
	}
}

func TestPeerMaxTries(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want int
	}{{"", 4}, {"2", 2}, {"0", 0}, {"-1", 4}, {"x", 4}} {
		t.Setenv("SIC_PEER_MAX_TRIES", tc.env)
		if got := FromEnv().PeerMaxTries; got != tc.want {
			t.Fatalf("SIC_PEER_MAX_TRIES=%q: got %d, want %d", tc.env, got, tc.want)
		}
	}
}

func TestCoordinateSettings(t *testing.T) {
	t.Setenv("SIC_COORDINATE", "")
	t.Setenv("SIC_COORDINATE_WAIT", "")
	if c := FromEnv(); !c.Coordinate || c.CoordWait != 20*time.Second {
		t.Fatalf("defaults: %v, %v", c.Coordinate, c.CoordWait)
	}
	t.Setenv("SIC_COORDINATE", "false")
	t.Setenv("SIC_COORDINATE_WAIT", "7s")
	if c := FromEnv(); c.Coordinate || c.CoordWait != 7*time.Second {
		t.Fatalf("set: %v, %v", c.Coordinate, c.CoordWait)
	}
	for _, bad := range []string{"0", "-1s", "x"} {
		t.Setenv("SIC_COORDINATE_WAIT", bad)
		if got := FromEnv().CoordWait; got != 20*time.Second {
			t.Fatalf("SIC_COORDINATE_WAIT=%q: got %v", bad, got)
		}
	}
}
