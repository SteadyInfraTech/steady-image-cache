package config

import (
	"reflect"
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
