package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen, CacheDir                                  string
	Peers                                             []string
	PeerPort                                          int
	UpstreamTimeout                                   time.Duration
	UpstreamEnabled                                   bool
	KubernetesDiscovery                               bool
	PodNamespace, PodName, NodeName, PodLabelSelector string
	TLSCert, TLSKey, TLSCA, TLSServerName             string
	ClientCert, ClientKey                             string
	ContainerdAddress, ContainerdNamespace            string
	ContainerdImport                                  bool
	ImportInterval                                    time.Duration
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func boolenv(k string, d bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	b, e := strconv.ParseBool(v)
	return e == nil && b
}
func FromEnv() Config {
	c := Config{Listen: env("SIC_LISTEN", ":5000"), CacheDir: env("SIC_CACHE_DIR", "/var/lib/steady-image-cache"), PeerPort: 5000, UpstreamTimeout: 30 * time.Second, UpstreamEnabled: boolenv("SIC_UPSTREAM_ENABLED", false), KubernetesDiscovery: boolenv("SIC_KUBERNETES_DISCOVERY", true), PodNamespace: env("POD_NAMESPACE", "kube-system"), PodName: os.Getenv("POD_NAME"), NodeName: os.Getenv("NODE_NAME"), PodLabelSelector: env("SIC_POD_LABEL_SELECTOR", "app=steady-image-cache"), TLSCert: os.Getenv("SIC_TLS_CERT"), TLSKey: os.Getenv("SIC_TLS_KEY"), TLSCA: os.Getenv("SIC_TLS_CA"), ClientCert: os.Getenv("SIC_TLS_CLIENT_CERT"), ClientKey: os.Getenv("SIC_TLS_CLIENT_KEY"), TLSServerName: env("SIC_TLS_SERVER_NAME", "steady-image-cache"), ContainerdAddress: env("SIC_CONTAINERD_ADDRESS", "/run/containerd/containerd.sock"), ContainerdNamespace: env("SIC_CONTAINERD_NAMESPACE", "k8s.io"), ContainerdImport: boolenv("SIC_CONTAINERD_IMPORT", true), ImportInterval: 5 * time.Minute}
	if v := os.Getenv("SIC_PEER_PORT"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			c.PeerPort = n
		}
	}
	if v := os.Getenv("SIC_PEERS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				c.Peers = append(c.Peers, p)
			}
		}
	}
	if v := os.Getenv("SIC_UPSTREAM_TIMEOUT"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			c.UpstreamTimeout = d
		}
	}
	if v := os.Getenv("SIC_IMPORT_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil {
			c.ImportInterval = d
		}
	}
	return c
}
