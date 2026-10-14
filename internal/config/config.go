package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen, CacheDir string
	// AdminListen, when set, serves /healthz and /metrics over plain HTTP on a
	// separate address. The main listener may require client certificates,
	// which kubelet probes cannot present.
	AdminListen string
	Peers       []string
	PeerPort    int

	UpstreamTimeout time.Duration
	UpstreamEnabled bool
	// UpstreamRegistries lists the registries SIC may contact when upstream
	// fallback is enabled. Anything else named in ?ns= is refused.
	UpstreamRegistries []string

	KubernetesDiscovery                               bool
	PodNamespace, PodName, NodeName, PodLabelSelector string

	TLSCert, TLSKey, TLSCA, TLSServerName string
	ClientCert, ClientKey                 string

	ContainerdAddress, ContainerdNamespace string
	ContainerdImport                       bool
	ImportInterval                         time.Duration
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// boolenv reads a boolean. An unset or unparsable value yields the default, so
// a typo cannot silently flip a default-on setting off.
func boolenv(k string, d bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	b, e := strconv.ParseBool(v)
	if e != nil {
		return d
	}
	return b
}

func list(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func FromEnv() Config {
	c := Config{
		Listen:              env("SIC_LISTEN", ":5000"),
		CacheDir:            env("SIC_CACHE_DIR", "/var/lib/steady-image-cache"),
		AdminListen:         os.Getenv("SIC_ADMIN_LISTEN"),
		PeerPort:            5000,
		UpstreamTimeout:     30 * time.Second,
		UpstreamEnabled:     boolenv("SIC_UPSTREAM_ENABLED", false),
		UpstreamRegistries:  list(os.Getenv("SIC_UPSTREAM_REGISTRIES")),
		KubernetesDiscovery: boolenv("SIC_KUBERNETES_DISCOVERY", true),
		PodNamespace:        env("POD_NAMESPACE", "kube-system"),
		PodName:             os.Getenv("POD_NAME"),
		NodeName:            os.Getenv("NODE_NAME"),
		PodLabelSelector:    env("SIC_POD_LABEL_SELECTOR", "app=steady-image-cache"),
		TLSCert:             os.Getenv("SIC_TLS_CERT"),
		TLSKey:              os.Getenv("SIC_TLS_KEY"),
		TLSCA:               os.Getenv("SIC_TLS_CA"),
		ClientCert:          os.Getenv("SIC_TLS_CLIENT_CERT"),
		ClientKey:           os.Getenv("SIC_TLS_CLIENT_KEY"),
		TLSServerName:       env("SIC_TLS_SERVER_NAME", "steady-image-cache"),
		ContainerdAddress:   env("SIC_CONTAINERD_ADDRESS", "/run/containerd/containerd.sock"),
		ContainerdNamespace: env("SIC_CONTAINERD_NAMESPACE", "k8s.io"),
		ContainerdImport:    boolenv("SIC_CONTAINERD_IMPORT", true),
		ImportInterval:      5 * time.Minute,
	}
	if v := os.Getenv("SIC_PEER_PORT"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			c.PeerPort = n
		}
	}
	c.Peers = list(os.Getenv("SIC_PEERS"))
	if v := os.Getenv("SIC_UPSTREAM_TIMEOUT"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			c.UpstreamTimeout = d
		}
	}
	if v := os.Getenv("SIC_IMPORT_INTERVAL"); v != "" {
		// A non-positive interval would make time.NewTicker panic.
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			c.ImportInterval = d
		}
	}
	return c
}
