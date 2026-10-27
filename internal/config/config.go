package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen, CacheDir string
	// Insecure turns off TLS and client authentication altogether: plain HTTP,
	// open to anyone who can reach the port. For development only.
	Insecure bool
	// AdminListen, when set, serves /healthz and /metrics over plain HTTP on a
	// separate address. The main listener may require client certificates,
	// which kubelet probes cannot present.
	AdminListen string
	Peers       []string
	PeerPort    int
	// PeerTimeout is how long a peer may take to accept a connection and start
	// answering before it counts as failed. PeerBackoff is how long a failed
	// peer is then left alone.
	PeerTimeout, PeerBackoff time.Duration
	// PeerMaxTries limits how many peers one miss asks before it moves on to
	// the registry (or gives up). Zero means no limit.
	PeerMaxTries int
	// Coordinate makes nodes that miss on the same blob at the same moment
	// share one fetch through that blob's coordinator node. CoordWait is how
	// long the coordinator may take to start delivering before the asking node
	// fetches on its own. It needs the node's own address (HOST_IP with
	// Kubernetes discovery) and is otherwise off.
	Coordinate bool
	CoordWait  time.Duration
	// PeerWatch makes Kubernetes discovery follow pod changes with a watch
	// instead of listing every 30 seconds. It falls back to listing when the
	// service account may not watch.
	PeerWatch bool
	// MinFreeBytes and MinFreePercent are the free disk space that must remain
	// after a download; the larger applies. MaxCacheBytes limits the cache size
	// (zero: no limit). When a download would break either, the least recently
	// used blobs are evicted.
	MinFreeBytes   int64
	MinFreePercent int
	MaxCacheBytes  int64

	// ScrubInterval is the time between complete background passes that check
	// every cached blob against its digest (zero: never). ScrubRate limits how
	// fast they read, in bytes per second.
	ScrubInterval time.Duration
	ScrubRate     int64

	// TagTTL is how long a tag keeps pointing at the digest it was last
	// confirmed to have, when its registry can be asked (upstream fallback on
	// and the registry allowed). Zero: never ask again.
	TagTTL time.Duration

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
		Insecure:            boolenv("SIC_INSECURE", false),
		AdminListen:         os.Getenv("SIC_ADMIN_LISTEN"),
		PeerPort:            5000,
		PeerTimeout:         5 * time.Second,
		PeerBackoff:         30 * time.Second,
		PeerMaxTries:        4,
		Coordinate:          boolenv("SIC_COORDINATE", true),
		PeerWatch:           boolenv("SIC_PEER_WATCH", true),
		CoordWait:           20 * time.Second,
		MinFreeBytes:        1 << 30,
		MinFreePercent:      20,
		ScrubInterval:       24 * time.Hour,
		ScrubRate:           32 << 20,
		TagTTL:              10 * time.Minute,
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
		// Nodes normally use one certificate both to serve and to call peers.
		ClientCert:          env("SIC_TLS_CLIENT_CERT", os.Getenv("SIC_TLS_CERT")),
		ClientKey:           env("SIC_TLS_CLIENT_KEY", os.Getenv("SIC_TLS_KEY")),
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
	if v := os.Getenv("SIC_TAG_TTL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d >= 0 {
			c.TagTTL = d
		}
	}
	if v := os.Getenv("SIC_PEER_TIMEOUT"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			c.PeerTimeout = d
		}
	}
	if v := os.Getenv("SIC_PEER_BACKOFF"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d >= 0 {
			c.PeerBackoff = d
		}
	}
	if v := os.Getenv("SIC_COORDINATE_WAIT"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d > 0 {
			c.CoordWait = d
		}
	}
	if v := os.Getenv("SIC_PEER_MAX_TRIES"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 0 {
			c.PeerMaxTries = n
		}
	}
	if v := os.Getenv("SIC_MIN_FREE_BYTES"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil && n >= 0 {
			c.MinFreeBytes = n
		}
	}
	if v := os.Getenv("SIC_MIN_FREE_PERCENT"); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n >= 0 && n <= 90 {
			c.MinFreePercent = n
		}
	}
	if v := os.Getenv("SIC_MAX_CACHE_BYTES"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil && n >= 0 {
			c.MaxCacheBytes = n
		}
	}
	if v := os.Getenv("SIC_SCRUB_INTERVAL"); v != "" {
		if d, e := time.ParseDuration(v); e == nil && d >= 0 {
			c.ScrubInterval = d
		}
	}
	if v := os.Getenv("SIC_SCRUB_RATE"); v != "" {
		if n, e := strconv.ParseInt(v, 10, 64); e == nil && n > 0 {
			c.ScrubRate = n
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

// Validate rejects configurations that would leave the registry open or unable
// to talk to its peers. Without it, a missing SIC_TLS_CA quietly turns "mutual
// TLS" into TLS that accepts any client, and a missing certificate into plain
// HTTP on every interface.
func (c Config) Validate() error {
	if c.Insecure {
		return nil
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("SIC_TLS_CERT and SIC_TLS_KEY must be set together")
	}
	var missing []string
	for _, v := range []struct{ name, val string }{
		{"SIC_TLS_CERT", c.TLSCert},
		{"SIC_TLS_KEY", c.TLSKey},
		{"SIC_TLS_CA", c.TLSCA},
		{"SIC_TLS_CLIENT_CERT", c.ClientCert},
		{"SIC_TLS_CLIENT_KEY", c.ClientKey},
	} {
		if v.val == "" {
			missing = append(missing, v.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("mutual TLS is required but %s not set; set the missing settings, or set SIC_INSECURE=true to run without any authentication (development only)",
			plural(missing))
	}
	return nil
}

func plural(names []string) string {
	out := names[0]
	for _, n := range names[1:] {
		out += ", " + n
	}
	if len(names) == 1 {
		return out + " is"
	}
	return out + " are"
}
