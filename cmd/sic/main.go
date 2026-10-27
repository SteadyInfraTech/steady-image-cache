package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/config"
	cd "github.com/SteadyInfraTech/steady-image-cache/internal/containerd"
	"github.com/SteadyInfraTech/steady-image-cache/internal/health"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/proxy"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"github.com/SteadyInfraTech/steady-image-cache/internal/tlsutil"
	"github.com/SteadyInfraTech/steady-image-cache/internal/version"
)

func main() {
	if version.Requested(os.Args[1:]) {
		fmt.Println(version.String())
		return
	}
	log.Print(version.String())
	cfg := config.FromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}
	useTLS := !cfg.Insecure
	// One reloader serves the listener and, when the node uses the same
	// certificate to call its peers, the peer client too.
	var serverTLS, clientTLS *tlsutil.Reloader
	if useTLS {
		var err error
		if serverTLS, err = tlsutil.New(cfg.TLSCert, cfg.TLSKey, cfg.TLSCA, log.Printf); err != nil {
			log.Fatalf("tls: %v", err)
		}
		clientTLS = serverTLS
		if cfg.ClientCert != cfg.TLSCert || cfg.ClientKey != cfg.TLSKey {
			if clientTLS, err = tlsutil.New(cfg.ClientCert, cfg.ClientKey, cfg.TLSCA, log.Printf); err != nil {
				log.Fatalf("tls client: %v", err)
			}
		}
		// A certificate that peers or containerd will refuse shows up as handshake
		// errors that do not say why; say it here, now and after every rotation.
		if clientTLS == serverTLS {
			serverTLS.Audit(cfg.TLSServerName, tlsutil.Server|tlsutil.Client)
		} else {
			serverTLS.Audit(cfg.TLSServerName, tlsutil.Server)
			clientTLS.Audit(cfg.TLSServerName, tlsutil.Client)
		}
	} else {
		log.Printf("WARNING: SIC_INSECURE=true: serving plain HTTP with no authentication; anyone who can reach %s can read the cache", cfg.Listen)
	}

	st, err := store.New(cfg.CacheDir)
	if err != nil {
		log.Fatal(err)
	}
	st.SetMinFree(cfg.MinFreeBytes)
	st.SetMinFreePercent(cfg.MinFreePercent)
	st.SetMaxBytes(cfg.MaxCacheBytes)
	st.SetLogf(log.Printf)
	m := &metrics.Metrics{}
	m.Funcs = []metrics.Func{
		{Name: "sic_corrupt_blobs_total", Type: "counter", Value: func() int64 { return int64(st.Corrupt()) }},
		{Name: "sic_cache_bytes", Type: "gauge", Value: st.Used},
		{Name: "sic_evicted_blobs_total", Type: "counter", Value: func() int64 { n, _ := st.Evicted(); return int64(n) }},
		{Name: "sic_evicted_bytes_total", Type: "counter", Value: func() int64 { _, n := st.Evicted(); return int64(n) }},
	}
	// Verification happens in the background, off the request path: the record
	// of verified blobs is saved regularly and a slow pass re-checks them all.
	bgDone := make(chan struct{})
	go func() {
		defer close(bgDone)
		st.RunBackground(ctx, store.Background{ScrubEvery: cfg.ScrubInterval, ScrubRate: cfg.ScrubRate, Logf: log.Printf})
	}()
	var finders peer.Multi
	if len(cfg.Peers) > 0 {
		finders = append(finders, peer.NewStatic(cfg.Peers))
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	var self string // this node as the others list it, for coordination
	if cfg.KubernetesDiscovery {
		if ip := os.Getenv("HOST_IP"); net.ParseIP(ip) != nil {
			self = scheme + "://" + net.JoinHostPort(ip, strconv.Itoa(cfg.PeerPort))
		}
		k, err := peer.NewKubernetes(cfg.PodNamespace, cfg.PodLabelSelector, os.Getenv("HOST_IP"), cfg.PeerPort, scheme)
		if err != nil {
			log.Printf("kubernetes discovery disabled: %v", err)
		} else {
			finders = append(finders, k)
			go k.Run(ctx, 30*time.Second, log.Printf)
		}
	}
	// How many peers this node would ask right now. Zero on a node whose
	// neighbours exist but are not Ready yet, or have not been listed yet.
	m.Funcs = append(m.Funcs, metrics.Func{Name: "sic_peers", Type: "gauge", Value: func() int64 { return int64(len(finders.Candidates())) }})

	// The import walks the whole containerd content store, which can take a long
	// time on a busy node. It runs in the background so the registry is already
	// serving while it works.
	if cfg.ContainerdImport {
		imp := &cd.Importer{Address: cfg.ContainerdAddress, Namespace: cfg.ContainerdNamespace, Store: st, Metrics: m}
		go func() {
			if err := imp.Import(ctx); err != nil {
				log.Printf("initial %s import: %v", imp, err)
			}
			t := time.NewTicker(cfg.ImportInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := imp.Import(ctx); err != nil {
						log.Printf("containerd import: %v", err)
					}
				}
			}
		}()
	}

	h, err := proxy.New(st, finders, m, proxy.Options{
		Timeout:            cfg.UpstreamTimeout,
		PeerTimeout:        cfg.PeerTimeout,
		PeerBackoff:        cfg.PeerBackoff,
		PeerMaxTries:       cfg.PeerMaxTries,
		Self:               self,
		Coordinate:         cfg.Coordinate,
		CoordWait:          cfg.CoordWait,
		TagTTL:             cfg.TagTTL,
		Upstream:           cfg.UpstreamEnabled,
		UpstreamRegistries: cfg.UpstreamRegistries,
		TLS:                clientTLS,
		ServerName:         cfg.TLSServerName,
		Logf:               log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	if cfg.UpstreamEnabled && len(cfg.UpstreamRegistries) == 0 {
		log.Printf("upstream fallback is enabled but SIC_UPSTREAM_REGISTRIES is empty: no upstream registry will be contacted")
	}

	mux := http.NewServeMux()
	mux.Handle("/v2/", h)
	mux.Handle("/v2", h)
	mux.Handle("/metrics", m)
	mux.HandleFunc("/healthz", health.Live)
	mux.Handle("/readyz", health.Ready(st.Writable))
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// Health and metrics on a plain-HTTP address for kubelet probes and local
	// scraping; they stay on the main listener too.
	var admin *http.Server
	if cfg.AdminListen != "" {
		am := http.NewServeMux()
		am.Handle("/metrics", m)
		am.HandleFunc("/healthz", health.Live)
		am.Handle("/readyz", health.Ready(st.Writable))
		admin = &http.Server{Addr: cfg.AdminListen, Handler: am, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			log.Printf("SIC admin HTTP %s", cfg.AdminListen)
			if e := admin.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				log.Fatal(e)
			}
		}()
	}

	if useTLS {
		srv.TLSConfig = serverTLS.ServerConfig()
		go func() {
			log.Printf("SIC HTTPS %s peers=%v", cfg.Listen, finders.Candidates())
			if e := srv.ListenAndServeTLS("", ""); e != nil && e != http.ErrServerClosed {
				log.Fatal(e)
			}
		}()
	} else {
		go func() {
			log.Printf("SIC HTTP %s peers=%v", cfg.Listen, finders.Candidates())
			if e := srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				log.Fatal(e)
			}
		}()
	}

	<-ctx.Done()
	c, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	_ = srv.Shutdown(c)
	if admin != nil {
		_ = admin.Shutdown(c)
	}
	select { // RunBackground saves the verified-blob record on its way out
	case <-bgDone:
	case <-c.Done():
	}
}
