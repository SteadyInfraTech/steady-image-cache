package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/config"
	cd "github.com/SteadyInfraTech/steady-image-cache/internal/containerd"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/proxy"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
)

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func main() {
	cfg := config.FromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		log.Fatal("SIC_TLS_CERT and SIC_TLS_KEY must be set together")
	}
	useTLS := cfg.TLSCert != ""

	st, err := store.New(cfg.CacheDir)
	if err != nil {
		log.Fatal(err)
	}
	m := &metrics.Metrics{}
	var finders peer.Multi
	if len(cfg.Peers) > 0 {
		finders = append(finders, peer.NewStatic(cfg.Peers))
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	if cfg.KubernetesDiscovery {
		k, err := peer.NewKubernetes(cfg.PodNamespace, cfg.PodLabelSelector, os.Getenv("HOST_IP"), cfg.PeerPort, scheme)
		if err != nil {
			log.Printf("kubernetes discovery disabled: %v", err)
		} else {
			finders = append(finders, k)
			go k.Run(ctx, 30*time.Second, log.Printf)
		}
	}

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
		Upstream:           cfg.UpstreamEnabled,
		UpstreamRegistries: cfg.UpstreamRegistries,
		CA:                 cfg.TLSCA,
		Cert:               cfg.ClientCert,
		Key:                cfg.ClientKey,
		ServerName:         cfg.TLSServerName,
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
	mux.HandleFunc("/healthz", healthz)
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	// Health and metrics on a plain-HTTP address for kubelet probes and local
	// scraping; they stay on the main listener too.
	var admin *http.Server
	if cfg.AdminListen != "" {
		am := http.NewServeMux()
		am.Handle("/metrics", m)
		am.HandleFunc("/healthz", healthz)
		admin = &http.Server{Addr: cfg.AdminListen, Handler: am, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			log.Printf("SIC admin HTTP %s", cfg.AdminListen)
			if e := admin.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				log.Fatal(e)
			}
		}()
	}

	if useTLS {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.TLSCA != "" {
			b, e := os.ReadFile(cfg.TLSCA)
			if e != nil {
				log.Fatal(e)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				log.Fatal("SIC_TLS_CA contains no valid certificates")
			}
			tc.ClientCAs = pool
			tc.ClientAuth = tls.RequireAndVerifyClientCert
		}
		srv.TLSConfig = tc
		go func() {
			log.Printf("SIC HTTPS %s peers=%v", cfg.Listen, finders.Candidates())
			if e := srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey); e != nil && e != http.ErrServerClosed {
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
}
