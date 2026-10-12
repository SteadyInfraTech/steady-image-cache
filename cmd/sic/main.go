package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/SteadyInfraTech/steady-image-cache/internal/config"
	cd "github.com/SteadyInfraTech/steady-image-cache/internal/containerd"
	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/peer"
	"github.com/SteadyInfraTech/steady-image-cache/internal/proxy"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.FromEnv()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
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
	if cfg.TLSCert != "" {
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
	if cfg.ContainerdImport {
		imp := &cd.Importer{Address: cfg.ContainerdAddress, Namespace: cfg.ContainerdNamespace, Store: st, Metrics: m}
		if err := imp.Import(ctx); err != nil {
			log.Printf("initial %s import: %v", imp, err)
		}
		go func() {
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
	h, err := proxy.New(st, finders, cfg.UpstreamTimeout, m, cfg.UpstreamEnabled, cfg.TLSCA, cfg.ClientCert, cfg.ClientKey, cfg.TLSServerName)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v2/", h)
	mux.Handle("/v2", h)
	mux.Handle("/metrics", m)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.TLSCA != "" {
			b, e := os.ReadFile(cfg.TLSCA)
			if e != nil {
				log.Fatal(e)
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(b)
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
}
