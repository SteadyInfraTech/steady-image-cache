package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/SteadyInfraTech/steady-image-cache/internal/preload"
)

// runPreload implements `sic preload`: pull images through this node's SIC so
// the cache holds them before a rollout asks for them.
func runPreload(args []string) error {
	fs := flag.NewFlagSet("preload", flag.ContinueOnError)
	addr := fs.String("addr", "https://127.0.0.1:5000", "SIC's address on this node")
	ca := fs.String("ca", "/etc/steady-image-cache/ca.crt", "CA that signed SIC's certificate")
	cert := fs.String("cert", "/etc/steady-image-cache/tls.crt", "client certificate (the one containerd uses)")
	key := fs.String("key", "/etc/steady-image-cache/tls.key", "key of the client certificate")
	serverName := fs.String("server-name", envOr("SIC_TLS_SERVER_NAME", "steady-image-cache"), "name SIC's certificate carries")
	platform := fs.String("platform", runtime.GOOS+"/"+runtime.GOARCH, "platform of a multi-platform image to pull")
	file := fs.String("f", "", "file with one image per line (# starts a comment)")
	admin := fs.String("admin", "", "SIC's admin address (http://127.0.0.1:5001): also report where the bytes came from")
	parallel := fs.Int("parallel", 4, "blobs fetched at once")
	plain := fs.Bool("plain-http", false, "talk to SIC without TLS, and send credentials to an http token service (development only; SIC_INSECURE=true)")
	timeout := fs.Duration("timeout", 30*time.Minute, "give up after this long")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: sic preload [flags] registry/repository[:tag|@digest] ...")
		fmt.Fprintln(os.Stderr, "Registry credentials, if the registry needs them: SIC_PRELOAD_USERNAME and SIC_PRELOAD_PASSWORD in the environment.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	images := fs.Args()
	if *file != "" {
		more, err := readImageList(*file)
		if err != nil {
			return err
		}
		images = append(images, more...)
	}
	if len(images) == 0 {
		fs.Usage()
		return fmt.Errorf("no images given")
	}

	client := &http.Client{Timeout: 0}
	base := *addr
	if *plain {
		base = strings.Replace(base, "https://", "http://", 1)
	} else {
		cfg, err := clientTLS(*ca, *cert, *key, *serverName)
		if err != nil {
			return err
		}
		client.Transport = &http.Transport{TLSClientConfig: cfg}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	res, src, err := preload.Run(ctx, preload.Options{
		Addr: base, Client: client, Platform: *platform,
		Username: os.Getenv("SIC_PRELOAD_USERNAME"), Password: os.Getenv("SIC_PRELOAD_PASSWORD"),
		Parallel: *parallel, AdminURL: *admin, Logf: log.Printf, AllowPlainHTTPRealm: *plain,
	}, images)
	var blobs int
	var bytes int64
	for _, r := range res {
		blobs += r.Blobs
		bytes += r.Bytes
	}
	fmt.Printf("preloaded %d of %d images: %d blobs, %d bytes\n", len(res), len(images), blobs, bytes)
	if src.Known {
		fmt.Printf("this node took %d bytes from peers and %d bytes from registries meanwhile\n", src.FromPeers, src.FromRegistries)
	}
	return err
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func readImageList(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "#"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line != "" {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}

func clientTLS(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read the CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s holds no certificate", caPath)
	}
	c, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load the client certificate: %w", err)
	}
	return &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{c}, ServerName: serverName, MinVersion: tls.VersionTLS12}, nil
}
