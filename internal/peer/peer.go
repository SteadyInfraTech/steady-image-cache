package peer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Finder interface{ Candidates() []string }
type Static struct{ peers []string }

func NewStatic(p []string) *Static     { return &Static{peers: p} }
func (s *Static) Candidates() []string { return append([]string(nil), s.peers...) }

type Multi []Finder

func (m Multi) Candidates() []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range m {
		for _, p := range f.Candidates() {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Strings(out)
	return out
}

type Kubernetes struct {
	namespace, selector, selfIP string
	port                        int
	scheme                      string
	client                      *http.Client
	mu                          sync.RWMutex
	peers                       []string
}
type podList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			HostIP string `json:"hostIP"`
			Phase  string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

func NewKubernetes(namespace, selector, selfIP string, port int, scheme string) (*Kubernetes, error) {
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	k := &Kubernetes{namespace: namespace, selector: selector, selfIP: selfIP, port: port, scheme: scheme, client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}}
	k.client.Transport = &authTransport{token: strings.TrimSpace(string(token)), base: k.client.Transport}
	return k, nil
}

type authTransport struct {
	token string
	base  http.RoundTripper
}

func (a *authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+a.token)
	return a.base.RoundTrip(r)
}
func (k *Kubernetes) Refresh(ctx context.Context) error {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return fmt.Errorf("KUBERNETES_SERVICE_HOST is empty")
	}
	u := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/pods?labelSelector=%s", host, port, k.namespace, strings.ReplaceAll(k.selector, "=", "%3D"))
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("kubernetes pods: %s: %s", resp.Status, string(b))
	}
	var pl podList
	if err := json.NewDecoder(resp.Body).Decode(&pl); err != nil {
		return err
	}
	var peers []string
	seen := map[string]bool{}
	for _, p := range pl.Items {
		ip := p.Status.HostIP
		if ip == "" || ip == k.selfIP || p.Status.Phase != "Running" || seen[ip] {
			continue
		}
		seen[ip] = true
		peers = append(peers, fmt.Sprintf("%s://%s:%d", k.scheme, ip, k.port))
	}
	sort.Strings(peers)
	k.mu.Lock()
	k.peers = peers
	k.mu.Unlock()
	return nil
}
func (k *Kubernetes) Candidates() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return append([]string(nil), k.peers...)
}
func (k *Kubernetes) Run(ctx context.Context, interval time.Duration, logf func(string, ...any)) {
	for {
		if err := k.Refresh(ctx); err != nil {
			logf("peer discovery: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
