package peer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Finder interface{ Candidates() []string }

// Poker is a Finder that learns of its peers from somewhere slow, and can be
// told that its answer is probably out of date. Poke never blocks and may be
// called as often as is convenient; the finder decides how soon to look again.
type Poker interface{ Poke() }

type Static struct{ peers []string }

func NewStatic(p []string) *Static     { return &Static{peers: p} }
func (s *Static) Candidates() []string { return append([]string(nil), s.peers...) }

type Multi []Finder

// Poke passes the hint on to every finder that can use it.
func (m Multi) Poke() {
	for _, f := range m {
		if p, ok := f.(Poker); ok {
			p.Poke()
		}
	}
}

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

// Paths of the service account files inside a pod. Variables so tests can point
// them elsewhere.
var (
	tokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type Kubernetes struct {
	namespace, selector, selfIP string
	port                        int
	scheme                      string
	client                      *http.Client
	mu                          sync.RWMutex
	peers                       []string

	// poke asks Run to list the pods again before its interval is up. It holds
	// at most one request, so any number of pokes made while one is waiting
	// are one listing.
	poke chan struct{}
	// minGap is the shortest time between two listings, however often poked.
	minGap time.Duration
	// list does one listing; Refresh, except in tests.
	list func(context.Context) error
}

// DefaultMinGap is the shortest time between two pod listings by one node.
// Every node lists the pods, so on a cluster of N nodes a node that is poked
// all the time still costs the API server only N/DefaultMinGap listings a
// second in total.
const DefaultMinGap = 5 * time.Second

func (k *Kubernetes) setup() {
	k.poke = make(chan struct{}, 1)
	k.minGap = DefaultMinGap
	k.list = k.Refresh
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name              string  `json:"name"`
			DeletionTimestamp *string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Status struct {
			HostIP     string `json:"hostIP"`
			Phase      string `json:"phase"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

func NewKubernetes(namespace, selector, selfIP string, port int, scheme string) (*Kubernetes, error) {
	// Read the token once so a missing service account fails at startup. It is
	// read again for every request, because the kubelet rotates it.
	if _, err := os.ReadFile(tokenFile); err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		// An empty pool would pass startup and then fail every listing with an
		// unknown-authority error that does not say why.
		return nil, fmt.Errorf("no valid certificates in %s", caFile)
	}
	base := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	k := &Kubernetes{
		namespace: namespace, selector: selector, selfIP: selfIP, port: port, scheme: scheme,
		client: &http.Client{Timeout: 10 * time.Second, Transport: &authTransport{tokenFile: tokenFile, base: base}},
	}
	k.setup()
	return k, nil
}

// authTransport adds the pod's current service account token to every request.
type authTransport struct {
	tokenFile string
	base      http.RoundTripper
}

func (a *authTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	token, err := os.ReadFile(a.tokenFile)
	if err != nil {
		return nil, err
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	return a.base.RoundTrip(r)
}

// parsePeers turns a pod list into peer base URLs. It keeps only pods that are
// running, Ready and not being deleted: a pod on a failed node can stay
// "Running" for minutes, and every request sent to it waits for a timeout.
func parsePeers(pl podList, selfIP, scheme string, port int) []string {
	var peers []string
	seen := map[string]bool{}
	for _, p := range pl.Items {
		ip := p.Status.HostIP
		// hostIP comes from the API server; it becomes part of a URL, so it has
		// to be an address and nothing else.
		if net.ParseIP(ip) == nil || ip == selfIP || p.Status.Phase != "Running" || p.Metadata.DeletionTimestamp != nil || seen[ip] {
			continue
		}
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready = true
			}
		}
		if !ready {
			continue
		}
		seen[ip] = true
		peers = append(peers, scheme+"://"+net.JoinHostPort(ip, strconv.Itoa(port)))
	}
	sort.Strings(peers)
	return peers
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
	u := fmt.Sprintf("https://%s/api/v1/namespaces/%s/pods?%s",
		net.JoinHostPort(host, port), url.PathEscape(k.namespace), url.Values{"labelSelector": {k.selector}}.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
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
	peers := parsePeers(pl, k.selfIP, k.scheme, k.port)
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

// Poke asks for the pods to be listed again soon. A node that sees no peers
// calls it on a miss: a neighbour that was not Ready at the last listing, as
// when the whole DaemonSet starts together, would otherwise be unknown for up
// to the full interval. The listing is not waited for, so the request that
// poked still goes without that neighbour.
func (k *Kubernetes) Poke() {
	select {
	case k.poke <- struct{}{}:
	default: // one is waiting already
	}
}

// Run lists the pods every interval, and sooner when poked, but never twice
// within minGap.
func (k *Kubernetes) Run(ctx context.Context, interval time.Duration, logf func(string, ...any)) {
	for {
		started := time.Now()
		if err := k.list(ctx); err != nil {
			logf("peer discovery: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-k.poke:
			timer.Stop()
			if wait := k.minGap - time.Since(started); wait > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
			}
			// Pokes that came in while waiting are answered by the listing
			// that is about to happen.
			select {
			case <-k.poke:
			default:
			}
		}
	}
}
