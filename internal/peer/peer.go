package peer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
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
	watchClient                 *http.Client // no overall timeout: a watch is long
	mu                          sync.RWMutex // guards peers, pods and rv
	peers                       []string
	pods                        map[string]pod // by name, as of rv
	rv                          string         // resourceVersion to watch from

	// watch makes Run follow pod changes as they happen instead of listing on a
	// timer. noWatch is set once the API server refused to watch (no "watch"
	// permission): discovery then lists on the interval as it does without.
	watch, noWatch bool

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

type pod struct {
	Metadata struct {
		Name              string  `json:"name"`
		ResourceVersion   string  `json:"resourceVersion"`
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
}

type podList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []pod `json:"items"`
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
	auth := &authTransport{tokenFile: tokenFile, base: base}
	k := &Kubernetes{
		namespace: namespace, selector: selector, selfIP: selfIP, port: port, scheme: scheme,
		client:      &http.Client{Timeout: 10 * time.Second, Transport: auth},
		watchClient: &http.Client{Transport: auth},
		pods:        map[string]pod{},
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
	u, err := k.podsURL(url.Values{
		// Zero lets the API server answer from its own cache, which costs far
		// less than the quorum read of etcd that an unset version means, and is
		// no problem here: a watch from the list's version catches up with
		// anything the cache was behind on.
		"resourceVersion": {"0"},
	})
	if err != nil {
		return err
	}
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
	pods := make(map[string]pod, len(pl.Items))
	for _, p := range pl.Items {
		pods[p.Metadata.Name] = p
	}
	k.mu.Lock()
	k.pods, k.rv = pods, pl.Metadata.ResourceVersion
	k.peers = parsePeers(pl, k.selfIP, k.scheme, k.port)
	k.mu.Unlock()
	return nil
}

// podsURL is the address of the labelled pods, with q added to the query.
func (k *Kubernetes) podsURL(q url.Values) (string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return "", fmt.Errorf("KUBERNETES_SERVICE_HOST is empty")
	}
	q.Set("labelSelector", k.selector)
	return fmt.Sprintf("https://%s/api/v1/namespaces/%s/pods?%s",
		net.JoinHostPort(host, port), url.PathEscape(k.namespace), q.Encode()), nil
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

// EnableWatch makes Run follow changes to the pods as they happen. A node then
// learns of a neighbour becoming Ready, or going away, at once and not at the
// next listing, and the API server is asked for one list and one long-lived
// watch per node instead of a list every interval.
func (k *Kubernetes) EnableWatch() { k.watch = true }

// Run keeps the peers up to date. Without a watch it lists the pods every
// interval, and sooner when poked, but never twice within minGap. With one it
// lists, then follows the changes since that listing; it lists again when the
// watch ends (the server ends it after 10 intervals, which is also the
// safety net against a missed event), when the server says the version it was
// asked to resume from is gone, and when poked, within the same minGap.
func (k *Kubernetes) Run(ctx context.Context, interval time.Duration, logf func(string, ...any)) {
	for {
		started := time.Now()
		err := k.list(ctx)
		if err != nil {
			logf("peer discovery: %v", err)
		}
		if k.watch && !k.noWatch && err == nil {
			werr := k.watchOnce(ctx, 10*interval)
			if ctx.Err() != nil {
				return
			}
			if errors.Is(werr, errWatchUnsupported) {
				k.noWatch = true
				logf("peer discovery: %v; listing every %v instead (grant the \"watch\" verb on pods to follow changes as they happen)", werr, interval)
			} else if werr != nil {
				logf("peer discovery: watch: %v", werr)
			}
			if werr == nil || errors.Is(werr, errWatchGone) {
				// The watch ended as it is meant to: list again, not sooner
				// than minGap after the last listing.
				if wait := k.minGap - time.Since(started); wait > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(wait):
					}
				}
				continue
			}
		}
		if !k.waitForNext(ctx, started, interval) {
			return
		}
	}
}

// waitForNext waits for the interval, or for a poke and then for minGap since
// started. It returns false when ctx is done.
func (k *Kubernetes) waitForNext(ctx context.Context, started time.Time, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	select {
	case <-ctx.Done():
		timer.Stop()
		return false
	case <-timer.C:
	case <-k.poke:
		timer.Stop()
		if wait := k.minGap - time.Since(started); wait > 0 {
			select {
			case <-ctx.Done():
				return false
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
	return true
}
