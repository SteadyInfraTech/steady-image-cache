package peer

// EndpointSlice peer discovery for clusters with >500 nodes.
//
// The Kubernetes discovery.k8s.io/v1 EndpointSlice API batches endpoints into
// slices of ≤100 entries each. Compared to listing all Pods individually:
//
//  - The API server serialises far less data per watch event (only the changed
//    slice, not the whole pod), so it uses less CPU and memory on the server.
//  - Watch streams carry slices grouped by label selector, so the client sees
//    small, targeted events instead of single-pod churn across a flat list.
//  - EndpointSlices carry topology hints (forZone) that cloud-managed
//    clusters use for zone-local load balancing.
//
// When SIC_ENDPOINTSLICE_DISCOVERY=true, EndpointSliceKubernetes is used
// instead of (or alongside) the pod-based Kubernetes discovery. Both
// implement the Finder/Poker interfaces so they compose transparently in Multi.
//
// RBAC: the service account needs:
//
//	apiGroups: ["discovery.k8s.io"]
//	resources: ["endpointslices"]
//	verbs:     ["get", "list", "watch"]

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
	"sync"
	"time"
)

// endpointSlice is the relevant subset of discovery.k8s.io/v1 EndpointSlice.
type endpointSlice struct {
	Metadata struct {
		Name              string            `json:"name"`
		ResourceVersion   string            `json:"resourceVersion"`
		DeletionTimestamp *string           `json:"deletionTimestamp"`
		Labels            map[string]string `json:"labels"`
	} `json:"metadata"`
	Endpoints []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready *bool `json:"ready"`
		} `json:"conditions"`
		Zone *string `json:"zone"` // topology hint (Kubernetes ≥1.21)
		// TargetRef carries the pod name; unused but kept for debug logging.
		TargetRef *struct {
			Name string `json:"name"`
		} `json:"targetRef"`
	} `json:"endpoints"`
	Ports []struct {
		Port *int32 `json:"port"`
	} `json:"ports"`
}

// endpointSliceList is a list response from the API server.
type endpointSliceList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []endpointSlice `json:"items"`
}

// EndpointSliceKubernetes discovers SIC peers via EndpointSlice objects.
// It satisfies Finder and Poker.
type EndpointSliceKubernetes struct {
	namespace, selector, selfIP string
	port                        int
	scheme                      string
	client                      *http.Client
	watchClient                 *http.Client
	resolver                    *ZoneResolver // optional; nil disables zone tracking

	mu     sync.RWMutex
	peers  []string                 // sorted peer base URLs
	slices map[string]endpointSlice // by slice name, as of rv
	rv     string

	watch, noWatch bool
	poke           chan struct{}
	minGap         time.Duration
	list           func(context.Context) error
}

// NewEndpointSliceKubernetes creates a finder that uses EndpointSlice discovery.
// resolver may be nil.
func NewEndpointSliceKubernetes(namespace, selector, selfIP string, port int, scheme string, resolver *ZoneResolver) (*EndpointSliceKubernetes, error) {
	if _, err := os.ReadFile(tokenFile); err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool, err := certPoolFromPEM(ca)
	if err != nil {
		return nil, fmt.Errorf("endpointslice: %w", err)
	}
	base := tlsRoundTripper(pool)
	auth := &authTransport{tokenFile: tokenFile, base: base}
	e := &EndpointSliceKubernetes{
		namespace: namespace, selector: selector, selfIP: selfIP, port: port, scheme: scheme,
		client:      &http.Client{Timeout: 10 * time.Second, Transport: auth},
		watchClient: &http.Client{Transport: auth},
		slices:      map[string]endpointSlice{},
		poke:        make(chan struct{}, 1),
		minGap:      DefaultMinGap,
		resolver:    resolver,
	}
	e.list = e.Refresh
	return e, nil
}

// EnableWatch makes Run follow changes as they happen (recommended).
func (e *EndpointSliceKubernetes) EnableWatch() { e.watch = true }

// Candidates returns the current peer list.
func (e *EndpointSliceKubernetes) Candidates() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]string(nil), e.peers...)
}

// Poke asks for an early re-list.
func (e *EndpointSliceKubernetes) Poke() {
	select {
	case e.poke <- struct{}{}:
	default:
	}
}

// Run keeps the peer list up to date. It mirrors Kubernetes.Run behaviour.
func (e *EndpointSliceKubernetes) Run(ctx context.Context, interval time.Duration, logf func(string, ...any)) {
	for {
		started := time.Now()
		if err := e.list(ctx); err != nil {
			logf("endpointslice discovery: %v", err)
		}
		if e.watch && !e.noWatch {
			werr := e.watchOnce(ctx, 10*interval)
			if ctx.Err() != nil {
				return
			}
			if errors.Is(werr, errWatchUnsupported) {
				e.noWatch = true
				logf("endpointslice discovery: %v; listing every %v instead", werr, interval)
			} else if werr != nil {
				logf("endpointslice discovery: watch: %v", werr)
			}
			if werr == nil || errors.Is(werr, errWatchGone) {
				if wait := e.minGap - time.Since(started); wait > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(wait):
					}
				}
				continue
			}
		}
		if !e.waitForNext(ctx, started, interval) {
			return
		}
	}
}

func (e *EndpointSliceKubernetes) waitForNext(ctx context.Context, started time.Time, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	select {
	case <-ctx.Done():
		timer.Stop()
		return false
	case <-timer.C:
	case <-e.poke:
		timer.Stop()
		if wait := e.minGap - time.Since(started); wait > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(wait):
			}
		}
		select {
		case <-e.poke:
		default:
		}
	}
	return true
}

// slicesURL builds the EndpointSlice list/watch URL.
func (e *EndpointSliceKubernetes) slicesURL(q url.Values) (string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if host == "" {
		return "", fmt.Errorf("KUBERNETES_SERVICE_HOST is empty")
	}
	q.Set("labelSelector", e.selector)
	return fmt.Sprintf("https://%s/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices?%s",
		net.JoinHostPort(host, port), url.PathEscape(e.namespace), q.Encode()), nil
}

// Refresh lists all EndpointSlices and rebuilds the peer list.
func (e *EndpointSliceKubernetes) Refresh(ctx context.Context) error {
	u, err := e.slicesURL(url.Values{"resourceVersion": {"0"}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("endpointslices: %s: %s", resp.Status, string(b))
	}
	var list endpointSliceList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return err
	}
	slices := make(map[string]endpointSlice, len(list.Items))
	for _, s := range list.Items {
		slices[s.Metadata.Name] = s
	}
	e.mu.Lock()
	e.slices, e.rv = slices, list.Metadata.ResourceVersion
	e.recomputeLocked()
	e.mu.Unlock()
	return nil
}

// watchOnce follows EndpointSlice changes until the server ends the watch.
func (e *EndpointSliceKubernetes) watchOnce(ctx context.Context, maxAge time.Duration) error {
	e.mu.RLock()
	rv := e.rv
	e.mu.RUnlock()
	u, err := e.slicesURL(url.Values{
		"watch":               {"1"},
		"resourceVersion":     {rv},
		"allowWatchBookmarks": {"true"},
		"timeoutSeconds":      {strconv.Itoa(int(maxAge / time.Second))},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, maxAge+30*time.Second)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-e.poke:
			cancel()
		case <-stop:
		}
	}()
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	resp, err := e.watchClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusGone:
		return errWatchGone
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
		return fmt.Errorf("%w: %s", errWatchUnsupported, resp.Status)
	default:
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("endpointslices watch: %s: %s", resp.Status, string(b))
	}
	dec := json.NewDecoder(resp.Body)
	for {
		var ev watchEvent
		if err := dec.Decode(&ev); err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := e.applySliceEvent(ev); err != nil {
			return err
		}
	}
}

// applySliceEvent folds one watch event into the slice map.
func (e *EndpointSliceKubernetes) applySliceEvent(ev watchEvent) error {
	switch ev.Type {
	case "ADDED", "MODIFIED", "DELETED":
		var s endpointSlice
		if err := json.Unmarshal(ev.Object, &s); err != nil {
			return fmt.Errorf("endpointslice watch event: %w", err)
		}
		e.mu.Lock()
		if ev.Type == "DELETED" {
			delete(e.slices, s.Metadata.Name)
		} else {
			e.slices[s.Metadata.Name] = s
		}
		if s.Metadata.ResourceVersion != "" {
			e.rv = s.Metadata.ResourceVersion
		}
		e.recomputeLocked()
		e.mu.Unlock()
	case "BOOKMARK":
		var b struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(ev.Object, &b); err != nil {
			return fmt.Errorf("endpointslice bookmark: %w", err)
		}
		if b.Metadata.ResourceVersion != "" {
			e.mu.Lock()
			e.rv = b.Metadata.ResourceVersion
			e.mu.Unlock()
		}
	case "ERROR":
		var st struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(ev.Object, &st)
		if st.Code == http.StatusGone {
			return errWatchGone
		}
		return fmt.Errorf("endpointslice watch error %d: %s", st.Code, st.Message)
	}
	return nil
}

// recomputeLocked rebuilds peers from slices. e.mu must be held.
func (e *EndpointSliceKubernetes) recomputeLocked() {
	seen := map[string]bool{}
	var peers []string
	for _, s := range e.slices {
		if s.Metadata.DeletionTimestamp != nil {
			continue
		}
		// Determine the port from the slice; fall back to e.port.
		port := e.port
		for _, p := range s.Ports {
			if p.Port != nil {
				port = int(*p.Port)
				break
			}
		}
		for _, ep := range s.Endpoints {
			ready := ep.Conditions.Ready == nil || *ep.Conditions.Ready
			if !ready {
				continue
			}
			for _, addr := range ep.Addresses {
				if net.ParseIP(addr) == nil || addr == e.selfIP || seen[addr] {
					continue
				}
				seen[addr] = true
				base := e.scheme + "://" + net.JoinHostPort(addr, strconv.Itoa(port))
				peers = append(peers, base)
				// Update zone resolver if available.
				if e.resolver != nil && ep.Zone != nil && *ep.Zone != "" {
					e.resolver.SetZone(base, *ep.Zone)
				}
			}
		}
	}
	sort.Strings(peers)
	if e.resolver != nil {
		// A peer that is gone takes its zone with it.
		now := make(map[string]bool, len(peers))
		for _, b := range peers {
			now[b] = true
		}
		for b := range e.resolver.Zones() {
			if !now[b] {
				e.resolver.DeleteZone(b)
			}
		}
	}
	e.peers = peers
}

// certPoolFromPEM parses PEM certificates into an x509.CertPool.
func certPoolFromPEM(ca []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("no valid certificates in CA bundle")
	}
	return pool, nil
}

// tlsRoundTripper builds an http.RoundTripper that uses pool for server verification.
func tlsRoundTripper(pool *x509.CertPool) http.RoundTripper {
	return &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
}
