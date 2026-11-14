// Package preload pulls images through a node's own SIC so that the cache holds
// them before a rollout asks for them. It speaks to SIC as containerd does: the
// registry API, with ?ns= naming the registry. It adds no way into the cache.
package preload

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// Options configures a run.
type Options struct {
	// Addr is SIC's base URL, e.g. https://127.0.0.1:5000.
	Addr string
	// Client is the HTTP client to reach SIC with (its TLS configuration carries the client certificate).
	Client *http.Client
	// Platform is "os/arch" ("linux/amd64"); only that platform of an index is pulled.
	Platform string
	// Username and Password are for a registry's token service. SIC never holds
	// them; they are used here to get a token that SIC then forwards.
	Username, Password string
	// AllowPlainHTTPRealm lets a token service at an http:// address receive the
	// credentials (development only). By default only https realms do.
	AllowPlainHTTPRealm bool
	// Parallel is how many blobs are fetched at once (default 4).
	Parallel int
	// AdminURL, when set, is SIC's admin address (http://127.0.0.1:5001); the
	// byte counters from it before and after give the sources of the bytes.
	AdminURL string
	// Logf receives progress lines.
	Logf func(string, ...any)
}

// Result is what one image took.
type Result struct {
	Image          string
	ManifestDigest string
	Blobs          int
	Bytes          int64
}

// Sources are the node's byte counters over a run: what it took from peers and
// from registries, which is what a preload is for.
type Sources struct {
	FromPeers, FromRegistries int64
	Known                     bool
}

// Ref is a parsed image reference.
type Ref struct {
	Registry, Repo, Reference string // Reference is a tag or a sha256: digest
}

// ParseRef splits "registry/repo/path:tag" or "...@sha256:..." . A reference with
// no registry host is refused: SIC has to be told which registry the content is of.
func ParseRef(s string) (Ref, error) {
	i := strings.Index(s, "/")
	if i <= 0 {
		return Ref{}, fmt.Errorf("%q: want registry/repository[:tag|@digest]", s)
	}
	reg, rest := s[:i], s[i+1:]
	if !strings.ContainsAny(reg, ".:") && reg != "localhost" {
		return Ref{}, fmt.Errorf("%q: the first part %q is not a registry host (write docker.io/library/%s for a Docker Hub image)", s, reg, s)
	}
	ref := Ref{Registry: reg, Reference: "latest"}
	// repo[:tag][@digest]. With both, the digest is the reference and the tag
	// is only a comment on it.
	var digest string
	if j := strings.Index(rest, "@"); j >= 0 {
		rest, digest = rest[:j], rest[j+1:]
	}
	repo, tag := rest, ""
	if j := strings.LastIndex(rest, ":"); j >= 0 && !strings.Contains(rest[j:], "/") {
		repo, tag = rest[:j], rest[j+1:]
	}
	ref.Repo = repo
	switch {
	case digest != "":
		ref.Reference = digest
	case tag != "":
		ref.Reference = tag
	}
	if ref.Repo == "" || ref.Reference == "" {
		return Ref{}, fmt.Errorf("%q: empty repository or reference", s)
	}
	if reg == "docker.io" && !strings.Contains(ref.Repo, "/") {
		ref.Repo = "library/" + ref.Repo
	}
	return ref, nil
}

const accept = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

type descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	Platform  *struct {
		OS, Architecture, Variant string
	} `json:"platform"`
}

type manifest struct {
	MediaType string       `json:"mediaType"`
	Config    *descriptor  `json:"config"`
	Layers    []descriptor `json:"layers"`
	Manifests []descriptor `json:"manifests"`
}

type runner struct {
	o      Options
	mu     sync.Mutex
	tokens map[string]string // by scope
}

// Run pulls every image and returns what each took and, when AdminURL is set,
// where the node's bytes came from over the whole run.
func Run(ctx context.Context, o Options, images []string) ([]Result, Sources, error) {
	if o.Client == nil || o.Addr == "" {
		return nil, Sources{}, errors.New("preload: no address or client")
	}
	if o.Parallel <= 0 {
		o.Parallel = 4
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	r := &runner{o: o, tokens: map[string]string{}}
	var src Sources
	var before [2]int64
	if o.AdminURL != "" {
		if p, u, err := r.counters(ctx); err == nil {
			before, src.Known = [2]int64{p, u}, true
		} else {
			o.Logf("preload: the admin address could not be read, the sources of the bytes are not reported: %v", err)
		}
	}
	var out []Result
	var firstErr error
	for _, img := range images {
		res, err := r.image(ctx, img)
		if err != nil {
			o.Logf("preload: %s: %v", img, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", img, err)
			}
			continue
		}
		out = append(out, res)
	}
	if src.Known {
		if p, u, err := r.counters(ctx); err == nil {
			src.FromPeers, src.FromRegistries = p-before[0], u-before[1]
		} else {
			src.Known = false
		}
	}
	return out, src, firstErr
}

func (r *runner) image(ctx context.Context, s string) (Result, error) {
	ref, err := ParseRef(s)
	if err != nil {
		return Result{}, err
	}
	body, mdigest, err := r.getManifest(ctx, ref, ref.Reference)
	if err != nil {
		return Result{}, err
	}
	var m manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return Result{}, fmt.Errorf("manifest: %w", err)
	}
	// An index names manifests per platform, and may itself name indexes: go
	// down until a manifest that names a config and layers.
	for depth := 0; len(m.Manifests) > 0; depth++ {
		if depth == 4 {
			return Result{}, errors.New("indexes nested more than four deep")
		}
		d, err := r.pick(m.Manifests)
		if err != nil {
			return Result{}, err
		}
		if body, mdigest, err = r.getManifest(ctx, ref, d); err != nil {
			return Result{}, err
		}
		m = manifest{}
		if err := json.Unmarshal(body, &m); err != nil {
			return Result{}, fmt.Errorf("manifest %s: %w", d, err)
		}
	}
	if m.Config == nil && len(m.Layers) == 0 {
		return Result{}, fmt.Errorf("manifest %s names no config and no layers (an artifact this tool does not pull)", mdigest)
	}
	var blobs []descriptor
	if m.Config != nil {
		blobs = append(blobs, *m.Config)
	}
	blobs = append(blobs, m.Layers...)
	res := Result{Image: s, ManifestDigest: mdigest}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, r.o.Parallel)
	var firstErr error
	for _, b := range blobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(b descriptor) {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := r.getBlob(ctx, ref, b.Digest)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("blob %s: %w", b.Digest, err)
				}
				return
			}
			res.Blobs++
			res.Bytes += n
		}(b)
	}
	wg.Wait()
	if firstErr != nil {
		return Result{}, firstErr
	}
	r.o.Logf("preload: %s: %s, %d blobs, %d bytes", s, mdigest, res.Blobs, res.Bytes)
	return res, nil
}

func (r *runner) pick(ms []descriptor) (string, error) {
	want := r.o.Platform
	for _, d := range ms {
		if d.Platform == nil {
			continue
		}
		if d.Platform.OS+"/"+d.Platform.Architecture == want {
			return d.Digest, nil
		}
	}
	return "", fmt.Errorf("the index has no manifest for %s", want)
}

func (r *runner) getManifest(ctx context.Context, ref Ref, reference string) ([]byte, string, error) {
	resp, err := r.get(ctx, ref, "manifests/"+reference, accept)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, "", err
	}
	d := resp.Header.Get("Docker-Content-Digest")
	sum := sha256.Sum256(b)
	got := "sha256:" + hex.EncodeToString(sum[:])
	if strings.HasPrefix(reference, "sha256:") && got != reference {
		return nil, "", fmt.Errorf("manifest %s came back as %s", reference, got)
	}
	if d == "" {
		d = got
	}
	return b, d, nil
}

// getBlob reads the blob to the end, checking its digest, and returns its size.
func (r *runner) getBlob(ctx context.Context, ref Ref, digest string) (int64, error) {
	if !strings.HasPrefix(digest, "sha256:") {
		return 0, fmt.Errorf("unsupported digest %q", digest)
	}
	resp, err := r.get(ctx, ref, "blobs/"+digest, "")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	h := sha256.New()
	n, err := io.Copy(h, resp.Body)
	if err != nil {
		return n, err
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != digest {
		return n, fmt.Errorf("content is %s", got)
	}
	return n, nil
}

// get asks SIC for a path of the repository, answering a token challenge once.
func (r *runner) get(ctx context.Context, ref Ref, path, accept string) (*http.Response, error) {
	u := strings.TrimRight(r.o.Addr, "/") + "/v2/" + ref.Repo + "/" + path + "?ns=" + url.QueryEscape(ref.Registry)
	scope := "repository:" + ref.Repo + ":pull"
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		r.mu.Lock()
		tok := r.tokens[ref.Registry+" "+scope]
		r.mu.Unlock()
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := r.o.Client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		challenge := resp.Header.Get("WWW-Authenticate")
		status := resp.StatusCode
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		if status != http.StatusUnauthorized || attempt == 1 {
			return nil, fmt.Errorf("%s: %s", path, strconv.Itoa(status)+" "+http.StatusText(status))
		}
		newTok, err := r.token(ctx, challenge, scope)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		r.mu.Lock()
		r.tokens[ref.Registry+" "+scope] = newTok
		r.mu.Unlock()
	}
	return nil, errors.New("unreachable")
}

// token answers a Bearer challenge with the registry's token service.
func (r *runner) token(ctx context.Context, challenge, scope string) (string, error) {
	p, ok := parseBearer(challenge)
	if !ok {
		return "", fmt.Errorf("the registry wants authentication this tool cannot answer (%q)", challenge)
	}
	q := url.Values{}
	if p["service"] != "" {
		q.Set("service", p["service"])
	}
	if p["scope"] != "" {
		q.Set("scope", p["scope"])
	} else {
		q.Set("scope", scope)
	}
	realm, err := url.Parse(p["realm"])
	if err != nil || realm.Host == "" {
		return "", fmt.Errorf("the registry's token realm %q is not a URL", p["realm"])
	}
	// A challenge comes from the registry (through SIC), and the credentials go
	// where it says: only to an https realm, unless plain http is allowed for
	// development.
	if realm.Scheme != "https" && (realm.Scheme != "http" || !r.o.AllowPlainHTTPRealm) {
		return "", fmt.Errorf("the registry's token realm %q is not https; credentials are not sent to it", p["realm"])
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p["realm"]+"?"+q.Encode(), nil)
	if err != nil {
		return "", err
	}
	if r.o.Username != "" {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(r.o.Username+":"+r.o.Password)))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token service answered %d", resp.StatusCode)
	}
	var t struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&t); err != nil {
		return "", fmt.Errorf("token service: %w", err)
	}
	if t.Token != "" {
		return t.Token, nil
	}
	if t.AccessToken != "" {
		return t.AccessToken, nil
	}
	return "", errors.New("token service gave no token")
}

func parseBearer(h string) (map[string]string, bool) {
	rest, ok := strings.CutPrefix(h, "Bearer ")
	if !ok {
		return nil, false
	}
	p := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			p[k] = strings.Trim(v, `"`)
		}
	}
	return p, p["realm"] != ""
}

// counters reads the two byte counters from the admin address.
func (r *runner) counters(ctx context.Context) (peers, upstream int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(r.o.AdminURL, "/")+"/metrics", nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, 0, err
	}
	var gotP, gotU bool
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		switch f[0] {
		case "sic_bytes_from_peers_total":
			peers, err = strconv.ParseInt(f[1], 10, 64)
			gotP = err == nil
		case "sic_bytes_from_upstream_total":
			upstream, err = strconv.ParseInt(f[1], 10, 64)
			gotU = err == nil
		}
	}
	if !gotP || !gotU {
		return 0, 0, errors.New("the byte counters are not in the metrics")
	}
	return peers, upstream, nil
}
