package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	// errWatchGone: the version to resume from is too old for the server. List
	// again.
	errWatchGone = errors.New("the watch's resource version is gone")
	// errWatchUnsupported: this service account may not watch pods.
	errWatchUnsupported = errors.New("the API server refuses to watch pods")
)

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// watchOnce follows the pods from the version the last listing returned until
// the server ends the watch after about maxAge (nil), the version is gone
// (errWatchGone), a poke asks for a fresh listing (nil), or it fails.
func (k *Kubernetes) watchOnce(ctx context.Context, maxAge time.Duration) error {
	k.mu.RLock()
	rv := k.rv
	k.mu.RUnlock()
	u, err := k.podsURL(url.Values{
		"watch":               {"1"},
		"resourceVersion":     {rv},
		"allowWatchBookmarks": {"true"},
		"timeoutSeconds":      {strconv.Itoa(int(maxAge / time.Second))},
	})
	if err != nil {
		return err
	}
	// The server ends the watch at maxAge; the context is for one that does not.
	ctx, cancel := context.WithTimeout(ctx, maxAge+30*time.Second)
	defer cancel()
	// A poke means "list again soon": stop watching so that Run does.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-k.poke:
			cancel()
		case <-stop:
		}
	}()

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	resp, err := k.watchClient.Do(req)
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
		return fmt.Errorf("kubernetes pods watch: %s: %s", resp.Status, string(b))
	}
	dec := json.NewDecoder(resp.Body)
	for {
		var ev watchEvent
		if err := dec.Decode(&ev); err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil // the server's end of the watch, or a poke
			}
			return err
		}
		if err := k.apply(ev); err != nil {
			return err
		}
	}
}

// apply folds one watch event into the pods and recomputes the peers.
func (k *Kubernetes) apply(ev watchEvent) error {
	switch ev.Type {
	case "ADDED", "MODIFIED", "DELETED":
		var p pod
		if err := json.Unmarshal(ev.Object, &p); err != nil {
			return fmt.Errorf("watch event: %w", err)
		}
		k.mu.Lock()
		if ev.Type == "DELETED" {
			delete(k.pods, p.Metadata.Name)
		} else {
			k.pods[p.Metadata.Name] = p
		}
		if p.Metadata.ResourceVersion != "" {
			k.rv = p.Metadata.ResourceVersion
		}
		k.recomputeLocked()
		k.mu.Unlock()
	case "BOOKMARK":
		// Only says how far the server has got, so a resume starts late enough.
		var b struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(ev.Object, &b); err != nil {
			return fmt.Errorf("watch bookmark: %w", err)
		}
		if b.Metadata.ResourceVersion != "" {
			k.mu.Lock()
			k.rv = b.Metadata.ResourceVersion
			k.mu.Unlock()
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
		return fmt.Errorf("watch error %d: %s", st.Code, st.Message)
	}
	return nil
}

// recomputeLocked rebuilds the peers from the pods. k.mu must be held.
func (k *Kubernetes) recomputeLocked() {
	pl := podList{Items: make([]pod, 0, len(k.pods))}
	for _, p := range k.pods {
		pl.Items = append(pl.Items, p)
	}
	k.peers = parsePeers(pl, k.selfIP, k.scheme, k.port)
}
