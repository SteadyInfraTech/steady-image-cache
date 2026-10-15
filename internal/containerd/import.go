package containerd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/SteadyInfraTech/steady-image-cache/internal/metrics"
	"github.com/SteadyInfraTech/steady-image-cache/internal/store"
	containerd "github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/namespaces"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type Importer struct {
	Address, Namespace string
	Store              *store.Store
	Metrics            *metrics.Metrics
}

func (i *Importer) Import(ctx context.Context) error {
	c, err := containerd.New(i.Address)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx = namespaces.WithNamespace(ctx, i.Namespace)
	cs := c.ContentStore()
	if err := cs.Walk(ctx, func(info content.Info) error {
		d := info.Digest.String()
		cached, err := cachedContentValid(i.Store, d)
		if err != nil {
			return fmt.Errorf("verify cached containerd content %s: %w", d, err)
		}
		if cached {
			return nil
		}
		ra, err := cs.ReaderAt(ctx, ocispec.Descriptor{Digest: info.Digest, Size: info.Size})
		if err != nil {
			return fmt.Errorf("open containerd content %s: %w", d, err)
		}
		// The size is known, so it also bounds the copy and keeps the import from
		// filling the disk (see Store.SetMinFree). Close the reader in this
		// callback rather than deferring it until the entire content walk ends.
		n, err := cacheContent(i.Store, d, ra)
		if err != nil {
			return fmt.Errorf("import containerd content %s: %w", d, err)
		}
		i.Metrics.ImportedBlobs.Add(1)
		i.Metrics.ImportedBytes.Add(uint64(n))
		return nil
	}); err != nil {
		return err
	}
	imgs, err := c.ListImages(ctx)
	if err != nil {
		return err
	}
	for _, img := range imgs {
		registry, repo, ref, ok := splitImageRef(img.Name())
		if !ok {
			continue
		}
		if err := i.Store.SetAlias(registry, repo, ref, img.Target().Digest.String()); err != nil {
			return fmt.Errorf("persist alias for image %q: %w", img.Name(), err)
		}
	}
	return nil
}

// cachedContentValid distinguishes a valid cached blob from a missing or corrupt
// one. Missing/corrupt blobs should be imported from containerd; other errors
// indicate that validation itself failed and must stop the import.
func cachedContentValid(s *store.Store, d string) (bool, error) {
	f, err := s.Open(d)
	if err == nil {
		if closeErr := f.Close(); closeErr != nil {
			return false, closeErr
		}
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, store.ErrDigestMismatch) {
		return false, nil
	}
	return false, err
}

// cacheContent stores a containerd reader and closes it immediately. Returning
// either error makes an incomplete import visible to the caller instead of
// reporting a successful import with missing or unverified content.
func cacheContent(dst *store.Store, d string, ra content.ReaderAt) (int64, error) {
	n, putErr := dst.PutVerifiedLimit(d, io.NewSectionReader(ra, 0, ra.Size()), ra.Size())
	closeErr := ra.Close()
	if putErr != nil {
		return n, fmt.Errorf("store content: %w", putErr)
	}
	if closeErr != nil {
		return n, fmt.Errorf("close content reader: %w", closeErr)
	}
	return n, nil
}

// splitImageRef splits an image name into registry, repository and tag. A name
// may be name, name:tag, name@digest or name:tag@digest. A reference that is
// only a digest is not worth an alias, since a digest resolves to itself, so
// it is reported as not ok; with both tag and digest the tag is the alias.
func splitImageRef(s string) (registry, repo, ref string, ok bool) {
	slash := strings.IndexByte(s, '/')
	if slash <= 0 {
		return "", "", "", false
	}
	registry, rest := s[:slash], s[slash+1:]
	hasDigest := false
	if at := strings.LastIndex(rest, "@"); at > 0 {
		rest, hasDigest = rest[:at], true
	}
	repo, tag := rest, ""
	if colon := strings.LastIndex(rest, ":"); colon > strings.LastIndex(rest, "/") {
		repo, tag = rest[:colon], rest[colon+1:]
	}
	switch {
	case tag != "":
		return registry, repo, tag, true
	case hasDigest:
		return "", "", "", false
	default:
		return registry, repo, "latest", true
	}
}

func (i *Importer) String() string {
	return "containerd socket=" + i.Address + " namespace=" + i.Namespace
}
