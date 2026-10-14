package containerd

import (
	"context"
	"io"
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
		if _, err := i.Store.Stat(d); err == nil {
			return nil
		}
		ra, err := cs.ReaderAt(ctx, ocispec.Descriptor{Digest: info.Digest, Size: info.Size})
		if err != nil {
			return nil
		}
		defer ra.Close()
		n, err := i.Store.PutVerified(d, io.NewSectionReader(ra, 0, ra.Size()))
		if err == nil {
			i.Metrics.ImportedBlobs.Add(1)
			i.Metrics.ImportedBytes.Add(uint64(n))
		}
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
		_ = i.Store.SetAlias(registry, repo, ref, img.Target().Digest.String())
	}
	return nil
}

func splitImageRef(s string) (registry, repo, ref string, ok bool) {
	slash := strings.IndexByte(s, '/')
	if slash <= 0 {
		return "", "", "", false
	}
	registry, rest := s[:slash], s[slash+1:]
	if at := strings.LastIndex(rest, "@"); at > 0 {
		return registry, rest[:at], rest[at+1:], true
	}
	colon := strings.LastIndex(rest, ":")
	if colon <= strings.LastIndex(rest, "/") {
		return registry, rest, "latest", true
	}
	return registry, rest[:colon], rest[colon+1:], true
}

func (i *Importer) String() string {
	return "containerd socket=" + i.Address + " namespace=" + i.Namespace
}
