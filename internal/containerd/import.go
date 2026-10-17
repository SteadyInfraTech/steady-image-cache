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
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/namespaces"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type Importer struct {
	Address, Namespace string
	Store              *store.Store
	Metrics            *metrics.Metrics
}

// maxKeptErrors bounds how many individual import errors are remembered; the
// rest are only counted. A store with thousands of blobs that all fail would
// otherwise produce one enormous error.
const maxKeptErrors = 5

// importFailures collects what could not be imported while the import went on.
type importFailures struct {
	count int
	errs  []error
}

func (f *importFailures) add(err error) {
	f.count++
	if len(f.errs) < maxKeptErrors {
		f.errs = append(f.errs, err)
	}
}

// addAll records err, merging it when it is itself a list of failures so that
// the counts add up and the message does not nest.
func (f *importFailures) addAll(err error) {
	var inner *importFailures
	if errors.As(err, &inner) {
		f.count += inner.count
		for _, e := range inner.errs {
			if len(f.errs) < maxKeptErrors {
				f.errs = append(f.errs, e)
			}
		}
		return
	}
	f.add(err)
}

func (f *importFailures) Error() string {
	msg := fmt.Sprintf("%d import steps failed, first: %v", f.count, f.errs[0])
	if f.count > len(f.errs) {
		msg += fmt.Sprintf(" (and %d more not shown)", f.count-len(f.errs))
	}
	return msg
}

func (f *importFailures) Unwrap() []error { return f.errs }

func (f *importFailures) err() error {
	if f.count == 0 {
		return nil
	}
	return f
}

// Import copies containerd's content into the cache and records the image names
// as aliases. A step that fails does not stop the others: containerd's garbage
// collector can delete a blob between listing and reading it, and one blob that
// does not fit must not keep every later blob, or the aliases, from being
// imported. What failed is returned at the end.
func (i *Importer) Import(ctx context.Context) error {
	c, err := containerd.New(i.Address)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx = namespaces.WithNamespace(ctx, i.Namespace)
	var failed importFailures
	if err := i.importContent(ctx, c.ContentStore()); err != nil {
		failed.addAll(err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	imgs, err := c.ListImages(ctx)
	if err != nil {
		failed.add(err)
		return failed.err()
	}
	named := make([]namedImage, len(imgs))
	for n, img := range imgs {
		named[n] = img
	}
	if err := i.importAliases(named); err != nil {
		failed.addAll(err)
	}
	return failed.err()
}

// namedImage is the part of containerd.Image the alias import needs.
type namedImage interface {
	Name() string
	Target() ocispec.Descriptor
}

// importAliases records every image name as an alias for its target digest and
// keeps going when one cannot be saved.
func (i *Importer) importAliases(imgs []namedImage) error {
	var failed importFailures
	for _, img := range imgs {
		registry, repo, ref, ok := splitImageRef(img.Name())
		if !ok {
			continue
		}
		if err := i.Store.SetAlias(registry, repo, ref, img.Target().Digest.String()); err != nil {
			failed.add(fmt.Errorf("persist alias for image %q: %w", img.Name(), err))
		}
	}
	return failed.err()
}

// errStop ends the walk early; it is never reported.
var errStop = errors.New("stop walking")

// contentWalker is the narrow portion of containerd's content store needed by
// the importer. Keeping it narrow lets tests exercise the import orchestration
// without a running containerd daemon.
type contentWalker interface {
	Walk(context.Context, content.WalkFunc, ...string) error
	ReaderAt(context.Context, ocispec.Descriptor) (content.ReaderAt, error)
}

// importContent imports missing or corrupt content and leaves already-verified
// cache entries untouched. A blob that fails is recorded and the walk goes on,
// except when the cache has no room left: then every further blob would fail
// the same way, so it stops there. Blobs that containerd deleted in the
// meantime are not an error. The returned error describes what failed.
func (i *Importer) importContent(ctx context.Context, cs contentWalker) error {
	var failed importFailures
	err := cs.Walk(ctx, func(info content.Info) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		d := info.Digest.String()
		// Content the cache just evicted would only be copied back and evicted
		// again; leave it to be fetched when somebody asks for it.
		if i.Store.WasEvicted(d) {
			return nil
		}
		cached, err := cachedContentValid(i.Store, d)
		if err != nil {
			failed.add(fmt.Errorf("verify cached containerd content %s: %w", d, err))
			i.countFailure()
			return nil
		}
		if cached {
			return nil
		}
		ra, err := cs.ReaderAt(ctx, ocispec.Descriptor{Digest: info.Digest, Size: info.Size})
		if err != nil {
			if errdefs.IsNotFound(err) {
				return nil // garbage-collected since it was listed
			}
			failed.add(fmt.Errorf("open containerd content %s: %w", d, err))
			i.countFailure()
			return nil
		}
		// The size is known, so it also bounds the copy and keeps the import from
		// filling the disk (see Store.SetMinFree). The reader is closed inside this
		// callback rather than when the whole walk ends.
		n, err := cacheContent(i.Store, d, ra)
		if err != nil {
			failed.add(fmt.Errorf("import containerd content %s: %w", d, err))
			i.countFailure()
			if errors.Is(err, store.ErrNoSpace) {
				return errStop
			}
			return nil
		}
		i.Metrics.ImportedBlobs.Add(1)
		i.Metrics.ImportedBytes.Add(uint64(n))
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		failed.add(err)
	}
	return failed.err()
}

func (i *Importer) countFailure() {
	if i.Metrics != nil {
		i.Metrics.ImportErrors.Add(1)
	}
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
	n, putErr := dst.PutIfRoom(d, io.NewSectionReader(ra, 0, ra.Size()), ra.Size())
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
