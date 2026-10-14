package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Store struct {
	root    string
	mu      sync.RWMutex
	aliases map[string]string
}

// ValidDigest reports whether d is a well-formed sha256 digest: "sha256:"
// followed by exactly 64 lowercase hex characters. Digests end up in file
// paths, so everything that reaches the store must pass this check.
func ValidDigest(d string) bool {
	h, ok := strings.CutPrefix(d, "sha256:")
	if !ok || len(h) != 64 {
		return false
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func New(root string) (*Store, error) {
	blobs := filepath.Join(root, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "manifests"), 0755); err != nil {
		return nil, err
	}
	removePartials(blobs)
	s := &Store{root: root, aliases: map[string]string{}}
	aliasFile := filepath.Join(root, "aliases.json")
	b, err := os.ReadFile(aliasFile)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.aliases); err != nil {
			// Aliases can be rebuilt from peers and the containerd import, so
			// set the damaged file aside instead of refusing to start.
			_ = os.Rename(aliasFile, aliasFile+".corrupt")
			s.aliases = map[string]string{}
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	return s, nil
}

// removePartials deletes temporary files left behind by an interrupted download.
func removePartials(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".part-") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func (s *Store) path(d string) (string, error) {
	if !ValidDigest(d) {
		return "", errors.New("unsupported digest")
	}
	return filepath.Join(s.root, "blobs", "sha256", strings.TrimPrefix(d, "sha256:")), nil
}

func (s *Store) Open(d string) (*os.File, error) {
	p, e := s.path(d)
	if e != nil {
		return nil, e
	}
	return os.Open(p)
}

func (s *Store) Stat(d string) (os.FileInfo, error) {
	p, e := s.path(d)
	if e != nil {
		return nil, e
	}
	return os.Stat(p)
}

// PutVerified stores the content of r under digest d. The content only becomes
// visible once it has been fully written, flushed and checked against d.
func (s *Store) PutVerified(d string, r io.Reader) (int64, error) {
	p, e := s.path(d)
	if e != nil {
		return 0, e
	}
	if st, e := os.Stat(p); e == nil {
		return st.Size(), nil
	}
	tmp, e := os.CreateTemp(filepath.Dir(p), ".part-")
	if e != nil {
		return 0, e
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(tmp, h), r)
	if e != nil {
		tmp.Close()
		return n, e
	}
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") {
		tmp.Close()
		return n, errors.New("digest mismatch")
	}
	if e = tmp.Sync(); e != nil {
		tmp.Close()
		return n, e
	}
	if e = tmp.Close(); e != nil {
		return n, e
	}
	if e = os.Rename(tmp.Name(), p); e != nil {
		return n, e
	}
	return n, nil
}

func key(ns, repo, ref string) string { return ns + "/" + repo + "@" + ref }

// SetAlias points ns/repo:ref at digest d. Writing an alias that already
// exists with the same digest is a no-op, so the periodic containerd import
// does not rewrite the alias file on every run.
func (s *Store) SetAlias(ns, repo, ref, d string) error {
	if !ValidDigest(d) {
		return errors.New("unsupported digest")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(ns, repo, ref)
	if s.aliases[k] == d {
		return nil
	}
	s.aliases[k] = d
	return s.saveLocked()
}

// saveLocked writes the alias file via a temporary file and a rename, so a
// crash can never leave a half-written aliases.json behind.
func (s *Store) saveLocked() error {
	b, err := json.MarshalIndent(s.aliases, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.root, ".aliases-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(s.root, "aliases.json"))
}

// Resolve maps a tag to a digest. A digest reference resolves to itself,
// whether or not its content is present locally.
func (s *Store) Resolve(ns, repo, ref string) (string, bool) {
	if strings.HasPrefix(ref, "sha256:") {
		return ref, ValidDigest(ref)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.aliases[key(ns, repo, ref)]
	return d, ok
}
