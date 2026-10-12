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

func New(root string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(root, "blobs", "sha256"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "manifests"), 0755); err != nil {
		return nil, err
	}
	s := &Store{root: root, aliases: map[string]string{}}
	b, _ := os.ReadFile(filepath.Join(root, "aliases.json"))
	_ = json.Unmarshal(b, &s.aliases)
	return s, nil
}
func (s *Store) path(d string) (string, error) {
	p := strings.SplitN(d, ":", 2)
	if len(p) != 2 || p[0] != "sha256" || len(p[1]) != 64 {
		return "", errors.New("unsupported digest")
	}
	return filepath.Join(s.root, "blobs", "sha256", p[1]), nil
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
	if e = tmp.Close(); e != nil {
		return n, e
	}
	if hex.EncodeToString(h.Sum(nil)) != strings.TrimPrefix(d, "sha256:") {
		return n, errors.New("digest mismatch")
	}
	if e = os.Rename(tmp.Name(), p); e != nil {
		return n, e
	}
	return n, nil
}
func (s *Store) ImportFile(d, src string) (int64, bool, error) {
	p, e := s.path(d)
	if e != nil {
		return 0, false, e
	}
	if st, e := os.Stat(p); e == nil {
		return st.Size(), false, nil
	}
	if e = os.Link(src, p); e == nil {
		st, _ := os.Stat(p)
		return st.Size(), true, nil
	}
	f, e := os.Open(src)
	if e != nil {
		return 0, false, e
	}
	defer f.Close()
	n, e := s.PutVerified(d, f)
	return n, e == nil, e
}
func key(ns, repo, ref string) string { return ns + "/" + repo + "@" + ref }
func (s *Store) SetAlias(ns, repo, ref, d string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aliases[key(ns, repo, ref)] = d
	b, _ := json.MarshalIndent(s.aliases, "", "  ")
	return os.WriteFile(filepath.Join(s.root, "aliases.json"), b, 0644)
}
func (s *Store) Resolve(ns, repo, ref string) (string, bool) {
	if strings.HasPrefix(ref, "sha256:") {
		return ref, true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.aliases[key(ns, repo, ref)]
	return d, ok
}
