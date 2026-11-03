package initnode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Options holds configuration for initializing containerd mirror configuration
// and provisioning client certificates on a node.
type Options struct {
	// Registries to configure mirrors for (e.g. ["docker.io", "ghcr.io"] or ["_default"]).
	Registries []string

	// HostPrefix is the path prefix when running inside a container with the host
	// root mounted (e.g. "/host"). Empty when running directly on the host.
	HostPrefix string

	// CertsDDir is the path on the host to containerd's certs.d directory
	// (default "/etc/containerd/certs.d").
	CertsDDir string

	// ClientCertDir is the path on the host where containerd client certs live
	// (default "/etc/steady-image-cache").
	ClientCertDir string

	// TLSSourceDir is the directory where the secret/certificates are mounted
	// inside the pod (default "/tls").
	TLSSourceDir string

	// MirrorURL is the local address containerd uses to contact SIC
	// (default "https://127.0.0.1:5000").
	MirrorURL string

	// Capabilities containerd should use for SIC (default ["pull"]).
	Capabilities []string

	// Interval, when positive, makes RunFromEnv repeat the setup that often
	// (SIC_INIT_INTERVAL), so certificates that are renewed in place reach the
	// node's copy. Zero runs it once.
	Interval time.Duration

	// Logf is the logger function. If nil, log.Printf is used.
	Logf func(string, ...any)
}

// DefaultOptions returns the default Options.
func DefaultOptions() Options {
	return Options{
		Registries:    []string{"docker.io", "ghcr.io"},
		CertsDDir:     "/etc/containerd/certs.d",
		ClientCertDir: "/etc/steady-image-cache",
		TLSSourceDir:  "/tls",
		MirrorURL:     "https://127.0.0.1:5000",
		Capabilities:  []string{"pull"},
		Logf:          log.Printf,
	}
}

// OptionsFromEnv constructs Options from environment variables, defaulting
// sensibly for Kubernetes init-container deployments.
func OptionsFromEnv() Options {
	opts := DefaultOptions()

	if v := os.Getenv("SIC_INIT_REGISTRIES"); v != "" {
		var regs []string
		for _, r := range strings.Split(v, ",") {
			if r = strings.TrimSpace(r); r != "" {
				regs = append(regs, r)
			}
		}
		if len(regs) > 0 {
			opts.Registries = regs
		}
	}

	opts.HostPrefix = os.Getenv("SIC_INIT_HOST_PREFIX")
	if opts.HostPrefix == "" {
		// Detect if /host is mounted (standard Kubernetes host-root mount)
		if fi, err := os.Stat("/host"); err == nil && fi.IsDir() {
			opts.HostPrefix = "/host"
		}
	}

	if v := os.Getenv("SIC_INIT_CERTS_D_DIR"); v != "" {
		opts.CertsDDir = v
	}
	if v := os.Getenv("SIC_INIT_CLIENT_CERT_DIR"); v != "" {
		opts.ClientCertDir = v
	}
	if v := os.Getenv("SIC_INIT_TLS_DIR"); v != "" {
		opts.TLSSourceDir = v
	}
	if v := os.Getenv("SIC_INIT_MIRROR_URL"); v != "" {
		opts.MirrorURL = v
	}
	if v := os.Getenv("SIC_INIT_CAPABILITIES"); v != "" {
		var caps []string
		for _, c := range strings.Split(v, ",") {
			if c = strings.TrimSpace(c); c != "" {
				caps = append(caps, c)
			}
		}
		if len(caps) > 0 {
			opts.Capabilities = caps
		}
	}

	if v := os.Getenv("SIC_INIT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.Interval = d
		} else {
			log.Printf("init-node: ignoring SIC_INIT_INTERVAL=%q: not a positive duration", v)
		}
	}

	return opts
}

// Run executes the host initialization workflow:
// 1. Provisions client certificates and CA to ClientCertDir.
// 2. Generates hosts.toml files in CertsDDir for each configured registry.
func Run(opts Options) error {
	logf := opts.Logf
	if logf == nil {
		logf = log.Printf
	}

	targetCertDir := filepath.Join(opts.HostPrefix, opts.ClientCertDir)

	// The certificates and the mirror configuration are one change: containerd
	// must never be left with a mixed set of certificate files, or with new
	// certificates for a configuration that failed. Whatever goes wrong below,
	// the files are put back as they were.
	before, err := snapshotCerts(targetCertDir)
	if err != nil {
		return fmt.Errorf("read the certificates on the node: %w", err)
	}
	if err := apply(opts, targetCertDir, logf); err != nil {
		if rerr := restoreCerts(before); rerr != nil {
			return fmt.Errorf("%w (and putting the previous certificates back failed: %v)", err, rerr)
		}
		return err
	}

	logf("init-node: containerd host configuration completed successfully")
	return nil
}

// apply provisions the certificates and writes the mirror configuration.
func apply(opts Options, targetCertDir string, logf func(string, ...any)) error {
	logf("init-node: provisioning client certificates from %s to %s", opts.TLSSourceDir, targetCertDir)
	if err := provisionCerts(opts.TLSSourceDir, targetCertDir, logf); err != nil {
		return fmt.Errorf("provision certificates: %w", err)
	}

	targetCertsD := filepath.Join(opts.HostPrefix, opts.CertsDDir)
	logf("init-node: configuring containerd mirrors in %s for registries: %v", targetCertsD, opts.Registries)
	for _, reg := range opts.Registries {
		if err := configureRegistry(reg, targetCertsD, opts, logf); err != nil {
			return fmt.Errorf("configure registry %q: %w", reg, err)
		}
	}
	return nil
}

// certFiles are the certificate files init-node keeps on the node, with their modes.
var certFiles = []struct {
	name string
	mode os.FileMode
}{
	{"ca.crt", 0o644},
	{"tls.crt", 0o644},
	{"tls.key", 0o600},
}

// savedFile is a certificate file as it was before a run.
type savedFile struct {
	path    string
	data    []byte
	mode    os.FileMode
	existed bool
}

// snapshotCerts records the certificate files in dir.
func snapshotCerts(dir string) ([]savedFile, error) {
	var out []savedFile
	for _, f := range certFiles {
		p := filepath.Join(dir, f.name)
		data, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			out = append(out, savedFile{path: p})
			continue
		}
		if err != nil {
			return nil, err
		}
		fi, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		out = append(out, savedFile{path: p, data: data, mode: fi.Mode().Perm(), existed: true})
	}
	return out, nil
}

// restoreCerts puts the files back as snapshotCerts saw them, removing those
// that were not there. It tries every file and reports the first failure.
func restoreCerts(saved []savedFile) error {
	var first error
	for _, f := range saved {
		var err error
		if f.existed {
			err = writeAtomic(f.path, f.data, f.mode)
		} else if e := os.Remove(f.path); e != nil && !errors.Is(e, os.ErrNotExist) {
			err = e
		}
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

// RunFromEnv reads settings from the environment and executes the initialization
// once, or until ctx ends when SIC_INIT_INTERVAL asks for it to be repeated.
func RunFromEnv(ctx context.Context) error {
	opts := OptionsFromEnv()
	if opts.Interval > 0 {
		return Watch(ctx, opts)
	}
	return Run(opts)
}

// Watch runs the setup now and then every opts.Interval until ctx ends. The
// first run must succeed (a pod that cannot configure its node should say so);
// a later failure is logged and the files of the last good run stay in place.
func Watch(ctx context.Context, opts Options) error {
	logf := opts.Logf
	if logf == nil {
		logf = log.Printf
	}
	if err := Run(opts); err != nil {
		return err
	}
	t := time.NewTicker(opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := Run(opts); err != nil {
				logf("init-node: refresh failed, keeping the previous files: %v", err)
			}
		}
	}
}

// provisionCerts copies ca.crt, tls.crt, and tls.key from srcDir to dstDir with
// strict atomic writes and appropriate file permissions.
func provisionCerts(srcDir, dstDir string, logf func(string, ...any)) error {
	if err := os.MkdirAll(dstDir, 0o750); err != nil {
		return err
	}

	files := certFiles

	// Everything is read before anything is written: a source that cannot be read
	// (a half-finished rotation) must not leave a new certificate beside the old key.
	data := make([][]byte, len(files))
	for i, f := range files {
		src := filepath.Join(srcDir, f.name)
		b, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("read %s: %w", src, err)
		}
		data[i] = b
	}

	for i, f := range files {
		dst := filepath.Join(dstDir, f.name)
		srcData := data[i]

		if existing, err := os.ReadFile(dst); err == nil {
			if fi, err := os.Stat(dst); err == nil && bytes.Equal(existing, srcData) && fi.Mode().Perm() == f.mode {
				// Already identical and permissions match
				continue
			}
		}

		if err := writeAtomic(dst, srcData, f.mode); err != nil {
			return fmt.Errorf("write %s: %w", dst, err)
		}
		logf("init-node: updated %s (mode %04o)", dst, f.mode)
	}
	return nil
}

// configureRegistry creates or updates <certsDDir>/<registry>/hosts.toml.
func configureRegistry(reg, certsDDir string, opts Options, logf func(string, ...any)) error {
	regDir := filepath.Join(certsDDir, reg)
	if err := os.MkdirAll(regDir, 0o750); err != nil {
		return err
	}

	hostsTomlPath := filepath.Join(regDir, "hosts.toml")
	content := generateHostsToml(reg, opts)

	if existing, err := os.ReadFile(hostsTomlPath); err == nil {
		if bytes.Equal(existing, []byte(content)) {
			// Already matches
			return nil
		}
	}

	if err := writeAtomic(hostsTomlPath, []byte(content), 0o644); err != nil {
		return err
	}
	logf("init-node: wrote %s", hostsTomlPath)
	return nil
}

// generateHostsToml generates the hosts.toml content for a registry.
func generateHostsToml(reg string, opts Options) string {
	var b strings.Builder
	b.WriteString("# Generated by Steady Image Cache (SIC) init-node. DO NOT EDIT.\n")

	// Host-side paths that containerd on the node must read
	caPath := filepath.Join(opts.ClientCertDir, "ca.crt")
	certPath := filepath.Join(opts.ClientCertDir, "tls.crt")
	keyPath := filepath.Join(opts.ClientCertDir, "tls.key")

	capsList := formatCapabilities(opts.Capabilities)

	if reg == "_default" {
		// Air-gapped fallback where SIC is the only host
		b.WriteString(fmt.Sprintf("server = %q\n", opts.MirrorURL))
		b.WriteString(fmt.Sprintf("capabilities = %s\n", capsList))
		b.WriteString(fmt.Sprintf("ca = %q\n", caPath))
		b.WriteString(fmt.Sprintf("client = [[%q, %q]]\n", certPath, keyPath))
		return b.String()
	}

	// Normal registry mirror configuration with fallback to upstream
	serverURL := upstreamServerURL(reg)
	b.WriteString(fmt.Sprintf("server = %q\n\n", serverURL))
	b.WriteString(fmt.Sprintf("[host.%q]\n", opts.MirrorURL))
	b.WriteString(fmt.Sprintf("  capabilities = %s\n", capsList))
	b.WriteString(fmt.Sprintf("  ca = %q\n", caPath))
	b.WriteString(fmt.Sprintf("  client = [[%q, %q]]\n", certPath, keyPath))

	return b.String()
}

// upstreamServerURL returns the default upstream URL for a registry name.
func upstreamServerURL(reg string) string {
	switch strings.ToLower(reg) {
	case "docker.io":
		return "https://registry-1.docker.io"
	default:
		if strings.HasPrefix(reg, "http://") || strings.HasPrefix(reg, "https://") {
			return reg
		}
		return "https://" + reg
	}
}

func formatCapabilities(caps []string) string {
	if len(caps) == 0 {
		return `["pull"]`
	}
	var quoted []string
	for _, c := range caps {
		quoted = append(quoted, fmt.Sprintf("%q", c))
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// writeAtomic writes data to dst using a temporary file and atomic rename.
func writeAtomic(dst string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, ".tmp-init-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := io.Copy(tmp, bytes.NewReader(data)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}
