package initnode

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateHostsToml(t *testing.T) {
	opts := Options{
		Registries:    []string{"docker.io", "ghcr.io", "_default"},
		ClientCertDir: "/etc/steady-image-cache",
		MirrorURL:     "https://127.0.0.1:5000",
		Capabilities:  []string{"pull"},
	}

	t.Run("docker.io", func(t *testing.T) {
		out := generateHostsToml("docker.io", opts)
		if !strings.Contains(out, `server = "https://registry-1.docker.io"`) {
			t.Errorf("expected docker.io server URL, got:\n%s", out)
		}
		if !strings.Contains(out, `[host."https://127.0.0.1:5000"]`) {
			t.Errorf("missing host entry:\n%s", out)
		}
		if !strings.Contains(out, `ca = "/etc/steady-image-cache/ca.crt"`) {
			t.Errorf("missing ca path:\n%s", out)
		}
		if !strings.Contains(out, `client = [["/etc/steady-image-cache/tls.crt", "/etc/steady-image-cache/tls.key"]]`) {
			t.Errorf("missing client cert path:\n%s", out)
		}
	})

	t.Run("ghcr.io", func(t *testing.T) {
		out := generateHostsToml("ghcr.io", opts)
		if !strings.Contains(out, `server = "https://ghcr.io"`) {
			t.Errorf("expected ghcr.io server URL, got:\n%s", out)
		}
	})

	t.Run("_default", func(t *testing.T) {
		out := generateHostsToml("_default", opts)
		if !strings.Contains(out, `server = "https://127.0.0.1:5000"`) {
			t.Errorf("expected _default server to be SIC, got:\n%s", out)
		}
		if strings.Contains(out, `[host.`) {
			t.Errorf("_default should not define a sub-host mirror, got:\n%s", out)
		}
	})
}

func TestProvisionCerts(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src")
	dst := filepath.Join(tmp, "dst")
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(src, "ca.crt"), []byte("ca-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "tls.crt"), []byte("cert-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "tls.key"), []byte("key-content"), 0o600); err != nil {
		t.Fatal(err)
	}

	logs := []string{}
	logf := func(format string, args ...any) {
		logs = append(logs, format)
	}

	if err := provisionCerts(src, dst, logf); err != nil {
		t.Fatalf("provisionCerts failed: %v", err)
	}

	// Verify key permissions
	keyInfo, err := os.Stat(filepath.Join(dst, "tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := keyInfo.Mode().Perm(); perm != 0o600 {
		t.Errorf("expected tls.key mode 0600, got %04o", perm)
	}

	certInfo, err := os.Stat(filepath.Join(dst, "tls.crt"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := certInfo.Mode().Perm(); perm != 0o644 {
		t.Errorf("expected tls.crt mode 0644, got %04o", perm)
	}

	// Run again to verify idempotency
	if err := provisionCerts(src, dst, logf); err != nil {
		t.Fatalf("idempotent provisionCerts failed: %v", err)
	}
}

func TestRunEndToEnd(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "tls")
	hostRoot := filepath.Join(tmp, "host")

	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(src, "ca.crt"), []byte("ca"), 0o644)
	_ = os.WriteFile(filepath.Join(src, "tls.crt"), []byte("cert"), 0o644)
	_ = os.WriteFile(filepath.Join(src, "tls.key"), []byte("key"), 0o600)

	opts := Options{
		Registries:    []string{"docker.io", "quay.io"},
		HostPrefix:    hostRoot,
		CertsDDir:     "/etc/containerd/certs.d",
		ClientCertDir: "/etc/steady-image-cache",
		TLSSourceDir:  src,
		MirrorURL:     "https://127.0.0.1:5000",
		Capabilities:  []string{"pull"},
		Logf:          func(string, ...any) {},
	}

	if err := Run(opts); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Check files created on hostRoot
	dockerToml := filepath.Join(hostRoot, "etc/containerd/certs.d/docker.io/hosts.toml")
	b, err := os.ReadFile(dockerToml)
	if err != nil {
		t.Fatalf("missing docker.io hosts.toml: %v", err)
	}
	if !strings.Contains(string(b), "registry-1.docker.io") {
		t.Errorf("expected registry-1.docker.io in docker.io hosts.toml, got: %s", string(b))
	}

	quayToml := filepath.Join(hostRoot, "etc/containerd/certs.d/quay.io/hosts.toml")
	b, err = os.ReadFile(quayToml)
	if err != nil {
		t.Fatalf("missing quay.io hosts.toml: %v", err)
	}
	if !strings.Contains(string(b), "https://quay.io") {
		t.Errorf("expected https://quay.io in quay.io hosts.toml, got: %s", string(b))
	}

	clientKey := filepath.Join(hostRoot, "etc/steady-image-cache/tls.key")
	if _, err := os.Stat(clientKey); err != nil {
		t.Fatalf("missing tls.key on host: %v", err)
	}
}
