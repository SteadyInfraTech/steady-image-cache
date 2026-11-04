package initnode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		// The root server is a host of its own in containerd 2.x, and takes its
		// capabilities, CA and client certificate from the root of the file.
		root := out[:strings.Index(out, "[host.")]
		for _, want := range []string{"capabilities = ", `ca = "/etc/steady-image-cache/ca.crt"`, "client = [["} {
			if !strings.Contains(root, want) {
				t.Errorf("_default: no %q at the root of the file, the server entry would lack it:\n%s", want, out)
			}
		}
	})

	// containerd 1.x refuses a hosts.toml that has no [host] table ("invalid
	// `host` tree"), whatever else it holds, so each file must have one naming
	// SIC, and that is where the certificates are given.
	for _, reg := range opts.Registries {
		out := generateHostsToml(reg, opts)
		if !strings.Contains(out, "\n[host.\"https://127.0.0.1:5000\"]\n") {
			t.Errorf("%s: no [host] table for SIC, containerd 1.x would reject the file:\n%s", reg, out)
		}
		if !strings.Contains(out, "  ca = ") || !strings.Contains(out, "  client = ") {
			t.Errorf("%s: the host table lacks the CA or the client certificate:\n%s", reg, out)
		}
	}
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

// A certificate renewed in place in the pod reaches the node's copy while the
// refresh loop runs, and a failed run keeps what was there.
func TestWatchCarriesARenewedCertificateToTheNode(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "tls")
	hostRoot := filepath.Join(tmp, "host")
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	write := func(cert string) {
		t.Helper()
		for name, body := range map[string]string{"ca.crt": "ca", "tls.crt": cert, "tls.key": "key"} {
			if err := os.WriteFile(filepath.Join(src, name), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("old")
	opts := Options{
		Registries: []string{"docker.io"}, HostPrefix: hostRoot,
		CertsDDir: "/etc/containerd/certs.d", ClientCertDir: "/etc/steady-image-cache",
		TLSSourceDir: src, MirrorURL: "https://127.0.0.1:5000",
		Interval: 10 * time.Millisecond, Logf: func(string, ...any) {},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Watch(ctx, opts) }()

	host := filepath.Join(hostRoot, "etc/steady-image-cache/tls.crt")
	waitFor := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if b, err := os.ReadFile(host); err == nil && string(b) == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("the node's certificate never became %q", want)
	}
	waitFor("old")

	// A half-finished rotation: the files are gone for a moment.
	if err := os.Remove(filepath.Join(src, "tls.key")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	waitFor("old")

	write("new")
	waitFor("new")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Watch: %v", err)
	}
}

func TestWatchFailsWhenTheFirstRunCannot(t *testing.T) {
	opts := Options{TLSSourceDir: filepath.Join(t.TempDir(), "missing"), HostPrefix: t.TempDir(),
		CertsDDir: "/c", ClientCertDir: "/e", Interval: time.Hour, Logf: func(string, ...any) {}}
	if err := Watch(context.Background(), opts); err == nil {
		t.Fatal("Watch reported success although the first run failed")
	}
}

// A run that cannot read all of the sources changes nothing on the node: a new
// certificate must never sit beside the old key.
func TestFailedRunLeavesNoNewCertificateBesideTheOldKey(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "tls")
	dst := filepath.Join(tmp, "node")
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	put := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(src, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	put("ca.crt", "ca")
	put("tls.crt", "old-cert")
	put("tls.key", "old-key")
	logf := func(string, ...any) {}
	if err := provisionCerts(src, dst, logf); err != nil {
		t.Fatal(err)
	}
	put("tls.crt", "new-cert")
	if err := os.Remove(filepath.Join(src, "tls.key")); err != nil {
		t.Fatal(err)
	}
	if err := provisionCerts(src, dst, logf); err == nil {
		t.Fatal("a missing key was not reported")
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "tls.crt")); string(b) != "old-cert" {
		t.Fatalf("the node's certificate is %q after a failed run, want the old one", b)
	}
}

// If the mirror configuration cannot be written, the certificates of the same
// run are not kept: the node is left as it was, or clean if it was new.
func TestFailedRunRestoresTheCertificatesItHadChanged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		previous bool
	}{{"node had certificates", true}, {"new node", false}} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			src := filepath.Join(tmp, "tls")
			host := filepath.Join(tmp, "host")
			if err := os.MkdirAll(src, 0o750); err != nil {
				t.Fatal(err)
			}
			for name, body := range map[string]string{"ca.crt": "ca2", "tls.crt": "cert2", "tls.key": "key2"} {
				if err := os.WriteFile(filepath.Join(src, name), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			nodeDir := filepath.Join(host, "etc/steady-image-cache")
			if tc.previous {
				if err := os.MkdirAll(nodeDir, 0o755); err != nil {
					t.Fatal(err)
				}
				for name, body := range map[string]string{"ca.crt": "ca1", "tls.crt": "cert1", "tls.key": "key1"} {
					if err := os.WriteFile(filepath.Join(nodeDir, name), []byte(body), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// certs.d is a file, so no registry directory can be made in it.
			if err := os.MkdirAll(filepath.Join(host, "etc/containerd"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(host, "etc/containerd/certs.d"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			opts := Options{Registries: []string{"docker.io"}, HostPrefix: host, CertsDDir: "/etc/containerd/certs.d",
				ClientCertDir: "/etc/steady-image-cache", TLSSourceDir: src, MirrorURL: "https://127.0.0.1:5000",
				Logf: func(string, ...any) {}}
			if err := Run(opts); err == nil {
				t.Fatal("Run succeeded although the mirror configuration could not be written")
			}
			for name, want := range map[string]string{"ca.crt": "ca1", "tls.crt": "cert1", "tls.key": "key1"} {
				b, err := os.ReadFile(filepath.Join(nodeDir, name))
				if tc.previous {
					if err != nil || string(b) != want {
						t.Fatalf("%s = %q (%v), want the previous %q", name, b, err, want)
					}
				} else if err == nil {
					t.Fatalf("%s was left on a node that had none", name)
				}
			}
		})
	}
}

// A registry whose configuration cannot be written takes the earlier registries'
// new configuration back with it.
func TestFailedRunRestoresTheHostsTomlOfEarlierRegistries(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "tls")
	host := filepath.Join(tmp, "host")
	certsD := filepath.Join(host, "etc/containerd/certs.d")
	if err := os.MkdirAll(filepath.Join(certsD, "a.example"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"ca.crt", "tls.crt", "tls.key"} {
		if err := os.WriteFile(filepath.Join(src, n), []byte(n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := filepath.Join(certsD, "a.example", "hosts.toml")
	if err := os.WriteFile(old, []byte("server = \"https://old\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// b.example is a file: its directory cannot be made.
	if err := os.WriteFile(filepath.Join(certsD, "b.example"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	opts := Options{Registries: []string{"a.example", "b.example"}, HostPrefix: host, CertsDDir: "/etc/containerd/certs.d",
		ClientCertDir: "/etc/steady-image-cache", TLSSourceDir: src, MirrorURL: "https://127.0.0.1:5000",
		Logf: func(string, ...any) {}}
	if err := Run(opts); err == nil {
		t.Fatal("Run succeeded although the second registry could not be configured")
	}
	if b, _ := os.ReadFile(old); string(b) != "server = \"https://old\"\n" {
		t.Fatalf("the earlier registry's hosts.toml is %q, want the old one", b)
	}
}
