package preload

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// DockerConfigPath is where the Docker client keeps its configuration:
// $DOCKER_CONFIG/config.json, else ~/.docker/config.json. It is "" if neither
// can be named.
func DockerConfigPath() string {
	if d := os.Getenv("DOCKER_CONFIG"); d != "" {
		return filepath.Join(d, "config.json")
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return filepath.Join(h, ".docker", "config.json")
	}
	return ""
}

// DockerCredentials reads the "auths" of a Docker config file and returns a
// lookup of the user name and password stored for a registry ("" and "" if
// there are none). A file that does not exist is not an error: there are no
// credentials then. Credential helpers and stores ("credsStore", "credHelpers")
// and identity tokens are not supported; helpers is true when the file names
// one, so that the caller can say why a registry found no credentials.
func DockerCredentials(path string) (lookup func(registry string) (user, pass string), helpers bool, err error) {
	if path == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var cfg struct {
		Auths map[string]struct {
			Auth     string `json:"auth"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"auths"`
		CredsStore  string            `json:"credsStore"`
		CredHelpers map[string]string `json:"credHelpers"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}
	type cred struct{ user, pass string }
	byRegistry := map[string]cred{}
	for key, a := range cfg.Auths {
		reg := registryOfAuthKey(key)
		if reg == "" {
			continue
		}
		u, p := a.Username, a.Password
		if a.Auth != "" {
			dec, err := base64.StdEncoding.DecodeString(a.Auth)
			if err != nil {
				continue
			}
			var ok bool
			if u, p, ok = strings.Cut(string(dec), ":"); !ok {
				continue
			}
		}
		if u == "" {
			continue
		}
		byRegistry[reg] = cred{u, p}
	}
	return func(registry string) (string, string) {
		c := byRegistry[NormalizeRegistry(registry)]
		return c.user, c.pass
	}, cfg.CredsStore != "" || len(cfg.CredHelpers) > 0, nil
}

// registryOfAuthKey is the normalised registry an "auths" key names: Docker
// writes "https://index.docker.io/v1/" for Docker Hub and a bare host (and
// port) for the others.
func registryOfAuthKey(key string) string {
	k := strings.TrimSpace(key)
	if strings.Contains(k, "://") {
		u, err := url.Parse(k)
		if err != nil {
			return ""
		}
		k = u.Host
	} else if i := strings.Index(k, "/"); i >= 0 {
		k = k[:i]
	}
	return NormalizeRegistry(k)
}
