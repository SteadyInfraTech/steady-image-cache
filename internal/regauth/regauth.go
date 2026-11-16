// Package regauth holds what the proxy and the preload command share of the
// registry token protocol: reading a Bearer challenge and, for public content,
// asking its realm for a token without credentials.
package regauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ParseBearer reads a Bearer challenge: its parameters, and whether it names a
// realm. Values are quoted strings, as registries send them (a realm with a
// query string is always quoted); an unquoted value ends at the next comma. A
// challenge without a service or scope gives a token request without them,
// which a registry refuses for anything but public content.
func ParseBearer(h string) (map[string]string, bool) {
	// The scheme name is case-insensitive (RFC 9110).
	scheme, rest, ok := strings.Cut(strings.TrimSpace(h), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return nil, false
	}
	p := map[string]string{}
	// key=value pairs separated by commas; a value may be a quoted string, in
	// which a comma is part of the value and a backslash escapes the next byte.
	for i := 0; i < len(rest); {
		for i < len(rest) && (rest[i] == ' ' || rest[i] == ',') {
			i++
		}
		j := i
		for j < len(rest) && rest[j] != '=' && rest[j] != ',' {
			j++
		}
		if j >= len(rest) || rest[j] != '=' {
			i = j
			continue
		}
		key := strings.TrimSpace(rest[i:j])
		i = j + 1
		var val strings.Builder
		if i < len(rest) && rest[i] == '"' {
			i++
			for i < len(rest) && rest[i] != '"' {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
				}
				val.WriteByte(rest[i])
				i++
			}
			if i < len(rest) {
				i++ // the closing quote
			}
		} else {
			for i < len(rest) && rest[i] != ',' {
				val.WriteByte(rest[i])
				i++
			}
		}
		p[key] = strings.TrimSpace(val.String())
	}
	return p, p["realm"] != ""
}

// DefaultTTL is how long a token lives when the realm does not say; no token is
// kept longer than maxTTL, whatever the realm says.
const (
	DefaultTTL = 60 * time.Second
	maxTTL     = 10 * time.Minute
	maxBody    = 64 << 10
)

// AnonymousToken asks the realm of a Bearer challenge for a token without any
// credentials, for the service and scope the challenge names. The realm must be
// an https URL; the client must not follow redirects (the caller's client
// refuses them), so a realm cannot hand the request on. It returns the token
// and how long to keep it. A realm that wants credentials answers with an
// error, or with a token that the registry then refuses: neither is a failure
// of this function.
func AnonymousToken(ctx context.Context, c *http.Client, challenge string) (string, time.Duration, error) {
	p, ok := ParseBearer(challenge)
	if !ok {
		return "", 0, errors.New("not a Bearer challenge with a realm")
	}
	u, err := url.Parse(p["realm"])
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", 0, fmt.Errorf("refusing the token realm %q: only https is used", p["realm"])
	}
	q := u.Query()
	if s := p["service"]; s != "" {
		q.Set("service", s)
	}
	if s := p["scope"]; s != "" {
		q.Set("scope", s)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("token service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("token service answered %d", resp.StatusCode)
	}
	var t struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(&t); err != nil {
		return "", 0, fmt.Errorf("token service: %w", err)
	}
	tok := t.Token
	if tok == "" {
		tok = t.AccessToken
	}
	if tok == "" || strings.ContainsAny(tok, " \r\n") {
		return "", 0, errors.New("token service gave no usable token")
	}
	ttl := time.Duration(t.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return tok, min(ttl, maxTTL), nil
}
