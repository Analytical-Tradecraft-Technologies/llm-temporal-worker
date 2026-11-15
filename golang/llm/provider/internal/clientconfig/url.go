package clientconfig

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// BaseURL validates an adapter endpoint without accepting credentials or
// browser-only URL features. Plain HTTP is reserved for loopback test
// servers; deployed endpoints must use HTTPS.
func BaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("base URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("base URL must be an absolute URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("base URL must not contain userinfo, query, or fragment")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && loopbackHost(u.Hostname())) {
		return "", fmt.Errorf("base URL must use HTTPS outside loopback")
	}
	return strings.TrimRight(raw, "/") + "/", nil
}

// LoopbackHTTP reports whether a validated base URL is a plain-HTTP loopback
// endpoint. SDKs that refuse to send credentials over HTTP need an explicit
// opt-in for these local endpoints.
func LoopbackHTTP(baseURL string) bool {
	u, err := url.Parse(baseURL)
	return err == nil && u.Scheme == "http" && loopbackHost(u.Hostname())
}

func loopbackHost(host string) bool {
	return host == "127.0.0.1" || host == "localhost" || host == "::1"
}

func Secret(name, value string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

// KeyedClient validates the settings of a client that authenticates with an
// API key: the base URL (which must equal exactBaseURL when that is set), the
// key keyName names, and the HTTP client. It returns the validated base URL;
// errors are prefixed with label, as in "openai chat".
func KeyedClient(label, rawBaseURL, exactBaseURL, keyName, key string, httpClient *http.Client) (string, error) {
	baseURL, err := BaseURL(rawBaseURL)
	if err != nil {
		return "", fmt.Errorf("%s: %w", label, err)
	}
	if exactBaseURL != "" && baseURL != exactBaseURL {
		return "", fmt.Errorf("%s: base URL must be exactly %q", label, exactBaseURL)
	}
	if err := Secret(keyName, key); err != nil {
		return "", fmt.Errorf("%s: %w", label, err)
	}
	if httpClient == nil {
		return "", fmt.Errorf("%s: HTTP client is required", label)
	}
	return baseURL, nil
}
