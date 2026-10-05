package clientconfig

import "testing"

func TestBaseURLRejectsUnsafeForms(t *testing.T) {
	for _, value := range []string{"", "api.example.com", "https://user@example.com", "https://example.com/path?q=1", "http://example.com"} {
		if _, err := BaseURL(value); err == nil {
			t.Errorf("BaseURL(%q) unexpectedly succeeded", value)
		}
	}
	for _, value := range []string{"https://api.example.com/v1", "http://127.0.0.1:8080"} {
		if got, err := BaseURL(value); err != nil || got[len(got)-1] != '/' {
			t.Errorf("BaseURL(%q) = %q, %v", value, got, err)
		}
	}
}

func TestLoopbackHTTPOnlyMatchesPlainHTTPLoopback(t *testing.T) {
	for value, want := range map[string]bool{
		"http://127.0.0.1:8080/v1/": true,
		"http://localhost/v1/":      true,
		"http://[::1]:8080/":        true,
		"https://127.0.0.1/v1/":     false,
		"https://api.openai.com/":   false,
		"http://example.com/":       false,
	} {
		if got := LoopbackHTTP(value); got != want {
			t.Errorf("LoopbackHTTP(%q) = %v, want %v", value, got, want)
		}
	}
}
