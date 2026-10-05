package llm

import "testing"

func TestMediaURLPolicyRejectsLocalPrivateAndCredentialURLs(t *testing.T) {
	for _, raw := range []string{
		"http://169.254.169.254/latest/meta-data/",
		"file:///etc/passwd",
		"http://localhost:6379/",
		"http://api.localhost/x",
		"gopher://10.0.0.1/",
		"https://user:pw@internal.example/",
		"http://10.0.0.1/image.png",
		"http://[::1]/image.png",
		"http://[::ffff:127.0.0.1]/image.png",
		"https://168.63.129.16/doc.pdf",
		"https://0.0.0.0/doc.pdf",
		"ftp://example.com/doc.pdf",
	} {
		if err := validateMediaSource(raw, nil, nil, "image/png", "image"); err == nil {
			t.Errorf("media URL %q accepted", raw)
		}
	}
	for _, raw := range []string{
		"https://example.com/image.png",
		"http://cdn.example.com/a.png",
		"https://8.8.8.8/image.png",
	} {
		if err := validateMediaSource(raw, nil, nil, "image/png", "image"); err != nil {
			t.Errorf("media URL %q rejected: %v", raw, err)
		}
	}
}
