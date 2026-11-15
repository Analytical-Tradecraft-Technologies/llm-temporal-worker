package adaptertest

import (
	"io"
	"net/http"
	"testing"
)

func TestJSONResponseSetsContentTypeHeadersAndBody(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := JSONResponse(request, http.StatusAccepted, `{"ok":true}`, "X-Request-Id", "req-1", "Retry-After", "2")
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusAccepted || response.Request != request || string(body) != `{"ok":true}` {
		t.Fatalf("response = %#v body %s", response, body)
	}
	for name, want := range map[string]string{"Content-Type": "application/json", "X-Request-Id": "req-1", "Retry-After": "2"} {
		if got := response.Header.Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestRedirectTransportCountsRequestsAndRedirects(t *testing.T) {
	var calls int
	transport := RedirectTransport(&calls, `{}`)
	for range 2 {
		request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") == "" {
			t.Fatalf("response = %#v, want a redirect with a location", response)
		}
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}
