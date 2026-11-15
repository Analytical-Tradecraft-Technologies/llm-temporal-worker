package clientconfig

import (
	"net/http"
	"testing"
)

func TestKeyedClientChecksInOrder(t *testing.T) {
	client := &http.Client{}
	for _, test := range []struct {
		base, exact, key string
		client           *http.Client
		want             string
	}{
		{"", "", "k", client, "test: base URL is required"},
		{"https://a.example", "https://b.example/", "k", client, `test: base URL must be exactly "https://b.example/"`},
		{"https://a.example", "", " ", client, "test: Test key is required"},
		{"https://a.example", "", "k", nil, "test: HTTP client is required"},
	} {
		if _, err := KeyedClient("test", test.base, test.exact, "Test key", test.key, test.client); err == nil || err.Error() != test.want {
			t.Errorf("KeyedClient(%q, %q, %q) = %v, want %q", test.base, test.exact, test.key, err, test.want)
		}
	}
	if got, err := KeyedClient("test", "https://b.example", "https://b.example/", "Test key", "k", client); err != nil || got != "https://b.example/" {
		t.Fatalf("KeyedClient(valid) = %q, %v", got, err)
	}
}
