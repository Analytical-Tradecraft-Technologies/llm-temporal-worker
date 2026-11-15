package openaiwire

import (
	"net/http"
	"testing"
)

func TestClientOptionsReportsLoopback(t *testing.T) {
	client := &http.Client{}
	if options, loopback := ClientOptions("https://api.example/", "k", client); loopback || len(options) != 4 {
		t.Fatalf("ClientOptions(https) = %d options, loopback %v", len(options), loopback)
	}
	if options, loopback := ClientOptions("http://127.0.0.1:8080/", "k", client); !loopback || len(options) != 3 {
		t.Fatalf("ClientOptions(loopback) = %d options, loopback %v", len(options), loopback)
	}
}
