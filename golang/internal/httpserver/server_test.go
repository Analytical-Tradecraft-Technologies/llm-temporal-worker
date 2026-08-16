package httpserver

import (
	"testing"
	"time"
)

func TestNewConfiguresReadTimeouts(t *testing.T) {
	t.Run("defaults read timeout from read header timeout", func(t *testing.T) {
		server, err := New(Options{Address: "127.0.0.1:0"})
		if err != nil {
			t.Fatal(err)
		}
		if server.httpServer.ReadHeaderTimeout != defaultReadHeaderTimeout {
			t.Fatalf("ReadHeaderTimeout = %v, want %v", server.httpServer.ReadHeaderTimeout, defaultReadHeaderTimeout)
		}
		if server.httpServer.ReadTimeout != defaultReadHeaderTimeout {
			t.Fatalf("ReadTimeout = %v, want %v", server.httpServer.ReadTimeout, defaultReadHeaderTimeout)
		}
	})

	t.Run("inherits custom read header timeout", func(t *testing.T) {
		timeout := 3 * time.Second
		server, err := New(Options{Address: "127.0.0.1:0", ReadHeaderTimeout: timeout})
		if err != nil {
			t.Fatal(err)
		}
		if server.httpServer.ReadTimeout != timeout {
			t.Fatalf("ReadTimeout = %v, want %v", server.httpServer.ReadTimeout, timeout)
		}
	})

	t.Run("honors explicit read timeout", func(t *testing.T) {
		readTimeout := 7 * time.Second
		readHeaderTimeout := 2 * time.Second
		server, err := New(Options{
			Address:           "127.0.0.1:0",
			ReadTimeout:       readTimeout,
			ReadHeaderTimeout: readHeaderTimeout,
		})
		if err != nil {
			t.Fatal(err)
		}
		if server.httpServer.ReadTimeout != readTimeout {
			t.Fatalf("ReadTimeout = %v, want %v", server.httpServer.ReadTimeout, readTimeout)
		}
		if server.httpServer.ReadHeaderTimeout != readHeaderTimeout {
			t.Fatalf("ReadHeaderTimeout = %v, want %v", server.httpServer.ReadHeaderTimeout, readHeaderTimeout)
		}
	})
}
