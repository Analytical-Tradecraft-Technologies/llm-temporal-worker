package httpserver

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewConfiguresReadTimeouts(t *testing.T) {
	t.Run("defaults read timeout from read header timeout", func(t *testing.T) {
		server, err := New(Options{Address: "127.0.0.1:0"})
		testassert.NoError(t, err)
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
		testassert.NoError(t, err)
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
		testassert.NoError(t, err)
		if server.httpServer.ReadTimeout != readTimeout {
			t.Fatalf("ReadTimeout = %v, want %v", server.httpServer.ReadTimeout, readTimeout)
		}
		if server.httpServer.ReadHeaderTimeout != readHeaderTimeout {
			t.Fatalf("ReadHeaderTimeout = %v, want %v", server.httpServer.ReadHeaderTimeout, readHeaderTimeout)
		}
	})
}

func TestConcurrentServerStartHasExactlyOneOwner(t *testing.T) {
	server, err := New(Options{Address: "127.0.0.1:0"})
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	const callers = 64
	start := make(chan struct{})
	errors := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			errors <- server.Start()
		}()
	}
	ready.Wait()
	close(start)

	successes := 0
	for range callers {
		err := <-errors
		if err == nil {
			successes++
			continue
		}
		if !strings.Contains(err.Error(), "already started") {
			t.Fatalf("concurrent Start() error = %v, want already-started error", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent Start() successes = %d, want exactly 1", successes)
	}
	if server.Addr() == "" {
		t.Fatal("successful Start did not publish its listener address")
	}

	testassert.NoError(t, server.Shutdown(context.Background()))
	select {
	case _, ok := <-server.Errors():
		if ok {
			t.Fatal("server reported an unexpected serve error")
		}
	case <-time.After(time.Second):
		t.Fatal("server error stream did not close after shutdown")
	}
}
