package httpserver_test

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/internal/httpserver"
)

func TestProbeStateFailsClosedAndDoesNotExposeDetails(t *testing.T) {
	state := httpserver.NewHealthState()
	handler := httpserver.Handler(state, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("metric-value\n"))
	}))

	check := func(path string, status int, body string) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != status {
			t.Fatalf("%s status = %d, want %d", path, response.Code, status)
		}
		data, err := io.ReadAll(response.Result().Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != body {
			t.Fatalf("%s body = %q, want %q", path, data, body)
		}
	}

	check(httpserver.LivePath, http.StatusOK, "ok\n")
	check(httpserver.ReadyPath, http.StatusServiceUnavailable, "not ready\n")
	state.SetReady(true)
	check(httpserver.ReadyPath, http.StatusOK, "ok\n")
	state.SetLive(false)
	check(httpserver.LivePath, http.StatusServiceUnavailable, "not live\n")
	check(httpserver.MetricsPath, http.StatusOK, "metric-value\n")
}

func TestProbeEndpointsAllowOnlyGetAndHead(t *testing.T) {
	state := httpserver.NewHealthState()
	state.SetReady(true)
	handler := httpserver.Handler(state, nil)

	for _, path := range []string{httpserver.LivePath, httpserver.ReadyPath} {
		t.Run(path, func(t *testing.T) {
			for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
				request := httptest.NewRequest(method, path, nil)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusMethodNotAllowed {
					t.Fatalf("%s %s status = %d, want %d", method, path, response.Code, http.StatusMethodNotAllowed)
				}
				if got := response.Header().Get("Allow"); got != "GET, HEAD" {
					t.Fatalf("%s %s Allow = %q, want GET, HEAD", method, path, got)
				}
				if body := response.Body.String(); body != "method not allowed\n" {
					t.Fatalf("%s %s body = %q, want bounded method error", method, path, body)
				}
			}

			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, path, nil))
			if head.Code != http.StatusOK {
				t.Fatalf("HEAD %s status = %d, want %d", path, head.Code, http.StatusOK)
			}
			if body := head.Body.String(); body != "" {
				t.Fatalf("HEAD %s body = %q, want empty", path, body)
			}
		})
	}
}

func TestHandlerDefaultsAreSafeAndExposeOnlyProbeResponses(t *testing.T) {
	handler := httpserver.Handler(nil, nil)

	check := func(path string, status int, body string) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != status {
			t.Fatalf("%s status = %d, want %d", path, response.Code, status)
		}
		data, err := io.ReadAll(response.Result().Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != body {
			t.Fatalf("%s body = %q, want %q", path, data, body)
		}
	}

	check(httpserver.LivePath, http.StatusOK, "ok\n")
	check(httpserver.ReadyPath, http.StatusServiceUnavailable, "not ready\n")
	check(httpserver.MetricsPath, http.StatusNotFound, "404 page not found\n")
	check("/unexpected", http.StatusNotFound, "404 page not found\n")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, httpserver.LivePath, nil))
	if got := response.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("probe content type = %q, want text/plain; charset=utf-8", got)
	}
}

func TestNewRequiresAddress(t *testing.T) {
	if _, err := httpserver.New(httpserver.Options{}); err == nil {
		t.Fatal("New accepted an empty address")
	}
}

func TestServerNilAndDuplicateStartErrors(t *testing.T) {
	var nilServer *httpserver.Server
	if err := nilServer.Start(); err == nil {
		t.Fatal("nil server Start returned nil")
	}
	if nilServer.Addr() != "" {
		t.Fatalf("nil server Addr = %q, want empty", nilServer.Addr())
	}
	if nilServer.Errors() != nil {
		t.Fatal("nil server Errors returned a channel")
	}
	if err := nilServer.Shutdown(t.Context()); err != nil {
		t.Fatalf("nil server Shutdown() error = %v", err)
	}

	preStartServer, err := httpserver.New(httpserver.Options{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := preStartServer.Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown before Start() error = %v", err)
	}

	server, err := httpserver.New(httpserver.Options{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("sandbox does not permit binding a TCP listener")
		}
		t.Fatal(err)
	}
	defer server.Shutdown(t.Context())
	if err := server.Start(); err == nil {
		t.Fatal("duplicate Start returned nil")
	}
}

func TestServerReportsBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("sandbox does not permit binding a TCP listener")
		}
		t.Fatal(err)
	}

	server, err := httpserver.New(httpserver.Options{Address: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err == nil || !strings.Contains(err.Error(), "listen for health server") {
		t.Fatalf("Start() error = %v, want wrapped listen error", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start() after failed bind error = %v, want retry to succeed", err)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestServerBindsAndShutsDown(t *testing.T) {
	state := httpserver.NewHealthState()
	server, err := httpserver.New(httpserver.Options{Address: "127.0.0.1:0", Health: state})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("sandbox does not permit binding a TCP listener")
		}
		t.Fatal(err)
	}
	if server.Addr() == "" {
		t.Fatal("server did not expose a bound address")
	}
	client := &http.Client{}
	response, err := client.Get("http://" + server.Addr() + httpserver.LivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("live probe status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok\n" {
		t.Fatalf("live probe body = %q, want %q", body, "ok\n")
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}
