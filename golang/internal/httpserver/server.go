package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	LivePath                 = "/health/live"
	ReadyPath                = "/health/ready"
	MetricsPath              = "/metrics"
	defaultReadHeaderTimeout = 5 * time.Second
)

// Handler builds the deliberately small probe surface. Probe responses do
// not include configuration, dependency errors, or request content.
func Handler(state *HealthState, metrics http.Handler) http.Handler {
	if state == nil {
		state = NewHealthState()
	}
	if metrics == nil {
		metrics = http.NotFoundHandler()
	}
	mux := http.NewServeMux()
	mux.HandleFunc(LivePath, func(writer http.ResponseWriter, request *http.Request) {
		if !probeMethodAllowed(writer, request) {
			return
		}
		if !state.Live() {
			probeResponseForRequest(writer, request, http.StatusServiceUnavailable, "not live\n")
			return
		}
		probeResponseForRequest(writer, request, http.StatusOK, "ok\n")
	})
	mux.HandleFunc(ReadyPath, func(writer http.ResponseWriter, request *http.Request) {
		if !probeMethodAllowed(writer, request) {
			return
		}
		if !state.Ready() {
			probeResponseForRequest(writer, request, http.StatusServiceUnavailable, "not ready\n")
			return
		}
		probeResponseForRequest(writer, request, http.StatusOK, "ok\n")
	})
	mux.Handle(MetricsPath, metrics)
	return mux
}

func probeResponse(writer http.ResponseWriter, status int, body string) {
	probeResponseForRequest(writer, nil, status, body)
}

func probeResponseForRequest(writer http.ResponseWriter, request *http.Request, status int, body string) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	if request != nil && request.Method == http.MethodHead {
		return
	}
	_, _ = writer.Write([]byte(body))
}

func probeMethodAllowed(writer http.ResponseWriter, request *http.Request) bool {
	if request != nil && (request.Method == http.MethodGet || request.Method == http.MethodHead) {
		return true
	}
	writer.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
	probeResponse(writer, http.StatusMethodNotAllowed, "method not allowed\n")
	return false
}

type Options struct {
	Address           string
	Health            *HealthState
	Metrics           http.Handler
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

type serverState uint8

const (
	// A failed bind returns to idle because no listener or Serve goroutine was
	// published. Once Serve is published, shutdown or exit closes the server.
	serverIdle serverState = iota
	serverStarting
	serverStarted
	serverClosed
)

// Server owns only the probe listener. It is intentionally independent of
// the worker so readiness can be turned off before worker shutdown begins.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	errCh      chan error

	mu        sync.Mutex
	state     serverState
	startDone chan struct{}
	errOnce   sync.Once
}

func New(options Options) (*Server, error) {
	if options.Address == "" {
		return nil, fmt.Errorf("health server address is required")
	}
	if options.ReadHeaderTimeout <= 0 {
		options.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = options.ReadHeaderTimeout
	}
	return &Server{
		httpServer: &http.Server{
			Addr:              options.Address,
			Handler:           Handler(options.Health, options.Metrics),
			ReadTimeout:       options.ReadTimeout,
			ReadHeaderTimeout: options.ReadHeaderTimeout,
		},
		errCh: make(chan error, 1),
	}, nil
}

// Start binds before returning, which makes a failed bind a startup error and
// gives callers a stable address for tests using :0.
func (server *Server) Start() error {
	if server == nil || server.httpServer == nil {
		return fmt.Errorf("health server is not initialized")
	}
	server.mu.Lock()
	if server.state != serverIdle {
		server.mu.Unlock()
		return fmt.Errorf("health server is already started")
	}
	server.state = serverStarting
	server.startDone = make(chan struct{})
	address := server.httpServer.Addr
	server.mu.Unlock()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		server.finishStart(serverIdle, nil)
		return fmt.Errorf("listen for health server: %w", err)
	}
	server.finishStart(serverStarted, listener)
	go func() {
		err := server.httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			server.errCh <- err
		}
		server.closeErrorStream()
		server.mu.Lock()
		server.state = serverClosed
		server.mu.Unlock()
	}()
	return nil
}

func (server *Server) finishStart(state serverState, listener net.Listener) {
	server.mu.Lock()
	server.state = state
	server.listener = listener
	done := server.startDone
	server.startDone = nil
	if done != nil {
		close(done)
	}
	server.mu.Unlock()
}

func (server *Server) closeErrorStream() {
	server.errOnce.Do(func() { close(server.errCh) })
}

func (server *Server) Addr() string {
	if server == nil {
		return ""
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.listener == nil {
		return ""
	}
	return server.listener.Addr().String()
}

func (server *Server) Errors() <-chan error {
	if server == nil {
		return nil
	}
	return server.errCh
}

func (server *Server) Shutdown(ctx context.Context) error {
	if server == nil || server.httpServer == nil {
		return nil
	}
	for {
		server.mu.Lock()
		switch server.state {
		case serverStarting:
			done := server.startDone
			server.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		case serverIdle:
			server.state = serverClosed
			server.mu.Unlock()
			server.closeErrorStream()
			return server.httpServer.Shutdown(ctx)
		case serverClosed:
			server.mu.Unlock()
			return nil
		case serverStarted:
			server.mu.Unlock()
			err := server.httpServer.Shutdown(ctx)
			if err == nil {
				server.mu.Lock()
				server.state = serverClosed
				server.mu.Unlock()
			}
			return err
		default:
			server.mu.Unlock()
			return nil
		}
	}
}
