package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
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

// Server owns only the probe listener. It is intentionally independent of
// the worker so readiness can be turned off before worker shutdown begins.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	errCh      chan error
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
	if server.listener != nil {
		return fmt.Errorf("health server is already started")
	}
	listener, err := net.Listen("tcp", server.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen for health server: %w", err)
	}
	server.listener = listener
	go func() {
		err := server.httpServer.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			server.errCh <- err
		}
		close(server.errCh)
	}()
	return nil
}

func (server *Server) Addr() string {
	if server == nil || server.listener == nil {
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
	return server.httpServer.Shutdown(ctx)
}
