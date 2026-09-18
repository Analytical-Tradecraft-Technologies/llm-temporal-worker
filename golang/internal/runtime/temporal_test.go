package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func temporalDialTestPolicy() temporalDialRetryPolicy {
	return temporalDialRetryPolicy{
		maxAttempts:    3,
		initialBackoff: time.Millisecond,
		maxBackoff:     2 * time.Millisecond,
	}
}

func temporalLazyClient(t *testing.T) client.Client {
	t.Helper()
	value, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: "default"})
	if err != nil {
		t.Fatalf("create lazy Temporal client: %v", err)
	}
	t.Cleanup(value.Close)
	return value
}

func TestDialTemporalWithRetryRetriesTransientFrontendUnavailable(t *testing.T) {
	var attempts atomic.Int32
	value := temporalLazyClient(t)
	dial := func(context.Context, client.Options) (client.Client, error) {
		if attempts.Add(1) < 3 {
			return nil, status.Error(codes.Unavailable, "frontend is still starting")
		}
		return value, nil
	}

	got, err := dialTemporalWithRetry(context.Background(), dial, client.Options{}, temporalDialTestPolicy())
	if err != nil {
		t.Fatalf("dial Temporal with transient frontend outage: %v", err)
	}
	if got != value {
		t.Fatal("retry returned a different Temporal client")
	}
	if got, want := attempts.Load(), int32(3); got != want {
		t.Fatalf("dial attempts = %d, want %d", got, want)
	}
}

func TestDialTemporalWithRetryStopsAtBoundedAttemptBudget(t *testing.T) {
	var attempts atomic.Int32
	wantErr := status.Error(codes.Unavailable, "frontend unavailable")
	dial := func(context.Context, client.Options) (client.Client, error) {
		attempts.Add(1)
		return nil, wantErr
	}

	_, err := dialTemporalWithRetry(context.Background(), dial, client.Options{}, temporalDialTestPolicy())
	if !errors.Is(err, wantErr) {
		t.Fatalf("bounded dial error = %v, want %v", err, wantErr)
	}
	if got, want := attempts.Load(), int32(3); got != want {
		t.Fatalf("dial attempts = %d, want bounded budget %d", got, want)
	}
}

func TestDialTemporalWithRetryDoesNotRetryPermanentErrors(t *testing.T) {
	var attempts atomic.Int32
	wantErr := status.Error(codes.PermissionDenied, "invalid Temporal credentials")
	dial := func(context.Context, client.Options) (client.Client, error) {
		attempts.Add(1)
		return nil, wantErr
	}

	_, err := dialTemporalWithRetry(context.Background(), dial, client.Options{}, temporalDialTestPolicy())
	if !errors.Is(err, wantErr) {
		t.Fatalf("permanent dial error = %v, want %v", err, wantErr)
	}
	if got, want := attempts.Load(), int32(1); got != want {
		t.Fatalf("permanent-error dial attempts = %d, want %d", got, want)
	}
}

func TestDialTemporalWithRetryHonorsCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	dial := func(context.Context, client.Options) (client.Client, error) {
		attempts.Add(1)
		cancel()
		return nil, status.Error(codes.Unavailable, "frontend is still starting")
	}

	_, err := dialTemporalWithRetry(ctx, dial, client.Options{}, temporalDialTestPolicy())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial error = %v, want context.Canceled", err)
	}
	if got, want := attempts.Load(), int32(1); got != want {
		t.Fatalf("canceled dial attempts = %d, want %d", got, want)
	}
}

func TestDefaultTemporalClientFactoryUsesBoundedDialRetry(t *testing.T) {
	var attempts atomic.Int32
	value := temporalLazyClient(t)
	factory := DefaultTemporalClientFactory{
		Identity: "test-worker",
		DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
			if got := options.HostPort; got != "temporal.example.test:7233" {
				t.Errorf("Temporal target = %q, want temporal.example.test:7233", got)
			}
			if got := options.Namespace; got != "test" {
				t.Errorf("Temporal namespace = %q, want test", got)
			}
			if options.DataConverter == nil {
				t.Error("Temporal client did not receive the bounded Activity payload converter")
			} else if _, err := options.DataConverter.ToPayload(strings.Repeat("x", 128)); err == nil {
				t.Error("Temporal client converter accepted payload above configured inline limit")
			}
			if attempts.Add(1) < 2 {
				return nil, status.Error(codes.Unavailable, "frontend is still starting")
			}
			return value, nil
		},
	}

	got, err := factory.New(context.Background(), config.Config{Temporal: config.TemporalConfig{
		Target:    "temporal.example.test:7233",
		Namespace: "test",
	}, Server: config.ServerConfig{InlinePayloadBytes: 64}})
	if err != nil {
		t.Fatalf("factory Temporal dial: %v", err)
	}
	if got != value {
		t.Fatal("factory returned a different Temporal client")
	}
	if got, want := attempts.Load(), int32(2); got != want {
		t.Fatalf("factory dial attempts = %d, want %d", got, want)
	}
}

func TestDefaultTemporalClientFactoryLoadsCompactJWTOverTLS(t *testing.T) {
	token := "eyJhbGciOiJSUzI1NiJ9.e30.AQ"
	caPEM := testCertificateAuthorityPEM(t)
	value := temporalLazyClient(t)
	factory := DefaultTemporalClientFactory{
		Identity: "test-worker",
		ReadFile: func(path string) ([]byte, error) {
			switch path {
			case "/var/run/secrets/llmtw/temporal-api-key":
				return []byte(token), nil
			case "/var/run/secrets/llmtw/temporal-ca.pem":
				return caPEM, nil
			default:
				return nil, errors.New("unexpected file")
			}
		},
		DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
			if options.Credentials == nil {
				t.Fatal("Temporal client credentials are nil")
			}
			if options.ConnectionOptions.TLS == nil {
				t.Fatal("Temporal client TLS configuration is nil")
			}
			if got := options.ConnectionOptions.TLS.MinVersion; got != tls.VersionTLS13 {
				t.Fatalf("minimum TLS version = %#x, want TLS 1.3", got)
			}
			return value, nil
		},
	}

	got, err := factory.New(context.Background(), config.Config{
		Environment: "production",
		Server:      config.ServerConfig{InlinePayloadBytes: 64},
		Temporal: config.TemporalConfig{
			Target:     "temporal-frontend.temporal.svc.cluster.local:7233",
			Namespace:  "ai-ach",
			APIKeyFile: "/var/run/secrets/llmtw/temporal-api-key",
			TLS: config.TLSConfig{
				Enabled:    true,
				ServerName: "temporal-frontend.temporal.svc.cluster.local",
				CAFile:     "/var/run/secrets/llmtw/temporal-ca.pem",
			},
		},
	})
	if err != nil {
		t.Fatalf("factory Temporal dial: %v", err)
	}
	if got != value {
		t.Fatal("factory returned a different Temporal client")
	}
}

func TestDefaultTemporalClientFactoryRejectsJWTBeforeDial(t *testing.T) {
	for _, test := range []struct {
		name    string
		jwtFile string
		token   string
	}{
		{name: "missing", jwtFile: ""},
		{name: "opaque", jwtFile: "/jwt", token: "secret-opaque-value"},
		{name: "Bearer prefix", jwtFile: "/jwt", token: "Bearer eyJhbGciOiJSUzI1NiJ9.e30.AQ"},
		{name: "surrounding whitespace", jwtFile: "/jwt", token: " eyJhbGciOiJSUzI1NiJ9.e30.AQ\n"},
		{name: "too large", jwtFile: "/jwt", token: strings.Repeat("a", (16<<10)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			dialed := false
			factory := DefaultTemporalClientFactory{
				ReadFile: func(string) ([]byte, error) { return []byte(test.token), nil },
				DialContext: func(context.Context, client.Options) (client.Client, error) {
					dialed = true
					return nil, errors.New("must not dial")
				},
			}
			_, err := factory.New(context.Background(), config.Config{
				Environment: "production",
				Server:      config.ServerConfig{InlinePayloadBytes: 64},
				Temporal: config.TemporalConfig{
					APIKeyFile: test.jwtFile,
					TLS: config.TLSConfig{
						Enabled:    true,
						ServerName: "temporal-frontend.temporal.svc.cluster.local",
						CAFile:     "/ca",
					},
				},
			})
			if err == nil {
				t.Fatal("invalid Temporal JWT was accepted")
			}
			if dialed {
				t.Fatal("Temporal was dialed after JWT validation failed")
			}
			if test.token != "" && strings.Contains(err.Error(), test.token) {
				t.Fatal("Temporal JWT error exposed credential bytes")
			}
		})
	}
}

func TestLoadTemporalJWTDoesNotExposeCredentialOrReaderError(t *testing.T) {
	const marker = "credential-marker-that-must-not-leak"
	for _, readFile := range []func(string) ([]byte, error){
		func(string) ([]byte, error) { return []byte(marker), nil },
		func(string) ([]byte, error) { return nil, errors.New(marker) },
	} {
		_, err := loadTemporalJWT("/jwt", readFile)
		if err == nil {
			t.Fatal("unsafe Temporal JWT input was accepted")
		}
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("Temporal JWT error exposed secret material: %v", err)
		}
	}
}

func TestReadBoundedTemporalAPIKeyFileRejectsOversizedCredential(t *testing.T) {
	path := t.TempDir() + "/temporal-api-key"
	if err := os.WriteFile(path, []byte(strings.Repeat("a", (16<<10)+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedTemporalAPIKeyFile(path); err == nil {
		t.Fatal("bounded Temporal JWT reader accepted an oversized credential")
	}
}
