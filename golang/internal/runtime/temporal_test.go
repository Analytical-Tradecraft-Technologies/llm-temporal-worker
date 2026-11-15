package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	testassert.NoError(t, err, "create lazy Temporal client")
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
	testassert.NoError(t, err, "dial Temporal with transient frontend outage")
	if got != value {
		t.Fatal("retry returned a different Temporal client")
	}
	requireCount(t, &attempts, 3, "dial attempts")
}

// A transient outage stops at the bounded attempt budget; a permanent error
// is not retried at all.
func TestDialTemporalWithRetryStopsOnFailure(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		attempts int32
	}{
		{name: "bounded", err: status.Error(codes.Unavailable, "frontend unavailable"), attempts: 3},
		{name: "permanent", err: status.Error(codes.PermissionDenied, "invalid Temporal credentials"), attempts: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			dial := func(context.Context, client.Options) (client.Client, error) {
				attempts.Add(1)
				return nil, test.err
			}
			_, err := dialTemporalWithRetry(context.Background(), dial, client.Options{}, temporalDialTestPolicy())
			if !errors.Is(err, test.err) {
				t.Fatalf("%s dial error = %v, want %v", test.name, err, test.err)
			}
			requireCount(t, &attempts, test.attempts, test.name+" dial attempts")
		})
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
	testassert.ErrorIs(t, err, context.Canceled, "canceled dial")
	requireCount(t, &attempts, 1, "canceled dial attempts")
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
	testassert.NoError(t, err, "factory Temporal dial")
	if got != value {
		t.Fatal("factory returned a different Temporal client")
	}
	requireCount(t, &attempts, 2, "factory dial attempts")
}

func TestTemporalClientFactoryBindsConfiguredLogger(t *testing.T) {
	var output bytes.Buffer
	logger := testLogger(t, observability.LogOptions{Output: &output, Level: "warn"})
	called := false
	factory := DefaultTemporalClientFactory{
		Logger: logger,
		DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
			called = true
			if options.Logger == nil {
				t.Fatal("SDK default logger would be used")
			}
			options.Logger.Info("filtered SDK event")
			options.Logger.Warn("SDK event", "TaskQueue", "queue-1", "Error", errors.New("private-provider-error"))
			return nil, nil
		},
	}
	testassert.NoError(t, testassert.Err(factory.New(context.Background(), config.Config{})))
	if !called || strings.Contains(output.String(), "filtered") || strings.Contains(output.String(), "private-provider-error") || !strings.Contains(output.String(), `"task_queue":"queue-1"`) {
		t.Fatalf("unexpected SDK logging: called=%v logs=%s", called, output.String())
	}
}

func TestTemporalClientFactoryConfiguresSlogWhenLoggerOmitted(t *testing.T) {
	var value config.Config
	value.Telemetry.Logs.Level = "error"
	called := false
	factory := DefaultTemporalClientFactory{
		DialContext: func(_ context.Context, options client.Options) (client.Client, error) {
			called = true
			if options.Logger == nil {
				t.Fatal("SDK default logger would be used")
			}
			return nil, nil
		},
	}
	if _, err := factory.New(context.Background(), value); err != nil || !called {
		t.Fatalf("factory err=%v called=%v", err, called)
	}
	value.Telemetry.Logs.Format = "invalid"
	called = false
	if _, err := factory.New(context.Background(), value); err == nil || called {
		t.Fatalf("invalid logging config reached SDK: err=%v called=%v", err, called)
	}
}
