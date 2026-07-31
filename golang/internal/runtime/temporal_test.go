package runtime

import (
	"context"
	"errors"
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
