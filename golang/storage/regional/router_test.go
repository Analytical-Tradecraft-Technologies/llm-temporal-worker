package regional

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
)

func TestFailureRouting(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		mutation   bool
		wantCalls  int
		wantActive int
	}{
		{"read outage", contracts.ErrUnavailable, false, 2, 1},
		{"known rejected write", contracts.ErrUnavailable, true, 2, 1},
		{"unknown write", &contracts.StorageError{Kind: contracts.ErrUnavailable, OutcomeUnknown: true}, true, 1, 1},
		{"conditional conflict", contracts.ErrConflict, true, 1, 0},
		{"permission", contracts.ErrPermissionDenied, false, 1, 0},
		{"missing record", contracts.ErrNotFound, false, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Router{Count: 2, Timeout: time.Second}
			calls := 0
			retry := RetryRead
			if tc.mutation {
				retry = RetryMutation
			}
			_, err := Run(context.Background(), r, retry, func(_ context.Context, i int) (int, error) {
				calls++
				if i == 0 {
					return 0, tc.err
				}
				return 1, nil
			})
			if calls != tc.wantCalls || int(r.Active.Load()) != tc.wantActive {
				t.Fatalf("calls=%d active=%d err=%v", calls, r.Active.Load(), err)
			}
			if calls == 1 && !errors.Is(err, tc.err) {
				t.Fatalf("lost original error: %v", err)
			}
		})
	}
}
func TestAttemptReservesTimeForFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r := &Router{Count: 2, Timeout: time.Second}
	value, err := Run(ctx, r, RetryRead, func(ctx context.Context, i int) (int, error) {
		if i == 0 {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return i, nil
	})
	if err != nil || value != 1 {
		t.Fatalf("fallback=%d err=%v", value, err)
	}
}
func TestParentCancellationStopsRouting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{Count: 2, Timeout: time.Second}
	calls := 0
	_, err := Run(ctx, r, RetryRead, func(context.Context, int) (int, error) { calls++; cancel(); return 0, contracts.ErrUnavailable })
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
func TestConcurrentRouting(t *testing.T) {
	r := &Router{Count: 3, Timeout: time.Second}
	var wg sync.WaitGroup
	for n := 0; n < 30; n++ {
		wg.Go(func() {
			_, err := Run(context.Background(), r, RetryRead, func(_ context.Context, i int) (int, error) {
				if i == 0 {
					return 0, contracts.ErrUnavailable
				}
				return i, nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}
func TestUnavailableAndUnreplicatedDoesNotBecomeAbsent(t *testing.T) {
	r := &Router{Count: 2, Timeout: time.Second}
	_, err := Run(context.Background(), r, func(err error) bool { return Unavailable(err) || Missing(err) }, func(_ context.Context, i int) (int, error) {
		if i == 0 {
			return 0, contracts.ErrUnavailable
		}
		return 0, contracts.ErrNotFound
	})
	if !errors.Is(err, contracts.ErrUnavailable) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("lost outage behind replication lag: %v", err)
	}
}
