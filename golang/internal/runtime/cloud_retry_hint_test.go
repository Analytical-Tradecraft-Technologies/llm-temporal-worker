package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// retryHintCloud is a bounded cloud fixture with a second, lower-priority
// route on alternate-endpoint. limited lists the endpoints that answer with a
// rate limit carrying retryAfter; calls counts submissions per endpoint.
func retryHintCloud(t *testing.T, retryAfter time.Duration, limited ...string) (*boundedCloudFixture, func() map[string]int) {
	t.Helper()
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.addFallbackRoute(t, "alternate", "alternate-endpoint")
	})
	var mu sync.Mutex
	calls := map[string]int{}
	answer := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		mu.Lock()
		calls[call.EndpointID]++
		mu.Unlock()
		for _, endpoint := range limited {
			if call.EndpointID == endpoint {
				f.submits.Add(1)
				failure := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "rate limited")
				failure.RetryAfter = retryAfter
				return provider.Result{}, failure
			}
		}
		return answer(ctx, call, o)
	}
	f.cap.Adapters.(engine.AdapterMap)["alternate-endpoint"] = f.adapter.executionSyncAdapter
	f.restart(t)
	return f, func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		copied := make(map[string]int, len(calls))
		for endpoint, count := range calls {
			copied[endpoint] = count
		}
		return copied
	}
}

// A rate-limited route's Retry-After binds only that route: with a healthy
// alternative the next acquisition fails over at once (#1334).
func TestCloudRetryAfterDoesNotDelayFailover(t *testing.T) {
	f, calls := retryHintCloud(t, time.Hour, "endpoint")
	ctx, v := f.generateStep(t, llm.ExecutionFailed)
	if !v.Retryable {
		t.Fatal("rate limit not retryable")
	}
	ref := f.ref(v)
	f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	v = f.step(t, ctx, f.request, llm.ExecutionProviderCompleted)
	if got := calls(); got["endpoint"] != 1 || got["alternate-endpoint"] != 1 {
		t.Fatalf("calls = %v, want one per route", got)
	}
}

// When every route is rate limited, a replacement that comes back to a hinted
// route still waits out that route's Retry-After, even past a failover.
func TestCloudRetryAfterHoldsTheHintedRoute(t *testing.T) {
	f, calls := retryHintCloud(t, time.Hour, "endpoint", "alternate-endpoint")
	f.options.MaxAttempts = 3
	f.restart(t)
	ctx, v := f.generateStep(t, llm.ExecutionFailed)
	ref := f.ref(v)
	f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	f.now = f.now.Add(time.Minute)
	f.step(t, ctx, f.request, llm.ExecutionFailed)
	// The third attempt returns to the first route, still 59 minutes into its
	// hour; the second route's own hint is a minute longer.
	v = f.acquire(t, ctx, ref, llm.ExecutionBudgetWait)
	if v.RetryAfterSeconds != 59*60 {
		t.Fatalf("retry after = %d, want the first route's remaining hint", v.RetryAfterSeconds)
	}
	// After the hint and an unused quote renewal, the first route is retried.
	f.now = f.now.Add(59 * time.Minute)
	v = f.acquire(t, ctx, ref, llm.ExecutionAcquired)
	if got := calls(); got["endpoint"] != 1 || got["alternate-endpoint"] != 1 {
		t.Fatalf("calls = %v, want no dispatch before the hint", got)
	}
}

// A request with no attempts left fails at once instead of sleeping out the
// last attempt's Retry-After first.
func TestCloudExhaustionDoesNotWaitForRetryAfter(t *testing.T) {
	f := boundedCloud(t, false)
	f.options.MaxAttempts = 1
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		failure := provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "rate limited")
		failure.RetryAfter = 6 * time.Hour
		return provider.Result{}, failure
	}
	ctx, v := f.generateStep(t, llm.ExecutionFailed)
	ref := f.ref(v)
	v = f.acquire(t, ctx, ref, llm.ExecutionFailed)
	if v.Retryable || v.ErrorCode != string(provider.CodeProviderRateLimited) {
		t.Fatalf("exhausted result = %+v, want the terminal rate limit", v)
	}
	requireCount(t, &f.submits, 1, "exhausted request submitted again")
}
