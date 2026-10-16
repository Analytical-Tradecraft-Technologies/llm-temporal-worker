package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func providerUnauthorized() *provider.Error {
	failure := provider.NewError(provider.CodeAuthentication, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "provider authentication failed")
	failure.SafeDetails = map[string]string{"status": "401"}
	return failure
}

func providerPaymentRequired() *provider.Error {
	failure := provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "provider account has insufficient credit")
	failure.SafeDetails = map[string]string{"status": "402"}
	return failure
}

// credentialFailoverCloud is a bounded cloud fixture with keyed route health.
// With alternate set it adds a second, lower-priority route on
// alternate-endpoint. The primary endpoint always answers with rejection;
// calls counts submissions per endpoint.
func credentialFailoverCloud(t *testing.T, alternate bool, rejection func() *provider.Error) (*boundedCloudFixture, *keyedHealthFixture, func() map[string]int) {
	t.Helper()
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		if !alternate {
			return
		}
		model := b.source.value.Routes.Models["alias"]
		second := model.Routes[0]
		second.ID, second.EndpointID = "alternate", "alternate-endpoint"
		model.Routes = append(model.Routes, second)
		b.source.value.Routes.Models["alias"] = model
		b.source.value.BudgetPolicies[0].Match.EndpointID = ""
		entry := b.entry
		entry.EndpointID = "alternate-endpoint"
		b.prices(t, []pricing.Entry{b.entry, entry})
	})
	shared := &keyedHealthFixture{}
	f.cap.ProviderRouteStatus, f.cap.ProviderStatusRecorder = shared, shared
	var mu sync.Mutex
	calls := map[string]int{}
	answer := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		mu.Lock()
		calls[call.EndpointID]++
		mu.Unlock()
		if call.EndpointID == "endpoint" {
			f.submits.Add(1)
			if err := o.BeforePossibleWrite(ctx); err != nil {
				return provider.Result{}, err
			}
			return provider.Result{}, rejection()
		}
		return answer(ctx, call, o)
	}
	if alternate {
		f.cap.Adapters.(engine.AdapterMap)["alternate-endpoint"] = f.adapter.executionSyncAdapter
	}
	f.restart(t)
	return f, shared, func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		copied := make(map[string]int, len(calls))
		for endpoint, count := range calls {
			copied[endpoint] = count
		}
		return copied
	}
}

// A provider 401 or 402 rejects the route, not the request: the failing
// request moves to a healthy route instead of failing outright, and later
// requests skip the held route (#1335, #1336).
func TestCloudCredentialRejectionFailsOverToAnotherRoute(t *testing.T) {
	for _, test := range []struct {
		name      string
		rejection func() *provider.Error
		code      provider.Code
	}{
		{name: "401", rejection: providerUnauthorized, code: provider.CodeAuthentication},
		{name: "402", rejection: providerPaymentRequired, code: provider.CodePermissionDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _, calls := credentialFailoverCloud(t, true, test.rejection)
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if !v.Retryable || v.ErrorCode != string(test.code) {
				t.Fatalf("rejection = %+v, want a retryable %s", v, test.code)
			}
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			// Past the ordinary cooldown, a new request still skips the
			// rejected route instead of failing on it again.
			f.now = f.now.Add(time.Minute)
			f.restart(t)
			f.request.OperationKey = "after-rejection"
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			if got := calls(); got["endpoint"] != 1 || got["alternate-endpoint"] != 2 {
				t.Fatalf("calls = %v, want one rejected call and the rest on the alternate", got)
			}
		})
	}
}

// With no other route, a credential rejection must not leave the request
// waiting for the route's hold: the replacement's selection reports the
// rejection as terminal and nothing is dispatched again.
func TestCloudCredentialRejectionWithoutAlternativeIsTerminal(t *testing.T) {
	for _, test := range []struct {
		name      string
		rejection func() *provider.Error
		code      provider.Code
	}{
		{name: "401", rejection: providerUnauthorized, code: provider.CodeAuthentication},
		{name: "402", rejection: providerPaymentRequired, code: provider.CodePermissionDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _, _ := credentialFailoverCloud(t, false, test.rejection)
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
			f.now = f.now.Add(2 * time.Second)
			_, err = f.runtime.AcquireBudgetV1(ctx, ref)
			var classified *provider.Error
			if !errors.As(err, &classified) || classified.Code != test.code || classified.Retry != provider.RetryNever {
				t.Fatalf("acquire error = %v, want a terminal %s", err, test.code)
			}
			if f.submits.Load() != 1 {
				t.Fatalf("submits = %d, want the held route not dispatched again", f.submits.Load())
			}
		})
	}
}

// Without shared route health the replacement goes back to the only route; the
// attempt limit still ends the request with the terminal rejection.
func TestCloudCredentialRejectionExhaustsAttempts(t *testing.T) {
	f, _, _ := credentialFailoverCloud(t, false, providerUnauthorized)
	f.cap.ProviderRouteStatus, f.cap.ProviderStatusRecorder = nil, nil
	f.options.MaxAttempts = 2
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	f.now = f.now.Add(2 * time.Second)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	f.now = f.now.Add(2 * time.Second)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionFailed)
	if v.Retryable || v.ErrorCode != string(provider.CodeAuthentication) || v.FailureCode != "provider_rejected" {
		t.Fatalf("exhausted result = %+v, want the terminal authentication failure", v)
	}
	if f.submits.Load() != 2 {
		t.Fatalf("submits = %d, want one per attempt", f.submits.Load())
	}
}

// flakyHealthReader fails reads while failing is set.
type flakyHealthReader struct {
	*keyedHealthFixture
	failing bool
}

func (h *flakyHealthReader) GetRouteStatus(ctx context.Context, digest [32]byte, route, endpoint, model, tier string) (control.RouteStatus, error) {
	if h.failing {
		return control.RouteStatus{}, errors.New("health unavailable")
	}
	return h.keyedHealthFixture.GetRouteStatus(ctx, digest, route, endpoint, model, tier)
}

// An unused quote renewal between the replacement and the rejected attempt
// must not hide the rejection: selection still ends the request terminally.
func TestCloudCredentialRejectionTerminalAfterRenewal(t *testing.T) {
	f, shared, _ := credentialFailoverCloud(t, false, providerUnauthorized)
	reader := &flakyHealthReader{keyedHealthFixture: shared}
	f.cap.ProviderRouteStatus = reader
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	f.now = f.now.Add(2 * time.Second)
	reader.failing = true
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeStateUnavailable {
		t.Fatalf("acquire error = %v, want the health read failure", err)
	}
	// The replacement attempt is now old enough to be renewed unused.
	reader.failing = false
	f.now = f.now.Add(cache.MaxFillLease)
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	if !errors.As(err, &classified) || classified.Code != provider.CodeAuthentication || classified.Retry != provider.RetryNever {
		t.Fatalf("acquire error = %v, want a terminal authentication failure", err)
	}
	if f.submits.Load() != 1 {
		t.Fatalf("submits = %d, want the held route not dispatched again", f.submits.Load())
	}
}

// A 402 holds its route at once, past the ordinary cooldown, while a plain 403
// only counts toward the threshold. The credit hold is short and lapses.
func TestCloudRouteHealthHoldsInsufficientCredit(t *testing.T) {
	f, shared, _ := credentialFailoverCloud(t, false, providerPaymentRequired)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if len(shared.records) != 1 {
		t.Fatalf("records = %+v, want one", shared.records)
	}
	for _, status := range shared.records {
		if status.Circuit != control.CircuitOpen || status.Credit != control.CreditOK || !status.StaleAfter.Equal(f.now.Add(creditFailureHold)) {
			t.Fatalf("status = %+v, want an open, non-sticky credit hold", status)
		}
	}

	g, plain, _ := credentialFailoverCloud(t, false, func() *provider.Error {
		failure := provider.NewError(provider.CodePermissionDenied, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNever, "provider permission was denied")
		failure.SafeDetails = map[string]string{"status": "403"}
		return failure
	})
	v, err = g.runtime.GenerateStepV1(ctx, g.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if v.Retryable {
		t.Fatalf("403 = %+v, want a terminal request failure", v)
	}
	for _, status := range plain.records {
		if status.Circuit == control.CircuitOpen {
			t.Fatalf("status = %+v, a single 403 opened the route", status)
		}
	}
}
