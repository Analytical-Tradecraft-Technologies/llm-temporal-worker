package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	memory "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/memory"
)

// updateSnapshot applies change to the harness's static snapshot and installs
// the result.
func (harness testHarness) updateSnapshot(change func(*Snapshot)) {
	snapshot := harness.engine.dependencies.Snapshots.(StaticSnapshot).Value
	change(&snapshot)
	harness.engine.dependencies.Snapshots = StaticSnapshot{Value: snapshot}
}

// useMemoryContinuations installs a real in-memory continuation store whose
// keyring secret is filled with keySeed and whose nonce source is filled with
// keySeed+1.
func (harness testHarness) useMemoryContinuations(t *testing.T, keySeed byte) *memory.ContinuationStore {
	t.Helper()
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{keySeed}, 32), Primary: true}}, bytes.NewReader(bytes.Repeat([]byte{keySeed + 1}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	store, err := memory.NewContinuationStore(memory.ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return harness.clock }})
	if err != nil {
		t.Fatal(err)
	}
	harness.engine.dependencies.Continuations = store
	return store
}

// operation loads the durable admission record for operationID.
func (harness testHarness) operation(t *testing.T, operationID string) admission.Operation {
	t.Helper()
	operation, err := harness.admission.Get(context.Background(), operationID)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

// testOperationIdentity is the engine's operation identity for a request.
type testOperationIdentity struct {
	normalized llm.Request
	digest     [32]byte
	id         string
	scopeKey   string
}

// normalizedIdentityForTest derives the operation identity from the
// normalized request, as the pre-admission paths do.
func normalizedIdentityForTest(t *testing.T, request llm.Request) testOperationIdentity {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return identityForTest(t, normalized)
}

// preparedIdentityForTest derives the operation identity from the request as
// the harness estimator prepares it for admission.
func preparedIdentityForTest(t *testing.T, request llm.Request) testOperationIdentity {
	t.Helper()
	prepared, err := (budget.Estimator{MaxOutput: 1}).PrepareRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return identityForTest(t, prepared)
}

func identityForTest(t *testing.T, normalized llm.Request) testOperationIdentity {
	t.Helper()
	digest, err := llm.RequestDigest(normalized)
	if err != nil {
		t.Fatal(err)
	}
	id, scopeKey := operationIdentity(normalized, digest)
	return testOperationIdentity{normalized: normalized, digest: digest, id: id, scopeKey: scopeKey}
}

// hourlyBudgetPolicy is a policy with one hourly window of 1,000 microUSD.
func hourlyBudgetPolicy(id string, match budget.Matcher) budget.Policy {
	return budget.Policy{
		ID:      id,
		Match:   match,
		Windows: []budget.Window{{ID: id + "/hour", Duration: time.Hour, Bucket: time.Minute, Limit: 1_000}},
	}
}

// tenantClassBudgetPolicy budgets tenant-1 requests of one service class.
func tenantClassBudgetPolicy(id string, class llm.ServiceClass) budget.Policy {
	return hourlyBudgetPolicy(id, budget.Matcher{Tenant: "tenant-1", ServiceClass: class})
}

// requireProviderError fails with unexpectedSuccess when err is nil and
// otherwise requires err to wrap a *provider.Error.
func requireProviderError(t *testing.T, err error, unexpectedSuccess string) *provider.Error {
	t.Helper()
	if err == nil {
		t.Fatal(unexpectedSuccess)
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("error = %T, want *provider.Error", err)
	}
	return mapped
}

// isNotDispatchedFailure reports whether err wraps a non-retryable,
// not-dispatched provider error with code in phase.
func isNotDispatchedFailure(err error, code provider.Code, phase provider.Phase) (*provider.Error, bool) {
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		return nil, false
	}
	return mapped, mapped.Code == code && mapped.Phase == phase && mapped.Dispatch == provider.DispatchNotDispatched && mapped.Retry == provider.RetryNever
}

// requireNoEligiblePrice requires the no-route price failure that planning
// returns when no candidate has a usable price.
func requireNoEligiblePrice(t *testing.T, err error, unexpectedSuccess string) {
	t.Helper()
	if err == nil {
		t.Fatal(unexpectedSuccess)
	}
	mapped, ok := isNotDispatchedFailure(err, provider.CodeNoRoute, provider.PhasePrice)
	if !ok {
		t.Fatalf("error = %#v, want no-route price failure", err)
	}
	if mapped.SafeDetails["reason"] != "no_eligible_price" {
		t.Fatalf("safe details = %#v, want no_eligible_price", mapped.SafeDetails)
	}
}

// invokeCount reads how many times the provider was invoked.
func (adapter *fakeAdapter) invokeCount() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.invokes
}

// requireInvokes fails with format (which receives the observed count) unless
// the provider was invoked exactly want times.
func (adapter *fakeAdapter) requireInvokes(t *testing.T, want int, format string) {
	t.Helper()
	if got := adapter.invokeCount(); got != want {
		t.Fatalf(format, got)
	}
}

// requireOnlyStandardCall requires exactly one provider call, on the standard
// class.
func (adapter *fakeAdapter) requireOnlyStandardCall(t *testing.T, want string) {
	t.Helper()
	calls := adapter.recordedCalls()
	if len(calls) != 1 || calls[0].ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("provider calls = %#v, want %s", calls, want)
	}
}

// testTrace records spans in memory for one test.
type testTrace struct {
	ctx      context.Context
	tracer   *observability.Tracer
	exporter *observability.MemoryExporter
}

func newTestTrace() testTrace {
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	return testTrace{ctx: observability.WithTracer(context.Background(), tracer), tracer: tracer, exporter: exporter}
}

// shutdown flushes every span to the exporter.
func (trace testTrace) shutdown(t *testing.T) {
	t.Helper()
	if err := trace.tracer.Shutdown(trace.ctx); err != nil {
		t.Fatal(err)
	}
}
