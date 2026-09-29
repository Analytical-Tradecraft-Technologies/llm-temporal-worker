package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

type recordingCloudRequests struct {
	operation                       cloudstate.Operation
	record                          cloudstate.Record
	beginErr, completeErr, probeErr error
	complete                        func(context.Context)
	checkpointStore                 state.CheckpointStore
}

func (s *recordingCloudRequests) Checkpoints() state.CheckpointStore { return s.checkpointStore }

func (s *recordingCloudRequests) BeginOperation(_ context.Context, op cloudstate.Operation) (cloudstate.Record, error) {
	s.operation = op
	if s.record.Status == "" {
		s.record = cloudstate.Record{Request: cloudstate.CreateRequest{ID: "llmtw_req_00000000-0000-4000-8000-000000000001", Scope: op.Scope}, Status: cloudstate.StatusRunning}
	}
	return s.record, s.beginErr
}
func (s *recordingCloudRequests) CompleteOperation(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, data json.RawMessage, now time.Time) (cloudstate.Record, error) {
	if s.complete != nil {
		s.complete(ctx)
	}
	if s.completeErr != nil {
		return cloudstate.Record{}, s.completeErr
	}
	s.record.Status, s.record.Progress = cloudstate.StatusCompleted, append(json.RawMessage(nil), data...)
	return s.record, nil
}
func (s *recordingCloudRequests) Probe(context.Context) error { return s.probeErr }

type cloudInnerRuntime struct {
	generate func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error)
	compact  func(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error)
	queries  int
}

func (r *cloudInnerRuntime) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	return r.generate(ctx, request)
}
func (r *cloudInnerRuntime) CompactV1(ctx context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return r.compact(ctx, request)
}
func (r *cloudInnerRuntime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	r.queries++
	return llm.QueryResponseV1{}, nil
}

func cloudRuntimeFixture() (*cloudRequestRuntime, *recordingCloudRequests, *cloudInnerRuntime, llm.GenerateRequestV1) {
	repository, inner := &recordingCloudRequests{}, &cloudInnerRuntime{}
	request := llm.GenerateRequestV1{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
	r := &cloudRequestRuntime{requests: repository, inner: inner, clock: time.Now, finalizationTimeout: time.Second}
	return r, repository, inner, request
}

func TestCloudRuntimeRecordsBeforeExecutionAndReplaysAfterRestart(t *testing.T) {
	r, repository, inner, request := cloudRuntimeFixture()
	request.Cache = &llm.CachePolicyV1{Variant: 3, MaxAgeSeconds: 60}
	calls := 0
	inner.generate = func(_ context.Context, got llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		calls++
		if repository.operation.Key != request.OperationKey || repository.operation.RequestIndex != 3 || repository.operation.Scope.Tenant != "tenant" {
			t.Fatal("request not recorded before dispatch")
		}
		var restored llm.GenerateRequestV1
		if json.Unmarshal(repository.operation.Manifest, &restored) != nil || restored.Context.Actor != request.Context.Actor {
			t.Fatal("recovery manifest incomplete")
		}
		return builderFinalization(got, "operation-id").Response, nil
	}
	first, err := r.GenerateV1(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh runtime process has no in-memory replay cache.
	restarted := &cloudRequestRuntime{requests: repository, inner: inner, clock: time.Now, finalizationTimeout: time.Second}
	second, err := restarted.GenerateV1(context.Background(), request)
	if err != nil || calls != 1 || !reflect.DeepEqual(first, second) {
		t.Fatalf("duplicate execution: calls=%d err=%v", calls, err)
	}
	if _, err := restarted.QueryV1(context.Background(), llm.QueryRequestV1{}); err != nil || inner.queries != 1 {
		t.Fatal("query path changed", err)
	}
}

func TestCloudRuntimeStorageAndPendingFailuresDoNotDispatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status cloudstate.Status
		code   provider.Code
	}{
		{"storage", errors.New("private SDK details"), "", provider.CodeStateUnavailable},
		{"conflict", contracts.ErrConflict, "", provider.CodeOperationConflict},
		{"invalid", cloudstate.ErrInvalid, "", provider.CodeInvalidArgument},
		{"corrupt", cloudstate.ErrCorrupt, "", provider.CodeStateCorrupt},
		{"unknown", nil, cloudstate.StatusOutcomeUnknown, provider.CodeOperationConflict},
		{"polling", nil, cloudstate.StatusProviderPending, provider.CodeOperationConflict},
		{"failed", nil, cloudstate.StatusFailed, provider.CodeOperationConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, repository, inner, request := cloudRuntimeFixture()
			repository.beginErr, repository.record.Status = test.err, test.status
			inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
				t.Fatal("dispatched after storage failure")
				return llm.GenerateResponseV1{}, nil
			}
			_, err := r.GenerateV1(context.Background(), request)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != test.code || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("unsafe classification: %v", err)
			}
		})
	}
}

func TestCloudRuntimeFailureRemainsRecoverableAndPreservesBudgetError(t *testing.T) {
	r, repository, inner, request := cloudRuntimeFixture()
	paid := provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "unknown provider outcome")
	inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		return llm.GenerateResponseV1{}, paid
	}
	if _, err := r.GenerateV1(context.Background(), request); err != paid {
		t.Fatal("changed budget/retry policy", err)
	}
	if repository.record.Status != cloudstate.StatusRunning || len(repository.record.Progress) != 0 {
		t.Fatal("failed request cached as a success")
	}
}

func TestCloudRuntimeFinalizationFailureRetriesOnlyInnerReplay(t *testing.T) {
	r, repository, inner, request := cloudRuntimeFixture()
	var saved *llm.GenerateResponseV1
	paidCalls := 0
	inner.generate = func(_ context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		if saved == nil {
			result := builderFinalization(request, "operation-id").Response
			saved = &result
			paidCalls++
		}
		return *saved, nil
	}
	repository.completeErr = contracts.ErrOutcomeUnknown
	_, err := r.GenerateV1(context.Background(), request)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAccepted || mapped.Retry != provider.RetrySameOperation {
		t.Fatalf("lost paid result classification: %v", err)
	}
	repository.completeErr = nil
	if _, err := r.GenerateV1(context.Background(), request); err != nil || paidCalls != 1 {
		t.Fatalf("charged again: %d %v", paidCalls, err)
	}
}

func TestCloudRuntimeFinalizesReturnedResponseWithBoundedDetachedContext(t *testing.T) {
	r, repository, inner, request := cloudRuntimeFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		cancel()
		return builderFinalization(request, "operation-id").Response, nil
	}
	repository.complete = func(ctx context.Context) {
		if ctx.Err() != nil {
			t.Fatal("abandoned returned paid response")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("unbounded finalization")
		}
	}
	if _, err := r.GenerateV1(ctx, request); err != nil {
		t.Fatal(err)
	}
}

func TestCloudRuntimeCompactPersistsAndReplays(t *testing.T) {
	r, repository, inner, base := cloudRuntimeFixture()
	request := llm.CompactRequestV1{OperationKey: base.OperationKey, Context: base.Context, Parent: "parent"}
	calls := 0
	inner.compact = func(_ context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
		calls++
		cost := "0"
		return llm.CompactResponseV1{OperationKey: request.OperationKey, OperationID: "compact-id", Checkpoint: llm.CheckpointMetadata{Handle: "child", Parent: &request.Parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &cost, Method: "provider_reported"}}, nil
	}
	if _, err := r.CompactV1(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if repository.operation.Kind != "compact" || repository.operation.RequestIndex != 0 {
		t.Fatal("wrong compact binding")
	}
	if _, err := r.CompactV1(context.Background(), request); err != nil || calls != 1 {
		t.Fatalf("compact replay: %d %v", calls, err)
	}
}

func TestCloudRuntimeRejectsCorruptCompletion(t *testing.T) {
	for _, data := range []string{`{`, `{"version":2,"response":{}}`, `{"version":1,"response":null}`, `{"version":1,"response":{}}`} {
		r, repository, _, request := cloudRuntimeFixture()
		repository.record.Status, repository.record.Progress = cloudstate.StatusCompleted, json.RawMessage(data)
		if _, err := r.GenerateV1(context.Background(), request); err == nil {
			t.Fatal("accepted corrupt response")
		}
	}
}

func TestCloudRuntimeInvalidInputAndMismatchedResult(t *testing.T) {
	r, repository, inner, request := cloudRuntimeFixture()
	invalid := request
	invalid.OperationKey = ""
	if _, err := r.GenerateV1(context.Background(), invalid); err == nil || repository.operation.Key != "" {
		t.Fatal("invalid input reached storage")
	}
	inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		response := builderFinalization(request, "operation-id").Response
		response.OperationKey = "another-operation"
		return response, nil
	}
	if _, err := r.GenerateV1(context.Background(), request); err == nil || repository.record.Status == cloudstate.StatusCompleted {
		t.Fatal("cached response from another operation")
	}
}

func TestCloudRuntimeReplaysIncompleteOperationWithoutCreatingSuccessCache(t *testing.T) {
	r, _, inner, request := cloudRuntimeFixture()
	calls := 0
	inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		calls++
		response := builderFinalization(request, "operation-id").Response
		response.Status = llm.ResponseStatusLength
		return response, nil
	}
	for i := 0; i < 2; i++ {
		response, err := r.GenerateV1(context.Background(), request)
		if err != nil || response.Status != llm.ResponseStatusLength || response.Cache.Disposition != "disabled" {
			t.Fatalf("incomplete response changed: %v", err)
		}
	}
	if calls != 1 {
		t.Fatal("incomplete operation was resubmitted")
	}
}
