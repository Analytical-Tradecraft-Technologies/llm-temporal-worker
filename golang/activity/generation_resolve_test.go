package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

const resolveRequestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5"

// resolveRuntime plans with plan and prepares with prepare, recording the
// preparation requests it receives.
type resolveRuntime struct {
	UnconfiguredV1Runtime
	plan     llm.GenerationPlanV1
	planErr  error
	prepare  func(llm.PrepareExecutionV1) (llm.ExecutionResultV1, error)
	prepared []llm.PrepareExecutionV1
}

func (r *resolveRuntime) PlanGenerationV1(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
	return r.plan, r.planErr
}
func (r *resolveRuntime) PrepareExecutionV1(_ context.Context, request llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	r.prepared = append(r.prepared, request)
	return r.prepare(request)
}
func (r *resolveRuntime) AcquireBudgetV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return llm.ExecutionResultV1{}, errors.New("unexpected acquire")
}
func (r *resolveRuntime) GenerateStepV1(context.Context, llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	return llm.ExecutionResultV1{}, errors.New("unexpected submit")
}
func (r *resolveRuntime) CompactStepV1(context.Context, llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	return llm.ExecutionResultV1{}, errors.New("unexpected submit")
}
func (r *resolveRuntime) PollExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return llm.ExecutionResultV1{}, errors.New("unexpected poll")
}
func (r *resolveRuntime) CompleteExecutionV1(context.Context, llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return llm.ExecutionResultV1{}, errors.New("unexpected complete")
}

func completedResolution(request llm.PrepareExecutionV1) llm.ExecutionResultV1 {
	return llm.ExecutionResultV1{RequestID: resolveRequestID, Kind: "generate", State: llm.ExecutionCompleted, Capability: "ref_v1.test", Generate: &llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: request.Generate.OperationKey, OperationID: resolveRequestID,
		Status:     llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp-generate", Kind: "cache_replay"},
		Cache:      llm.CacheDispositionV1{Disposition: "hit"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}}
}

// The resolve Activity returns the plan, and a completed result only when a
// cached request that needs no compaction prepares as complete. Every other
// preparation outcome, an error included, is left to the request workflow.
func TestResolveGenerationReturnsOnlyCompletedPreparation(t *testing.T) {
	parent := llm.CheckpointHandle("ckp-parent")
	for _, tc := range []struct {
		name     string
		cache    bool
		plan     llm.GenerationPlanV1
		prepare  func(llm.PrepareExecutionV1) (llm.ExecutionResultV1, error)
		prepares int
		resolved bool
	}{
		{name: "hit", cache: true, prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) { return completedResolution(r), nil }, prepares: 1, resolved: true},
		{name: "uncached request", prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) { return completedResolution(r), nil }},
		{name: "compaction first", cache: true, plan: llm.GenerationPlanV1{CompactBeforeGenerate: true}, prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) { return completedResolution(r), nil }},
		{name: "miss", cache: true, prepare: func(llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
			return llm.ExecutionResultV1{RequestID: resolveRequestID, Kind: "generate", State: llm.ExecutionBudgetRequired, Capability: "ref_v1.test"}, nil
		}, prepares: 1},
		{name: "fill busy", cache: true, prepare: func(llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
			return llm.ExecutionResultV1{RequestID: resolveRequestID, Kind: "generate", State: llm.ExecutionCacheWait, RetryAfterSeconds: 5, Capability: "ref_v1.test"}, nil
		}, prepares: 1},
		{name: "cache error", cache: true, prepare: func(llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
			return llm.ExecutionResultV1{}, errors.New("cache unavailable")
		}, prepares: 1},
		{name: "no capability", cache: true, prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
			result := completedResolution(r)
			result.Capability = ""
			return result, nil
		}, prepares: 1},
		{name: "other operation", cache: true, prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
			result := completedResolution(r)
			result.Generate.OperationKey = "other"
			return result, nil
		}, prepares: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := validGenerateV1Request()
			request.Parent = &parent
			if tc.cache {
				request.Cache = &llm.CachePolicyV1{}
			}
			runtime := &resolveRuntime{plan: tc.plan, prepare: tc.prepare}
			resolution, err := (&Activities{V1Runtime: runtime}).ResolveGenerationV1(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(resolution.Plan, tc.plan) || len(runtime.prepared) != tc.prepares || (resolution.Result != nil) != tc.resolved {
				t.Fatalf("resolution = %+v after %d preparations", resolution, len(runtime.prepared))
			}
			want := llm.PrepareExecutionV1{Generate: &request, OriginalGenerate: &llm.GenerationOriginV1{Parent: &parent}}
			for _, prepared := range runtime.prepared {
				if !reflect.DeepEqual(prepared, want) {
					t.Fatal("preparation differs from the request workflow's")
				}
			}
			if tc.resolved {
				if _, err := resolution.MarshalJSON(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestResolveGenerationFailsWithPlanning(t *testing.T) {
	request := validGenerateV1Request()
	request.Cache = &llm.CachePolicyV1{}
	runtime := &resolveRuntime{planErr: errors.New("planning failed"), prepare: func(r llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) { return completedResolution(r), nil }}
	if _, err := (&Activities{V1Runtime: runtime}).ResolveGenerationV1(context.Background(), request); err == nil || len(runtime.prepared) != 0 {
		t.Fatalf("planning failure: %v after %d preparations", err, len(runtime.prepared))
	}
}
