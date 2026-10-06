package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

type completeBeforeAttemptLoad struct {
	cloudExecutionStore
	complete func()
}

func (s *completeBeforeAttemptLoad) LoadRequestAttempt(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.RequestAttempt, error) {
	if s.complete != nil {
		complete := s.complete
		s.complete = nil
		complete()
	}
	return s.cloudExecutionStore.LoadRequestAttempt(ctx, scope, id)
}

func TestCloudExecutionConcurrentCompletionReplaysWinner(t *testing.T) {
	for _, step := range []string{"prepare", "acquire", "generate", "poll", "complete"} {
		t.Run(step, func(t *testing.T) {
			f := boundedCloud(t, false)
			f.request.Cache = &llm.CachePolicyV1{}
			ctx := context.Background()
			result, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, result, err, llm.ExecutionProviderCompleted)
			ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
			other := *f.runtime
			var winner llm.ExecutionResultV1
			store := &completeBeforeAttemptLoad{cloudExecutionStore: f.runtime.store}
			store.complete = func() {
				var err error
				winner, err = other.CompleteExecutionV1(ctx, ref)
				boundedState(t, winner, err, llm.ExecutionCompleted)
			}
			f.runtime.store = store
			switch step {
			case "prepare":
				result, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			case "acquire":
				result, err = f.runtime.AcquireBudgetV1(ctx, ref)
			case "generate":
				result, err = f.runtime.GenerateStepV1(ctx, f.request)
			case "poll":
				result, err = f.runtime.PollExecutionV1(ctx, ref)
			case "complete":
				result, err = f.runtime.CompleteExecutionV1(ctx, ref)
			}
			boundedState(t, result, err, llm.ExecutionCompleted)
			if store.complete != nil || !reflect.DeepEqual(result, winner) || f.submits.Load() != 1 {
				t.Fatal("stale caller failed to replay the winner without another submission")
			}
		})
	}
}
