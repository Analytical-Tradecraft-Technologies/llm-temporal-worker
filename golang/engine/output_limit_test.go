package engine

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type outputLimitAdapter struct {
	*streamingAdapter
	t        *testing.T
	compiled int
}

func (adapter *outputLimitAdapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	adapter.t.Helper()
	if input.Request.Output == nil || input.Request.Output.MaxTokens == nil || *input.Request.Output.MaxTokens != 16 {
		adapter.t.Fatalf("provider request exceeds reservation: %+v", input.Request.Output)
	}
	adapter.compiled++
	return adapter.fakeAdapter.Compile(ctx, input)
}

func TestGenerateAndStreamCompileReservedOutputLimit(t *testing.T) {
	adapter := &outputLimitAdapter{t: t, streamingAdapter: &streamingAdapter{fakeAdapter: &fakeAdapter{name: "bounded", response: successfulResponse()}, events: []provider.Event{provider.StreamCompleted{Response: successfulResponse()}}}}
	harness := newHarness(t, adapter)
	harness.engine.dependencies.Estimator.MaxOutput = 16
	for _, streaming := range []bool{false, true} {
		request := baseRequest("bounded-generate")
		if streaming {
			request.OperationKey = "bounded-stream"
		}
		request.Output = nil
		if streaming {
			stream, err := harness.engine.Stream(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			events := readTerminalStream(t, stream)
			_ = stream.Close()
			if _, ok := events[len(events)-1].(llm.ResponseCompleted); !ok {
				t.Fatalf("stream failed: %#v", events)
			}
		} else if _, err := harness.engine.Generate(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if request.Output != nil {
			t.Fatal("caller request mutated")
		}
	}
	if adapter.compiled < 2 {
		t.Fatal("both execution paths must compile the bound")
	}
}
