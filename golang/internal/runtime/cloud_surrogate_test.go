package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// A model's tool call arguments with a lone surrogate escape are saved and
// returned with \ufffd, so strict JSON clients can decode the paid response
// (#1110).
func TestCloudGenerateSanitizesLoneSurrogatesInModelJSON(t *testing.T) {
	f := boundedCloud(t, false)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		v := executionResponse(call)
		v.Result.Response.Status = llm.ResponseStatusToolCalls
		v.Result.Response.Output = []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x\ud83d"}`)}}
		return v.Result, nil
	}
	result := f.finish(t)
	encoded, err := json.Marshal(result.Generate.Output)
	if err != nil {
		t.Fatal(err)
	}
	// The canonical encoder may write the replacement as the \ufffd escape or
	// as the character itself; both are valid for every strict decoder.
	if strings.Contains(string(encoded), `\ud83d`) || (!strings.Contains(string(encoded), `x\ufffd`) && !strings.Contains(string(encoded), "x\ufffd")) {
		t.Fatalf("output = %s, want the lone surrogate replaced", encoded)
	}
}
