package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
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

// Raw provider values, such as Exa citation metadata and usage facts, with a
// lone surrogate escape are saved and returned with \ufffd too (#1330).
func TestCloudGenerateSanitizesLoneSurrogatesInProviderValues(t *testing.T) {
	f := boundedCloud(t, false)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		v := executionResponse(call)
		v.Result.Response.Output = append(v.Result.Response.Output, llm.Reference{URI: "https://example.com/a", Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"Emoji cut \ud83d"`)}})
		v.Result.Response.Usage.ProviderRaw = map[string]json.RawMessage{"note": json.RawMessage(`"\udc00"`)}
		v.Result.Response.Provider.Raw = map[string]json.RawMessage{"exa_citations": json.RawMessage(`[{"title":"Emoji cut \ud83d"}]`)}
		return v.Result, nil
	}
	result := f.finish(t)
	encoded, err := json.Marshal(result.Generate)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `\ud83d`) || strings.Contains(string(encoded), `\udc00`) {
		t.Fatalf("response = %s, want the lone surrogates replaced", encoded)
	}
	if !strings.Contains(string(encoded), "Emoji cut ") {
		t.Fatalf("response = %s, want the reference kept", encoded)
	}
}
