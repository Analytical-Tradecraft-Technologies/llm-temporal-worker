package bedrockmessages

import (
	"context"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Hosted-tool flags are refused at compile, never silently dropped (#1506).
func TestCompileRejectsHostedToolFlags(t *testing.T) {
	var wire []byte
	adapter := recordingBedrockAdapter(t, `{}`, &wire)
	for name, mutate := range map[string]func(*llm.Request){
		"web_search":     func(r *llm.Request) { r.WebSearch = true },
		"web_fetch":      func(r *llm.Request) { r.WebFetch = true },
		"code_execution": func(r *llm.Request) { r.CodeExecution = true },
	} {
		for _, strict := range []bool{true, false} {
			request := llm.Request{Input: []llm.Item{llm.HumanText("hi")}}
			mutate(&request)
			_, err := adapter.Compile(context.Background(), bedrockCompileInput(request, strict))
			testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeUnsupportedCapability, Dispatch: provider.DispatchNotDispatched}, fmt.Sprintf("%s strict=%v: compile", name, strict))
		}
	}
	if wire != nil {
		t.Fatalf("a hosted-tool request reached the provider: %s", wire)
	}
}
