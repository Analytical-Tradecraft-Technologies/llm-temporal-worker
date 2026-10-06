package openaichat

import (
	"encoding/json"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	openai "github.com/openai/openai-go/v3"
	"testing"
)

func TestOpenRouterNativeSearchOnWire(t *testing.T) {
	profile := wireAuditProfiles(t)[2]
	profile.model = "openai/gpt-test"
	profile.profile.ExpectedModel = profile.model
	request := llm.Request{OperationKey: "search", Model: profile.model, WebSearch: true, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "search"}}}}}
	wire := captureWireBody(t, profile, request)
	tools := wire["tools"].([]any)
	if len(tools) != 1 || tools[0].(map[string]any)["type"] != "openrouter:web_search" {
		t.Fatalf("tools=%#v", tools)
	}
	if tools[0].(map[string]any)["parameters"].(map[string]any)["engine"] != "native" || wire["max_tool_calls"] != float64(3) {
		t.Fatal("native search controls lost")
	}
	request.WebSearch = false
	request.CodeExecution = true
	if _, err := lowerRequest(request, profile.profile, ""); err == nil {
		t.Fatal("unsupported OpenRouter execution accepted")
	}
}
func TestOpenRouterSearchReportedTotalIncludesTools(t *testing.T) {
	for _, reported := range []bool{false, true} {
		body := `{"id":"generation","model":"openai/gpt-test","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"answer","annotations":[{"type":"url_citation","url_citation":{"url":"https://example.com","title":"source"}}]}}],"usage":{"prompt_tokens":2,"completion_tokens":2`
		if reported {
			body += `,"cost":0.0123`
		}
		body += `}}`
		var raw openai.ChatCompletion
		if err := json.Unmarshal([]byte(body), &raw); err != nil {
			t.Fatal(err)
		}
		got := llm.Response{}
		if err := augmentOpenRouter(provider.Call{Model: "openai/gpt-test", Metadata: provider.CallMetadata{WebSearch: true}}, &raw, &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Output) != 1 {
			t.Fatalf("citation lost: %#v", got.Output)
		}
		if reported {
			if got.Cost.ActualCostUSD == nil || got.Cost.ActualCostUSD.String() != "0.012300000000000000" {
				t.Fatalf("cost=%#v", got.Cost)
			}
		} else if got.Cost.Status != llm.CostStatusUnknown {
			t.Fatal("missing search charge treated as known")
		}
	}
}
