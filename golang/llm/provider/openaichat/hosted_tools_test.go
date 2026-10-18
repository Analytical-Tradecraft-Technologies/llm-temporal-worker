package openaichat

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	openai "github.com/openai/openai-go/v3"
)

func TestOpenRouterServerSearchOnWire(t *testing.T) {
	for _, model := range []string{"openai/gpt-test", "anthropic/claude-test", "google/gemini-test", "x-ai/grok-test", "deepseek/deepseek-test", "qwen/qwen-test", "z-ai/glm-test", "other/model"} {
		t.Run(model, func(t *testing.T) {
			profile := wireAuditProfiles(t)[2]
			profile.model = model
			profile.profile.ExpectedModel = model
			request := llm.Request{OperationKey: "search", Model: model, WebSearch: true, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "search"}}}}}
			wire := captureWireBody(t, profile, request)
			tools := wire["tools"].([]any)
			if len(tools) != 1 || tools[0].(map[string]any)["type"] != "openrouter:web_search" {
				t.Fatalf("tools=%#v", tools)
			}
			parameters := tools[0].(map[string]any)["parameters"].(map[string]any)
			if parameters["engine"] != "auto" || parameters["max_uses"] != float64(llm.MaxWebSearchCalls) || wire["max_tool_calls"] != float64(llm.MaxWebSearchCalls) {
				t.Fatalf("server search controls lost: %#v", wire)
			}
			// With require_parameters, OpenRouter drops every upstream that
			// does not list parallel_tool_calls, which strands models such
			// as x-ai/grok when there are no function tools for it to govern.
			if _, sent := wire["parallel_tool_calls"]; sent {
				t.Fatalf("parallel_tool_calls sent without function tools: %#v", wire)
			}
			request.WebSearch = false
			request.CodeExecution = true
			if _, err := lowerRequest(request, profile.profile, ""); err == nil {
				t.Fatal("unsupported OpenRouter execution accepted")
			}
		})
	}
}

func TestSearchRejectsOtherChatProfiles(t *testing.T) {
	for _, profile := range wireAuditProfiles(t) {
		if profile.profile.ExpectedBaseURL == openRouterBaseURL {
			continue
		}
		t.Run(profile.id, func(t *testing.T) {
			request := llm.Request{OperationKey: "search", Model: profile.model, WebSearch: true}
			if _, err := lowerRequest(request, profile.profile, ""); err == nil {
				t.Fatal("server search accepted by a non-OpenRouter profile")
			}
		})
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

func TestOpenRouterSearchCountReadsBothServerToolUseShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		server string
		calls  string
	}{
		{name: "server_tool_use", server: `,"server_tool_use":{"web_search_requests":4}`, calls: "4"},
		{name: "server_tool_use_details", server: `,"server_tool_use_details":{"tool_calls_executed":2,"tool_calls_requested":2,"web_search_requests":2}`, calls: "2"},
		{name: "both agree", server: `,"server_tool_use":{"web_search_requests":3},"server_tool_use_details":{"web_search_requests":3}`, calls: "3"},
		{name: "details without count", server: `,"server_tool_use":{"web_search_requests":1},"server_tool_use_details":{"tool_calls_executed":1}`, calls: "1"},
		{name: "zero searches", server: `,"server_tool_use_details":{"web_search_requests":0}`, calls: "0"},
		{name: "both disagree", server: `,"server_tool_use":{"web_search_requests":1},"server_tool_use_details":{"web_search_requests":2}`, calls: "null"},
		{name: "negative", server: `,"server_tool_use_details":{"web_search_requests":-1}`, calls: "null"},
		{name: "malformed", server: `,"server_tool_use_details":{"web_search_requests":"2"}`, calls: "null"},
		{name: "not reported", calls: "null"},
	} {
		for _, reported := range []bool{false, true} {
			name := test.name
			if reported {
				name += " with reported cost"
			}
			t.Run(name, func(t *testing.T) {
				body := `{"id":"generation","model":"openai/gpt-test","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"answer"}}],"usage":{"prompt_tokens":2,"completion_tokens":2` + test.server
				if reported {
					body += `,"cost":0.060018`
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
				if calls := string(got.Usage.ProviderRaw["web_search_calls"]); calls != test.calls {
					t.Fatalf("web_search_calls = %s, want %s", calls, test.calls)
				}
				switch {
				case reported:
					// The reported total already includes search; it is kept
					// exactly and the count never adds a second charge.
					if got.Cost.ActualCostUSD == nil || got.Cost.ActualCostUSD.String() != "0.060018000000000000" || got.Cost.Method != "openrouter_reported" {
						t.Fatalf("cost = %#v", got.Cost)
					}
				default:
					// The auto engine's per-search rate is not reported, so a
					// count alone never makes the cost known.
					if got.Cost.Status != llm.CostStatusUnknown || got.Cost.ActualCostUSD != nil {
						t.Fatalf("search without a reported total settled as known: %#v", got.Cost)
					}
				}
			})
		}
	}
}
