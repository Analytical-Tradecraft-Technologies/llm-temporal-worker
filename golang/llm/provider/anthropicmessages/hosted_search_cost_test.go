package anthropicmessages

import (
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestHostedSearchUsageDistinguishesUnusedFromUnreported(t *testing.T) {
	const text = `[{"type":"text","text":"answer"}]`
	const thinking = `[{"type":"thinking","thinking":"reasoning","signature":"synthetic"},{"type":"text","text":"answer"}]`
	const search = `[{"type":"server_tool_use","id":"search-1","name":"web_search","input":{"query":"example"}},{"type":"web_search_tool_result","tool_use_id":"search-1","content":[]},{"type":"text","text":"answer"}]`
	const resultOnly = `[{"type":"web_search_tool_result","tool_use_id":"search-1","content":[]},{"type":"text","text":"answer"}]`
	const fetch = `[{"type":"server_tool_use","id":"fetch-1","name":"web_fetch","input":{"url":"https://example.com"}},{"type":"text","text":"answer"}]`

	for _, tc := range []struct {
		name    string
		content string
		server  string
		calls   string
		fee     string
	}{
		{name: "search enabled but unused", content: text, calls: "0", fee: "0"},
		{name: "reasoning without search", content: thinking, calls: "0", fee: "0"},
		{name: "empty server usage without search", content: text, server: `{}`, calls: "0", fee: "0"},
		{name: "fetch without search", content: fetch, server: `{"web_fetch_requests":1}`, calls: "0", fee: "0"},
		{name: "explicit zero", content: text, server: `{"web_search_requests":0}`, calls: "0", fee: "0"},
		{name: "reported searches", content: search, server: `{"web_search_requests":2}`, calls: "2", fee: "0.02"},
		{name: "unbilled failed search", content: search, server: `{"web_search_requests":0}`, calls: "0", fee: "0"},
		{name: "search missing usage", content: search, calls: "null"},
		{name: "search result missing usage", content: resultOnly, calls: "null"},
		{name: "search count omitted", content: search, server: `{}`, calls: "null"},
		{name: "null server usage", content: text, server: `null`, calls: "null"},
		{name: "missing content evidence", content: `null`, calls: "null"},
		{name: "non-object server usage", content: text, server: `[]`, calls: "null"},
		{name: "null search count", content: text, server: `{"web_search_requests":null}`, calls: "null"},
		{name: "non-integer search count", content: text, server: `{"web_search_requests":"2"}`, calls: "null"},
		{name: "negative search count", content: text, server: `{"web_search_requests":-1}`, calls: "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage := `"input_tokens":123,"output_tokens":45,"service_tier":"standard","output_tokens_details":{"thinking_tokens":7}`
			if tc.server != "" {
				usage += `,"server_tool_use":` + tc.server
			}
			var message anthropic.Message
			if err := json.Unmarshal([]byte(`{"id":"msg-synthetic","type":"message","role":"assistant","model":"claude-contract","stop_reason":"end_turn","content":`+tc.content+`,"usage":{`+usage+`}}`), &message); err != nil {
				t.Fatal(err)
			}
			got, err := mustProfile(t, testProfile()).liftResponse(provider.Call{
				OperationKey: "search-cost", Model: "claude-contract", ServiceClass: llm.ServiceClassStandard,
				Metadata: provider.CallMetadata{WebSearch: true, WebFetch: true},
			}, &message, "req-synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if calls := string(got.Usage.ProviderRaw["web_search_calls"]); calls != tc.calls {
				t.Fatalf("search usage = %s, want %s", calls, tc.calls)
			}
			if got.Usage.InputTokens != 123 || got.Usage.OutputTokens != 45 || got.Usage.ReasoningTokens != 7 {
				t.Fatalf("token usage lost: %#v", got.Usage)
			}
			fee, err := llm.HostedToolCharge(got.Usage, got.Route.ResolvedModel)
			if tc.calls == "null" {
				if err == nil || got.Cost.Status != llm.CostStatusUnknown {
					t.Fatalf("unreported search charge = %s, %v, cost = %#v", fee, err, got.Cost)
				}
			} else if err != nil || fee.Cmp(pricing.MustUSD(tc.fee)) != 0 || got.Cost.Status == llm.CostStatusUnknown {
				t.Fatalf("search charge = %s, %v, cost = %#v; want %s available for settlement", fee, err, got.Cost, tc.fee)
			}
		})
	}
}
