package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestAnthropicSearchCostFromSDKResponseThroughPublication(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		server  string
		total   string
	}{
		{name: "unused search", content: `[{"type":"text","text":"answer"}]`, total: "0.0000200001"},
		{name: "reported search", content: `[{"type":"server_tool_use","id":"search-1","name":"web_search","input":{"query":"example"}},{"type":"text","text":"answer"}]`, server: `,"server_tool_use":{"web_search_requests":2}`, total: "0.0200200001"},
		{name: "unreported search", content: `[{"type":"server_tool_use","id":"search-1","name":"web_search","input":{"query":"example"}},{"type":"text","text":"answer"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{
				BaseURL: "http://127.0.0.1/contract", APIKey: "synthetic-test-key",
				HTTPClient: &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
					calls++
					body := `{"id":"msg-synthetic","type":"message","role":"assistant","model":"claude-contract","stop_reason":"end_turn","content":` + tc.content + `,"usage":{"input_tokens":10,"output_tokens":5,"service_tier":"standard"` + tc.server + `}}`
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := anthropicmessages.New(client, "anthropic-contract", anthropicmessages.DefaultProfile("anthropic-contract"))
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "search-cost", Model: "claude-contract", WebSearch: true},
				Query:   provider.CapabilityQuery{EndpointID: "anthropic-contract", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
				Strict:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			if err != nil || calls != 1 {
				t.Fatalf("SDK result = %#v, %v; calls = %d", result, err, calls)
			}
			priceExecutionResponse(settlementPricingPlan(false), &result.Response)
			published := publicationCost(result.Response.Cost)
			if tc.total == "" {
				if published.Status != "unknown" || published.ActualCostUSD != nil {
					t.Fatalf("invented a charge for unreported searches: %#v", published)
				}
				return
			}
			if published.Status != "exact" || published.ActualCostUSD == nil || result.Response.Cost.ActualCostUSD.Cmp(pricing.MustUSD(tc.total)) != 0 {
				t.Fatalf("published cost = %#v, want %s", published, tc.total)
			}
			receipt, err := json.Marshal(llm.GenerateResponseV1{
				OperationKey: "search-cost", OperationID: "origin-search-cost", Status: result.Response.Status,
				Output: result.Response.Output, Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-synthetic", Kind: "generation"},
				Cache: llm.CacheDispositionV1{Disposition: "miss_populated"}, Cost: published,
			})
			if err != nil {
				t.Fatal(err)
			}
			diagnostics, err := withCacheOriginCost(nil, cache.ResponseEntry{OriginOperationID: "origin-search-cost", Response: receipt})
			if err != nil {
				t.Fatal(err)
			}
			assertOriginCost(t, diagnostics, "origin-search-cost", *published.ActualCostUSD)
		})
	}
}
