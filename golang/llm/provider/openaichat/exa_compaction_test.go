package openaichat

import (
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// Exa always appends citations to its answer. A compaction summary from Exa
// therefore arrives as [message, reference...] and must publish only the
// text. A truncated summary must still be rejected.
func TestExaCompactionSummaryIgnoresCitations(t *testing.T) {
	const body = `{"id":"exa-summary","model":"exa","service_tier":"standard","choices":[{"index":0,"finish_reason":"%s","message":{"role":"assistant","content":"the summary",` +
		`"citations":[{"url":"https://example.com/one","title":"One"},{"url":"https://example.com/two","title":"Two"}]}}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`
	for _, strict := range []bool{true, false} {
		for finish, want := range map[string]string{"stop": "the summary", "length": ""} {
			t.Run(fmt.Sprintf("strict=%v/%s", strict, finish), func(t *testing.T) {
				payload := fmt.Sprintf(body, finish)
				input := []llm.Item{llm.HumanText("a long conversation")}
				request, err := compaction.PrepareRequest(llm.Request{OperationKey: "exa-op", Model: "exa", Input: input}, "exa-op/compact", input, compaction.DefaultPolicy())
				testassert.NoError(t, err)
				result := invokeExa(t, newExaTestAdapter(t, exaBody(payload)), request, strict)
				if len(result.Response.Output) != 3 {
					t.Fatalf("exa output = %#v, want message and two references", result.Response.Output)
				}
				summary, err := compaction.PlainTextSummary(result.Response, 1024)
				if want == "" {
					if err == nil {
						t.Fatalf("truncated summary %q accepted", summary)
					}
					return
				}
				if err != nil || summary != want {
					t.Fatalf("summary = %q, err = %v; want %q", summary, err, want)
				}
			})
		}
	}
}
