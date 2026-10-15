package openaichat

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
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
				client, err := NewExaClient(ExaClientConfig{BaseURL: exaBaseURL, APIKey: "exa-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(payload)), Request: request}, nil
				})}})
				if err != nil {
					t.Fatal(err)
				}
				profile, err := NewExaProfile(ExaProfileConfig{
					ID: "exa-chat", CapabilityVersion: "exa/v1", BaseURL: exaBaseURL, Capabilities: profileTestCapabilities("exa/v1"),
					ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
					ActualServiceClasses:      map[string]llm.ServiceClass{"standard": llm.ServiceClassStandard},
					MissingActualServiceClass: llm.ServiceClassStandard,
				})
				if err != nil {
					t.Fatal(err)
				}
				adapter, err := New(client, "exa-a", profile)
				if err != nil {
					t.Fatal(err)
				}
				input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "a long conversation"}}}}
				request, err := compaction.PrepareRequest(llm.Request{OperationKey: "exa-op", Model: "exa", Input: input}, "exa-op/compact", input, compaction.DefaultPolicy())
				if err != nil {
					t.Fatal(err)
				}
				call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Query: provider.CapabilityQuery{EndpointID: "exa-a", Family: provider.FamilyOpenAIChat, Model: "exa"}, Strict: strict})
				if err != nil {
					t.Fatal(err)
				}
				result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
				if err != nil {
					t.Fatal(err)
				}
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
