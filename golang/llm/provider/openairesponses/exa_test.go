package openairesponses

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

const exaAgentResponse = `{"id":"run_1","object":"response","created_at":1,"status":"completed","model":"exa-agent",
"output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer","annotations":[]}]}],
"usage":{"input_tokens":0,"input_tokens_details":{"cached_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":0},
"costDollars":{"total":0.025}}`

func exaAgentAdapter(t *testing.T, captured **http.Request, body *[]byte) *Adapter {
	t.Helper()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		*captured = request
		*body, _ = io.ReadAll(request.Body)
		return adaptertest.JSONResponse(request, http.StatusOK, exaAgentResponse), nil
	})
	client, err := NewExaClient(ExaClientConfig{BaseURL: "https://api.exa.ai", APIKey: "exa-key", HTTPClient: &http.Client{Transport: transport}})
	testassert.NoError(t, err)
	adapter, err := New(client, "exa-agent", "exa-agent/v1", WithExaAgent())
	testassert.NoError(t, err)
	return adapter
}

func compileExaAgent(adapter *Adapter, reasoning *llm.ReasoningSpec) (provider.Call, error) {
	request := llm.Request{OperationKey: "exa-op", Model: ExaAgentModel, ServiceClass: llm.ServiceClassStandard, Reasoning: reasoning,
		Input: []llm.Item{llm.HumanText("research this")}}
	return adapter.Compile(context.Background(), provider.CompileInput{Request: request,
		Query:    provider.CapabilityQuery{EndpointID: "exa-agent", Family: provider.FamilyOpenAIResponses, Model: ExaAgentModel},
		Metadata: provider.CallMetadata{ProviderTier: "standard"}})
}

func TestExaAgentSendsAFixedEffortAndReportsExaCost(t *testing.T) {
	var got *http.Request
	var body []byte
	adapter := exaAgentAdapter(t, &got, &body)
	call, err := compileExaAgent(adapter, nil)
	testassert.NoError(t, err)
	result := adaptertest.Invoke(t, adapter, call)
	if got.URL.String() != "https://api.exa.ai/responses" || got.Header.Get("x-api-key") != "exa-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("request = %s headers %#v", got.URL, got.Header)
	}
	wire := contract.DecodeJSON[map[string]any](t, body)
	// Exa's own default effort is metered; the adapter always fixes one.
	if reasoning, _ := wire["reasoning"].(map[string]any); reasoning["effort"] != "medium" {
		t.Fatalf("reasoning = %#v, want medium", wire["reasoning"])
	}
	for _, field := range []string{"service_tier", "store", "include"} {
		if _, present := wire[field]; present {
			t.Fatalf("request carries OpenAI-only field %q: %s", field, body)
		}
	}
	cost := result.Response.Cost
	if cost.ActualCostUSD == nil || cost.ActualCostUSD.String() != "0.025000000000000000" || cost.Method != "exa_reported" {
		t.Fatalf("cost = %#v", cost)
	}
}

func TestExaAgentAcceptsOnlySynchronousEfforts(t *testing.T) {
	var got *http.Request
	var body []byte
	adapter := exaAgentAdapter(t, &got, &body)
	call, err := compileExaAgent(adapter, &llm.ReasoningSpec{Effort: llm.ReasoningEffortLow})
	testassert.NoError(t, err)
	adaptertest.Invoke(t, adapter, call)
	if !strings.Contains(string(body), `"effort":"low"`) {
		t.Fatalf("request body = %s, want effort low", body)
	}
	for _, effort := range []llm.ReasoningEffort{llm.ReasoningEffortHigh, llm.ReasoningEffortMaximum} {
		if _, err := compileExaAgent(adapter, &llm.ReasoningSpec{Effort: effort}); err == nil {
			t.Fatalf("effort %q was accepted although Exa rejects it synchronously", effort)
		}
	}
	other := llm.Request{OperationKey: "exa-op", Model: "gpt-5.4", ServiceClass: llm.ServiceClassStandard}
	if _, err := adaptertest.Compile(adapter, "exa-agent", provider.FamilyOpenAIResponses, other, false); err == nil {
		t.Fatal("the Exa agent endpoint accepted another model")
	}
	if _, err := NewExaClient(ExaClientConfig{BaseURL: "https://api.openai.com/v1", APIKey: "k", HTTPClient: http.DefaultClient}); err == nil {
		t.Fatal("the Exa client accepted a non-Exa base URL")
	}
}
