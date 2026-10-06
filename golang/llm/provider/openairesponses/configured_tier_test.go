package openairesponses

import (
	"context"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// The endpoint's configured provider value is what routing and pricing key the
// request on, so Compile must send that tier rather than the canonical one.
func TestCompileSendsTheConfiguredProviderTier(t *testing.T) {
	adapter := newFixtureAdapter(t, []byte(`{"id":"unused"}`))
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request:  llm.Request{OperationKey: "op-tier", Model: "gpt-contract", ServiceClass: llm.ServiceClassStandard},
		Query:    provider.CapabilityQuery{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict:   true,
		Metadata: provider.CallMetadata{ProviderTier: "auto"},
	})
	if err != nil {
		t.Fatal(err)
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		t.Fatalf("SDK params type = %T", call.SDKParams)
	}
	if got := marshalParams(t, params)["service_tier"]; got != "auto" || call.Metadata.ProviderTier != "auto" {
		t.Fatalf("service_tier = %#v, metadata tier = %q; want the configured auto", got, call.Metadata.ProviderTier)
	}
}
