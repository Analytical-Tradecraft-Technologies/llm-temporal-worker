package openaichat

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestAzureChatUsesDeploymentPathAndApiKey(t *testing.T) {
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return adaptertest.JSONResponse(request, http.StatusOK, `{"id":"azure-chat-1","model":"deployment-a","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`, "X-Request-Id", "azure-req-1"), nil
	})
	endpoint := "https://127.0.0.1"
	client, err := NewAzureClient(AzureClientConfig{Endpoint: endpoint, APIVersion: "2025-01-01", APIKey: "test-azure-key", HTTPClient: &http.Client{Transport: transport}})
	testassert.NoError(t, err)
	adapter, err := New(client, "azure-a", azureDeploymentProfile(t, endpoint))
	testassert.NoError(t, err)
	call := adaptertest.CompileStrict(t, adapter, "azure-a", provider.FamilyOpenAIChat, llm.Request{OperationKey: "azure-op", Model: "deployment-a", ServiceClass: llm.ServiceClassPriority})
	result := adaptertest.Invoke(t, adapter, call)
	if got == nil || got.URL.Path != "/openai/deployments/deployment-a/chat/completions" || got.URL.Query().Get("api-version") != "2025-01-01" {
		t.Fatalf("azure request = %#v", got.URL)
	}
	if got.Header.Get("Api-Key") != "test-azure-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("azure auth headers = %#v", got.Header)
	}
	if result.Response.Service.Actual == nil || *result.Response.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("azure downgrade = %#v", result.Response.Service)
	}
	if result.Response.Provider.RequestID != "azure-req-1" {
		t.Fatalf("azure request ID = %#v", result.Response.Provider)
	}
}

// azureDeploymentProfile is the Azure profile for "deployment-a" at baseURL,
// which supports Standard and Priority but not Economy.
func azureDeploymentProfile(t *testing.T, baseURL string) Profile {
	t.Helper()
	profile, err := NewAzureProfile(AzureProfileConfig{
		ID:                "azure-deployment-a",
		CapabilityVersion: "azure-chat/v1",
		BaseURL:           baseURL,
		Deployment:        "deployment-a",
		Capabilities:      profileTestCapabilities("azure-chat/v1"),
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "default",
			llm.ServiceClassPriority: "priority",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{
			"default":  llm.ServiceClassStandard,
			"priority": llm.ServiceClassPriority,
		},
	})
	testassert.NoError(t, err)
	return profile
}

func TestAzureChatRejectsUnsupportedEconomyBeforeDispatch(t *testing.T) {
	client := &Client{baseURL: "http://127.0.0.1/"}
	adapter, err := New(client, "azure-a", azureDeploymentProfile(t, "http://127.0.0.1"))
	testassert.NoError(t, err)
	_, err = adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "economy-op", Model: "deployment-a", ServiceClass: llm.ServiceClassEconomy}, Query: provider.CapabilityQuery{EndpointID: "azure-a", Family: provider.FamilyOpenAIChat, Model: "deployment-a"}, Strict: true})
	testassert.ErrorContains(t, err, "unsupported")
}
