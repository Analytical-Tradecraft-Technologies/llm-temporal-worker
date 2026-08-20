package openaichat

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestAzureChatUsesDeploymentPathAndApiKey(t *testing.T) {
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"azure-req-1"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"azure-chat-1","model":"deployment-a","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)),
			Request:    request,
		}, nil
	})
	endpoint := "http://127.0.0.1"
	client, err := NewAzureClient(AzureClientConfig{Endpoint: endpoint, APIVersion: "2025-01-01", APIKey: "test-azure-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewAzureProfile(AzureProfileConfig{
		ID:                "azure-deployment-a",
		CapabilityVersion: "azure-chat/v1",
		BaseURL:           endpoint,
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
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "azure-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "azure-op", Model: "deployment-a", ServiceClass: llm.ServiceClassPriority},
		Query:   provider.CapabilityQuery{EndpointID: "azure-a", Family: provider.FamilyOpenAIChat, Model: "deployment-a"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
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

func TestAzureChatRejectsUnsupportedEconomyBeforeDispatch(t *testing.T) {
	client := &Client{baseURL: "http://127.0.0.1/"}
	profile, err := NewAzureProfile(AzureProfileConfig{
		ID:                "azure-deployment-a",
		CapabilityVersion: "azure-chat/v1",
		BaseURL:           "http://127.0.0.1",
		Deployment:        "deployment-a",
		Capabilities:      profileTestCapabilities("azure-chat/v1"),
		ServiceTiers: map[llm.ServiceClass]string{
			llm.ServiceClassEconomy:  "",
			llm.ServiceClassStandard: "default",
			llm.ServiceClassPriority: "priority",
		},
		ActualServiceClasses: map[string]llm.ServiceClass{"default": llm.ServiceClassStandard, "priority": llm.ServiceClassPriority},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "azure-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "economy-op", Model: "deployment-a", ServiceClass: llm.ServiceClassEconomy}, Query: provider.CapabilityQuery{EndpointID: "azure-a", Family: provider.FamilyOpenAIChat, Model: "deployment-a"}, Strict: true})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("economy compile error = %v", err)
	}
}
