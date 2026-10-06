package openaichat

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func invokeOpenRouterWithBody(t *testing.T, body string) error {
	t.Helper()
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"or-req-1"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	client, err := NewOpenRouterClient(OpenRouterClientConfig{BaseURL: openRouterBaseURL, APIKey: "or-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewOpenRouterProfile(OpenRouterProfileConfig{
		ID: "openrouter-errors", CapabilityVersion: "openrouter/v1", BaseURL: openRouterBaseURL, Model: "router-model",
		Capabilities:              profileTestCapabilities("openrouter/v1"),
		ServiceTiers:              map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "standard", llm.ServiceClassPriority: ""},
		ActualServiceClasses:      map[string]llm.ServiceClass{"default": llm.ServiceClassStandard, "standard": llm.ServiceClassStandard},
		MissingActualServiceClass: llm.ServiceClassStandard,
		ProviderOrder:             []string{"ProviderA"},
		RequireParameters:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "openrouter-a", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "or-op", Model: "router-model"}, Query: provider.CapabilityQuery{EndpointID: "openrouter-a", Family: provider.FamilyOpenAIChat, Model: "router-model"}, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	return err
}

func TestOpenRouterMapsBodyLevelErrorsThroughTheStatusTable(t *testing.T) {
	for _, test := range []struct {
		name     string
		body     string
		code     provider.Code
		dispatch provider.DispatchCertainty
		retry    provider.RetryDisposition
	}{
		{name: "upstream unavailable", body: `{"id":"gen-1","model":"router-model","choices":[],"error":{"code":502,"message":"upstream failed"}}`, code: provider.CodeProviderUnavailable, dispatch: provider.DispatchRejected, retry: provider.RetrySameOperation},
		{name: "rate limited as string code", body: `{"id":"gen-1","model":"router-model","choices":[],"error":{"code":"429","message":"slow down"}}`, code: provider.CodeProviderRateLimited, dispatch: provider.DispatchRejected, retry: provider.RetryAfter},
		{name: "bad request", body: `{"id":"gen-1","model":"router-model","choices":[],"error":{"code":400,"message":"bad"}}`, code: provider.CodeInvalidArgument, dispatch: provider.DispatchRejected, retry: provider.RetryNever},
		{name: "mid-generation failure", body: `{"id":"gen-1","model":"router-model","choices":[{"index":0,"finish_reason":"error","message":{"role":"assistant","content":"partial"},"error":{"code":502,"message":"upstream died"}}]}`, code: provider.CodeProviderUnavailable, dispatch: provider.DispatchAccepted, retry: provider.RetryNever},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := invokeOpenRouterWithBody(t, test.body)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != test.code || mapped.Dispatch != test.dispatch || mapped.Retry != test.retry || mapped.OperationID != "or-op" {
				t.Fatalf("Invoke() error = %#v, want %s/%s/%s", err, test.code, test.dispatch, test.retry)
			}
		})
	}
	if err := invokeOpenRouterWithBody(t, `{"id":"gen-1","model":"router-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`); err != nil {
		t.Fatalf("successful response rejected: %v", err)
	}
}
