package openaichat

import (
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// invokeOpenRouterWithBody invokes an empty request on an OpenRouter
// endpoint whose response is body.
func invokeOpenRouterWithBody(t *testing.T, body string) error {
	t.Helper()
	return invokeOpenRouterRequest(t, llm.Request{OperationKey: "or-op", Model: "router-model"}, body)
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
		{name: "insufficient credit", body: `{"id":"gen-1","model":"router-model","choices":[],"error":{"code":402,"message":"insufficient credits"}}`, code: provider.CodePermissionDenied, dispatch: provider.DispatchRejected, retry: provider.RetryNever},
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
	testassert.NoError(t, invokeOpenRouterWithBody(t, `{"id":"gen-1","model":"router-model","service_tier":"default","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`), "successful response rejected")
}
