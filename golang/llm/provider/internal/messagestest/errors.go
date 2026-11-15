package messagestest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// TimeoutErrorRemainsAmbiguous checks that mapAPIError keeps an Anthropic
// timeout_error ambiguous whatever status carries it, since the provider
// reports a request that timed out while processing that way.
func TimeoutErrorRemainsAmbiguous(t *testing.T, mapAPIError func(*anthropic.Error) *provider.Error) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusGatewayTimeout} {
		apiErr := &anthropic.Error{StatusCode: status}
		if err := json.Unmarshal([]byte(`{"type":"error","error":{"type":"timeout_error","message":"Request timed out"}}`), apiErr); err != nil {
			t.Fatal(err)
		}
		mapped := mapAPIError(apiErr)
		if mapped.Dispatch != provider.DispatchAmbiguous || mapped.SafeDetails["provider_type"] != "timeout_error" {
			t.Fatalf("timeout_error with status %d became a rejection: %#v", status, mapped)
		}
	}
}
