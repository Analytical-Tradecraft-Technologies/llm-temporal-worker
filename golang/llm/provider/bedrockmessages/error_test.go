package bedrockmessages

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/messagestest"
)

func TestMapErrorClassifiesProviderNeutralFailures(t *testing.T) {
	adaptertest.ProviderNeutralFailures(t, func(err error) *provider.Error { return mapError(err, "bedrock_anthropic") }, provider.NewDispatchContextError)
}

func TestMapAPIErrorClassifiesCommonHTTPStatuses(t *testing.T) {
	adaptertest.HTTPStatuses(t, func(status int, response *http.Response) *provider.Error {
		return mapAPIError(&anthropic.Error{StatusCode: status, Response: response}, "bedrock-messages")
	})
}

func TestModelProcessingFailuresRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusFailedDependency} {
		cause := &anthropic.Error{StatusCode: status, RequestID: "request-id"}
		mapped := mapError(fmt.Errorf("wrapped: %w", cause), "bedrock-messages")
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderUnavailable, Dispatch: provider.DispatchAmbiguous, Retry: provider.RetrySameOperation}, fmt.Sprintf("status %d became a rejection", status))
		if !errors.Is(mapped, cause) || mapped.Provider.RequestID != "request-id" {
			t.Fatal("provider error identity was lost")
		}
	}
}

func TestDefiniteMessagesRejectionsRemainRejected(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 422, 429} {
		mapped := mapAPIError(&anthropic.Error{StatusCode: status}, "bedrock-messages")
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Dispatch: provider.DispatchRejected}, fmt.Sprintf("status %d became ambiguous", status))
	}
}

// Anthropic reports a request that timed out while processing as
// timeout_error; that type is ambiguous whatever status carries it.
func TestTimeoutErrorRemainsAmbiguous(t *testing.T) {
	messagestest.TimeoutErrorRemainsAmbiguous(t, func(apiErr *anthropic.Error) *provider.Error { return mapAPIError(apiErr, "bedrock-messages") })
}
