package bedrockconverse

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestMapErrorMapsRetryAfterFromWrappedSmithyResponse(t *testing.T) {
	smithyErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"2"}},
		}},
		Err: errors.New("provider rate limited the request"),
	}
	mapped := mapError(fmt.Errorf("converse request: %w", smithyErr), "bedrock-converse")
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderRateLimited, Retry: provider.RetryAfter})
	if got, want := mapped.RetryAfter, 2*time.Second; got != want {
		t.Fatalf("retry after = %s, want %s", got, want)
	}
	if got, want := mapped.SafeDetails["retry_after"], "2"; got != want {
		t.Fatalf("safe retry after = %q, want %q", got, want)
	}
}

func TestModelProcessingFailuresRemainAmbiguous(t *testing.T) {
	cases := map[string]error{
		"timeout":     &types.ModelTimeoutException{},
		"model error": &types.ModelErrorException{},
	}
	for _, status := range []int{http.StatusRequestTimeout, http.StatusFailedDependency} {
		cases[fmt.Sprint(status)] = &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}}, Err: errors.New("sensitive provider body")}
	}
	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			mapped := mapError(fmt.Errorf("wrapped: %w", cause), "converse")
			testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeProviderUnavailable, Dispatch: provider.DispatchAmbiguous, Retry: provider.RetrySameOperation}, "model failure became a rejection")
			testassert.ErrorIs(t, mapped, cause, "underlying error was lost")
			if strings.Contains(mapped.SafeMessage, "sensitive") {
				t.Fatal("provider details leaked")
			}
		})
	}
}

func TestDefiniteConverseRejectionsRemainRejected(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 422, 429} {
		mapped := mapHTTPError(status, errors.New("rejection"), "converse")
		testkit.ProviderError(t, mapped, testkit.ProviderClass{Dispatch: provider.DispatchRejected}, fmt.Sprintf("status %d became ambiguous", status))
	}
}

func TestMapErrorClassifiesProviderNeutralFailures(t *testing.T) {
	adaptertest.ProviderNeutralFailures(t, func(err error) *provider.Error { return mapError(err, "converse") }, dispatchContextError)
}

func TestMapErrorClassifiesCommonHTTPStatuses(t *testing.T) {
	adaptertest.HTTPStatuses(t, func(status int, response *http.Response) *provider.Error {
		if response == nil {
			response = &http.Response{}
		}
		response.StatusCode = status
		return mapError(fmt.Errorf("wrapped: %w", &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: response}, Err: errors.New("provider body")}), "converse")
	})
}

func TestConversePaymentRequiredIsAccountDenial(t *testing.T) {
	mapped := mapHTTPError(http.StatusPaymentRequired, errors.New("billing"), "converse")
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodePermissionDenied, Dispatch: provider.DispatchRejected, Retry: provider.RetryNever}, "402")
}
