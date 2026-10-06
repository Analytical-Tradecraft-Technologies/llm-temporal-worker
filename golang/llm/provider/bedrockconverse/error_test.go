package bedrockconverse

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"net/http"
	"strings"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
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
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Retry != provider.RetryAfter {
		t.Fatalf("mapped error = %#v", mapped)
	}
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
			if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetrySameOperation {
				t.Fatalf("model failure became a rejection: %#v", mapped)
			}
			if !errors.Is(mapped, cause) {
				t.Fatal("underlying error was lost")
			}
			if strings.Contains(mapped.SafeMessage, "sensitive") {
				t.Fatal("provider details leaked")
			}
		})
	}
}

func TestDefiniteConverseRejectionsRemainRejected(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 422, 429} {
		mapped := mapHTTPError(status, errors.New("rejection"), "converse")
		if mapped.Dispatch != provider.DispatchRejected {
			t.Fatalf("status %d became ambiguous", status)
		}
	}
}
