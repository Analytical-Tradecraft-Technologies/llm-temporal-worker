package bedrockconverse

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
