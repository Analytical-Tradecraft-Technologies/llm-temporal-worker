package bedrockconverse

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type countingTransport struct{ calls int }

func (transport *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls++
	return nil, errors.New("transport must not be reached")
}

// statusCredentialError is a credential-chain failure that carries the STS or
// IMDS HTTP status, which must not be read as a Bedrock response status.
type statusCredentialError struct{ status int }

func (err statusCredentialError) Error() string       { return "credential endpoint returned an error" }
func (err statusCredentialError) HTTPStatusCode() int { return err.status }

// A credential failure happens while the SDK signs the request, before it
// reaches the transport: it is a pre-dispatch authentication failure that may
// move to another route, never an ambiguous, possibly paid outcome (#1316).
func TestConverseCredentialFailureIsPreDispatch(t *testing.T) {
	for name, credentialErr := range map[string]error{
		"no status":   errors.New("dial tcp 169.254.169.254:80: connect: network is unreachable"),
		"IMDS status": statusCredentialError{status: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			transport := &countingTransport{}
			_, err := invokeRealConverse(t, transport, func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{}, credentialErr
			})
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Code != provider.CodeAuthentication || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
				t.Fatalf("Invoke() error = %#v, want a not-dispatched next-route authentication failure", err)
			}
			if !errors.Is(err, provider.ErrProviderPreDispatch) || !errors.Is(err, provider.ErrProviderCredentials) {
				t.Fatalf("Invoke() error = %v, want the pre-dispatch and credential markers", err)
			}
			if mapped.SafeDetails["endpoint"] != "endpoint" || mapped.SafeDetails["status"] != "" || transport.calls != 0 {
				t.Fatalf("details = %v, transport calls = %d, want no status and no transport call", mapped.SafeDetails, transport.calls)
			}
		})
	}
}

// A caller cancellation seen while resolving credentials keeps its own
// classification; only the caller's intent decides it.
func TestConverseCredentialContextErrorKeepsItsClassification(t *testing.T) {
	transport := &countingTransport{}
	_, err := invokeRealConverse(t, transport, func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{}, context.Canceled
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeCanceled || errors.Is(err, provider.ErrProviderPreDispatch) || transport.calls != 0 {
		t.Fatalf("Invoke() error = %#v, calls = %d, want the unchanged cancellation classification", err, transport.calls)
	}
}
