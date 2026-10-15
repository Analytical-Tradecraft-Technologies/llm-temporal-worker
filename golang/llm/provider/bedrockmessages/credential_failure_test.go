package bedrockmessages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// statusCredentialError is a credential-chain failure that carries the STS or
// IMDS HTTP status, which must not be read as a Bedrock response status.
type statusCredentialError struct{ status int }

func (err statusCredentialError) Error() string       { return "credential endpoint returned an error" }
func (err statusCredentialError) HTTPStatusCode() int { return err.status }

// A credential failure happens while the Bedrock middleware signs the
// request, before it reaches the transport: it is a pre-dispatch
// authentication failure that may move to another route, never an ambiguous,
// possibly paid outcome (#1316).
func TestBedrockCredentialFailureIsPreDispatch(t *testing.T) {
	for name, credentialErr := range map[string]error{
		"no status":   errors.New("dial tcp 169.254.169.254:80: connect: network is unreachable"),
		"IMDS status": statusCredentialError{status: http.StatusNotFound},
		"canceled":    context.Canceled,
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			client, err := NewClient(context.Background(), ClientConfig{
				BaseURL: "http://127.0.0.1",
				HTTPClient: &http.Client{Transport: bedrockRoundTrip(func(*http.Request) (*http.Response, error) {
					calls++
					return nil, errors.New("transport must not be reached")
				})},
				AWSConfig: aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
					return aws.Credentials{}, credentialErr
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, "http://127.0.0.1"))
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{
				Request: llm.Request{OperationKey: "bedrock-credentials", Model: "claude-contract"},
				Query:   provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"}, Strict: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Invoke(context.Background(), call, nil)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || calls != 0 {
				t.Fatalf("Invoke() error = %#v, transport calls = %d, want a classified error and no transport call", err, calls)
			}
			if errors.Is(credentialErr, context.Canceled) {
				// Caller intent keeps its own classification.
				if mapped.Code != provider.CodeCanceled || errors.Is(err, provider.ErrProviderPreDispatch) {
					t.Fatalf("Invoke() error = %#v, want the unchanged cancellation classification", err)
				}
				return
			}
			if mapped.Code != provider.CodeAuthentication || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
				t.Fatalf("Invoke() error = %#v, want a not-dispatched next-route authentication failure", err)
			}
			if !errors.Is(err, provider.ErrProviderPreDispatch) || !errors.Is(err, provider.ErrProviderCredentials) {
				t.Fatalf("Invoke() error = %v, want the pre-dispatch and credential markers", err)
			}
			if mapped.SafeDetails["endpoint"] != "bedrock-prod" || mapped.SafeDetails["status"] != "" {
				t.Fatalf("details = %v, want the endpoint and no status", mapped.SafeDetails)
			}
		})
	}
}
