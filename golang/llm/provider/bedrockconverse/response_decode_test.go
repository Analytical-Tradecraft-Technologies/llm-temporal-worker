package bedrockconverse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type cannedConverseTransport struct{ body string }

func (transport cannedConverseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Amzn-Requestid": []string{"request-decode"}},
		Body:       io.NopCloser(strings.NewReader(transport.body)),
		Request:    request,
	}, nil
}

// invokeCannedConverse pushes one HTTP response body through the real AWS SDK
// client and the adapter, so SDK decoding is part of what is exercised.
func invokeCannedConverse(t *testing.T, body string) (provider.Result, error) {
	t.Helper()
	return invokeRealConverse(t, cannedConverseTransport{body: body}, fixtureCredentials)
}

func fixtureCredentials(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "fixture", SecretAccessKey: "fixture"}, nil
}

func invokeRealConverse(t *testing.T, transport http.RoundTripper, credentials func(context.Context) (aws.Credentials, error)) (provider.Result, error) {
	t.Helper()
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL:    "https://bedrock-runtime.us-east-1.amazonaws.com",
		HTTPClient: &http.Client{Transport: transport},
		AWSConfig:  aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(credentials)},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(client, "endpoint", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	request := llm.Request{OperationKey: "op-decode", Model: "amazon.nova-pro-v1:0",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "Hello"}}}}}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request, Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Invoke(context.Background(), call, provider.NopObserver{})
}

func TestCannedConverseTextResponseDecodes(t *testing.T) {
	result, err := invokeCannedConverse(t, `{"output":{"message":{"role":"assistant","content":[{"text":"hello"}]}},"stopReason":"end_turn","usage":{"inputTokens":4,"outputTokens":3,"totalTokens":7}}`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Response.Status != llm.ResponseStatusCompleted || len(result.Response.Output) != 1 || result.Response.Provider.RequestID != "request-decode" {
		t.Fatalf("response = %#v", result.Response)
	}
}

func TestUnknownContentBlockIsAcceptedInvalidResponseNotPanic(t *testing.T) {
	_, err := invokeCannedConverse(t, `{"output":{"message":{"role":"assistant","content":[{"futureBlock":{"value":1}},{"text":"hello"}]}},"stopReason":"end_turn","usage":{"inputTokens":4,"outputTokens":3,"totalTokens":7}}`)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted || mapped.OperationID != "op-decode" {
		t.Fatalf("error = %v, want an accepted invalid response", err)
	}
}

type panickingConverse struct{}

func (panickingConverse) Converse(context.Context, *bedrockruntime.ConverseInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	panic("sdk decode failure")
}

type panickingTransport struct{}

func (panickingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	panic("transport failure")
}

// A panic is classified by how far the request got, not by assumption: only
// a panic after a response arrived is a paid, accepted response. A panic
// before the request reached the transport was never dispatched and stays
// retryable on another route; one in the transport itself is ambiguous.
func TestConverseSDKPanicIsClassifiedByDispatchEvidence(t *testing.T) {
	type want struct {
		code     provider.Code
		dispatch provider.DispatchCertainty
		retry    provider.RetryDisposition
	}
	tests := map[string]struct {
		invoke func(t *testing.T) error
		want   want
	}{
		"before the request is sent": {
			invoke: func(t *testing.T) error {
				_, err := invokeRealConverse(t, cannedConverseTransport{body: "{}"}, func(context.Context) (aws.Credentials, error) {
					panic("credential lookup failure")
				})
				return err
			},
			want: want{code: provider.CodeInternal, dispatch: provider.DispatchNotDispatched, retry: provider.RetryNextRoute},
		},
		"outside the middleware stack": {
			invoke: func(t *testing.T) error {
				adapter, err := New(&Client{converse: panickingConverse{}}, "endpoint", DefaultProfile("nova"))
				if err != nil {
					t.Fatal(err)
				}
				_, err = adapter.Invoke(context.Background(), provider.Call{EndpointID: "endpoint", Family: provider.FamilyBedrockConverse, OperationKey: "op-decode", SDKParams: bedrockruntime.ConverseInput{}}, provider.NopObserver{})
				return err
			},
			want: want{code: provider.CodeInternal, dispatch: provider.DispatchNotDispatched, retry: provider.RetryNextRoute},
		},
		"in the transport": {
			invoke: func(t *testing.T) error {
				_, err := invokeRealConverse(t, panickingTransport{}, fixtureCredentials)
				return err
			},
			want: want{code: provider.CodeInternal, dispatch: provider.DispatchAmbiguous, retry: provider.RetryNever},
		},
		"while decoding the response": {
			invoke: func(t *testing.T) error {
				_, err := invokeCannedConverse(t, `{"output":{"message":{"role":"assistant","content":[{"futureBlock":{"value":1}}]}},"stopReason":"end_turn"}`)
				return err
			},
			want: want{code: provider.CodeProviderInvalidResponse, dispatch: provider.DispatchAccepted, retry: provider.RetryNever},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.invoke(t)
			var mapped *provider.Error
			if !errors.As(err, &mapped) {
				t.Fatalf("error = %v, want a classified provider error", err)
			}
			if mapped.Code != test.want.code || mapped.Dispatch != test.want.dispatch || mapped.Retry != test.want.retry || mapped.OperationID != "op-decode" {
				t.Fatalf("error = %#v, want %+v", mapped, test.want)
			}
		})
	}
}

func TestMalformedStopReasonsKeepThePaidResponse(t *testing.T) {
	for _, reason := range []string{"malformed_model_output", "malformed_tool_use"} {
		result, err := invokeCannedConverse(t, `{"output":{"message":{"role":"assistant","content":[{"text":"partial"}]}},"stopReason":"`+reason+`","usage":{"inputTokens":4,"outputTokens":3,"totalTokens":7}}`)
		if err != nil {
			t.Fatalf("%s: %v", reason, err)
		}
		response := result.Response
		if response.Status != llm.ResponseStatusLength || response.Provider.FinishReason != reason || response.Usage.InputTokens != 4 || response.Usage.OutputTokens != 3 || len(response.Output) != 1 {
			t.Fatalf("%s response = %#v", reason, response)
		}
	}
}
