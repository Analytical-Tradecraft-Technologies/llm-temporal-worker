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

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
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
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL:    "https://bedrock-runtime.us-east-1.amazonaws.com",
		HTTPClient: &http.Client{Transport: cannedConverseTransport{body: body}},
		AWSConfig: aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fixture", SecretAccessKey: "fixture"}, nil
		})},
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

func TestConverseSDKPanicIsAcceptedInvalidResponse(t *testing.T) {
	adapter, err := New(&Client{converse: panickingConverse{}}, "endpoint", DefaultProfile("nova"))
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "endpoint", Family: provider.FamilyBedrockConverse, OperationKey: "op-panic", SDKParams: bedrockruntime.ConverseInput{}}
	_, err = adapter.Invoke(context.Background(), call, provider.NopObserver{})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted || mapped.OperationID != "op-panic" {
		t.Fatalf("error = %v, want an accepted invalid response", err)
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
