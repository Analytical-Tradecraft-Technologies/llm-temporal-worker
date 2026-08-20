package anthropicmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestAWSGatewayUsesOfficialMessagesClientExactlyOnce(t *testing.T) {
	responseBody := string(mustReadAWSFixture(t, "response.completed.json"))
	calls := 0
	var requestBody []byte
	client, err := NewAWSClient(context.Background(), AWSClientConfig{
		BaseURL: "http://127.0.0.1/aws-contract",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			if request.URL.Path != "/aws-contract/v1/messages" {
				t.Errorf("request path = %q", request.URL.Path)
			}
			var err error
			requestBody, err = io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"aws-req-1"}}, Body: io.NopCloser(strings.NewReader(responseBody)), Request: request}, nil
		})},
		AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", SkipAuth: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/aws-contract"
	adapter, err := New(client, "anthropic-aws", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "aws-once", Model: "claude-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-aws", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, nil)
	if err != nil {
		var providerErr *provider.Error
		if errors.As(err, &providerErr) {
			t.Fatalf("invoke error: %#v cause=%v", providerErr, providerErr.Cause)
		}
		t.Fatalf("invoke error: %#v", err)
	}
	if calls != 1 {
		t.Fatalf("AWS gateway request count = %d, want 1", calls)
	}
	assertFixture(t, mustReadAWSFixture(t, "request.wire.json"), requestBody)
	response := result.Response
	if response.Provider.ResponseID != "msg-aws-fixture" || response.Provider.RequestID != "aws-req-1" || response.Usage.InputTokens != 4 || response.Usage.OutputTokens != 3 {
		t.Fatalf("AWS gateway response facts = %#v", response)
	}
	if response.Service.Actual == nil || *response.Service.Actual != llm.ServiceClassStandard || response.Continuation != nil {
		t.Fatalf("AWS gateway service/continuation = %#v %#v", response.Service, response.Continuation)
	}
}

func TestAWSConfigAndCallsDoNotSerializeCredentialValues(t *testing.T) {
	const secret = "aws-secret-must-not-cross-boundary"
	config := AWSClientConfig{AWSConfig: anthropicaws.ClientConfig{AWSAccessKey: "access", AWSSecretAccessKey: secret, AWSRegion: "us-east-1", SkipAuth: true}}
	encodedConfig, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedConfig), secret) {
		t.Fatal("resolved credential value appeared in AWS client configuration serialization")
	}
	adapter := &Adapter{endpointID: "anthropic-aws", profile: mustProfile(t, func() Profile {
		profile := testProfile()
		profile.ExpectedBaseURL = ""
		return profile
	}())}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "aws-credentials", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-aws", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedCall, err := json.Marshal(call)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encodedCall), secret) {
		t.Fatal("credential value appeared in provider call serialization")
	}
}

func TestAWSClientRequiresContextAndHTTPClient(t *testing.T) {
	if _, err := NewAWSClient(nil, AWSClientConfig{HTTPClient: http.DefaultClient, AWSConfig: anthropicaws.ClientConfig{SkipAuth: true, AWSRegion: "us-east-1", WorkspaceID: "ws-contract"}}); err == nil {
		t.Fatal("nil context unexpectedly succeeded")
	}
	if _, err := NewAWSClient(context.Background(), AWSClientConfig{AWSConfig: anthropicaws.ClientConfig{SkipAuth: true, AWSRegion: "us-east-1", WorkspaceID: "ws-contract"}}); err == nil {
		t.Fatal("nil HTTP client unexpectedly succeeded")
	}
}

func TestAWSClientRequiresExplicitRegionAndWorkspace(t *testing.T) {
	_, err := NewAWSClient(context.Background(), AWSClientConfig{BaseURL: "http://127.0.0.1/aws-contract", HTTPClient: http.DefaultClient, AWSConfig: anthropicaws.ClientConfig{WorkspaceID: "ws-contract", SkipAuth: true}})
	if err == nil || !strings.Contains(err.Error(), "AWS region is required") {
		t.Fatalf("missing AWS region error = %v", err)
	}
	_, err = NewAWSClient(context.Background(), AWSClientConfig{BaseURL: "http://127.0.0.1/aws-contract", HTTPClient: http.DefaultClient, AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1"}})
	if err == nil || !strings.Contains(err.Error(), "AWS workspace ID is required") {
		t.Fatalf("missing AWS workspace ID error = %v", err)
	}
}

func TestAWSGatewayClientRejectsSecretOnlyAuthentication(t *testing.T) {
	base := AWSClientConfig{BaseURL: "http://127.0.0.1/aws-contract", HTTPClient: http.DefaultClient, AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract"}}
	static := base
	static.AWSConfig.APIKey = "test-key"
	if _, err := NewAWSGatewayClient(context.Background(), static); err == nil || !strings.Contains(err.Error(), "aws_default_chain") {
		t.Fatalf("static credential error = %v", err)
	}
	t.Setenv("ANTHROPIC_AWS_API_KEY", "test-key")
	if _, err := NewAWSGatewayClient(context.Background(), base); err == nil || !strings.Contains(err.Error(), "ANTHROPIC_AWS_API_KEY") {
		t.Fatalf("environment API key error = %v", err)
	}
}

func TestAWSGatewayClassifiesAuthenticationFixture(t *testing.T) {
	calls := 0
	body := string(mustReadAWSFixture(t, "error.authentication.json"))
	client, err := NewAWSClient(context.Background(), AWSClientConfig{
		BaseURL: "http://127.0.0.1/aws-contract",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"aws-auth-1"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		})},
		AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", SkipAuth: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/aws-contract"
	adapter, err := New(client, "anthropic-aws", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "aws-auth", Model: "claude-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-aws", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeAuthentication || providerErr.Provider.RequestID != "aws-auth-1" {
		t.Fatalf("AWS authentication error = %#v", err)
	}
	if calls != 1 {
		t.Fatalf("AWS gateway request count = %d, want 1", calls)
	}
}

func TestAWSGatewayFixtureTextOnlyResponseDoesNotLiftContinuation(t *testing.T) {
	var response anthropic.Message
	if err := json.Unmarshal(mustReadAWSFixture(t, "response.completed.json"), &response); err != nil {
		t.Fatal(err)
	}
	call := provider.Call{EndpointID: "anthropic-aws", Family: provider.FamilyAnthropicMessages, Model: "claude-contract", OperationKey: "aws-fixture", ServiceClass: llm.ServiceClassStandard}
	lifted, err := mustProfile(t, testProfile()).liftResponse(call, &response, "aws-fixture-request")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Route.APIFamily != string(provider.FamilyAnthropicMessages) || lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassStandard || lifted.Continuation != nil {
		t.Fatalf("AWS fixture response = %#v", lifted)
	}
}

func mustReadAWSFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/contracts/anthropic-aws/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
