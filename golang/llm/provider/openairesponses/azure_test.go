package openairesponses

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestAzureResponsesUsesV1PathAndAPIKey(t *testing.T) {
	responseBody := readContractFixture(t, "azure-responses", "response.completed.json")
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"azure-resp-1"}},
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
			Request:    request,
		}, nil
	})
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://127.0.0.1",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureAdapter(client, "azure-responses", "azure-responses/v1")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "azure-op", Model: "gpt-contract", ServiceClass: llm.ServiceClassStandard},
		Query:   provider.CapabilityQuery{EndpointID: "azure-responses", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("Azure request was not captured")
	}
	if got.URL.Path != "/openai/v1/responses" || got.URL.Query().Get("api-version") != "v1" {
		t.Fatalf("Azure Responses request URL = %s", got.URL)
	}
	if got.Header.Get("Api-Key") != "test-azure-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("Azure Responses auth headers = %#v", got.Header)
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"model":"gpt-contract"`)) {
		t.Fatalf("Azure Responses request body = %s", body)
	}
	if result.Response.OperationKey != "azure-op" || result.Response.Provider.RequestID != "azure-resp-1" {
		t.Fatalf("Azure Responses result = %#v", result.Response)
	}
}

func TestAzureResponsesValidatesResolvedConfig(t *testing.T) {
	valid := AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: http.DefaultClient,
	}
	for name, config := range map[string]AzureClientConfig{
		"missing endpoint":    {APIVersion: valid.APIVersion, APIKey: valid.APIKey, HTTPClient: valid.HTTPClient},
		"missing api version": {Endpoint: valid.Endpoint, APIKey: valid.APIKey, HTTPClient: valid.HTTPClient},
		"missing api key":     {Endpoint: valid.Endpoint, APIVersion: valid.APIVersion, HTTPClient: valid.HTTPClient},
		"missing http client": {Endpoint: valid.Endpoint, APIVersion: valid.APIVersion, APIKey: valid.APIKey},
		"unsafe endpoint":     {Endpoint: "http://example.com", APIVersion: valid.APIVersion, APIKey: valid.APIKey, HTTPClient: valid.HTTPClient},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewAzureClient(config); err == nil {
				t.Fatalf("NewAzureClient(%#v) unexpectedly succeeded", config)
			}
		})
	}
}

func TestAzureResponsesTokenCredentialUsesBearerAndV1Path(t *testing.T) {
	var got *http.Request
	credential := &fakeAzureTokenCredential{}
	client, err := NewAzureTokenClient(AzureTokenClientConfig{
		Endpoint:        "https://127.0.0.1",
		APIVersion:      "v1",
		TokenCredential: credential,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			got = request
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(readContractFixture(t, "azure-responses", "response.completed.json"))),
				Request:    request,
			}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-contract")})
	if err != nil {
		t.Fatal(err)
	}
	if credential.calls == 0 {
		t.Fatal("token credential was not called")
	}
	if got == nil || got.URL.Path != "/openai/v1/responses" || got.URL.Query().Get("api-version") != "v1" {
		t.Fatalf("Azure token Responses request URL = %v", gotURL(got))
	}
	if got.Header.Get("Authorization") != "Bearer azure-token" || got.Header.Get("Api-Key") != "" {
		t.Fatalf("Azure token Responses auth headers = %#v", got.Header)
	}
}

func TestAzureResponsesTokenClientRejectsMissingCredential(t *testing.T) {
	base := AzureTokenClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		HTTPClient: http.DefaultClient,
	}
	for name, credential := range map[string]azcore.TokenCredential{
		"nil interface": nil,
		"typed nil":     (*fakeAzureTokenCredential)(nil),
	} {
		t.Run(name, func(t *testing.T) {
			base.TokenCredential = credential
			_, err := NewAzureTokenClient(base)
			if err == nil || !strings.Contains(err.Error(), "token credential is required") {
				t.Fatalf("missing token credential error = %v", err)
			}
		})
	}
}

func TestAzureResponsesAdapterAliases(t *testing.T) {
	client, err := NewAzureOpenAIClient(AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: http.DefaultClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureOpenAIAdapter(client, "azure-responses", "azure-responses/v1")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Name() != adapterName {
		t.Fatalf("adapter name = %q, want %q", adapter.Name(), adapterName)
	}
}

func TestAzureResponsesPreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	deadline := newPreDialDeadlineContext()
	transport := &preDialDeadlineTransport{started: make(chan struct{})}
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureAdapter(client, "azure-responses", "azure-responses/v1")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "azure-pre-dial-deadline", Model: "gpt-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "azure-responses", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	result := make(chan error, 1)
	go func() {
		_, invokeErr := adapter.Invoke(deadline, call, observer)
		result <- invokeErr
	}()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("Azure provider transport was not reached")
	}
	deadline.expire()
	var invokeErr error
	select {
	case invokeErr = <-result:
	case <-time.After(time.Second):
		t.Fatal("Azure Invoke() did not return after the pre-dial deadline")
	}
	var mapped *provider.Error
	if !errors.As(invokeErr, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", invokeErr, invokeErr)
	}
	if mapped.Code != provider.CodeDeadlineExceeded || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped error = %#v, want non-retryable pre-dispatch caller deadline", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatalf("mapped error = %v, want certified pre-dispatch caller deadline", mapped)
	}
	if transport.calls != 1 || transport.bodyReads != 0 || observer.before != 1 || observer.headers != 0 {
		t.Fatalf("transport/observer = calls=%d bodyReads=%d observer=%#v", transport.calls, transport.bodyReads, observer)
	}
}

func TestAzureResponsesRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls int
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{
					StatusCode: http.StatusTemporaryRedirect,
					Header: http.Header{
						"Content-Type": []string{"application/json"},
						"Location":     []string{"https://redirect.example/continuation-secret"},
					},
					Body:    io.NopCloser(strings.NewReader(`{"error":{"message":"redirect"}}`)),
					Request: request,
				}, nil
			}),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAzureAdapter(client, "azure-responses", "azure-responses/v1")
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "azure-redirect", Model: "gpt-contract"},
		Query:   provider.CapabilityQuery{EndpointID: "azure-responses", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
		t.Fatalf("mapped redirect = %#v, want ambiguous non-retriable provider-unavailable", mapped)
	}
	if calls != 1 || observer.before != 1 || observer.headers != 0 {
		t.Fatalf("transport/observer = calls=%d observer=%#v, want one request, one pre-write mark, and no headers", calls, observer)
	}
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "azure-responses" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

func TestAzureResponsesPathShimLeavesOtherRoutesUntouched(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "https://resource.openai.azure.com/openai/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	_, err = azureResponsesPathMiddleware(request, func(got *http.Request) (*http.Response, error) {
		called = true
		if got.URL.Path != "/openai/models" {
			t.Fatalf("non-Responses path = %q", got.URL.Path)
		}
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("path middleware result: called=%v err=%v", called, err)
	}
}

type fakeAzureTokenCredential struct {
	calls int
}

func (credential *fakeAzureTokenCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	credential.calls++
	if len(options.Scopes) != 1 || options.Scopes[0] != "https://cognitiveservices.azure.com/.default" {
		return azcore.AccessToken{}, fmt.Errorf("unexpected scopes: %v", options.Scopes)
	}
	return azcore.AccessToken{Token: "azure-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func gotURL(request *http.Request) any {
	if request == nil {
		return nil
	}
	return request.URL
}
