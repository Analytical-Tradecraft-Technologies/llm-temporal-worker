package openairesponses

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestAzureResponsesUsesV1PathAndAPIKey(t *testing.T) {
	responseBody := readContractFixture(t, "azure-responses", "response.completed.json")
	var got *http.Request
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return adaptertest.JSONResponse(request, http.StatusOK, string(responseBody), "X-Request-Id", "azure-resp-1"), nil
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
	call := adaptertest.CompileStrict(t, adapter, "azure-responses", provider.FamilyOpenAIResponses, llm.Request{OperationKey: "azure-op", Model: "gpt-contract", ServiceClass: llm.ServiceClassStandard})
	result := adaptertest.Invoke(t, adapter, call)
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
			return adaptertest.JSONResponse(request, http.StatusOK, string(readContractFixture(t, "azure-responses", "response.completed.json"))), nil
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
			testassert.ErrorContains(t, err, "token credential is required")
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
	transport := adaptertest.NewPreDialDeadlineTransport()
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileAzureCall(t, client, "azure-pre-dial-deadline")
	assertCallerDeadlineBeforeDial(t, transport, adapter, call)
}

func TestAzureResponsesRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	var calls int
	client, err := NewAzureClient(AzureClientConfig{
		Endpoint:   "https://resource.openai.azure.com",
		APIVersion: "v1",
		APIKey:     "test-azure-key",
		HTTPClient: &http.Client{
			Transport:     adaptertest.RedirectTransport(&calls, `{"error":{"message":"redirect"}}`),
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileAzureCall(t, client, "azure-redirect")
	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 || observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("transport/observer = calls=%d observer=%#v, want one request, one pre-write mark, and no headers", calls, observer)
	}
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "azure-responses" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

// compileAzureCall builds the "azure-responses" adapter over client and
// strictly compiles an empty gpt-contract request under operationKey.
func compileAzureCall(t *testing.T, client *Client, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	adapter, err := NewAzureAdapter(client, "azure-responses", "azure-responses/v1")
	if err != nil {
		t.Fatal(err)
	}
	return adapter, adaptertest.CompileStrict(t, adapter, "azure-responses", provider.FamilyOpenAIResponses, llm.Request{OperationKey: operationKey, Model: "gpt-contract"})
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

func TestAzureResponsesAcceptsDocumentedEndpointForms(t *testing.T) {
	for _, endpoint := range []string{
		"https://127.0.0.1",
		"https://127.0.0.1/",
		"https://127.0.0.1/openai",
		"https://127.0.0.1/openai/v1",
		"https://127.0.0.1/openai/v1/",
	} {
		for _, auth := range []string{"api-key", "token"} {
			t.Run(auth+" "+endpoint, func(t *testing.T) {
				var got *http.Request
				httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					got = request
					return adaptertest.JSONResponse(request, http.StatusOK, string(readContractFixture(t, "azure-responses", "response.completed.json"))), nil
				})}
				var client *Client
				var err error
				if auth == "api-key" {
					client, err = NewAzureClient(AzureClientConfig{Endpoint: endpoint, APIVersion: "2024-10-21", APIKey: "test-azure-key", HTTPClient: httpClient})
				} else {
					client, err = NewAzureTokenClient(AzureTokenClientConfig{Endpoint: endpoint, APIVersion: "2024-10-21", TokenCredential: &fakeAzureTokenCredential{}, HTTPClient: httpClient})
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.sdk.Responses.New(context.Background(), responses.ResponseNewParams{Model: shared.ResponsesModel("gpt-contract")}); err != nil {
					t.Fatal(err)
				}
				if got == nil || got.URL.Path != "/openai/v1/responses" || got.URL.Query().Get("api-version") != "2024-10-21" {
					t.Fatalf("Azure Responses request URL = %v", gotURL(got))
				}
			})
		}
	}
}

func TestAzureResourceRootOnlyStripsPathSuffixes(t *testing.T) {
	for endpoint, want := range map[string]string{
		"https://openai/":   "https://openai/",
		"https://openai/v1": "https://openai/v1",
		"https://example.openai.azure.com/openai/v1/": "https://example.openai.azure.com/",
		"https://proxy.test/tenant/openai":            "https://proxy.test/tenant/",
	} {
		if got := azureResourceRoot(endpoint); got != want {
			t.Errorf("azureResourceRoot(%q) = %q, want %q", endpoint, got, want)
		}
	}
}

type failingAzureTokenCredential struct{ err error }

func (credential failingAzureTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, credential.err
}

func invokeAzureTokenAdapter(t *testing.T, credential azcore.TokenCredential, transport roundTripFunc) (error, *adaptertest.Observer) {
	t.Helper()
	client, err := NewAzureTokenClient(AzureTokenClientConfig{Endpoint: "https://resource.openai.azure.com", APIVersion: "v1",
		TokenCredential: credential, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, call := compileAzureCall(t, client, "azure-token")
	observer := &adaptertest.Observer{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	return err, observer
}

// A token-credential failure happens before anything is sent, so it is a
// pre-dispatch authentication failure that may move to another route, not
// an ambiguous outcome charged at the full reservation (#1495).
func TestAzureResponsesTokenCredentialFailureIsNotDispatched(t *testing.T) {
	cause := errors.New("workload identity token expired")
	sent := 0
	err, observer := invokeAzureTokenAdapter(t, failingAzureTokenCredential{err: cause}, func(*http.Request) (*http.Response, error) {
		sent++
		return nil, errors.New("unexpected request")
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("Invoke() error = %T %v, want *provider.Error", err, err)
	}
	if mapped.Code != provider.CodeAuthentication || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped error = %#v, want a not-dispatched credential failure", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderPreDispatch) || !errors.Is(mapped, provider.ErrProviderCredentials) {
		t.Fatalf("mapped error = %v, want certified pre-dispatch credential failure", mapped)
	}
	if sent != 0 || observer.Before != 1 || observer.Headers != 0 {
		t.Fatalf("transport calls=%d observer=%#v", sent, observer)
	}
}

// A transport failure after the token was attached may have reached the
// provider, so it stays ambiguous.
func TestAzureResponsesTokenTransportFailureStaysAmbiguous(t *testing.T) {
	sent := 0
	err, _ := invokeAzureTokenAdapter(t, &fakeAzureTokenCredential{}, func(request *http.Request) (*http.Response, error) {
		sent++
		if request.Header.Get("Authorization") != "Bearer azure-token" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		return nil, io.ErrUnexpectedEOF
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || errors.Is(mapped, provider.ErrProviderPreDispatch) || sent != 1 {
		t.Fatalf("Invoke() error = %#v after %d sends, want an ambiguous dispatch", err, sent)
	}
}
