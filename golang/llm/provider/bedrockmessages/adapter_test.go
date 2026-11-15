package bedrockmessages

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/contract"
)

func TestStreamingCapabilityCannotOutrunAdapterPort(t *testing.T) {
	adaptertest.StreamingCapabilityCannotOutrunAdapterPort(t, (*Adapter)(nil), DefaultProfile("bedrock-no-stream").Capabilities, func(streaming provider.Capability) (provider.CapabilitySet, provider.CapabilitySet, error) {
		profile := DefaultProfile("bedrock-no-stream")
		profile.Capabilities.Features[provider.FeatureStreaming] = streaming
		validated, err := NewProfile(profile)
		if err != nil {
			return provider.CapabilitySet{}, provider.CapabilitySet{}, err
		}
		adapter := &Adapter{endpointID: validated.ID, profile: validated}
		reported, err := adapter.Capabilities(context.Background(), provider.CapabilityQuery{EndpointID: validated.ID, Family: provider.FamilyBedrockMessages})
		return validated.Capabilities, reported, err
	})
}

func TestCompileMapsOnlyTheThreePublicBedrockServiceClasses(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	for _, test := range []struct {
		class llm.ServiceClass
		tier  string
	}{
		{class: llm.ServiceClassEconomy, tier: "flex"},
		{class: llm.ServiceClassStandard, tier: "default"},
		{class: llm.ServiceClassPriority, tier: "priority"},
	} {
		call := adaptertest.CompileStrict(t, adapter, "bedrock-prod", provider.FamilyBedrockMessages, llm.Request{OperationKey: "tier-" + string(test.class), Model: "claude-contract", ServiceClass: test.class})
		if call.Metadata.ProviderTier != test.tier {
			t.Fatalf("class %q provider tier = %q, want %q", test.class, call.Metadata.ProviderTier, test.tier)
		}
		// The tier travels as an InvokeModel header, never in the Anthropic body.
		wire := contract.JSONObject(t, call.SDKParams)
		if tier, present := wire["service_tier"]; present {
			t.Fatalf("class %q wire body carried service_tier %#v", test.class, tier)
		}
	}
}

func TestCompileRejectsPinnedAnthropicAWSContinuation(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{
			OperationKey: "bedrock-reject-aws-continuation",
			Model:        "claude-contract",
			Continuation: &llm.Continuation{
				Handle:     "anthropic-messages:msg-aws-fixture",
				EndpointID: "anthropic-aws",
				Model:      "claude-contract",
				Pinned:     true,
			},
		},
		Query:  provider.CapabilityQuery{EndpointID: "bedrock-prod", Family: provider.FamilyBedrockMessages, Model: "claude-contract"},
		Strict: true,
	})
	testassert.ErrorContains(t, err, "continuation endpoint")
}

func TestProfileRejectsReservedCapacityAsPublicTier(t *testing.T) {
	profile := DefaultProfile("bedrock-contract")
	profile.ServiceTiers[llm.ServiceClassEconomy] = "reserved"
	testassert.ErrorContains(t, testassert.Err(NewProfile(profile)), "invalid provider tier")
}

func TestInvokeUsesBedrockMiddlewareOnceAndLiftsAWSRequestID(t *testing.T) {
	responseBody := string(mustReadBedrockFixture(t, "invoke.response.json"))
	calls := 0
	var requestBody []byte
	var requestHeaders http.Header
	client := newBedrockTestClient(t, "http://127.0.0.1", &http.Client{Transport: adaptertest.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		requestBody, _ = io.ReadAll(request.Body)
		requestHeaders = request.Header.Clone()
		if !strings.HasSuffix(request.URL.Path, "/model/claude-contract/invoke") {
			t.Errorf("Bedrock request path = %q", request.URL.Path)
		}
		return adaptertest.JSONResponse(request, http.StatusOK, responseBody, "X-Amzn-Requestid", "bedrock-req-1"), nil
	})}, nil)
	adapter, call := compileBedrockProdCall(t, client, "http://127.0.0.1", "bedrock-once")
	result, err := adapter.Invoke(context.Background(), call, nil)
	if err != nil {
		var providerErr *provider.Error
		if errors.As(err, &providerErr) {
			t.Fatalf("invoke error: %#v cause=%v", providerErr, providerErr.Cause)
		}
		t.Fatalf("invoke error: %#v", err)
	}
	if calls != 1 || result.Response.Provider.RequestID != "bedrock-req-1" || result.Response.Service.Actual == nil || *result.Response.Service.Actual != llm.ServiceClassStandard {
		t.Fatalf("calls/response = %d %#v", calls, result.Response)
	}
	// InvokeModel takes the requested tier as a header; the header is set
	// before the Bedrock middleware runs, so SigV4 covers it.
	if strings.Contains(string(requestBody), "service_tier") {
		t.Fatalf("Bedrock request body carried a service tier: %s", requestBody)
	}
	if got := requestHeaders.Get(serviceTierHeader); got != "default" {
		t.Fatalf("Bedrock request tier header = %q, want %q", got, "default")
	}
	if authorization := requestHeaders.Get("Authorization"); !strings.Contains(strings.ToLower(authorization), strings.ToLower(serviceTierHeader)) {
		t.Fatalf("SigV4 signed headers omit %s: %s", serviceTierHeader, authorization)
	}
	if strings.Contains(string(requestBody), "contract-secret") {
		t.Fatal("AWS secret appeared in Bedrock request body")
	}
}

func TestBedrockCallSerializationDoesNotContainResolvedCredentials(t *testing.T) {
	adapter := &Adapter{endpointID: "bedrock-prod", profile: mustBedrockProfile(t, "")}
	call := adaptertest.CompileStrict(t, adapter, "bedrock-prod", provider.FamilyBedrockMessages, llm.Request{OperationKey: "bedrock-credentials", Model: "claude-contract"})
	encoded := contract.MarshalJSON(t, call)
	if strings.Contains(string(encoded), "contract-secret") || strings.Contains(string(encoded), "contract-access") {
		t.Fatalf("credential value appeared in call serialization: %s", encoded)
	}
}

func TestInvokeRedirectResponseIsAmbiguousAndNotFollowed(t *testing.T) {
	const baseURL = "https://bedrock-runtime.us-east-1.amazonaws.com"
	var calls int
	client := newBedrockTestClient(t, baseURL, &http.Client{
		Transport:     adaptertest.RedirectTransport(&calls, `{"error":{"type":"api_error","message":"redirect"}}`),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil)
	adapter, call := compileBedrockProdCall(t, client, baseURL, "bedrock-redirect")
	_, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	mapped := adaptertest.AmbiguousRedirect(t, err)
	if calls != 1 {
		t.Fatalf("provider calls = %d, want one without redirect follow", calls)
	}
	if mapped.SafeDetails["status"] != "307" || mapped.SafeDetails["endpoint"] != "bedrock-prod" {
		t.Fatalf("safe details = %#v, want redirect status and endpoint only", mapped.SafeDetails)
	}
}

func TestInvokePreDispatchCallerDeadlineStaysRetryNever(t *testing.T) {
	const baseURL = "https://bedrock-runtime.us-east-1.amazonaws.com"
	transport := adaptertest.NewPreDialDeadlineTransport()
	client := newBedrockTestClient(t, baseURL, &http.Client{Transport: transport}, nil)
	adapter, call := compileBedrockProdCall(t, client, baseURL, "bedrock-pre-dial-deadline")
	adaptertest.CallerDeadlineBeforeDial(t, transport, func(ctx context.Context) error {
		_, err := adapter.Invoke(ctx, call, provider.NopObserver{})
		return err
	})
}

// newBedrockTestClient builds a us-east-1 client for baseURL over
// httpClient, signing with static contract credentials unless creds is set.
func newBedrockTestClient(t *testing.T, baseURL string, httpClient *http.Client, creds aws.CredentialsProvider) *Client {
	t.Helper()
	if creds == nil {
		creds = credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")
	}
	client, err := NewClient(context.Background(), ClientConfig{BaseURL: baseURL, HTTPClient: httpClient, AWSConfig: aws.Config{Region: "us-east-1", Credentials: creds}})
	testassert.NoError(t, err)
	return client
}

// compileBedrockProdCall builds the "bedrock-prod" adapter over client with
// a contract profile expecting baseURL, and strictly compiles an empty
// claude-contract request under operationKey.
func compileBedrockProdCall(t *testing.T, client *Client, baseURL, operationKey string) (*Adapter, provider.Call) {
	t.Helper()
	adapter, err := New(client, "bedrock-prod", mustBedrockProfile(t, baseURL))
	testassert.NoError(t, err)
	return adapter, adaptertest.CompileStrict(t, adapter, "bedrock-prod", provider.FamilyBedrockMessages, llm.Request{OperationKey: operationKey, Model: "claude-contract"})
}

func mustBedrockProfile(t *testing.T, baseURL string) Profile {
	t.Helper()
	profile := DefaultProfile("bedrock-contract")
	profile.CapabilityVersion = "bedrock-contract/v1"
	profile.Capabilities.Version = profile.CapabilityVersion
	profile.ExpectedBaseURL = baseURL
	profile.ExpectedModel = "claude-contract"
	validated, err := NewProfile(profile)
	testassert.NoError(t, err)
	return validated
}

func mustReadBedrockFixture(t *testing.T, name string) []byte {
	t.Helper()
	return contract.ReadFixture(t, "testdata/contracts/bedrock-anthropic/"+name)
}
