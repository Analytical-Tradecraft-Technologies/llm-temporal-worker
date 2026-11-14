package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

const anthropicDirectFixtureEndpoint = "anthropic-prod"

func TestAnthropicDirectContractProfileIsEnforced(t *testing.T) {
	anthropicDirectFixture.testProfileIsEnforced(t)
}

func TestAnthropicDirectContractFixturesMatchCurrentLoweringAndLifting(t *testing.T) {
	anthropicDirectFixture.testLoweringAndLifting(t)
}

func TestAnthropicDirectContractFixturesVerifySemanticRoundTrip(t *testing.T) {
	anthropicDirectFixture.testSemanticRoundTrip(t)
}

func TestAnthropicDirectContractFixturesCoverUsageClassesLossErrorsAndContinuation(t *testing.T) {
	profile := anthropicDirectFixtureProfile(t)
	anthropicDirectFixture.assertUsageClassesLossAndContinuation(t, profile)
	assertAnthropicDirectFixtureClassifiedError(t)
	anthropicDirectFixture.assertRedaction(t)
}

func TestAnthropicDirectContractFixtureDecoderIsFragmentationInvariant(t *testing.T) {
	wire := anthropicDirectFixture.read(t, "stream.decoder.events")
	metadata := anthropicDirectFixture.loadMetadata(t)
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 {
		t.Fatal("stream fixture decoded no events")
	}
	for split := 1; split < len(wire); split++ {
		got, err := DecodeStream(streamtest.NewChunkReader([][]byte{wire[:split], {}, wire[split:]}))
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split %d changed decoded events\n got: %#v\nwant: %#v", split, got, want)
		}
	}
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 29, 11)} {
		got, err := DecodeStream(streamtest.NewChunkReader(chunks))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	assembler := provider.NewAssembler("fixture-anthropic-direct-stream")
	for _, event := range want {
		if err := assembler.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := assembler.Result(); err != nil {
		t.Fatal(err)
	}
	if err := contracttest.VerifyStreamAssemblyEquivalent(wire, anthropicDirectFixture.read(t, "stream.decoder.semantic.json"), func(events []byte) ([]byte, error) {
		return assembleAnthropicFixtureStream(events, "fixture-anthropic-direct-stream")
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func assembleAnthropicFixtureStream(wire []byte, operationKey string) ([]byte, error) {
	events, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	assembler := provider.NewAssembler(operationKey)
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			return nil, err
		}
	}
	response, err := assembler.Result()
	if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func TestAnthropicDirectContractFixturesUseIndependentProfileTransport(t *testing.T) {
	request := anthropicDirectFixture.loadRequest(t, "request.semantic.json")
	responseBody := anthropicDirectFixture.read(t, "response.completed.json")
	var got *http.Request
	client := newAnthropicDirectFixtureClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Request-Id": []string{"req-fixture"}},
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
			Request:    request,
		}, nil
	}))
	adapter, err := New(client, anthropicDirectFixtureEndpoint, anthropicDirectFixtureProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: anthropicDirectFixtureEndpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Invoke(context.Background(), call, provider.NopObserver{})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("fixture transport was not called")
	}
	body, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	anthropicDirectFixture.assertCanonical(t, body, "request.wire.json")
	if got.URL.Path != "/v1/messages" || got.Header.Get("X-Api-Key") != "fixture-anthropic-key" || got.Header.Get("Authorization") != "" {
		t.Fatalf("Anthropic direct transport = %s %#v", got.URL, got.Header)
	}
	gotSemantic, err := json.Marshal(result.Response)
	if err != nil {
		t.Fatal(err)
	}
	anthropicDirectFixture.assertCanonical(t, gotSemantic, "response.semantic.json")
}

func newAnthropicDirectFixtureClient(t *testing.T, transport http.RoundTripper) *Client {
	t.Helper()
	if transport == nil {
		transport = http.DefaultTransport
	}
	client, err := NewClient(ClientConfig{
		BaseURL:    "https://api.anthropic.com",
		APIKey:     "fixture-anthropic-key",
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func anthropicDirectFixtureProfile(t *testing.T) Profile {
	t.Helper()
	profile := testProfile()
	// The official SDK owns the /v1/messages path. Profiles pin the endpoint
	// origin from the production configuration rather than duplicating that path.
	profile.ExpectedBaseURL = "https://api.anthropic.com"
	return mustProfile(t, profile)
}

func assertAnthropicDirectFixtureClassifiedError(t *testing.T) {
	t.Helper()
	var fixture struct {
		Status     int    `json:"status"`
		RequestID  string `json:"request_id"`
		RetryAfter string `json:"retry_after"`
		Message    string `json:"message"`
	}
	if err := json.Unmarshal(anthropicDirectFixture.read(t, "classified-error.wire.json"), &fixture); err != nil {
		t.Fatal(err)
	}
	mapped := mapAPIError(&anthropic.Error{
		StatusCode: fixture.Status,
		RequestID:  fixture.RequestID,
		Response:   &http.Response{Header: http.Header{"Retry-After": []string{fixture.RetryAfter}}},
	}, "anthropic.messages/anthropic-contract")
	if mapped.Code != provider.CodeProviderRateLimited || mapped.Dispatch != provider.DispatchRejected || mapped.Retry != provider.RetryAfter || mapped.Provider.RequestID != fixture.RequestID || mapped.SafeDetails["retry_after"] != fixture.RetryAfter {
		t.Fatalf("mapped error = %#v", mapped)
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), fixture.Message) {
		t.Fatal("classified error leaked fixture body")
	}
}
