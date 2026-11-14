package anthropicmessages

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	yaml "go.yaml.in/yaml/v4"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
)

// anthropicContractFixture is one Anthropic Messages contract-fixture
// profile. The direct API and Claude Platform on AWS share every fixture
// check here; only these values and their client and profile differ.
type anthropicContractFixture struct {
	// id is the profile ID and the directory under testdata/contracts.
	id string
	// endpoint is the endpoint ID calls and adapters use.
	endpoint string
	// requestID is the provider request ID lifted responses carry.
	requestID string
	// usageOperationKey and usageRequestID identify the usage-cost lift.
	usageOperationKey string
	usageRequestID    string
	// otherEndpoint is a different endpoint a continuation must not cross to.
	otherEndpoint string
	// secrets are credential fragments a redacted error must not contain.
	secrets   []string
	profile   func(t *testing.T) Profile
	newClient func(t *testing.T, transport http.RoundTripper) *Client
}

var anthropicDirectFixture = anthropicContractFixture{
	id:                "anthropic-direct",
	endpoint:          anthropicDirectFixtureEndpoint,
	requestID:         "req-fixture",
	usageOperationKey: "fixture-usage-cost",
	usageRequestID:    "req-anthropic-usage",
	otherEndpoint:     "other-anthropic-endpoint",
	secrets:           []string{"fixture-anthropic-key", "Bearer sk-", "api-key-real"},
	profile:           anthropicDirectFixtureProfile,
	newClient:         newAnthropicDirectFixtureClient,
}

var anthropicAWSFixture = anthropicContractFixture{
	id:                "anthropic-aws",
	endpoint:          anthropicAWSFixtureEndpoint,
	requestID:         "req-anthropic-aws-fixture",
	usageOperationKey: "fixture-anthropic-aws-usage",
	usageRequestID:    "req-anthropic-aws-usage",
	otherEndpoint:     "anthropic-prod",
	secrets:           []string{"aws-secret", "AKIA", "Bearer ", "x-amz-security-token-real"},
	profile:           anthropicAWSFixtureProfile,
	newClient:         newAnthropicAWSFixtureClient,
}

func (f anthropicContractFixture) testProfileIsEnforced(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}

	wantPath := "llm/provider/anthropicmessages/testdata/contracts/" + f.id
	for _, profile := range report.Enforced {
		if profile.ID == f.id {
			if profile.Path != wantPath {
				t.Fatalf("%s fixture path = %q, want %q", f.id, profile.Path, wantPath)
			}
			return
		}
	}
	t.Fatalf("enforced report is missing %s: %#v", f.id, report)
}

func (f anthropicContractFixture) testLoweringAndLifting(t *testing.T) {
	profile := f.profile(t)
	request := f.loadRequest(t, "request.semantic.json")
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	tier, err := profile.providerTier(serviceClass)
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(normalized, profile, tier)
	if err != nil {
		t.Fatal(err)
	}
	gotWire, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	f.assertCanonical(t, gotWire, "request.wire.json")

	response := f.loadResponse(t, "response.completed.json")
	lifted, err := profile.liftResponse(f.call(normalized, serviceClass), &response, f.requestID)
	if err != nil {
		t.Fatal(err)
	}
	gotSemantic, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	f.assertCanonical(t, gotSemantic, "response.semantic.json")
}

func (f anthropicContractFixture) testSemanticRoundTrip(t *testing.T) {
	profile := f.profile(t)
	requestSemantic := f.read(t, "request.semantic.json")
	responseSemantic := f.read(t, "response.semantic.json")
	semantic, err := json.Marshal(struct {
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	}{Request: requestSemantic, Response: responseSemantic})
	if err != nil {
		t.Fatal(err)
	}
	metadata := f.loadMetadata(t)
	responseWire := f.read(t, "response.completed.json")
	if err := contracttest.VerifySemanticRoundTrip(semantic, func(semantic []byte) ([]byte, error) {
		var fixture struct {
			Request  json.RawMessage `json:"request"`
			Response json.RawMessage `json:"response"`
		}
		if err := json.Unmarshal(semantic, &fixture); err != nil {
			return nil, err
		}
		var request llm.Request
		if err := json.Unmarshal(fixture.Request, &request); err != nil {
			return nil, err
		}
		normalized, err := llm.NormalizeRequest(request)
		if err != nil {
			return nil, err
		}
		serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
		if err != nil {
			return nil, err
		}
		tier, err := profile.providerTier(serviceClass)
		if err != nil {
			return nil, err
		}
		if _, err := lowerRequest(normalized, profile, tier); err != nil {
			return nil, err
		}
		var response anthropic.Message
		if err := json.Unmarshal(responseWire, &response); err != nil {
			return nil, err
		}
		lifted, err := profile.liftResponse(f.call(normalized, serviceClass), &response, f.requestID)
		if err != nil {
			return nil, err
		}
		fixture.Response, err = json.Marshal(lifted)
		if err != nil {
			return nil, err
		}
		return json.Marshal(fixture)
	}, metadata.GeneratedFieldExemptions); err != nil {
		t.Fatal(err)
	}
}

func (f anthropicContractFixture) read(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", "contracts", f.id, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f anthropicContractFixture) loadRequest(t *testing.T, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(f.read(t, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func (f anthropicContractFixture) loadResponse(t *testing.T, name string) anthropic.Message {
	t.Helper()
	var response anthropic.Message
	if err := json.Unmarshal(f.read(t, name), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func (f anthropicContractFixture) loadMetadata(t *testing.T) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(f.read(t, "metadata.yaml"), &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func (f anthropicContractFixture) assertCanonical(t *testing.T, got []byte, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(f.read(t, name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("%s/%s mismatch\n got: %s\nwant: %s", f.id, name, gotCanonical, wantCanonical)
	}
}

func (f anthropicContractFixture) call(request llm.Request, serviceClass llm.ServiceClass) provider.Call {
	return provider.Call{
		EndpointID:   f.endpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        request.Model,
		OperationKey: request.OperationKey,
		ServiceClass: serviceClass,
	}
}

func (f anthropicContractFixture) assertClassFacts(t *testing.T, profile Profile) {
	t.Helper()
	var semantic []struct {
		Class  llm.ServiceClass `json:"class"`
		Actual llm.ServiceClass `json:"actual"`
	}
	if err := json.Unmarshal(f.read(t, "class-facts.semantic.json"), &semantic); err != nil {
		t.Fatal(err)
	}
	actualByClass := make(map[llm.ServiceClass]llm.ServiceClass, len(semantic))
	for _, fact := range semantic {
		actualByClass[fact.Class] = fact.Actual
	}

	var wire []struct {
		Class            llm.ServiceClass `json:"class"`
		Supported        bool             `json:"supported"`
		RequestedTier    string           `json:"requested_tier"`
		ActualTier       string           `json:"actual_tier"`
		PriorityCapacity *bool            `json:"priority_capacity"`
	}
	if err := json.Unmarshal(f.read(t, "class-facts.wire.json"), &wire); err != nil {
		t.Fatal(err)
	}
	for _, fact := range wire {
		fixtureProfile := profile
		if fact.PriorityCapacity != nil {
			fixtureProfile.PriorityCapacity = *fact.PriorityCapacity
		}
		tier, err := fixtureProfile.providerTier(fact.Class)
		if !fact.Supported {
			if err == nil {
				t.Fatalf("%s unexpectedly has provider tier %q", fact.Class, tier)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", fact.Class, err)
		}
		if tier != fact.RequestedTier {
			t.Fatalf("%s requested tier = %q, want %q", fact.Class, tier, fact.RequestedTier)
		}
		actual := fixtureProfile.actualClass(fact.ActualTier)
		if actual == nil || *actual != actualByClass[fact.Class] {
			t.Fatalf("%s actual class = %#v, want %q", fact.Class, actual, actualByClass[fact.Class])
		}
	}
}

func (f anthropicContractFixture) assertCompileError(t *testing.T, profile Profile, request llm.Request, expectedName string) {
	t.Helper()
	adapter, err := New(f.newClient(t, nil), f.endpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: f.endpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: request.Portability == llm.PortabilityStrict,
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) {
		t.Fatalf("%s compile error = %T %v, want provider error", request.OperationKey, err, err)
	}
	var expected struct {
		Code            provider.Code              `json:"code"`
		Phase           provider.Phase             `json:"phase"`
		Dispatch        provider.DispatchCertainty `json:"dispatch"`
		MessageContains string                     `json:"message_contains"`
	}
	if err := json.Unmarshal(f.read(t, expectedName), &expected); err != nil {
		t.Fatal(err)
	}
	if mapped.Code != expected.Code || mapped.Phase != expected.Phase || mapped.Dispatch != expected.Dispatch {
		t.Fatalf("%s compile error = %#v, want %#v", request.OperationKey, mapped, expected)
	}
	if expected.MessageContains != "" && !strings.Contains(mapped.SafeMessage, expected.MessageContains) {
		t.Fatalf("%s safe message = %q, want substring %q", request.OperationKey, mapped.SafeMessage, expected.MessageContains)
	}
}

func (f anthropicContractFixture) assertContinuation(t *testing.T, profile Profile) {
	t.Helper()
	request := f.loadRequest(t, "continuation-compatibility.semantic.json")
	adapter, err := New(f.newClient(t, nil), f.endpoint, profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query: provider.CapabilityQuery{
			EndpointID: f.endpoint,
			Family:     provider.FamilyAnthropicMessages,
			Model:      request.Model,
		},
		Strict: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	f.assertCanonical(t, wire, "continuation-compatibility.wire.json")

	incompatible := request
	continuation := *request.Continuation
	continuation.EndpointID = f.otherEndpoint
	incompatible.Continuation = &continuation
	f.assertCompileError(t, profile, incompatible, "continuation-incompatible.wire.json")
}

func (f anthropicContractFixture) assertRedaction(t *testing.T) {
	t.Helper()
	redaction := f.read(t, "security-redaction.wire.json")
	if !bytes.Contains(redaction, []byte("[REDACTED]")) {
		t.Fatal("redaction fixture has no explicit marker")
	}
	for _, unsafe := range f.secrets {
		if bytes.Contains(redaction, []byte(unsafe)) {
			t.Fatalf("redaction fixture contains %q", unsafe)
		}
	}
}

func (f anthropicContractFixture) assertUsageClassesLossAndContinuation(t *testing.T, profile Profile) {
	t.Helper()
	f.assertClassFacts(t, profile)

	response := f.loadResponse(t, "usage-cost.response.json")
	lifted, err := profile.liftResponse(provider.Call{
		EndpointID:   f.endpoint,
		Family:       provider.FamilyAnthropicMessages,
		Model:        "claude-contract",
		OperationKey: f.usageOperationKey,
		ServiceClass: llm.ServiceClassStandard,
	}, &response, f.usageRequestID)
	if err != nil {
		t.Fatal(err)
	}
	gotUsage, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	f.assertCanonical(t, gotUsage, "usage-cost.semantic.json")

	f.assertCompileError(t, profile, f.loadRequest(t, "strict-loss.semantic.json"), "strict-loss.wire.json")
	f.assertCompileError(t, profile, f.loadRequest(t, "best-effort-diagnostic.semantic.json"), "best-effort-diagnostic.wire.json")
	f.assertContinuation(t, profile)
}
