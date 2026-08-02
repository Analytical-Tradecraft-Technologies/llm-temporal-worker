package bedrockconverse

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/contracttest"
)

const (
	bedrockFixtureEndpoint  = "bedrock-fixture"
	bedrockFixtureRequestID = "bedrock-request-fixture"
	// Amazon Nova Lite is Standard-only. Nova Pro is the documented Converse
	// model used here because it supports Flex, Standard, and Priority.
	bedrockFixtureModel = "amazon.nova-pro-v1:0"
)

func TestBedrockConverseContractProfileIsEnforced(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := contracttest.ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range report.Enforced {
		if profile.ID == "bedrock-converse" {
			return
		}
	}
	t.Fatalf("bedrock-converse must be enforced; bootstrap profiles: %#v", report.Bootstrap)
}

func TestBedrockConverseContractFixturesMatchLoweringAndLifting(t *testing.T) {
	request := loadBedrockConverseRequestFixture(t, "request.semantic.json")
	if request.Model != bedrockFixtureModel {
		t.Fatalf("fixture model = %q, want tier-capable %q", request.Model, bedrockFixtureModel)
	}
	profile := mustBedrockConverseProfile(t)
	adapter := &Adapter{endpointID: bedrockFixtureEndpoint, profile: profile}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockConverse, Model: request.Model},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(call.SDKParams)
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockConverseCanonicalFixture(t, wire, "request.wire.json")

	response := bedrockConverseResponse("Hello from the fixture response.", 7, 6)
	assertBedrockConverseCanonicalFixture(t, mustJSON(t, response), "response.completed.json")
	lifted, err := adapter.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := json.Marshal(lifted)
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockConverseCanonicalFixture(t, semantic, "response.semantic.json")
}

func TestBedrockConverseContractFixturesCoverUsageAndServiceClass(t *testing.T) {
	profile := mustBedrockConverseProfile(t)
	adapter := &Adapter{endpointID: bedrockFixtureEndpoint, profile: profile}
	for _, fixture := range []struct {
		name, wire, semantic, operation string
		requested, actual               llm.ServiceClass
		providerTier                    types.ServiceTierType
	}{
		{name: "economy-flex", wire: "class-facts-economy.wire.json", semantic: "class-facts-economy.semantic.json", operation: "fixture-class-facts-economy", requested: llm.ServiceClassEconomy, actual: llm.ServiceClassEconomy, providerTier: types.ServiceTierTypeFlex},
		{name: "standard-default", wire: "class-facts-standard.wire.json", semantic: "class-facts-standard.semantic.json", operation: "fixture-class-facts-standard", requested: llm.ServiceClassStandard, actual: llm.ServiceClassStandard, providerTier: types.ServiceTierTypeDefault},
		{name: "priority", wire: "class-facts.wire.json", semantic: "class-facts.semantic.json", operation: "fixture-class-facts", requested: llm.ServiceClassPriority, actual: llm.ServiceClassPriority, providerTier: types.ServiceTierTypePriority},
		{name: "usage", wire: "usage-cost.response.json", semantic: "usage-cost.semantic.json", operation: "fixture-usage-cost", requested: llm.ServiceClassStandard, actual: llm.ServiceClassPriority, providerTier: types.ServiceTierTypePriority},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			text := "Usage fixture response."
			inputTokens, outputTokens := int32(9), int32(4)
			if fixture.name != "usage" {
				text, inputTokens, outputTokens = "Class fixture response.", 1, 1
			}
			response := bedrockConverseResponseForTier(text, inputTokens, outputTokens, fixture.providerTier)
			assertBedrockConverseCanonicalFixture(t, mustJSON(t, response), fixture.wire)
			request := llm.Request{OperationKey: fixture.operation, Model: bedrockFixtureModel, ServiceClass: fixture.requested, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "fixture"}}}}}
			call := provider.Call{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockConverse, Model: request.Model, OperationKey: fixture.operation, ServiceClass: fixture.requested}
			lifted, err := adapter.liftResponse(call, &response, bedrockFixtureRequestID)
			if err != nil {
				t.Fatal(err)
			}
			semantic, err := json.Marshal(lifted)
			if err != nil {
				t.Fatal(err)
			}
			assertBedrockConverseCanonicalFixture(t, semantic, fixture.semantic)
			if lifted.Usage.InputTokens <= 0 || lifted.Usage.OutputTokens <= 0 {
				t.Fatalf("usage was not lifted: %#v", lifted.Usage)
			}
			if lifted.Service.Actual == nil || *lifted.Service.Actual != fixture.actual {
				t.Fatalf("actual service class = %#v, want %q", lifted.Service.Actual, fixture.actual)
			}
		})
	}
}

func TestBedrockConverseContractFixturesDeclareRedactionAndClassifiedError(t *testing.T) {
	redacted := readBedrockConverseFixture(t, "security-redaction.wire.json")
	if string(redacted) == "" || !containsBytes(redacted, []byte("[REDACTED]")) {
		t.Fatal("security redaction fixture has no explicit marker")
	}
	for _, unsafe := range []string{"AKIA", "secret", "Bearer live-"} {
		if containsBytes(redacted, []byte(unsafe)) {
			t.Fatalf("security fixture contains %q", unsafe)
		}
	}
	err := mapHTTPError(http.StatusBadRequest, context.Canceled, "bedrock.converse/bedrock-converse")
	if err.Code != provider.CodeInvalidArgument || err.Dispatch != provider.DispatchRejected || err.Retry != provider.RetryNever {
		t.Fatalf("classified error = %#v", err)
	}
	if !containsBytes(readBedrockConverseFixture(t, "classified-error.wire.json"), []byte("provider rejected")) {
		t.Fatal("classified error fixture does not describe the mapped provider rejection")
	}
}

func TestBedrockConverseToolUseContractFixtures(t *testing.T) {
	request := loadBedrockConverseRequestFixture(t, "request.tool-use.semantic.json")
	profile := mustBedrockConverseProfile(t)
	adapter := &Adapter{endpointID: bedrockFixtureEndpoint, profile: profile}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: request,
		Query:   provider.CapabilityQuery{EndpointID: bedrockFixtureEndpoint, Family: provider.FamilyBedrockConverse, Model: request.Model},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockConverseCanonicalFixture(t, mustJSON(t, call.SDKParams), "request.tool-use.wire.json")
	params, ok := call.SDKParams.(bedrockruntime.ConverseInput)
	if !ok || params.ToolConfig == nil || len(params.ToolConfig.Tools) != 1 {
		t.Fatalf("tool request configuration = %#v", params.ToolConfig)
	}
	spec, ok := params.ToolConfig.Tools[0].(*types.ToolMemberToolSpec)
	var schemaBlock *types.ToolInputSchemaMemberJson
	var schemaOK bool
	if ok && spec.Value.InputSchema != nil {
		schemaBlock, schemaOK = spec.Value.InputSchema.(*types.ToolInputSchemaMemberJson)
	}
	if !ok || !schemaOK || schemaBlock.Value == nil {
		t.Fatalf("tool specification = %#v", params.ToolConfig.Tools[0])
	}
	schema, err := schemaBlock.Value.MarshalSmithyDocument()
	if err != nil || string(schema) != `{"properties":{"city":{"type":"string"}},"required":["city"],"type":"object"}` {
		t.Fatalf("tool input schema = %s, err=%v", schema, err)
	}

	response := bedrockConverseToolResponse()
	assertBedrockConverseCanonicalFixture(t, mustJSON(t, response), "response.tool-use.completed.json")
	lifted, err := adapter.liftResponse(call, &response, bedrockFixtureRequestID)
	if err != nil {
		t.Fatal(err)
	}
	assertBedrockConverseCanonicalFixture(t, mustJSON(t, lifted), "response.tool-use.semantic.json")
}

func mustBedrockConverseProfile(t *testing.T) Profile {
	t.Helper()
	profile, err := NewDefaultProfile("bedrock-converse")
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func bedrockConverseFixtureRoot() string {
	return filepath.Join("testdata", "contracts", "bedrock-converse")
}

func readBedrockConverseFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(bedrockConverseFixtureRoot(), name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return data
}

func loadBedrockConverseRequestFixture(t *testing.T, name string) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(readBedrockConverseFixture(t, name), &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func assertBedrockConverseCanonicalFixture(t *testing.T, got []byte, name string) {
	t.Helper()
	want := readBedrockConverseFixture(t, name)
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCanonical) != string(wantCanonical) {
		t.Fatalf("fixture %s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
}

func bedrockConverseResponse(text string, inputTokens, outputTokens int32) bedrockruntime.ConverseOutput {
	return bedrockConverseResponseForTier(text, inputTokens, outputTokens, types.ServiceTierTypePriority)
}

func bedrockConverseResponseForTier(text string, inputTokens, outputTokens int32, tier types.ServiceTierType) bedrockruntime.ConverseOutput {
	return bedrockruntime.ConverseOutput{
		Output:      &types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: text}}}},
		StopReason:  types.StopReasonEndTurn,
		Usage:       &types.TokenUsage{InputTokens: aws.Int32(inputTokens), OutputTokens: aws.Int32(outputTokens)},
		ServiceTier: &types.ServiceTier{Type: tier},
	}
}

func bedrockConverseToolResponse() bedrockruntime.ConverseOutput {
	return bedrockruntime.ConverseOutput{
		Output: &types.ConverseOutputMemberMessage{Value: types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{
			&types.ContentBlockMemberText{Value: "I will look that up."},
			&types.ContentBlockMemberToolUse{Value: types.ToolUseBlock{ToolUseId: aws.String("tool-call-1"), Name: aws.String("lookup"), Input: document.NewLazyDocument(map[string]any{"city": "Sydney"})}},
		}}},
		StopReason:  types.StopReasonToolUse,
		Usage:       &types.TokenUsage{InputTokens: aws.Int32(5), OutputTokens: aws.Int32(7)},
		ServiceTier: &types.ServiceTier{Type: types.ServiceTierTypePriority},
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func containsBytes(value, needle []byte) bool {
	for index := 0; index+len(needle) <= len(value); index++ {
		match := true
		for offset := range needle {
			if value[index+offset] != needle[offset] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
