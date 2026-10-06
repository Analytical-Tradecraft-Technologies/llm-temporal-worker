package openairesponses

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3/shared"
)

func TestLiftCompletedResponsePreservesItemsUsageAndContinuation(t *testing.T) {
	response := loadResponseFixture(t, "response.completed.json")
	call := provider.Call{EndpointID: "openai-prod", Family: provider.FamilyOpenAIResponses, Model: "gpt-contract", OperationKey: "op-lift", ServiceClass: llm.ServiceClassEconomy}
	lifted, err := liftResponse(call, &response, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if lifted.Status != llm.ResponseStatusToolCalls {
		t.Fatalf("status = %s, want tool_calls", lifted.Status)
	}
	if lifted.Service.Actual == nil || *lifted.Service.Actual != llm.ServiceClassEconomy || lifted.Service.ProviderValue != "flex" {
		t.Fatalf("service facts = %#v", lifted.Service)
	}
	if lifted.Usage.InputTokens != 6 || lifted.Usage.OutputTokens != 7 || lifted.Usage.ReasoningTokens != 2 || lifted.Usage.CacheReadTokens != 3 || lifted.Usage.CacheWriteTokens != 1 {
		t.Fatalf("usage = %#v", lifted.Usage)
	}
	if lifted.Provider.ResponseID != "resp-1" || lifted.Provider.RequestID != "req-1" {
		t.Fatalf("provider facts = %#v", lifted.Provider)
	}
	if lifted.Continuation == nil || lifted.Continuation.Handle != "openai-responses:resp-1" {
		t.Fatalf("continuation = %#v", lifted.Continuation)
	}
	if len(lifted.Output) != 3 {
		t.Fatalf("output length = %d", len(lifted.Output))
	}
	message, ok := lifted.Output[0].(llm.Message)
	if !ok || len(message.Content) != 1 || message.Content[0].(llm.TextPart).Text != "hello" {
		t.Fatalf("message = %#v", lifted.Output[0])
	}
	toolCall, ok := lifted.Output[1].(llm.ToolCall)
	if !ok || toolCall.ID != "call-1" || !json.Valid(toolCall.Arguments) {
		t.Fatalf("tool call = %#v", lifted.Output[1])
	}
	if _, ok := lifted.Output[2].(llm.ProviderState); !ok {
		t.Fatalf("reasoning output = %#v", lifted.Output[2])
	}
	if _, ok := lifted.Provider.Raw["output_item_ids"]; !ok {
		t.Fatalf("output IDs were not retained: %#v", lifted.Provider.Raw)
	}
}

func TestLiftRejectsNilResponseWithoutPanicking(t *testing.T) {
	call := provider.Call{
		EndpointID:   "openai-prod",
		Family:       provider.FamilyOpenAIResponses,
		Model:        "gpt-contract",
		OperationKey: "op-empty",
	}

	var panicked bool
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		_, err := liftResponse(call, nil, "req-empty")
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted {
			t.Fatalf("nil response error = %#v, want accepted provider invalid response", err)
		}
	}()
	if panicked {
		t.Fatal("liftResponse panicked for nil response")
	}
}

func TestLiftMapsActualTiersAndKeepsUnreportedTierUnclassified(t *testing.T) {
	for _, test := range []struct {
		tier responses.ResponseServiceTier
		want llm.ServiceClass
	}{
		{responses.ResponseServiceTierFlex, llm.ServiceClassEconomy},
		{responses.ResponseServiceTierDefault, llm.ServiceClassStandard},
		{responses.ResponseServiceTierPriority, llm.ServiceClassPriority},
	} {
		response := minimalResponse(test.tier, responses.ResponseStatusCompleted)
		call := provider.Call{EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt", OperationKey: "op", ServiceClass: llm.ServiceClassPriority}
		got, err := liftResponse(call, &response, "req")
		if err != nil {
			t.Fatalf("tier %s: %v", test.tier, err)
		}
		if got.Service.Actual == nil || *got.Service.Actual != test.want {
			t.Errorf("tier %s -> %#v, want %s", test.tier, got.Service.Actual, test.want)
		}
	}
	// A paid response is never discarded over its tier label: a missing or
	// unrecognized tier reports no actual class and keeps the raw value.
	for _, tier := range []responses.ResponseServiceTier{"", responses.ResponseServiceTierScale} {
		response := minimalResponse(tier, responses.ResponseStatusCompleted)
		got, err := liftResponse(provider.Call{EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt", OperationKey: "op", ServiceClass: llm.ServiceClassPriority}, &response, "req")
		if err != nil {
			t.Fatalf("tier %q: %v", tier, err)
		}
		if got.Service.Actual != nil || got.Service.Attempted != llm.ServiceClassPriority || got.Service.ProviderValue != string(tier) {
			t.Fatalf("tier %q service facts = %+v", tier, got.Service)
		}
	}
}

func TestLiftMapsIncompleteAndRefusal(t *testing.T) {
	response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusIncomplete)
	response.IncompleteDetails.Reason = "content_filter"
	got, err := liftResponse(provider.Call{EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}, &response, "req")
	if err != nil || got.Status != llm.ResponseStatusContentFiltered {
		t.Fatalf("content filter = %#v, %v", got, err)
	}
	response = minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
	response.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"no"}]}]`)
	got, err = liftResponse(provider.Call{EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt", OperationKey: "op", ServiceClass: llm.ServiceClassStandard}, &response, "req")
	if err != nil || got.Status != llm.ResponseStatusRefused {
		t.Fatalf("refusal = %#v, %v", got, err)
	}
}

func TestLiftLocallyValidatesRequestedJSONSchema(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)
	params, err := lowerRequest(llm.Request{
		Model: "gpt",
		Output: &llm.OutputSpec{Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema,
		}},
	}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{
		EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt",
		OperationKey: "op-json", ServiceClass: llm.ServiceClassStandard, SDKParams: params,
	}
	valid := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
	valid.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}","annotations":[]}]}]`)
	if _, err := liftResponse(call, &valid, "req"); err != nil {
		t.Fatalf("valid JSON response = %v", err)
	}
	invalid := valid
	invalid.ID = "invalid"
	invalid.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"output_text","text":"{\"answer\":3}","annotations":[]}]}]`)
	_, err = liftResponse(call, &invalid, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
		t.Fatalf("invalid JSON response error = %#v", err)
	}
}

func TestLiftLocallyValidatesRequestedJSONObject(t *testing.T) {
	params, err := lowerRequest(llm.Request{
		Model:  "gpt",
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSON}},
	}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{
		EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt",
		OperationKey: "op-json-object", ServiceClass: llm.ServiceClassStandard, SDKParams: params,
	}
	response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
	response.Output = decodeOutputItems(t, `[{
		"type":"message","id":"msg","role":"assistant","status":"completed",
		"content":[{"type":"output_text","text":"{\"answer\":\"ok\"}","annotations":[]}]
	}]`)
	if _, err := liftResponse(call, &response, "req"); err != nil {
		t.Fatalf("valid JSON object response = %v", err)
	}

	response.ID = "array"
	response.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"completed","content":[{"type":"output_text","text":"[1,2,3]","annotations":[]}]}]`)
	_, err = liftResponse(call, &response, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
		t.Fatalf("non-object JSON response error = %#v, want accepted provider invalid response", err)
	}
}

func loadResponseFixture(t *testing.T, name string) responses.Response {
	t.Helper()
	data, err := os.ReadFile("testdata/contracts/openai-responses/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var response responses.Response
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func minimalResponse(tier responses.ResponseServiceTier, status responses.ResponseStatus) responses.Response {
	return responses.Response{ID: "resp", Model: shared.ResponsesModel("gpt"), ServiceTier: tier, Status: status}
}

func decodeOutputItems(t *testing.T, raw string) []responses.ResponseOutputItemUnion {
	t.Helper()
	var items []responses.ResponseOutputItemUnion
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		t.Fatal(err)
	}
	return items
}

func TestLiftPreservesIncompleteJSONAndUsage(t *testing.T) {
	for _, kind := range []llm.OutputKind{llm.OutputKindJSON, llm.OutputKindJSONSchema} {
		format := llm.OutputFormat{Kind: kind}
		if kind == llm.OutputKindJSONSchema {
			format.Name = "answer"
			format.Strict = true
			format.Schema = json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)
		}

		params, err := lowerRequest(llm.Request{Model: "gpt", Output: &llm.OutputSpec{Format: format}}, llm.ServiceClassStandard)
		if err != nil {
			t.Fatal(err)
		}
		call := provider.Call{EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt", OperationKey: "partial", ServiceClass: llm.ServiceClassStandard, SDKParams: params}
		for _, reason := range []string{"max_output_tokens", "content_filter", "completed"} {
			response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusIncomplete)
			response.IncompleteDetails.Reason = reason
			if reason == "completed" {
				response.Status = responses.ResponseStatusCompleted
			}
			response.Usage = responses.ResponseUsage{InputTokens: 12, OutputTokens: 4, TotalTokens: 16}
			response.Output = decodeOutputItems(t, `[{"type":"message","id":"msg","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":"{\"answer\":","annotations":[]}]}]`)
			result, err := liftResponse(call, &response, "request")
			if reason == "completed" {
				if err == nil {
					t.Fatalf("completed %s must still validate JSON", kind)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s %s: %v", kind, reason, err)
			}
			want := llm.ResponseStatusLength
			if reason == "content_filter" {
				want = llm.ResponseStatusContentFiltered
			}
			text, ok := finalModelText(result.Output)
			if result.Status != want || !ok || text != `{"answer":` || result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 4 {
				t.Fatalf("lost incomplete response: %+v", result)
			}
		}
	}
}

func TestLiftValidatesFinalAnswerAfterCommentary(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)
	params, err := lowerRequest(llm.Request{
		Model: "gpt",
		Output: &llm.OutputSpec{Format: llm.OutputFormat{
			Kind: llm.OutputKindJSONSchema, Name: "answer", Strict: true, Schema: schema,
		}},
	}, llm.ServiceClassStandard)
	if err != nil {
		t.Fatal(err)
	}
	call := provider.Call{
		EndpointID: "endpoint", Family: provider.FamilyOpenAIResponses, Model: "gpt",
		OperationKey: "op-phase", ServiceClass: llm.ServiceClassStandard, SDKParams: params,
	}
	commentary := `{"type":"message","id":"msg-1","role":"assistant","status":"completed","phase":"commentary","content":[{"type":"output_text","text":"Let me work on that.","annotations":[]}]}`
	response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
	response.Output = decodeOutputItems(t, `[`+commentary+`,{"type":"message","id":"msg-2","role":"assistant","status":"completed","phase":"final_answer","content":[{"type":"output_text","text":"{\"answer\":\"ok\"}","annotations":[]}]}]`)
	lifted, err := liftResponse(call, &response, "req")
	if err != nil {
		t.Fatalf("commentary before the final answer = %v", err)
	}
	if lifted.Status != llm.ResponseStatusCompleted || len(lifted.Output) != 2 {
		t.Fatalf("lifted = %#v", lifted)
	}
	// The final answer is still held to the schema.
	response.Output = decodeOutputItems(t, `[`+commentary+`,{"type":"message","id":"msg-2","role":"assistant","status":"completed","phase":"final_answer","content":[{"type":"output_text","text":"{\"answer\":3}","annotations":[]}]}]`)
	_, err = liftResponse(call, &response, "req")
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Dispatch != provider.DispatchAccepted {
		t.Fatalf("invalid final answer error = %#v", err)
	}
}
