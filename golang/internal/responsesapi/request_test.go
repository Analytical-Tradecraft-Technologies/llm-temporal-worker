package responsesapi

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func mapBody(t *testing.T, body string) (mappedRequest, *apiError) {
	t.Helper()
	request, apiErr := decodeCreateRequest([]byte(body))
	if apiErr != nil {
		return mappedRequest{}, apiErr
	}
	return mapCreateRequest(request, testIdentity, "resp_test")
}

func TestParseAPIKey(t *testing.T) {
	for key, want := range map[string]*llm.RequestContext{
		"acme/search/batch-job":  {Tenant: "acme", Project: "search", Actor: "batch-job"},
		"acme/search/team/alice": {Tenant: "acme", Project: "search", Actor: "team/alice"},
		"acme/search":            nil,
		"acme//batch-job":        nil,
		"acme/search/":           nil,
		"acme/search/bad\nactor": nil,
		"":                       nil,
	} {
		got, ok := parseAPIKey(key)
		if (want == nil) == ok || (want != nil && !reflect.DeepEqual(got, *want)) {
			t.Errorf("parseAPIKey(%q) = %+v, %v", key, got, ok)
		}
	}
}

func TestServiceTierMapsToServiceClass(t *testing.T) {
	for tier, want := range map[string]llm.ServiceClass{
		"flex": llm.ServiceClassEconomy, "default": llm.ServiceClassStandard, "priority": llm.ServiceClassPriority, "auto": "",
	} {
		mapped, apiErr := mapBody(t, `{"model":"m","input":"hi","service_tier":"`+tier+`"}`)
		if apiErr != nil {
			t.Fatalf("%s: %v", tier, apiErr)
		}
		got := mapped.Generate.SettingsPatch.ServiceClass.Set
		if (want == "") != (got == nil) || (got != nil && *got != want) {
			t.Errorf("service_tier %s mapped to %v", tier, got)
		}
		if want != "" && serviceTierFromClass(want) != tier {
			t.Errorf("service class %s maps back to %s", want, serviceTierFromClass(want))
		}
	}
	if _, apiErr := mapBody(t, `{"model":"m","input":"hi","service_tier":"scale"}`); apiErr == nil {
		t.Error("scale tier accepted")
	}
}

func TestInputItemsMapToTheTranscript(t *testing.T) {
	mapped, apiErr := mapBody(t, `{
		"model": "m",
		"instructions": "be brief",
		"input": [
			{"role": "developer", "content": "use metric units"},
			{"role": "user", "content": [{"type": "input_text", "text": "weather?"}, {"type": "input_image", "image_url": "https://example.com/map.png"}]},
			{"type": "function_call", "call_id": "call_1", "name": "weather", "arguments": "{\"city\":\"Sydney\"}"},
			{"type": "function_call_output", "call_id": "call_1", "output": "22C"},
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "It is 22C.", "annotations": []}]}
		],
		"tools": [{"type": "function", "name": "weather", "parameters": {"type": "object"}, "strict": true}],
		"tool_choice": {"type": "function", "name": "weather"},
		"parallel_tool_calls": false,
		"temperature": 0.70,
		"max_output_tokens": 100,
		"text": {"format": {"type": "json_schema", "name": "answer", "schema": {"type": "object"}, "strict": true}},
		"reasoning": {"effort": "high", "summary": "auto"}
	}`)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	generate := mapped.Generate
	if _, err := generate.MarshalJSON(); err != nil {
		t.Fatalf("mapped request is not a valid v1 request: %v", err)
	}
	instructions := *generate.SettingsPatch.Instructions.Set
	if len(instructions) != 2 || instructions[0].Text != "be brief" {
		t.Fatalf("unexpected instructions: %+v", instructions)
	}
	if len(generate.Append) != 4 {
		t.Fatalf("unexpected transcript: %+v", generate.Append)
	}
	user := generate.Append[0].(llm.Message)
	if user.Actor != llm.ActorHuman || user.Content[1].(llm.ImagePart).MediaType != "image/png" {
		t.Fatalf("unexpected user message: %+v", user)
	}
	call := generate.Append[1].(llm.ToolCall)
	if call.ID != "call_1" || string(call.Arguments) != `{"city":"Sydney"}` {
		t.Fatalf("unexpected call: %+v", call)
	}
	if result := generate.Append[2].(llm.ToolResult); result.CallID != "call_1" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if model := generate.Append[3].(llm.Message); model.Actor != llm.ActorModel {
		t.Fatalf("unexpected model message: %+v", model)
	}
	policy := *generate.SettingsPatch.ToolPolicy.Set
	if policy.Mode != llm.ToolChoiceNamed || policy.Name != "weather" || policy.Parallel {
		t.Fatalf("unexpected tool policy: %+v", policy)
	}
	if *generate.SettingsPatch.Temperature.Set != "0.7" {
		t.Fatalf("unexpected temperature %v", *generate.SettingsPatch.Temperature.Set)
	}
	output := *generate.SettingsPatch.Output.Set
	if *output.MaxTokens != 100 || output.Format.Kind != llm.OutputKindJSONSchema || !output.Format.Strict {
		t.Fatalf("unexpected output: %+v", output)
	}
	if *generate.SettingsPatch.ReasoningEffort.Set != llm.ReasoningEffortHigh || *generate.SettingsPatch.ReasoningSummary.Set != llm.ReasoningSummaryAuto {
		t.Fatal("reasoning was not mapped")
	}
}

func TestInputRejectsWhatCannotBeRepresented(t *testing.T) {
	for name, body := range map[string]string{
		"late developer message": `{"model":"m","input":[{"role":"user","content":"a"},{"role":"developer","content":"b"}]}`,
		"reasoning item":         `{"model":"m","input":[{"type":"reasoning","id":"rs_1"}]}`,
		"file id":                `{"model":"m","input":[{"role":"user","content":[{"type":"input_file","file_id":"file_1"}]}]}`,
		"unknown image type":     `{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/map"}]}]}`,
		"bad arguments":          `{"model":"m","input":[{"type":"function_call","call_id":"c","name":"f","arguments":"not json"}]}`,
		"user refusal":           `{"model":"m","input":[{"role":"user","content":[{"type":"refusal","refusal":"no"}]}]}`,
		"metadata":               `{"model":"m","input":"hi","metadata":{"a":"b"}}`,
		"truncation auto":        `{"model":"m","input":"hi","truncation":"auto"}`,
		"negative temperature":   `{"model":"m","input":"hi","temperature":-1}`,
	} {
		if _, apiErr := mapBody(t, body); apiErr == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestDataURLImagesCarryTheirBytes(t *testing.T) {
	mapped, apiErr := mapBody(t, `{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8=","detail":"low"}]}]}`)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	image := mapped.Generate.Append[0].(llm.Message).Content[0].(llm.ImagePart)
	if string(image.Bytes) != "hello" || image.MediaType != "image/png" || image.Detail != "low" {
		t.Fatalf("unexpected image: %+v", image)
	}
}

func TestCompletedResponseMapsStatusesAndToolCalls(t *testing.T) {
	response := *generateResponse("resp_x", "partial")
	response.Status = llm.ResponseStatusLength
	response.Output = append(response.Output, llm.ToolCall{ID: "call_9", Name: "lookup", Arguments: json.RawMessage(`{"q":1}`)})
	object := completedResponse(responseMeta{ID: "resp_x"}, response)
	if object.Status != "incomplete" || object.IncompleteDetails.Reason != "max_output_tokens" {
		t.Fatalf("unexpected status: %+v", object)
	}
	call := object.Output[1].(outputFunctionCall)
	if call.CallID != "call_9" || call.Arguments != `{"q":1}` || call.ID != "fc_x_1" {
		t.Fatalf("unexpected call: %+v", call)
	}
	if *object.ServiceTier != "flex" {
		t.Fatalf("service tier %s", *object.ServiceTier)
	}
}

func TestWorkflowIDSeparatesProjects(t *testing.T) {
	other := testIdentity
	other.Project = "other"
	if workflowID(testIdentity, "resp_a") == workflowID(other, "resp_a") {
		t.Fatal("projects share a workflow ID")
	}
	actor := testIdentity
	actor.Actor = "someone-else"
	if workflowID(testIdentity, "resp_a") != workflowID(actor, "resp_a") {
		t.Fatal("the actor changes the workflow ID")
	}
}
