package runtime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCloudRequestLimitsRejectRequestsOverEachConfiguredBound(t *testing.T) {
	limits := CloudRequestLimits{Items: 2, PartsPerItem: 2, Tools: 1, SchemaBytes: 64, JSONDepth: 3}
	text := func(value string) llm.Part { return llm.TextPart{Text: value} }
	message := func(parts ...llm.Part) llm.Item { return llm.Message{Actor: llm.ActorHuman, Content: parts} }
	generate := func(mutate func(*llm.GenerateRequestV1)) llm.PrepareExecutionV1 {
		request := llm.GenerateRequestV1{OperationKey: "op", Append: []llm.Item{message(text("hi"))}}
		mutate(&request)
		return llm.PrepareExecutionV1{Generate: &request}
	}
	tools := func(values ...llm.Tool) *[]llm.Tool { return &values }
	if err := limits.validate(generate(func(*llm.GenerateRequestV1) {})); err != nil {
		t.Fatalf("request within limits rejected: %v", err)
	}
	for name, input := range map[string]llm.PrepareExecutionV1{
		"items": generate(func(r *llm.GenerateRequestV1) {
			r.Append = []llm.Item{message(text("a")), message(text("b")), message(text("c"))}
		}),
		"parts per item": generate(func(r *llm.GenerateRequestV1) { r.Append = []llm.Item{message(text("a"), text("b"), text("c"))} }),
		"tools": generate(func(r *llm.GenerateRequestV1) {
			r.SettingsPatch.Tools.Set = tools(llm.Tool{Name: "a", InputSchema: json.RawMessage(`{}`)}, llm.Tool{Name: "b", InputSchema: json.RawMessage(`{}`)})
		}),
		"schema bytes": generate(func(r *llm.GenerateRequestV1) {
			r.SettingsPatch.Tools.Set = tools(llm.Tool{Name: "a", InputSchema: json.RawMessage(`{"description":"` + strings.Repeat("x", 80) + `"}`)})
		}),
		"reference metadata depth": generate(func(r *llm.GenerateRequestV1) {
			r.Append = []llm.Item{llm.Reference{URI: "https://example.com/a", Metadata: map[string]json.RawMessage{"m": json.RawMessage(`{"a":{"b":{"c":{"d":1}}}}`)}}}
		}),
		"json depth": generate(func(r *llm.GenerateRequestV1) {
			r.Append = []llm.Item{message(llm.JSONPart{Value: json.RawMessage(`{"a":{"b":{"c":{"d":1}}}}`)})}
		}),
	} {
		err := limits.validate(input)
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || mapped.Retry != provider.RetryNever {
			t.Fatalf("%s: error = %v, want a non-retryable invalid_argument", name, err)
		}
	}
	if err := (CloudRequestLimits{}).validate(generate(func(r *llm.GenerateRequestV1) {
		r.Append = []llm.Item{message(text("a"), text("b"), text("c"))}
	})); err != nil {
		t.Fatalf("zero limits must be unbounded: %v", err)
	}
}

// A caller's tool call is held to the same depth as one an adapter accepts,
// whatever json_depth allows, so it can always be saved once wrapped.
func TestCloudRequestLimitsHoldToolCallArgumentsToTheToolArgumentDepth(t *testing.T) {
	nested := func(depth int) json.RawMessage {
		return json.RawMessage(strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1))
	}
	generate := func(depth int) llm.PrepareExecutionV1 {
		return llm.PrepareExecutionV1{Generate: &llm.GenerateRequestV1{OperationKey: "op", Append: []llm.Item{llm.ToolCall{ID: "call", Name: "lookup", Arguments: nested(depth)}}}}
	}
	for _, limits := range []CloudRequestLimits{{}, {JSONDepth: llm.DefaultCanonicalMaxDepth}} {
		if err := limits.validate(generate(llm.MaxToolArgumentsDepth)); err != nil {
			t.Fatalf("json_depth %d: tool call at the depth limit rejected: %v", limits.JSONDepth, err)
		}
		err := limits.validate(generate(llm.MaxToolArgumentsDepth + 1))
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeInvalidArgument || mapped.Retry != provider.RetryNever {
			t.Fatalf("json_depth %d: too-deep tool call error = %v", limits.JSONDepth, err)
		}
	}
}
