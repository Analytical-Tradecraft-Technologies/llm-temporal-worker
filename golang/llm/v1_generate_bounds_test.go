package llm_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestGenerateRequestV1AcceptsSchemaBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*llm.GenerateRequestV1)
	}{
		{name: "operation key unicode code points", mutate: func(request *llm.GenerateRequestV1) {
			request.OperationKey = strings.Repeat("界", 256)
		}},
		{name: "parent unicode code points", mutate: func(request *llm.GenerateRequestV1) {
			parent := llm.CheckpointHandle(strings.Repeat("界", 512))
			request.Parent = &parent
		}},
		{name: "append items", mutate: func(request *llm.GenerateRequestV1) {
			request.Append = repeatedGenerateItems(10000)
		}},
		{name: "context unicode code points", mutate: func(request *llm.GenerateRequestV1) {
			request.Context = llm.RequestContext{
				Tenant:  strings.Repeat("界", 256),
				Project: strings.Repeat("界", 256),
				Actor:   strings.Repeat("界", 256),
			}
		}},
		{name: "model unicode code points", mutate: func(request *llm.GenerateRequestV1) {
			model := strings.Repeat("界", 256)
			request.SettingsPatch.Model.Set = &model
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validGenerateBoundsRequest()
			test.mutate(&request)

			encoded, err := json.Marshal(request)
			testassert.NoError(t, err, "Marshal() rejected a valid boundary")
			testassert.UnmarshalJSON[llm.GenerateRequestV1](t, encoded, "Unmarshal() rejected a valid boundary")
		})
	}
}

// overlongContextField sets one context member to 257 code points on both the
// typed request (through set) and its wire form (under field).
func overlongContextField(field string, set func(*llm.RequestContext, string)) func(*llm.GenerateRequestV1, map[string]any) {
	return func(request *llm.GenerateRequestV1, wire map[string]any) {
		value := strings.Repeat("界", 257)
		set(&request.Context, value)
		wire["context"].(map[string]any)[field] = value
	}
}

func TestGenerateRequestV1RejectsValuesBeyondSchemaBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*llm.GenerateRequestV1, map[string]any)
	}{
		{name: "empty operation key", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			request.OperationKey = ""
			wire["operation_key"] = ""
		}},
		{name: "operation key 257", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			value := strings.Repeat("界", 257)
			request.OperationKey = value
			wire["operation_key"] = value
		}},
		{name: "parent 513", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			value := strings.Repeat("界", 513)
			parent := llm.CheckpointHandle(value)
			request.Parent = &parent
			wire["parent"] = value
		}},
		{name: "append 10001", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			items := repeatedGenerateItems(10001)
			request.Append = items
			wire["append"] = items
		}},
		{name: "tenant 257", mutate: overlongContextField("tenant", func(context *llm.RequestContext, value string) { context.Tenant = value })},
		{name: "project 257", mutate: overlongContextField("project", func(context *llm.RequestContext, value string) { context.Project = value })},
		{name: "actor 257", mutate: overlongContextField("actor", func(context *llm.RequestContext, value string) { context.Actor = value })},
		{name: "empty model set", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			value := ""
			request.SettingsPatch.Model.Set = &value
			wire["settings_patch"] = map[string]any{"model": map[string]any{"set": value}}
		}},
		{name: "model set 257", mutate: func(request *llm.GenerateRequestV1, wire map[string]any) {
			value := strings.Repeat("界", 257)
			request.SettingsPatch.Model.Set = &value
			wire["settings_patch"] = map[string]any{"model": map[string]any{"set": value}}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validGenerateBoundsRequest()
			wire := validGenerateBoundsWire()
			test.mutate(&request, wire)

			if _, err := json.Marshal(request); err == nil {
				t.Fatal("Marshal() accepted a value beyond the schema bound")
			}
			requireUnmarshalRejected[llm.GenerateRequestV1](t, testassert.MarshalJSON(t, wire), "Unmarshal() accepted a value beyond the schema bound")
		})
	}
}

func validGenerateBoundsRequest() llm.GenerateRequestV1 {
	return llm.GenerateRequestV1{
		OperationKey: "operation",
		Context:      llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
	}
}

func validGenerateBoundsWire() map[string]any {
	return map[string]any{
		"api_version":   llm.APIVersion,
		"operation_key": "operation",
		"context": map[string]any{
			"tenant":  "tenant",
			"project": "project",
			"actor":   "actor",
		},
		"append": []any{},
	}
}

func repeatedGenerateItems(count int) []llm.Item {
	item := llm.HumanText("x")
	items := make([]llm.Item, count)
	for index := range items {
		items[index] = item
	}
	return items
}
