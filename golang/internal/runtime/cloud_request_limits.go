package runtime

import (
	"encoding/json"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// CloudRequestLimits bounds the shape of one request before it creates any
// durable record: appended items, parts per item or instruction, tool
// definitions, schema bytes and the nesting depth of every caller-supplied
// JSON value. A zero field is unbounded; the cloud builder always supplies
// the configured positive values.
type CloudRequestLimits struct {
	Items        int
	PartsPerItem int
	Tools        int
	SchemaBytes  int
	JSONDepth    int
}

func cloudRequestLimitsFromConfig(limits config.LimitsConfig) CloudRequestLimits {
	return CloudRequestLimits{Items: limits.Items, PartsPerItem: limits.PartsPerItem, Tools: limits.Tools, SchemaBytes: limits.SchemaBytes, JSONDepth: limits.JSONDepth}
}

func (limits CloudRequestLimits) valid() bool {
	return limits.Items >= 0 && limits.PartsPerItem >= 0 && limits.Tools >= 0 && limits.SchemaBytes >= 0 && limits.JSONDepth >= 0
}

// validate rejects a request that exceeds any configured bound. The error is
// never retryable and carries no caller content.
func (limits CloudRequestLimits) validate(input llm.PrepareExecutionV1) error {
	if input.Compact != nil {
		if !limits.depthWithin(input.Compact.Policy) {
			return requestLimitError()
		}
		return nil
	}
	request := input.Generate
	if request == nil {
		return nil
	}
	if limits.exceeds(limits.Items, len(request.Append)) {
		return requestLimitError()
	}
	for _, item := range request.Append {
		if !limits.itemWithin(item) {
			return requestLimitError()
		}
	}
	patch := request.SettingsPatch
	if patch.Instructions.Set != nil {
		for _, instruction := range *patch.Instructions.Set {
			if limits.exceeds(limits.PartsPerItem, len(instruction.Content)) || !limits.partsWithin(instruction.Content) {
				return requestLimitError()
			}
		}
	}
	if patch.Tools.Set != nil {
		if limits.exceeds(limits.Tools, len(*patch.Tools.Set)) {
			return requestLimitError()
		}
		for _, tool := range *patch.Tools.Set {
			if !limits.schemaWithin(tool.InputSchema) || !limits.schemaWithin(tool.OutputSchema) {
				return requestLimitError()
			}
		}
	}
	if patch.Output.Set != nil && !limits.schemaWithin(patch.Output.Set.Format.Schema) {
		return requestLimitError()
	}
	if patch.CompactionPolicy.Set != nil && !limits.depthWithin(*patch.CompactionPolicy.Set) {
		return requestLimitError()
	}
	if patch.Extensions.Set != nil {
		for _, value := range *patch.Extensions.Set {
			if !limits.depthWithin(value) {
				return requestLimitError()
			}
		}
	}
	return nil
}

func (limits CloudRequestLimits) itemWithin(item llm.Item) bool {
	switch item := item.(type) {
	case llm.Message:
		return !limits.exceeds(limits.PartsPerItem, len(item.Content)) && limits.partsWithin(item.Content)
	case llm.ToolResult:
		return !limits.exceeds(limits.PartsPerItem, len(item.Content)) && limits.partsWithin(item.Content)
	case llm.ToolCall:
		return limits.depthWithin(item.Arguments)
	default:
		return true
	}
}

func (limits CloudRequestLimits) partsWithin(parts []llm.Part) bool {
	for _, part := range parts {
		if part, ok := part.(llm.JSONPart); ok && !limits.depthWithin(part.Value) {
			return false
		}
	}
	return true
}

// schemaWithin bounds a tool or output schema by both schema_bytes and
// json_depth. An absent schema is within bounds; malformed JSON is left to
// the codec, which has already rejected it.
func (limits CloudRequestLimits) schemaWithin(schema json.RawMessage) bool {
	if len(schema) == 0 {
		return true
	}
	if limits.exceeds(limits.SchemaBytes, len(schema)) {
		return false
	}
	return limits.depthWithin(schema)
}

func (limits CloudRequestLimits) depthWithin(value json.RawMessage) bool {
	if limits.JSONDepth <= 0 || len(value) == 0 {
		return true
	}
	_, err := llm.CanonicalJSONWithLimits(value, llm.DefaultCanonicalMaxBytes, limits.JSONDepth)
	return err == nil
}

func (CloudRequestLimits) exceeds(limit, count int) bool {
	return limit > 0 && count > limit
}

func requestLimitError() error {
	return provider.NewError(provider.CodeInvalidArgument, provider.PhaseDecode, provider.DispatchNotDispatched, provider.RetryNever, "request exceeds configured limits")
}
