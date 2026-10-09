package openairesponses

import (
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// schemaOrderOptions rewrites the output schema and each function tool's
// parameters in the request body with properties in "required" order. The SDK
// encodes these schemas from maps, which sorts their keys; providers generate
// structured output in schema order, so a reasoning-first schema needs the
// caller's intended order (#1096).
func schemaOrderOptions(params responses.ResponseNewParams) ([]option.RequestOption, error) {
	overrides, err := schemaorder.OpenAIOverrides(params, true)
	if err != nil {
		return nil, err
	}
	options := make([]option.RequestOption, 0, len(overrides))
	for _, override := range overrides {
		options = append(options, option.WithJSONSet(override.Path, override.Value))
	}
	return options, nil
}
