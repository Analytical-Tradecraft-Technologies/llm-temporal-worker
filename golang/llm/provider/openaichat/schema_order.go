package openaichat

import (
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// schemaOrderOptions rewrites the response-format schema and each function
// tool's parameters in the request body with properties in "required" order.
// The SDK encodes these schemas from maps, which sorts their keys; providers
// generate structured output in schema order (#1096).
func schemaOrderOptions(params openai.ChatCompletionNewParams) ([]option.RequestOption, error) {
	overrides, err := schemaorder.OpenAIOverrides(params, false)
	if err != nil {
		return nil, err
	}
	options := make([]option.RequestOption, 0, len(overrides))
	for _, override := range overrides {
		options = append(options, option.WithJSONSet(override.Path, override.Value))
	}
	return options, nil
}
