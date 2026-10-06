package openaichat

import (
	"fmt"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// schemaOrderOptions rewrites the response-format schema and each function
// tool's parameters in the request body with properties in "required" order.
// The SDK encodes these schemas from maps, which sorts their keys; providers
// generate structured output in schema order (#1096).
func schemaOrderOptions(params openai.ChatCompletionNewParams) ([]option.RequestOption, error) {
	var options []option.RequestOption
	if format := params.ResponseFormat.OfJSONSchema; format != nil && format.JSONSchema.Schema != nil {
		ordered, err := schemaorder.Ordered(format.JSONSchema.Schema)
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithJSONSet("response_format.json_schema.schema", ordered))
	}
	for index, tool := range params.Tools {
		if tool.OfFunction == nil || tool.OfFunction.Function.Parameters == nil {
			continue
		}
		ordered, err := schemaorder.Ordered(map[string]any(tool.OfFunction.Function.Parameters))
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithJSONSet(fmt.Sprintf("tools.%d.function.parameters", index), ordered))
	}
	return options, nil
}
