package openairesponses

import (
	"fmt"

	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// schemaOrderOptions rewrites the output schema and each function tool's
// parameters in the request body with properties in "required" order. The SDK
// encodes these schemas from maps, which sorts their keys; providers generate
// structured output in schema order, so a reasoning-first schema needs the
// caller's intended order (#1096).
func schemaOrderOptions(params responses.ResponseNewParams) ([]option.RequestOption, error) {
	var options []option.RequestOption
	if format := params.Text.Format.OfJSONSchema; format != nil && format.Schema != nil {
		ordered, err := schemaorder.Ordered(format.Schema)
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithJSONSet("text.format.schema", ordered))
	}
	for index, tool := range params.Tools {
		if tool.OfFunction == nil || tool.OfFunction.Parameters == nil {
			continue
		}
		ordered, err := schemaorder.Ordered(tool.OfFunction.Parameters)
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithJSONSet(fmt.Sprintf("tools.%d.parameters", index), ordered))
	}
	return options, nil
}
