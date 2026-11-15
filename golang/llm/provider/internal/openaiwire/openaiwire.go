// Package openaiwire holds the request lowering the OpenAI Chat Completions
// and Responses adapters share.
package openaiwire

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaischema"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// SchemaOrderOptions rewrites the output schema and each function tool's
// parameters in the request body with properties in "required" order. The SDK
// encodes these schemas from maps, which sorts their keys; providers generate
// structured output in schema order, so a reasoning-first schema needs the
// caller's intended order (#1096). params is the Chat Completions or, with
// responses set, the Responses SDK parameters.
func SchemaOrderOptions(params any, responses bool) ([]option.RequestOption, error) {
	overrides, err := schemaorder.OpenAIOverrides(params, responses)
	if err != nil {
		return nil, err
	}
	options := make([]option.RequestOption, 0, len(overrides))
	for _, override := range overrides {
		options = append(options, option.WithJSONSet(override.Path, override.Value))
	}
	return options, nil
}

// MediaURL returns rawURL when it is set, and otherwise a base64 data URL of
// data. Media held only in a blob has neither and is refused.
func MediaURL(rawURL string, data []byte, mediaType string) (string, error) {
	if rawURL != "" {
		return rawURL, nil
	}
	if len(data) == 0 {
		return "", fmt.Errorf("blob-backed media is not available to adapter")
	}
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(data), nil
}

// imageDetails are the image detail values the OpenAI APIs accept.
var imageDetails = map[string]struct{}{"low": {}, "high": {}, "auto": {}, "original": {}}

// ImageSource returns the URL and detail an image part lowers to: its media
// URL, and its detail, "auto" when unset, which must be one the OpenAI APIs
// accept.
func ImageSource(image llm.ImagePart) (url, detail string, err error) {
	url, err = MediaURL(image.URL, image.Bytes, image.MediaType)
	if err != nil {
		return "", "", fmt.Errorf("image: %w", err)
	}
	detail = image.Detail
	if detail == "" {
		detail = "auto"
	}
	if _, ok := imageDetails[detail]; !ok {
		return "", "", fmt.Errorf("image detail %q is not one of low, high, auto or original", detail)
	}
	return url, detail, nil
}

// ToolResultText joins tool result content into the single text output both
// wire formats carry. Only text and valid JSON parts can be represented.
func ToolResultText(content []llm.Part) (string, error) {
	var builder strings.Builder
	for index, part := range content {
		switch value := part.(type) {
		case llm.TextPart:
			builder.WriteString(value.Text)
		case llm.JSONPart:
			if !json.Valid(value.Value) {
				return "", fmt.Errorf("tool result part %d is invalid JSON", index)
			}
			builder.Write(value.Value)
		default:
			return "", fmt.Errorf("tool result part %d kind %q cannot be represented as text", index, part.PartKind())
		}
	}
	return builder.String(), nil
}

// PartFeature names the capability a content part needs on both wire
// formats: images and documents need their own, everything else is text.
func PartFeature(part llm.Part) provider.Feature {
	switch part.PartKind() {
	case llm.PartKindImage:
		return provider.FeatureImage
	case llm.PartKindDocument:
		return provider.FeatureDocument
	default:
		return provider.FeatureText
	}
}

// LowerError maps a lowering failure to the error contract. A request that
// structured output cannot represent is unsupported, not malformed.
func LowerError(err error) *provider.Error {
	var unsupported *openaischema.UnsupportedError
	if errors.As(err, &unsupported) {
		return provider.NewUnsupportedFeatureError(provider.FeatureStructuredOutput, err.Error())
	}
	return provider.NewCompileError(err.Error())
}
