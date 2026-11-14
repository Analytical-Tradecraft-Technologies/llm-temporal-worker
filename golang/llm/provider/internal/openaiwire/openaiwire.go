// Package openaiwire holds the request lowering the OpenAI Chat Completions
// and Responses adapters share.
package openaiwire

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaischema"
)

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
