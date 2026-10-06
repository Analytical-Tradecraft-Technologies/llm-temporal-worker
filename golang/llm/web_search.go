package llm

import (
	"encoding/json"
	"net/url"
	"strings"
)

// SupportsWebSearch limits the flag to reviewed native provider transports.
func SupportsWebSearch(provider, family, model string) bool {
	switch family {
	case "openai_responses":
		return provider == "openai"
	case "anthropic_messages":
		return provider == "anthropic"
	case "openai_chat":
		return provider == "openrouter" && (strings.HasPrefix(model, "openai/") || strings.HasPrefix(model, "anthropic/"))
	}
	return false
}

// WebSearchReferences lifts URL citations without treating hosted calls as
// application function calls. Original annotations remain attached as metadata.
func WebSearchReferences(raw []byte) []Item {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	var output []Item
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case []any:
			for _, child := range x {
				walk(child)
			}
		case map[string]any:
			kind, _ := x["type"].(string)
			if kind == "container_file_citation" {
				container, _ := x["container_id"].(string)
				file, _ := x["file_id"].(string)
				if container != "" && file != "" {
					encoded, _ := json.Marshal(x)
					output = append(output, Reference{URI: "https://api.openai.com/v1/containers/" + url.PathEscape(container) + "/files/" + url.PathEscape(file) + "/content", Metadata: map[string]json.RawMessage{"artifact": encoded}})
				}
			}
			if kind == "url_citation" || kind == "web_search_result_location" || kind == "web_fetch_result" {
				uri, _ := x["url"].(string)
				if uri == "" {
					if c, ok := x["url_citation"].(map[string]any); ok {
						uri, _ = c["url"].(string)
					}
				}
				if uri != "" && validateURI(uri) == nil {
					encoded, _ := json.Marshal(x)
					output = append(output, Reference{URI: uri, Metadata: map[string]json.RawMessage{"citation": encoded}})
				}
			}
			// Only traverse documented citation containers, avoiding references
			// accidentally inferred from arbitrary model-produced JSON/tool arguments.
			for _, key := range []string{"output", "content", "annotations", "citations", "choices", "message"} {
				if child, ok := x[key]; ok {
					walk(child)
				}
			}
		}
	}
	walk(value)
	return output
}

// SupportsHostedExecution excludes gateways until their tool transport is reviewed.
func SupportsHostedExecution(provider, family string) bool {
	return (provider == "openai" && family == "openai_responses") || (provider == "anthropic" && family == "anthropic_messages")
}
