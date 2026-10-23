// Package anthropicwire holds the request lowering the Anthropic Messages and
// Bedrock Messages adapters share. Both send the Anthropic Messages body; a
// Family only changes how the endpoint is named in error messages.
package anthropicwire

import (
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicschema"
)

// Family names one Anthropic Messages endpoint in error messages.
type Family struct {
	// Name is the endpoint family, as in "not supported by Anthropic Messages".
	Name string
	// Vendor is the short name, as in "adaptive Anthropic thinking".
	Vendor string
	// Package is the adapter package, as in "not supported by anthropicmessages".
	Package string
}

var (
	Anthropic = Family{Name: "Anthropic Messages", Vendor: "Anthropic", Package: "anthropicmessages"}
	Bedrock   = Family{Name: "Bedrock Messages", Vendor: "Bedrock", Package: "bedrockmessages"}
)

// PartLowering supplies the family-specific lowering of media and provider
// state parts; everything else in LowerPart is shared.
type PartLowering struct {
	Image         func(llm.ImagePart) (map[string]any, error)
	Document      func(llm.DocumentPart) (map[string]any, error)
	ProviderState func(llm.ProviderStatePart) (json.RawMessage, error)
}

// LowerParts lowers content parts in order, prefixing errors with the index.
func (family Family) LowerParts(parts []llm.Part, lowering PartLowering) ([]any, error) {
	content := make([]any, 0, len(parts))
	for index, part := range parts {
		lowered, err := family.LowerPart(part, lowering)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", index, err)
		}
		content = append(content, lowered)
	}
	return content, nil
}

// LowerPart lowers one content part to its Anthropic content block.
func (family Family) LowerPart(part llm.Part, lowering PartLowering) (any, error) {
	switch value := part.(type) {
	case llm.TextPart:
		return map[string]any{"type": "text", "text": value.Text}, nil
	case llm.JSONPart:
		if !json.Valid(value.Value) {
			return nil, fmt.Errorf("JSON part is invalid")
		}
		return map[string]any{"type": "text", "text": string(value.Value)}, nil
	case llm.ImagePart:
		return lowering.Image(value)
	case llm.DocumentPart:
		return lowering.Document(value)
	case llm.ProviderStatePart:
		return lowering.ProviderState(value)
	case llm.RefusalPart:
		return nil, fmt.Errorf("refusal part is not accepted as %s input", family.Vendor)
	default:
		return nil, fmt.Errorf("part kind %q is not supported by %s", part.PartKind(), family.Name)
	}
}

// LowerReasoning lowers the reasoning spec to the thinking object. A nil
// result with no error means the request sends no thinking object.
func (family Family) LowerReasoning(reasoning llm.ReasoningSpec, strict bool) (map[string]any, error) {
	if reasoning.Effort == llm.ReasoningEffortExtraHigh {
		return nil, provider.NewUnsupportedFeatureError(provider.FeatureReasoning, "xhigh reasoning effort is not supported by "+family.Package)
	}
	mode := reasoning.Mode
	if mode == "" {
		mode = llm.ReasoningModeProviderDefault
	}
	// The only display controls are "summarized" (the provider default) and
	// "omitted"; a summary detail level cannot be expressed.
	display := ""
	switch reasoning.Summary {
	case "", llm.ReasoningSummaryProviderDefault:
	case llm.ReasoningSummaryNone:
		display = "omitted"
	case llm.ReasoningSummaryAuto:
		display = "summarized"
	case llm.ReasoningSummaryConcise, llm.ReasoningSummaryDetailed:
		if strict {
			return nil, provider.NewStrictPortabilityError(fmt.Sprintf("reasoning summary %q is not supported by %s", reasoning.Summary, family.Name))
		}
		display = "summarized"
	default:
		return nil, fmt.Errorf("reasoning summary %q is not supported by %s", reasoning.Summary, family.Name)
	}
	if mode == llm.ReasoningModeProviderDefault {
		if reasoning.TokenBudget == nil {
			// Thinking is opt-in: with no thinking object the response has no
			// thinking blocks, so there is nothing to summarize or omit and
			// dropping the summary preference loses nothing, even in strict
			// mode. display has no wire form outside that object.
			return nil, nil
		}
		mode = llm.ReasoningModeEnabled
	}
	if reasoning.Effort != "" && reasoning.Effort != llm.ReasoningEffortProviderDefault && mode != llm.ReasoningModeAdaptive {
		return nil, fmt.Errorf("reasoning effort %q requires adaptive %s thinking", reasoning.Effort, family.Vendor)
	}
	result := map[string]any{}
	switch mode {
	case llm.ReasoningModeDisabled:
		return map[string]any{"type": "disabled"}, nil
	case llm.ReasoningModeAdaptive:
		result["type"] = "adaptive"
	case llm.ReasoningModeEnabled:
		if reasoning.TokenBudget == nil {
			return nil, fmt.Errorf("enabled %s thinking requires token_budget", family.Vendor)
		}
		if *reasoning.TokenBudget < 1024 {
			return nil, fmt.Errorf("%s thinking token_budget must be at least 1024", family.Vendor)
		}
		result["type"] = "enabled"
		result["budget_tokens"] = *reasoning.TokenBudget
	default:
		return nil, fmt.Errorf("reasoning mode %q is not supported by %s", reasoning.Mode, family.Name)
	}
	if display != "" {
		result["display"] = display
	}
	return result, nil
}

// ReasoningOutputEffort maps a reasoning effort to output_config.effort, or
// "" when the effort has no wire value.
func ReasoningOutputEffort(effort llm.ReasoningEffort) string {
	switch effort {
	case llm.ReasoningEffortMinimal, llm.ReasoningEffortLow:
		return "low"
	case llm.ReasoningEffortMedium:
		return "medium"
	case llm.ReasoningEffortHigh:
		return "high"
	case llm.ReasoningEffortMaximum:
		return "max"
	default:
		return ""
	}
}

// LowerSampling writes the supported sampling fields into target.
func (family Family) LowerSampling(sampling llm.SamplingSpec, target map[string]any) error {
	if sampling.Seed != nil || sampling.PresencePenalty != nil || sampling.FrequencyPenalty != nil {
		return fmt.Errorf("sampling field is not supported by %s", family.Name)
	}
	if sampling.Temperature != nil {
		if *sampling.Temperature < 0 || *sampling.Temperature > 1 {
			return fmt.Errorf("temperature %v is outside the %s range 0 to 1", *sampling.Temperature, family.Name)
		}
		target["temperature"] = *sampling.Temperature
	}
	if sampling.TopP != nil {
		target["top_p"] = *sampling.TopP
	}
	if sampling.TopK != nil {
		target["top_k"] = *sampling.TopK
	}
	if sampling.StopSequences != nil {
		target["stop_sequences"] = sampling.StopSequences
	}
	return nil
}

// LowerOutput writes the structured output format into target.
func (family Family) LowerOutput(output llm.OutputSpec, target map[string]any, strict bool) error {
	switch output.Format.Kind {
	case "", llm.OutputKindText:
		return nil
	case llm.OutputKindJSON:
		// Structured output requires a closed schema; a bare object schema is
		// not valid and would otherwise constrain the answer to {}.
		return fmt.Errorf("output format %q without a schema is not supported by %s", output.Format.Kind, family.Name)
	case llm.OutputKindJSONSchema:
		// Structured output returns a 400 for keywords outside its subset, so
		// the wire carries a lowered schema; the lift validates the final
		// JSON against the caller's original.
		schema, err := anthropicschema.Lower(output.Format.Schema, strict)
		if err != nil {
			return err
		}
		target["output_config"] = map[string]any{"format": map[string]any{
			"type":   "json_schema",
			"schema": schema,
		}}
		return nil
	default:
		return fmt.Errorf("output format %q is not supported by %s", output.Format.Kind, family.Name)
	}
}

// LowerToolPolicy lowers the tool policy to tool_choice.
func LowerToolPolicy(policy llm.ToolPolicy) (map[string]any, error) {
	mode := policy.Mode
	if mode == "" {
		mode = llm.ToolChoiceAuto
	}
	choice := map[string]any{}
	switch mode {
	case llm.ToolChoiceNone:
		choice["type"] = "none"
	case llm.ToolChoiceAuto:
		choice["type"] = "auto"
	case llm.ToolChoiceRequired:
		choice["type"] = "any"
	case llm.ToolChoiceNamed:
		if policy.Name == "" {
			return nil, fmt.Errorf("named tool policy requires a name")
		}
		choice["type"] = "tool"
		choice["name"] = policy.Name
	default:
		return nil, fmt.Errorf("tool policy mode %q is invalid", mode)
	}
	if mode != llm.ToolChoiceNone {
		choice["disable_parallel_tool_use"] = !policy.Parallel
	}
	return choice, nil
}

// HasMixedInstructionLevels reports whether instructions mix application and
// policy levels, which the single system prompt cannot keep apart.
func HasMixedInstructionLevels(instructions []llm.Instruction) bool {
	application := false
	policy := false
	for _, instruction := range instructions {
		if instruction.Level == llm.InstructionLevelPolicy {
			policy = true
		} else {
			application = true
		}
	}
	return application && policy
}

// EmptyModelMessage reports whether item is a model message with no content.
func EmptyModelMessage(item llm.Item) bool {
	message, ok := item.(llm.Message)
	return ok && message.Actor == llm.ActorModel && len(message.Content) == 0
}

// RequiredFeatures lists the capabilities a request needs from an Anthropic
// Messages endpoint, in first-use order.
func RequiredFeatures(request llm.Request) []provider.Feature {
	features := []provider.Feature{provider.FeatureText, provider.FeatureUsage}
	for _, instruction := range request.Instructions {
		for _, part := range instruction.Content {
			features = append(features, partFeature(part))
		}
	}
	for _, item := range request.Input {
		switch value := item.(type) {
		case llm.Message:
			for _, part := range value.Content {
				features = append(features, partFeature(part))
			}
		case llm.ToolCall, llm.ToolResult:
			features = append(features, provider.FeatureToolCall)
		case llm.ProviderState:
			features = append(features, provider.FeatureContinuation, provider.FeatureReasoning)
		}
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		features = append(features, provider.FeatureToolCall)
	}
	if request.Output != nil && request.Output.Format.Kind != "" && request.Output.Format.Kind != llm.OutputKindText {
		features = append(features, provider.FeatureStructuredOutput)
	}
	if request.Reasoning != nil {
		features = append(features, provider.FeatureReasoning)
	}
	if request.Continuation != nil {
		features = append(features, provider.FeatureContinuation)
	}
	return provider.UniqueFeatures(features)
}

func partFeature(part llm.Part) provider.Feature {
	switch part.PartKind() {
	case llm.PartKindImage:
		return provider.FeatureImage
	case llm.PartKindDocument:
		return provider.FeatureDocument
	case llm.PartKindProviderState:
		return provider.FeatureContinuation
	default:
		return provider.FeatureText
	}
}
