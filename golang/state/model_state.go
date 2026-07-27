package state

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// ModelState is the effective provider-neutral model configuration at one
// checkpoint.  A checkpoint stores patches; this value is only materialized
// in memory and is always returned as an independent copy.
type ModelState struct {
	Model                 string
	ServiceClass          llm.ServiceClass
	ServiceClassFallbacks []llm.ServiceClass
	Portability           llm.PortabilityMode
	Instructions          []llm.Instruction
	Tools                 []llm.Tool
	ToolPolicy            llm.ToolPolicy
	Output                *llm.OutputSpec
	Temperature           *float64
	// TemperatureDecimal is the exact v1 wire value. Temperature is retained
	// as the provider-facing projection, but durable checkpoint materialization
	// must not use it as the source of truth for decimal re-encoding.
	TemperatureDecimal *llm.DecimalV1
	ReasoningEffort    llm.ReasoningEffort
	ReasoningSummary   llm.ReasoningSummary
	CompactionPolicy   json.RawMessage
	Extensions         map[string]json.RawMessage
}

// RootModelState applies only public, deterministic defaults. Provider
// defaults are intentionally not invented here.
func RootModelState(model string) ModelState {
	return ModelState{Model: model, ServiceClass: llm.ServiceClassStandard, Portability: llm.PortabilityStrict}
}

func (state ModelState) Validate() error {
	if state.Model == "" {
		return fmt.Errorf("model is required")
	}
	serviceClass, err := llm.NormalizeServiceClass(state.ServiceClass)
	if err != nil {
		return err
	}
	if err := llm.ValidateServiceClassFallbacks(serviceClass, state.ServiceClassFallbacks); err != nil {
		return err
	}
	if state.Portability == "" {
		return fmt.Errorf("portability is required")
	}
	if !state.Portability.Valid() {
		return fmt.Errorf("portability %q is invalid", state.Portability)
	}
	if state.Temperature != nil && (math.IsNaN(*state.Temperature) || math.IsInf(*state.Temperature, 0) || *state.Temperature < 0) {
		return fmt.Errorf("temperature must be finite and non-negative")
	}
	if state.TemperatureDecimal != nil {
		canonical, err := llm.NewDecimalV1(state.TemperatureDecimal.String())
		if err != nil {
			return fmt.Errorf("temperature decimal is invalid: %w", err)
		}
		if _, err := canonical.Float64(); err != nil {
			return fmt.Errorf("temperature decimal cannot be represented by provider state: %w", err)
		}
	}
	return nil
}

// ApplySettingsPatch applies each leaf independently. Collection Set values
// replace the complete collection; Clear returns that leaf to its root value.
func ApplySettingsPatch(base ModelState, patch SettingsPatch) (ModelState, error) {
	if err := patch.Validate(); err != nil {
		return ModelState{}, err
	}
	result := base.Clone()
	if patch.Model.Set != nil {
		result.Model = *patch.Model.Set
	} else if patch.Model.Clear {
		result.Model = ""
	}
	if patch.ServiceClass.Set != nil {
		result.ServiceClass = *patch.ServiceClass.Set
	} else if patch.ServiceClass.Clear {
		result.ServiceClass = llm.ServiceClassStandard
	}
	if patch.ServiceClassFallbacks.Set != nil {
		result.ServiceClassFallbacks = append([]llm.ServiceClass(nil), (*patch.ServiceClassFallbacks.Set)...)
	} else if patch.ServiceClassFallbacks.Clear {
		result.ServiceClassFallbacks = nil
	}
	if patch.Portability.Set != nil {
		result.Portability = *patch.Portability.Set
	} else if patch.Portability.Clear {
		result.Portability = llm.PortabilityStrict
	}
	if patch.Instructions.Set != nil {
		result.Instructions = cloneInstructions(*patch.Instructions.Set)
	} else if patch.Instructions.Clear {
		result.Instructions = nil
	}
	if patch.Tools.Set != nil {
		result.Tools = cloneTools(*patch.Tools.Set)
	} else if patch.Tools.Clear {
		result.Tools = nil
	}
	if patch.ToolPolicy.Set != nil {
		result.ToolPolicy = *patch.ToolPolicy.Set
	} else if patch.ToolPolicy.Clear {
		result.ToolPolicy = llm.ToolPolicy{}
	}
	if patch.Output.Set != nil {
		result.Output = cloneOutput(patch.Output.Set)
	} else if patch.Output.Clear {
		result.Output = nil
	}
	if patch.TemperatureDecimal.Set != nil {
		value, err := patch.TemperatureDecimal.Set.Float64()
		if err != nil {
			return ModelState{}, fmt.Errorf("temperature decimal cannot be represented by provider state: %w", err)
		}
		decimal := *patch.TemperatureDecimal.Set
		result.Temperature = &value
		result.TemperatureDecimal = &decimal
	} else if patch.Temperature.Set != nil {
		value := *patch.Temperature.Set
		result.Temperature = &value
		decimal, err := llm.NewDecimalV1(strconv.FormatFloat(value, 'f', -1, 64))
		if err != nil {
			return ModelState{}, fmt.Errorf("temperature cannot be represented as v1 decimal: %w", err)
		}
		result.TemperatureDecimal = &decimal
	} else if patch.Temperature.Clear || patch.TemperatureDecimal.Clear {
		result.Temperature = nil
		result.TemperatureDecimal = nil
	}
	if patch.ReasoningEffort.Set != nil {
		result.ReasoningEffort = *patch.ReasoningEffort.Set
	} else if patch.ReasoningEffort.Clear {
		result.ReasoningEffort = ""
	}
	if patch.ReasoningSummary.Set != nil {
		result.ReasoningSummary = *patch.ReasoningSummary.Set
	} else if patch.ReasoningSummary.Clear {
		result.ReasoningSummary = ""
	}
	if patch.CompactionPolicy.Set != nil {
		result.CompactionPolicy = append(json.RawMessage(nil), (*patch.CompactionPolicy.Set)...)
	} else if patch.CompactionPolicy.Clear {
		result.CompactionPolicy = nil
	}
	if patch.Extensions.Set != nil {
		result.Extensions = cloneRawMap(*patch.Extensions.Set)
	} else if patch.Extensions.Clear {
		result.Extensions = nil
	}
	if result.ServiceClass == "" {
		result.ServiceClass = llm.ServiceClassStandard
	}
	if result.Portability == "" {
		result.Portability = llm.PortabilityStrict
	}
	if err := result.Validate(); err != nil {
		return ModelState{}, err
	}
	return result, nil
}

func (state ModelState) Clone() ModelState {
	result := state
	result.ServiceClassFallbacks = append([]llm.ServiceClass(nil), state.ServiceClassFallbacks...)
	result.Instructions = cloneInstructions(state.Instructions)
	result.Tools = cloneTools(state.Tools)
	result.Output = cloneOutput(state.Output)
	if state.Temperature != nil {
		value := *state.Temperature
		result.Temperature = &value
	}
	if state.TemperatureDecimal != nil {
		value := *state.TemperatureDecimal
		result.TemperatureDecimal = &value
	}
	result.CompactionPolicy = append(json.RawMessage(nil), state.CompactionPolicy...)
	result.Extensions = cloneRawMap(state.Extensions)
	return result
}

func cloneInstructions(values []llm.Instruction) []llm.Instruction {
	if values == nil {
		return nil
	}
	result := make([]llm.Instruction, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Content = cloneParts(value.Content)
	}
	return result
}

func cloneTools(values []llm.Tool) []llm.Tool {
	if values == nil {
		return nil
	}
	result := make([]llm.Tool, len(values))
	for i, value := range values {
		result[i] = value
		result[i].InputSchema = append(json.RawMessage(nil), value.InputSchema...)
		result[i].OutputSchema = append(json.RawMessage(nil), value.OutputSchema...)
	}
	return result
}

func cloneOutput(value *llm.OutputSpec) *llm.OutputSpec {
	if value == nil {
		return nil
	}
	result := *value
	result.Format.Schema = append(json.RawMessage(nil), value.Format.Schema...)
	if value.MaxTokens != nil {
		maxTokens := *value.MaxTokens
		result.MaxTokens = &maxTokens
	}
	return &result
}

func cloneParts(values []llm.Part) []llm.Part {
	if values == nil {
		return nil
	}
	result := make([]llm.Part, len(values))
	for i, part := range values {
		switch value := part.(type) {
		case llm.TextPart:
			result[i] = value
		case llm.ImagePart:
			copyValue := value
			copyValue.Bytes = append([]byte(nil), value.Bytes...)
			copyValue.Blob = cloneBlob(value.Blob)
			result[i] = copyValue
		case llm.DocumentPart:
			copyValue := value
			copyValue.Bytes = append([]byte(nil), value.Bytes...)
			copyValue.Blob = cloneBlob(value.Blob)
			result[i] = copyValue
		case llm.JSONPart:
			result[i] = llm.JSONPart{Value: append(json.RawMessage(nil), value.Value...)}
		case llm.RefusalPart:
			result[i] = value
		case llm.ProviderStatePart:
			copyValue := value
			copyValue.Opaque = append([]byte(nil), value.Opaque...)
			result[i] = copyValue
		default:
			result[i] = part
		}
	}
	return result
}

func cloneBlob(value *llm.BlobRef) *llm.BlobRef {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	if values == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}
