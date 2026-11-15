package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// ModelState is the effective provider-neutral model configuration at one
// checkpoint.  A checkpoint stores patches; this value is only materialized
// in memory and is always returned as an independent copy.
type ModelState struct {
	WebSearch             bool
	WebFetch              bool
	CodeExecution         bool
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
	// The leaves below were added after v1 checkpoints were first written.
	// omitempty keeps the snapshot digest of a state that does not use them
	// byte-identical to the digest computed before they existed.
	TopP                 *llm.DecimalV1    `json:",omitempty"`
	StopSequences        []string          `json:",omitempty"`
	Seed                 *int64            `json:",omitempty"`
	ReasoningMode        llm.ReasoningMode `json:",omitempty"`
	ReasoningTokenBudget *int              `json:",omitempty"`
	ReasoningEffort      llm.ReasoningEffort
	ReasoningSummary     llm.ReasoningSummary
	CompactionPolicy     json.RawMessage
	Extensions           map[string]json.RawMessage
}

// RootModelState applies only public, deterministic defaults. Provider
// defaults are intentionally not invented here.
func RootModelState(model string) ModelState {
	return ModelState{Model: model, ServiceClass: llm.ServiceClassStandard, Portability: llm.PortabilityStrict}
}

// ApplySettingsPatchV1 applies the public sparse patch without serializing and
// decoding the entire model state. It preserves the exact decimal alongside its
// provider projection and returns a detached value, as ApplySettingsPatch does.
func ApplySettingsPatchV1(base ModelState, wire llm.SettingsPatchV1) (ModelState, error) {
	if _, err := wire.MarshalJSON(); err != nil {
		return ModelState{}, err
	}
	if err := canonicalizeDecimal(&wire.Temperature.Set); err != nil {
		return ModelState{}, err
	}
	if err := canonicalizeDecimal(&wire.TopP.Set); err != nil {
		return ModelState{}, err
	}
	patch, err := settingsPatchFromWire(wire)
	if err != nil {
		return ModelState{}, err
	}
	return ApplySettingsPatch(base, patch)
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
	if state.TopP != nil {
		if err := llm.ValidateTopPV1(*state.TopP); err != nil {
			return err
		}
	}
	if state.StopSequences != nil {
		if err := llm.ValidateStopSequencesV1(state.StopSequences); err != nil {
			return err
		}
	}
	if state.Seed != nil {
		if err := llm.ValidateSeedV1(*state.Seed); err != nil {
			return err
		}
	}
	if state.ReasoningMode != "" && !llm.ValidReasoningModeV1(state.ReasoningMode) {
		return fmt.Errorf("reasoning mode %q is invalid", state.ReasoningMode)
	}
	if state.ReasoningTokenBudget != nil {
		if err := llm.ValidateReasoningTokenBudgetV1(*state.ReasoningTokenBudget); err != nil {
			return err
		}
	}
	return nil
}

// canonicalizeDecimal rewrites a set decimal in its canonical v1 form.
func canonicalizeDecimal(value **llm.DecimalV1) error {
	if *value == nil {
		return nil
	}
	canonical, err := llm.NewDecimalV1((*value).String())
	if err != nil {
		return err
	}
	*value = &canonical
	return nil
}

// ApplySettingsPatch applies each leaf independently. Collection Set values
// replace the complete collection; Clear returns that leaf to its root value.
func ApplySettingsPatch(base ModelState, patch SettingsPatch) (ModelState, error) {
	if err := patch.Validate(); err != nil {
		return ModelState{}, err
	}
	result := base.Clone()
	patch.Model.apply(&result.Model, "")
	patch.ServiceClass.apply(&result.ServiceClass, llm.ServiceClassStandard)
	patch.ServiceClassFallbacks.applyClone(&result.ServiceClassFallbacks, cloneSlice)
	patch.Portability.apply(&result.Portability, llm.PortabilityStrict)
	patch.Instructions.applyClone(&result.Instructions, cloneInstructions)
	patch.Tools.applyClone(&result.Tools, cloneTools)
	patch.ToolPolicy.apply(&result.ToolPolicy, llm.ToolPolicy{})
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
	patch.TopP.applyPointer(&result.TopP)
	patch.StopSequences.applyClone(&result.StopSequences, cloneSlice)
	patch.Seed.applyPointer(&result.Seed)
	patch.ReasoningMode.apply(&result.ReasoningMode, "")
	patch.ReasoningTokenBudget.applyPointer(&result.ReasoningTokenBudget)
	patch.ReasoningEffort.apply(&result.ReasoningEffort, "")
	patch.ReasoningSummary.apply(&result.ReasoningSummary, "")
	patch.CompactionPolicy.applyClone(&result.CompactionPolicy, cloneSlice)
	patch.WebFetch.apply(&result.WebFetch, false)
	patch.CodeExecution.apply(&result.CodeExecution, false)
	patch.WebSearch.apply(&result.WebSearch, false)
	patch.Extensions.applyClone(&result.Extensions, cloneRawMap)
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
	result.Temperature = clonePointer(state.Temperature)
	result.TemperatureDecimal = clonePointer(state.TemperatureDecimal)
	result.TopP = clonePointer(state.TopP)
	result.StopSequences = append([]string(nil), state.StopSequences...)
	result.Seed = clonePointer(state.Seed)
	result.ReasoningTokenBudget = clonePointer(state.ReasoningTokenBudget)
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
	result.MaxTokens = clonePointer(value.MaxTokens)
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
			copyValue.Bytes = bytes.Clone(value.Bytes)
			copyValue.Blob = cloneBlob(value.Blob)
			result[i] = copyValue
		case llm.DocumentPart:
			copyValue := value
			copyValue.Bytes = bytes.Clone(value.Bytes)
			copyValue.Blob = cloneBlob(value.Blob)
			result[i] = copyValue
		case llm.JSONPart:
			result[i] = llm.JSONPart{Value: bytes.Clone(value.Value)}
		case llm.RefusalPart:
			result[i] = value
		case llm.ProviderStatePart:
			copyValue := value
			copyValue.Opaque = bytes.Clone(value.Opaque)
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
