package state

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// Patch keeps the three wire states distinct: a nil Set is omitted, Set is an
// explicit replacement, and Clear resets the value to its root default.  It
// intentionally does not use a pointer to a pointer, which makes collection
// replacement and clearing unambiguous to callers.
type Patch[T any] struct {
	Set   *T
	Clear bool
}

func SetPatch[T any](value T) Patch[T] { return Patch[T]{Set: &value} }

func ClearPatch[T any]() Patch[T] { return Patch[T]{Clear: true} }

func (patch Patch[T]) Omitted() bool { return patch.Set == nil && !patch.Clear }

func (patch Patch[T]) Validate() error {
	if patch.Set != nil && patch.Clear {
		return fmt.Errorf("patch cannot contain both set and clear")
	}
	return nil
}

// SettingsPatch is the in-memory form of the v1 sparse settings patch.  It is
// deliberately independent from the wire codec so materialization can be
// tested without dispatching an Activity.
type SettingsPatch struct {
	Model                 Patch[string]
	ServiceClass          Patch[llm.ServiceClass]
	ServiceClassFallbacks Patch[[]llm.ServiceClass]
	Portability           Patch[llm.PortabilityMode]
	Instructions          Patch[[]llm.Instruction]
	Tools                 Patch[[]llm.Tool]
	ToolPolicy            Patch[llm.ToolPolicy]
	Output                Patch[llm.OutputSpec]
	Temperature           Patch[float64]
	// TemperatureDecimal retains the exact v1 wire value alongside the
	// float64 provider projection. It is populated by the wire decoder and
	// takes precedence when the patch is encoded again.
	TemperatureDecimal Patch[llm.DecimalV1]
	ReasoningEffort    Patch[llm.ReasoningEffort]
	ReasoningSummary   Patch[llm.ReasoningSummary]
	CompactionPolicy   Patch[json.RawMessage]
	Extensions         Patch[map[string]json.RawMessage]
}

func (patch SettingsPatch) Validate() error {
	fields := []struct {
		name  string
		value interface{ Validate() error }
	}{
		{"model", patch.Model}, {"service_class", patch.ServiceClass},
		{"service_class_fallbacks", patch.ServiceClassFallbacks}, {"portability", patch.Portability},
		{"instructions", patch.Instructions}, {"tools", patch.Tools}, {"tool_policy", patch.ToolPolicy},
		{"output", patch.Output}, {"temperature", patch.Temperature},
		{"temperature_decimal", patch.TemperatureDecimal},
		{"reasoning_effort", patch.ReasoningEffort}, {"reasoning_summary", patch.ReasoningSummary},
		{"compaction_policy", patch.CompactionPolicy}, {"extensions", patch.Extensions},
	}
	for _, field := range fields {
		if err := field.value.Validate(); err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}
	}
	if patch.Temperature.Set != nil {
		value := *patch.Temperature.Set
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("temperature must be finite and non-negative")
		}
	}
	if patch.TemperatureDecimal.Set != nil {
		canonical, err := llm.NewDecimalV1(patch.TemperatureDecimal.Set.String())
		if err != nil {
			return fmt.Errorf("temperature_decimal: %w", err)
		}
		if _, err := canonical.Float64(); err != nil {
			return fmt.Errorf("temperature_decimal: %w", err)
		}
	}
	if patch.ServiceClass.Set != nil {
		if _, err := llm.NormalizeServiceClass(*patch.ServiceClass.Set); err != nil {
			return fmt.Errorf("service_class: %w", err)
		}
	}
	if patch.ServiceClassFallbacks.Set != nil {
		for index, value := range *patch.ServiceClassFallbacks.Set {
			if _, err := llm.NormalizeServiceClass(value); err != nil {
				return fmt.Errorf("service_class_fallbacks[%d]: %w", index, err)
			}
		}
	}
	if patch.Portability.Set != nil && !patch.Portability.Set.Valid() {
		return fmt.Errorf("portability %q is invalid", *patch.Portability.Set)
	}
	if patch.ReasoningEffort.Set != nil && !validReasoningEffort(*patch.ReasoningEffort.Set) {
		return fmt.Errorf("reasoning effort %q is invalid", *patch.ReasoningEffort.Set)
	}
	if patch.ReasoningSummary.Set != nil && !validReasoningSummary(*patch.ReasoningSummary.Set) {
		return fmt.Errorf("reasoning summary %q is invalid", *patch.ReasoningSummary.Set)
	}
	if err := validateJSONLeaf("tool_policy", patch.ToolPolicy.Set); err != nil {
		return err
	}
	if err := validateJSONLeaf("output", patch.Output.Set); err != nil {
		return err
	}
	if err := validateJSONLeaf("instructions", patch.Instructions.Set); err != nil {
		return err
	}
	if err := validateJSONLeaf("tools", patch.Tools.Set); err != nil {
		return err
	}
	if patch.Model.Set != nil && *patch.Model.Set == "" {
		return fmt.Errorf("model cannot be set to an empty value")
	}
	return nil
}

func validateJSONLeaf(name string, value any) error {
	if value == nil {
		return nil
	}
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func validReasoningEffort(value llm.ReasoningEffort) bool {
	switch value {
	case llm.ReasoningEffortProviderDefault, llm.ReasoningEffortMinimal, llm.ReasoningEffortLow, llm.ReasoningEffortMedium, llm.ReasoningEffortHigh, llm.ReasoningEffortMaximum:
		return true
	default:
		return false
	}
}

func validReasoningSummary(value llm.ReasoningSummary) bool {
	switch value {
	case llm.ReasoningSummaryProviderDefault, llm.ReasoningSummaryNone, llm.ReasoningSummaryAuto, llm.ReasoningSummaryConcise, llm.ReasoningSummaryDetailed:
		return true
	default:
		return false
	}
}
