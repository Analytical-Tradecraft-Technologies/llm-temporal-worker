package llm

import (
	"encoding/json"
	"fmt"
)

// GenerationPlanV1 is an authorized compaction decision. It contains no
// materialized transcript, provider identifiers, or reservation authority.
type GenerationPlanV1 struct {
	CompactBeforeGenerate bool              `json:"compact_before_generate"`
	EffectiveParent       *CheckpointHandle `json:"effective_parent,omitempty"`
}

func (plan *GenerationPlanV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "compact_before_generate", "effective_parent"); err != nil {
		return err
	}
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	value, err := requiredBool(fields, "compact_before_generate")
	if err != nil {
		return err
	}
	candidate := GenerationPlanV1{CompactBeforeGenerate: value}
	if raw, ok := fields["effective_parent"]; ok {
		var parent CheckpointHandle
		if err := json.Unmarshal(raw, &parent); err != nil || parent == "" {
			return fmt.Errorf("effective parent is invalid")
		}
		candidate.EffectiveParent = &parent
	}
	*plan = candidate
	return nil
}
func (plan GenerationPlanV1) MarshalJSON() ([]byte, error) {
	if plan.EffectiveParent != nil && *plan.EffectiveParent == "" {
		return nil, fmt.Errorf("effective parent is invalid")
	}
	type wire GenerationPlanV1
	return json.Marshal(wire(plan))
}

// GenerationResolutionV1 is a generation plan together with the request's
// completed result when preparation already holds one, as for an exact cache
// hit. Result is absent whenever the request still needs the internal request
// workflow, and never accompanies a plan that compacts first.
type GenerationResolutionV1 struct {
	Plan   GenerationPlanV1   `json:"plan"`
	Result *ExecutionResultV1 `json:"result,omitempty"`
}

func (resolution GenerationResolutionV1) validate() error {
	if resolution.Result == nil {
		return nil
	}
	if resolution.Plan.CompactBeforeGenerate || resolution.Plan.EffectiveParent != nil {
		return fmt.Errorf("a resolved generation cannot compact first")
	}
	if resolution.Result.Kind != "generate" || resolution.Result.State != ExecutionCompleted {
		return fmt.Errorf("a resolved generation must be a completed Generate result")
	}
	return resolution.Result.Validate()
}

func (resolution *GenerationResolutionV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "plan", "result"); err != nil {
		return err
	}
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	raw, ok := fields["plan"]
	if !ok {
		return fmt.Errorf("generation plan is required")
	}
	var candidate GenerationResolutionV1
	if err := json.Unmarshal(raw, &candidate.Plan); err != nil {
		return err
	}
	if raw, ok := fields["result"]; ok {
		var result ExecutionResultV1
		if err := json.Unmarshal(raw, &result); err != nil {
			return err
		}
		candidate.Result = &result
	}
	if err := candidate.validate(); err != nil {
		return err
	}
	*resolution = candidate
	return nil
}

func (resolution GenerationResolutionV1) MarshalJSON() ([]byte, error) {
	if err := resolution.validate(); err != nil {
		return nil, err
	}
	type wire GenerationResolutionV1
	return json.Marshal(wire(resolution))
}
