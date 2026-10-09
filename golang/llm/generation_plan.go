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
