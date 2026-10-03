package llm

import "encoding/json"

// GenerationPlanV1 is an authorized compaction decision. It contains no
// materialized transcript, provider identifiers, or reservation authority.
type GenerationPlanV1 struct {
	CompactBeforeGenerate bool `json:"compact_before_generate"`
}

func (plan *GenerationPlanV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "compact_before_generate"); err != nil {
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
	*plan = GenerationPlanV1{CompactBeforeGenerate: value}
	return nil
}
func (plan GenerationPlanV1) MarshalJSON() ([]byte, error) {
	type wire GenerationPlanV1
	return json.Marshal(wire(plan))
}
