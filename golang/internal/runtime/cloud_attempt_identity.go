package runtime

import (
	"encoding/json"

	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// Independent paid attempts use distinct provider idempotency keys. The
// caller's public operation key remains unchanged in the original manifest.
// Records without an attempt link retain the existing single-attempt contract.
func cloudProviderOperationKey(record cloudstate.Record, original string) (string, error) {
	attempt, err := cloudRequestAttempt(record)
	if err != nil {
		return "", err
	}
	if attempt == nil {
		return original, nil
	}
	return "llmtw_attempt_" + string(attempt.ID), nil
}

func cloudRequestAttempt(record cloudstate.Record) (*cloudstate.RequestAttempt, error) {
	if len(record.Progress) == 0 {
		return nil, nil
	}
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil || progress == nil {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	data, present := progress["attempt_parent"]
	if !present {
		return nil, nil
	}
	var attempt cloudstate.RequestAttempt
	if json.Unmarshal(data, &attempt) != nil || attempt.Validate() != nil || attempt.ID != record.Request.ID || !attempt.CreatedAt.Equal(record.Request.CreatedAt) {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	return &attempt, nil
}
