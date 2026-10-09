package llm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// PrepareExecutionV1 selects exactly one public request. Provider jobs and Redis
// receipts never cross this boundary; subsequent steps use the internal ID.
type PrepareExecutionV1 struct {
	Generate         *GenerateRequestV1  `json:"generate,omitempty"`
	Compact          *CompactRequestV1   `json:"compact,omitempty"`
	OriginalGenerate *GenerationOriginV1 `json:"original_generate,omitempty"`
}

// GenerationOriginV1 carries only the original parent, avoiding duplication of
// the public transcript in Temporal payloads. Preparation reconstructs the
// original request from the effective request and checks its saved manifest.
type GenerationOriginV1 struct {
	Parent *CheckpointHandle `json:"parent,omitempty"`
}

func (origin GenerationOriginV1) MarshalJSON() ([]byte, error) {
	if origin.Parent != nil && *origin.Parent == "" {
		return nil, fmt.Errorf("original parent is invalid")
	}
	type wire GenerationOriginV1
	return json.Marshal(wire(origin))
}

func (origin *GenerationOriginV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "parent"); err != nil {
		return err
	}
	type wire GenerationOriginV1
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	candidate := GenerationOriginV1(value)
	if _, err := candidate.MarshalJSON(); err != nil {
		return err
	}
	*origin = candidate
	return nil
}

func (request PrepareExecutionV1) MarshalJSON() ([]byte, error) {
	if (request.Generate == nil) == (request.Compact == nil) {
		return nil, fmt.Errorf("exactly one execution request is required")
	}
	if request.OriginalGenerate != nil && request.Generate == nil {
		return nil, fmt.Errorf("original Generate requires an effective Generate")
	}
	type wire PrepareExecutionV1
	return json.Marshal(wire(request))
}
func (request *PrepareExecutionV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "generate", "compact", "original_generate"); err != nil {
		return err
	}
	type wire PrepareExecutionV1
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	candidate := PrepareExecutionV1(value)
	if _, err := candidate.MarshalJSON(); err != nil {
		return err
	}
	*request = candidate
	return nil
}

// ExecutionReferenceV1 is scoped to the caller, not a bearer credential. Runtime
// implementations must authorize Context before looking up RequestID.
type ExecutionReferenceV1 struct {
	RequestID string
	Context   RequestContext
}

func (request ExecutionReferenceV1) MarshalJSON() ([]byte, error) {
	if !validExecutionRequestID(request.RequestID) {
		return nil, fmt.Errorf("execution request ID is invalid")
	}
	caller, err := marshalRequestContextV1(request.Context)
	if err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"request_id": request.RequestID, "context": caller})
}
func (request *ExecutionReferenceV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = checkUnknownFields(fields, "request_id", "context"); err != nil {
		return err
	}
	id, err := requiredString(fields, "request_id")
	if err != nil || !validExecutionRequestID(id) {
		return fmt.Errorf("execution request ID is invalid")
	}
	raw, err := requireField(fields, "context")
	if err != nil {
		return err
	}
	caller, err := decodeRequestContextV1(raw)
	if err != nil {
		return err
	}
	*request = ExecutionReferenceV1{RequestID: id, Context: caller}
	return nil
}

// ExecutionStateV1 tells a workflow which bounded step to run next. Waiting is
// always performed by a workflow timer, never inside a budget or poll activity.
type ExecutionStateV1 string

const (
	ExecutionBudgetRequired    ExecutionStateV1 = "budget_required"
	ExecutionAcquired          ExecutionStateV1 = "acquired"
	ExecutionBudgetWait        ExecutionStateV1 = "budget_wait"
	ExecutionCacheWait         ExecutionStateV1 = "cache_wait"
	ExecutionPending           ExecutionStateV1 = "pending"
	ExecutionProviderCompleted ExecutionStateV1 = "provider_completed"
	ExecutionCompleted         ExecutionStateV1 = "completed"
	ExecutionFailed            ExecutionStateV1 = "failed"
	ExecutionOutcomeUnknown    ExecutionStateV1 = "outcome_unknown"
)

// ExecutionResultV1 is a closed discriminated result. Completed responses retain
// the existing Generate/Compact v1 records; intermediate results carry no paid
// response, provider job identifier, reservation, or raw diagnostic message.
type ExecutionResultV1 struct {
	RequestID   string           `json:"request_id"`
	Kind        string           `json:"kind"`
	State       ExecutionStateV1 `json:"state"`
	FailureCode string           `json:"failure_code,omitempty"`
	// ErrorCode and Dispatch carry the provider failure's stable error code
	// (for example invalid_argument or rate_limited) and dispatch certainty,
	// so callers can tell failures apart without the provider's message.
	ErrorCode         string              `json:"error_code,omitempty"`
	Dispatch          string              `json:"dispatch,omitempty"`
	Retryable         bool                `json:"retryable,omitempty"`
	RetryAfterSeconds int32               `json:"retry_after_seconds,omitempty"`
	Generate          *GenerateResponseV1 `json:"generate,omitempty"`
	Compact           *CompactResponseV1  `json:"compact,omitempty"`
}

func (result ExecutionResultV1) Validate() error {
	if !validExecutionRequestID(result.RequestID) || (result.Kind != "generate" && result.Kind != "compact") {
		return fmt.Errorf("execution identity is invalid")
	}
	if result.State == ExecutionFailed {
		switch result.FailureCode {
		case "provider_error", "provider_rejected", "incomplete_response":
		default:
			return fmt.Errorf("execution failure code is invalid")
		}
		if result.ErrorCode != "" && !safeExecutionToken(result.ErrorCode) {
			return fmt.Errorf("execution error code is invalid")
		}
		switch result.Dispatch {
		case "", "not_dispatched", "rejected", "accepted", "ambiguous":
		default:
			return fmt.Errorf("execution dispatch is invalid")
		}
		// provider_rejected means the provider definitely did not accept the
		// request, so it is only valid with a definite dispatch.
		if result.FailureCode == "provider_rejected" && result.Dispatch != "not_dispatched" && result.Dispatch != "rejected" {
			return fmt.Errorf("provider_rejected requires a definite dispatch")
		}
	} else if result.FailureCode != "" || result.ErrorCode != "" || result.Dispatch != "" || result.Retryable {
		return fmt.Errorf("execution state cannot contain failure details")
	}
	wait := false
	switch result.State {
	case ExecutionBudgetWait, ExecutionCacheWait, ExecutionPending:
		wait = true
	case ExecutionBudgetRequired, ExecutionAcquired, ExecutionProviderCompleted, ExecutionCompleted, ExecutionFailed, ExecutionOutcomeUnknown:
	default:
		return fmt.Errorf("execution state is invalid")
	}
	if wait {
		if result.RetryAfterSeconds <= 0 || result.RetryAfterSeconds > 86400 {
			return fmt.Errorf("execution wait interval is invalid")
		}
	} else if result.RetryAfterSeconds != 0 {
		return fmt.Errorf("execution state does not accept a wait interval")
	}
	if result.State == ExecutionCompleted {
		if result.Kind == "generate" {
			if result.Generate == nil || result.Compact != nil {
				return fmt.Errorf("completed Generate result is required")
			}
			_, err := result.Generate.MarshalJSON()
			return err
		}
		if result.Compact == nil || result.Generate != nil {
			return fmt.Errorf("completed Compact result is required")
		}
		_, err := result.Compact.MarshalJSON()
		return err
	}
	if result.Generate != nil || result.Compact != nil {
		return fmt.Errorf("nonterminal execution cannot expose a response")
	}
	return nil
}
func (result ExecutionResultV1) MarshalJSON() ([]byte, error) {
	if err := result.Validate(); err != nil {
		return nil, err
	}
	type wire ExecutionResultV1
	return json.Marshal(wire(result))
}
func (result *ExecutionResultV1) UnmarshalJSON(data []byte) error {
	if err := executionFields(data, "request_id", "kind", "state", "retry_after_seconds", "failure_code", "error_code", "dispatch", "retryable", "generate", "compact"); err != nil {
		return err
	}
	type wire ExecutionResultV1
	var value wire
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	candidate := ExecutionResultV1(value)
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	forbidden := []string{}
	if candidate.State != ExecutionBudgetWait && candidate.State != ExecutionCacheWait && candidate.State != ExecutionPending {
		forbidden = append(forbidden, "retry_after_seconds")
	}
	if candidate.State != ExecutionFailed {
		forbidden = append(forbidden, "failure_code", "error_code", "dispatch", "retryable")
	}
	for _, name := range forbidden {
		if _, present := fields[name]; present {
			return fmt.Errorf("execution field is not valid in this state")
		}
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	*result = candidate
	return nil
}

// safeExecutionToken accepts a bounded lowercase identifier such as a
// provider error code.
func safeExecutionToken(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && r != '_' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func validExecutionRequestID(id string) bool {
	const prefix = "llmtw_req_"
	if !strings.HasPrefix(id, prefix) {
		return false
	}
	parsed, err := uuid.Parse(strings.TrimPrefix(id, prefix))
	return err == nil && parsed != uuid.Nil && prefix+parsed.String() == id
}
func executionFields(data []byte, allowed ...string) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, allowed...); err != nil {
		return err
	}
	for _, value := range fields {
		if string(value) == "null" {
			return fmt.Errorf("execution fields cannot be null")
		}
	}
	return nil
}
