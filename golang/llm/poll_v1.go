package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const PollAPIVersion = "llm.temporal/poll/v1"

// PendingOperationV1 identifies a persisted submission. It carries no credentials,
// URL, budget amount or transcript. The server must verify every field against
// the operation owned by PollRequestV1.Context before contacting the provider.
type PendingOperationV1 struct {
	OperationID         string `json:"operation_id"`
	Kind                string `json:"kind"`
	Provider            string `json:"provider"`
	EndpointID          string `json:"endpoint_id"`
	ProviderOperationID string `json:"provider_operation_id"`
}

func (p PendingOperationV1) Validate() error {
	if p.Kind != "generate" && p.Kind != "compact" {
		return fmt.Errorf("invalid pending operation kind")
	}
	for _, v := range []string{p.OperationID, p.Provider, p.EndpointID, p.ProviderOperationID} {
		if v == "" || len(v) > 512 || strings.ContainsAny(v, "\x00\r\n\t") {
			return fmt.Errorf("invalid pending operation identity")
		}
	}
	return nil
}

func marshalPendingV1(kind, key, id string, pending *PendingOperationV1) ([]byte, error) {
	if pending == nil || pending.Validate() != nil || pending.Kind != kind || pending.OperationID != id || key == "" {
		return nil, fmt.Errorf("invalid pending response")
	}
	version := APIVersion
	if kind == "compact" {
		version = CompactAPIVersion
	}
	return marshalObject(map[string]any{"api_version": version, "operation_key": key, "operation_id": id, "status": "pending", "pending": pending})
}

func unmarshalPendingV1(data []byte, kind string) (*PendingOperationV1, string, string, bool, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, "", "", false, err
	}
	var status string
	_ = json.Unmarshal(fields["status"], &status)
	if status != "pending" {
		return nil, "", "", false, nil
	}
	if err = checkUnknownFields(fields, "api_version", "operation_key", "operation_id", "status", "pending"); err != nil {
		return nil, "", "", true, err
	}
	version, _ := requiredString(fields, "api_version")
	expected := APIVersion
	if kind == "compact" {
		expected = CompactAPIVersion
	}
	if version != expected {
		return nil, "", "", true, fmt.Errorf("invalid pending api version")
	}
	key, _ := requiredString(fields, "operation_key")
	id, _ := requiredString(fields, "operation_id")
	var pending PendingOperationV1
	if err = decodeClosed(fields["pending"], &pending); err != nil {
		return nil, "", "", true, err
	}
	_, err = marshalPendingV1(kind, key, id, &pending)
	return &pending, key, id, true, err
}

// PollRequestV1 asks for at most one provider status check. A handle is not
// authorization: scope is resolved and verified by the runtime.
type PollRequestV1 struct {
	Context RequestContext     `json:"context"`
	Pending PendingOperationV1 `json:"pending"`
}

func (p PollRequestV1) Validate() error {
	if err := validateRequestContextV1(p.Context); err != nil {
		return err
	}
	return p.Pending.Validate()
}
func (p PollRequestV1) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	c, err := marshalRequestContextV1(p.Context)
	if err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"api_version": PollAPIVersion, "context": c, "pending": p.Pending})
}
func (p *PollRequestV1) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = checkUnknownFields(fields, "api_version", "context", "pending"); err != nil {
		return err
	}
	version, _ := requiredString(fields, "api_version")
	if version != PollAPIVersion {
		return fmt.Errorf("invalid poll api version")
	}
	c, err := decodeRequestContextV1(fields["context"])
	if err != nil {
		return err
	}
	var pending PendingOperationV1
	if err = decodeClosed(fields["pending"], &pending); err != nil {
		return err
	}
	result := PollRequestV1{Context: c, Pending: pending}
	if err = result.Validate(); err != nil {
		return err
	}
	*p = result
	return nil
}

// PollFailureV1 contains only stable, sanitized classifications. Transport
// failures are Activity errors; failed here means a terminal provider outcome.
type PollFailureV1 struct {
	Code        string `json:"code"`
	CostUnknown bool   `json:"cost_unknown"`
}
type PollResponseV1 struct {
	Status   string              `json:"status"`
	Pending  PendingOperationV1  `json:"pending"`
	Generate *GenerateResponseV1 `json:"generate,omitempty"`
	Compact  *CompactResponseV1  `json:"compact,omitempty"`
	Failure  *PollFailureV1      `json:"failure,omitempty"`
}

func (p PollResponseV1) Validate() error {
	if err := p.Pending.Validate(); err != nil {
		return err
	}
	switch p.Status {
	case "pending":
		if p.Generate != nil || p.Compact != nil || p.Failure != nil {
			return fmt.Errorf("pending poll has terminal fields")
		}
	case "failed":
		if p.Generate != nil || p.Compact != nil || p.Failure == nil || (p.Failure.Code != "provider_failed" && p.Failure.Code != "provider_cancelled" && p.Failure.Code != "result_unavailable") || !p.Failure.CostUnknown {
			return fmt.Errorf("invalid poll failure")
		}
	case "completed":
		if p.Failure != nil {
			return fmt.Errorf("completed poll has failure")
		}
		if p.Pending.Kind == "generate" {
			if p.Generate == nil || p.Compact != nil || p.Generate.Pending != nil || p.Generate.OperationID != p.Pending.OperationID {
				return fmt.Errorf("invalid poll Generate result")
			}
			_, err := json.Marshal(p.Generate)
			return err
		}
		if p.Compact == nil || p.Generate != nil || p.Compact.Pending != nil || p.Compact.OperationID != p.Pending.OperationID {
			return fmt.Errorf("invalid poll Compact result")
		}
		_, err := json.Marshal(p.Compact)
		return err
	default:
		return fmt.Errorf("invalid poll status")
	}
	return nil
}
func (p PollResponseV1) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	type wire PollResponseV1
	return json.Marshal(struct {
		APIVersion string `json:"api_version"`
		wire
	}{PollAPIVersion, wire(p)})
}
func (p *PollResponseV1) UnmarshalJSON(data []byte) error {
	type wire PollResponseV1
	var w struct {
		APIVersion string `json:"api_version"`
		wire
	}
	if err := decodeClosed(data, &w); err != nil {
		return err
	}
	if w.APIVersion != PollAPIVersion {
		return fmt.Errorf("invalid poll api version")
	}
	value := PollResponseV1(w.wire)
	if err := value.Validate(); err != nil {
		return err
	}
	*p = value
	return nil
}

func decodeClosed(data []byte, target any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	if len(data) == 0 || string(data) == "null" {
		return fmt.Errorf("object is required")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}
