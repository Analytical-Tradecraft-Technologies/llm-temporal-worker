package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	ResourceCapacityAcquireAPIVersion   = "llm.resource_capacity.acquire/v1"
	ResourceCapacityRenewAPIVersion     = "llm.resource_capacity.renew/v1"
	ResourceCapacityReleaseAPIVersion   = "llm.resource_capacity.release/v1"
	ResourceCapacityAcquireActivityName = "llm.resource_capacity.acquire.v1"
	ResourceCapacityRenewActivityName   = "llm.resource_capacity.renew.v1"
	ResourceCapacityReleaseActivityName = "llm.resource_capacity.release.v1"
	ResourceClassPythonStage2           = "python-stage2"
	ResourceClassForecastEvent          = "forecast-event"
)

var resourceCapacityDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type ResourceCapacityAcquireRequestV1 struct {
	APIVersion     string         `json:"api_version"`
	Context        RequestContext `json:"context"`
	ResourceClass  string         `json:"resource_class"`
	LeaseKey       string         `json:"lease_key"`
	GenerationID   string         `json:"generation_id"`
	ManifestSHA256 string         `json:"manifest_sha256"`
	QueueDeadline  time.Time      `json:"queue_deadline"`
}

type ResourceCapacityLeaseV1 struct {
	APIVersion     string         `json:"api_version"`
	Context        RequestContext `json:"context"`
	ResourceClass  string         `json:"resource_class"`
	LeaseKey       string         `json:"lease_key"`
	LeaseID        string         `json:"lease_id"`
	GenerationID   string         `json:"generation_id"`
	ManifestSHA256 string         `json:"manifest_sha256"`
	QueueDeadline  time.Time      `json:"queue_deadline"`
	AcquiredAt     time.Time      `json:"acquired_at"`
	ExpiresAt      time.Time      `json:"expires_at"`
}

type ResourceCapacityRenewRequestV1 struct {
	APIVersion string                  `json:"api_version"`
	Lease      ResourceCapacityLeaseV1 `json:"lease"`
}

type ResourceCapacityReleaseRequestV1 struct {
	APIVersion string                  `json:"api_version"`
	Lease      ResourceCapacityLeaseV1 `json:"lease"`
}

type ResourceCapacityReleaseResponseV1 struct {
	APIVersion string `json:"api_version"`
	LeaseID    string `json:"lease_id"`
	Released   bool   `json:"released"`
}

func (v ResourceCapacityAcquireRequestV1) Validate() error {
	if v.APIVersion != ResourceCapacityAcquireAPIVersion {
		return fmt.Errorf("resource capacity acquire api_version %q is unsupported", v.APIVersion)
	}
	if v.Context.empty() {
		return errors.New("resource capacity context is required")
	}
	if v.ResourceClass != ResourceClassPythonStage2 && v.ResourceClass != ResourceClassForecastEvent {
		return errors.New("resource capacity class is unsupported")
	}
	if err := validateCapacityIdentifier("lease_key", v.LeaseKey); err != nil {
		return err
	}
	if err := validateCapacityIdentifier("generation_id", v.GenerationID); err != nil {
		return err
	}
	if !resourceCapacityDigestPattern.MatchString(v.ManifestSHA256) {
		return errors.New("resource capacity manifest_sha256 is invalid")
	}
	if v.QueueDeadline.IsZero() {
		return errors.New("resource capacity queue_deadline is required")
	}
	return nil
}

func (v ResourceCapacityLeaseV1) Validate() error {
	if v.APIVersion != ResourceCapacityAcquireAPIVersion {
		return errors.New("resource capacity lease api_version is unsupported")
	}
	request := ResourceCapacityAcquireRequestV1{APIVersion: v.APIVersion, Context: v.Context, ResourceClass: v.ResourceClass, LeaseKey: v.LeaseKey, GenerationID: v.GenerationID, ManifestSHA256: v.ManifestSHA256, QueueDeadline: v.QueueDeadline}
	if err := request.Validate(); err != nil {
		return err
	}
	if !resourceCapacityDigestPattern.MatchString(v.LeaseID) {
		return errors.New("resource capacity lease_id is invalid")
	}
	if v.AcquiredAt.IsZero() || v.ExpiresAt.IsZero() || !v.ExpiresAt.After(v.AcquiredAt) {
		return errors.New("resource capacity lease timestamps are invalid")
	}
	return nil
}

func (v ResourceCapacityRenewRequestV1) Validate() error {
	if v.APIVersion != ResourceCapacityRenewAPIVersion {
		return errors.New("resource capacity renew api_version is unsupported")
	}
	return v.Lease.Validate()
}
func (v ResourceCapacityReleaseRequestV1) Validate() error {
	if v.APIVersion != ResourceCapacityReleaseAPIVersion {
		return errors.New("resource capacity release api_version is unsupported")
	}
	return v.Lease.Validate()
}
func (v ResourceCapacityReleaseResponseV1) ValidateFor(request ResourceCapacityReleaseRequestV1) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if v.APIVersion != ResourceCapacityReleaseAPIVersion || v.LeaseID != request.Lease.LeaseID || !v.Released {
		return errors.New("resource capacity release receipt is invalid")
	}
	return nil
}

func validateCapacityIdentifier(name, value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 255 || strings.ContainsAny(value, "\x00\r\n\t") {
		return fmt.Errorf("resource capacity %s is invalid", name)
	}
	return nil
}

func (v ResourceCapacityAcquireRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	type plain ResourceCapacityAcquireRequestV1
	return json.Marshal(plain(v))
}
func (v *ResourceCapacityAcquireRequestV1) UnmarshalJSON(data []byte) error {
	type plain ResourceCapacityAcquireRequestV1
	var p plain
	if err := decodeCapacityClosed(data, &p, "api_version", "context", "resource_class", "lease_key", "generation_id", "manifest_sha256", "queue_deadline"); err != nil {
		return err
	}
	*v = ResourceCapacityAcquireRequestV1(p)
	return v.Validate()
}
func (v ResourceCapacityLeaseV1) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	type plain ResourceCapacityLeaseV1
	return json.Marshal(plain(v))
}
func (v *ResourceCapacityLeaseV1) UnmarshalJSON(data []byte) error {
	type plain ResourceCapacityLeaseV1
	var p plain
	if err := decodeCapacityClosed(data, &p, "api_version", "context", "resource_class", "lease_key", "lease_id", "generation_id", "manifest_sha256", "queue_deadline", "acquired_at", "expires_at"); err != nil {
		return err
	}
	*v = ResourceCapacityLeaseV1(p)
	return v.Validate()
}
func (v ResourceCapacityRenewRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	type plain ResourceCapacityRenewRequestV1
	return json.Marshal(plain(v))
}
func (v *ResourceCapacityRenewRequestV1) UnmarshalJSON(data []byte) error {
	type plain ResourceCapacityRenewRequestV1
	var p plain
	if err := decodeCapacityClosed(data, &p, "api_version", "lease"); err != nil {
		return err
	}
	*v = ResourceCapacityRenewRequestV1(p)
	return v.Validate()
}
func (v ResourceCapacityReleaseRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	type plain ResourceCapacityReleaseRequestV1
	return json.Marshal(plain(v))
}
func (v *ResourceCapacityReleaseRequestV1) UnmarshalJSON(data []byte) error {
	type plain ResourceCapacityReleaseRequestV1
	var p plain
	if err := decodeCapacityClosed(data, &p, "api_version", "lease"); err != nil {
		return err
	}
	*v = ResourceCapacityReleaseRequestV1(p)
	return v.Validate()
}
func (v ResourceCapacityReleaseResponseV1) MarshalJSON() ([]byte, error) {
	if v.APIVersion != ResourceCapacityReleaseAPIVersion || !resourceCapacityDigestPattern.MatchString(v.LeaseID) || !v.Released {
		return nil, errors.New("resource capacity release receipt is invalid")
	}
	type plain ResourceCapacityReleaseResponseV1
	return json.Marshal(plain(v))
}
func (v *ResourceCapacityReleaseResponseV1) UnmarshalJSON(data []byte) error {
	type plain ResourceCapacityReleaseResponseV1
	var p plain
	if err := decodeCapacityClosed(data, &p, "api_version", "lease_id", "released"); err != nil {
		return err
	}
	*v = ResourceCapacityReleaseResponseV1(p)
	if v.APIVersion != ResourceCapacityReleaseAPIVersion || !resourceCapacityDigestPattern.MatchString(v.LeaseID) || !v.Released {
		return errors.New("resource capacity release receipt is invalid")
	}
	return nil
}

func decodeCapacityClosed(data []byte, target any, allowed ...string) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	set := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		set[name] = struct{}{}
	}
	for name := range fields {
		if _, ok := set[name]; !ok {
			return fmt.Errorf("unknown field %q", name)
		}
	}
	type alias map[string]json.RawMessage
	encoded, err := json.Marshal(alias(fields))
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}
