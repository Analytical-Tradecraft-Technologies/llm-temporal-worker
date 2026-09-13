package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const (
	ReserveBatchAPIVersion          = "llm.cost.reserve_batch/v1"
	ReserveBatchActivityName        = "llm.cost.reserve_batch.v1"
	AllocateBatchGrantsAPIVersion   = "llm.cost.allocate_batch_grants/v1"
	AllocateBatchGrantsActivityName = "llm.cost.allocate_batch_grants.v1"
	CloseBatchAPIVersion            = "llm.cost.close_batch/v1"
	CloseBatchActivityName          = "llm.cost.close_batch.v1"
	MaxReserveBatchOperations       = 1024
	MaxReserveBatchTemplateCount    = 64
)

type ReserveBatchStatus string

const (
	ReserveBatchStatusReserved ReserveBatchStatus = "reserved"
	ReserveBatchStatusDenied   ReserveBatchStatus = "denied"
	ReserveBatchStatusEscrowed ReserveBatchStatus = "escrowed"
)

type ReserveBatchOperationV1 struct {
	OperationKey          string         `json:"operation_key"`
	Model                 string         `json:"model"`
	ServiceClass          ServiceClass   `json:"service_class"`
	ServiceClassFallbacks []ServiceClass `json:"service_class_fallbacks"`
	MaxInputTokens        int64          `json:"max_input_tokens"`
	MaxOutputTokens       int64          `json:"max_output_tokens"`
	MaxReasoningTokens    int64          `json:"max_reasoning_tokens"`
	MaxCacheReadTokens    int64          `json:"max_cache_read_tokens"`
	MaxCacheWriteTokens   int64          `json:"max_cache_write_tokens"`
}

func (v ReserveBatchOperationV1) validate() error {
	if err := reserveBatchID("operation_key", v.OperationKey); err != nil {
		return err
	}
	if err := reserveBatchID("model", v.Model); err != nil {
		return err
	}
	if !v.ServiceClass.Valid() {
		return fmt.Errorf("service_class %q is invalid", v.ServiceClass)
	}
	if len(v.ServiceClassFallbacks) > 2 {
		return fmt.Errorf("service_class_fallbacks must contain at most 2 values")
	}
	if err := ValidateServiceClassFallbacks(v.ServiceClass, v.ServiceClassFallbacks); err != nil {
		return err
	}
	if v.MaxInputTokens <= 0 || v.MaxOutputTokens <= 0 {
		return fmt.Errorf("max_input_tokens and max_output_tokens must be positive")
	}
	if v.MaxReasoningTokens < 0 || v.MaxCacheReadTokens < 0 || v.MaxCacheWriteTokens < 0 {
		return fmt.Errorf("reasoning and cache token bounds must not be negative")
	}
	return nil
}
func (v ReserveBatchOperationV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	fallbacks := v.ServiceClassFallbacks
	if fallbacks == nil {
		fallbacks = []ServiceClass{}
	}
	return marshalObject(map[string]any{"operation_key": v.OperationKey, "model": v.Model, "service_class": v.ServiceClass, "service_class_fallbacks": fallbacks, "max_input_tokens": v.MaxInputTokens, "max_output_tokens": v.MaxOutputTokens, "max_reasoning_tokens": v.MaxReasoningTokens, "max_cache_read_tokens": v.MaxCacheReadTokens, "max_cache_write_tokens": v.MaxCacheWriteTokens})
}
func (v *ReserveBatchOperationV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("reserve batch operation must be an object")
	}
	if err = checkUnknownFields(f, "operation_key", "model", "service_class", "service_class_fallbacks", "max_input_tokens", "max_output_tokens", "max_reasoning_tokens", "max_cache_read_tokens", "max_cache_write_tokens"); err != nil {
		return err
	}
	var out ReserveBatchOperationV1
	if out.OperationKey, err = requiredString(f, "operation_key"); err != nil {
		return err
	}
	if out.Model, err = requiredString(f, "model"); err != nil {
		return err
	}
	class, err := requiredString(f, "service_class")
	if err != nil {
		return err
	}
	out.ServiceClass = ServiceClass(class)
	raw, err := requireField(f, "service_class_fallbacks")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.ServiceClassFallbacks); err != nil || out.ServiceClassFallbacks == nil {
		return fmt.Errorf("service_class_fallbacks must be an array")
	}
	for name, dst := range map[string]*int64{"max_input_tokens": &out.MaxInputTokens, "max_output_tokens": &out.MaxOutputTokens, "max_reasoning_tokens": &out.MaxReasoningTokens, "max_cache_read_tokens": &out.MaxCacheReadTokens, "max_cache_write_tokens": &out.MaxCacheWriteTokens} {
		raw, err = requireField(f, name)
		if err != nil {
			return err
		}
		*dst, err = decodeInt64(raw)
		if err != nil {
			return err
		}
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}
func (v ReserveBatchOperationV1) OperationSHA256() (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return reserveBatchDigest(b), nil
}

// ReserveBatchTemplateV1 reserves a bounded number of operations before their
// exact operation keys are known.
type ReserveBatchTemplateV1 struct {
	TemplateKey           string         `json:"template_key"`
	Count                 int32          `json:"count"`
	Model                 string         `json:"model"`
	ServiceClass          ServiceClass   `json:"service_class"`
	ServiceClassFallbacks []ServiceClass `json:"service_class_fallbacks"`
	MaxInputTokens        int64          `json:"max_input_tokens"`
	MaxOutputTokens       int64          `json:"max_output_tokens"`
	MaxReasoningTokens    int64          `json:"max_reasoning_tokens"`
	MaxCacheReadTokens    int64          `json:"max_cache_read_tokens"`
	MaxCacheWriteTokens   int64          `json:"max_cache_write_tokens"`
}

func (v ReserveBatchTemplateV1) operation() ReserveBatchOperationV1 {
	return ReserveBatchOperationV1{OperationKey: v.TemplateKey, Model: v.Model, ServiceClass: v.ServiceClass, ServiceClassFallbacks: v.ServiceClassFallbacks, MaxInputTokens: v.MaxInputTokens, MaxOutputTokens: v.MaxOutputTokens, MaxReasoningTokens: v.MaxReasoningTokens, MaxCacheReadTokens: v.MaxCacheReadTokens, MaxCacheWriteTokens: v.MaxCacheWriteTokens}
}
func (v ReserveBatchTemplateV1) validate() error {
	if v.Count < 1 || v.Count > MaxReserveBatchTemplateCount {
		return fmt.Errorf("template count must be between 1 and %d", MaxReserveBatchTemplateCount)
	}
	if err := v.operation().validate(); err != nil {
		return err
	}
	return nil
}
func (v ReserveBatchTemplateV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	fallbacks := v.ServiceClassFallbacks
	if fallbacks == nil {
		fallbacks = []ServiceClass{}
	}
	return marshalObject(map[string]any{"template_key": v.TemplateKey, "count": v.Count, "model": v.Model, "service_class": v.ServiceClass, "service_class_fallbacks": fallbacks, "max_input_tokens": v.MaxInputTokens, "max_output_tokens": v.MaxOutputTokens, "max_reasoning_tokens": v.MaxReasoningTokens, "max_cache_read_tokens": v.MaxCacheReadTokens, "max_cache_write_tokens": v.MaxCacheWriteTokens})
}
func (v *ReserveBatchTemplateV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("reserve batch template must be an object")
	}
	if err = checkUnknownFields(f, "template_key", "count", "model", "service_class", "service_class_fallbacks", "max_input_tokens", "max_output_tokens", "max_reasoning_tokens", "max_cache_read_tokens", "max_cache_write_tokens"); err != nil {
		return err
	}
	var out ReserveBatchTemplateV1
	if out.TemplateKey, err = requiredString(f, "template_key"); err != nil {
		return err
	}
	if out.Model, err = requiredString(f, "model"); err != nil {
		return err
	}
	class, err := requiredString(f, "service_class")
	if err != nil {
		return err
	}
	out.ServiceClass = ServiceClass(class)
	raw, err := requireField(f, "count")
	if err != nil {
		return err
	}
	count, err := decodeInt32(raw)
	if err != nil {
		return err
	}
	out.Count = count
	raw, err = requireField(f, "service_class_fallbacks")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.ServiceClassFallbacks); err != nil || out.ServiceClassFallbacks == nil {
		return fmt.Errorf("service_class_fallbacks must be an array")
	}
	for name, dst := range map[string]*int64{"max_input_tokens": &out.MaxInputTokens, "max_output_tokens": &out.MaxOutputTokens, "max_reasoning_tokens": &out.MaxReasoningTokens, "max_cache_read_tokens": &out.MaxCacheReadTokens, "max_cache_write_tokens": &out.MaxCacheWriteTokens} {
		raw, err = requireField(f, name)
		if err != nil {
			return err
		}
		*dst, err = decodeInt64(raw)
		if err != nil {
			return err
		}
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}

type ReserveBatchRequestV1 struct {
	APIVersion                 string                    `json:"api_version"`
	Context                    RequestContext            `json:"context"`
	CustomerID                 string                    `json:"customer_id"`
	RunID                      string                    `json:"run_id"`
	BudgetID                   string                    `json:"budget_id"`
	BatchKey                   string                    `json:"batch_key"`
	PhaseKey                   string                    `json:"phase_key,omitempty"`
	PhaseExpiresAt             time.Time                 `json:"phase_expires_at,omitempty"`
	PricingGenerationID        string                    `json:"pricing_generation_id"`
	PricingManifestSHA256      string                    `json:"pricing_manifest_sha256"`
	RemainingMaxCostMicrounits int64                     `json:"remaining_max_cost_microunits"`
	Operations                 []ReserveBatchOperationV1 `json:"operations,omitempty"`
	Templates                  []ReserveBatchTemplateV1  `json:"templates,omitempty"`
}

func (v ReserveBatchRequestV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != ReserveBatchAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	if v.Context.Tenant == "" || v.Context.Project == "" || v.Context.Actor == "" {
		return fmt.Errorf("context requires tenant, project, and actor")
	}
	for n, s := range map[string]string{"customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "batch_key": v.BatchKey, "pricing_generation_id": v.PricingGenerationID} {
		if err := reserveBatchID(n, s); err != nil {
			return err
		}
	}
	if err := reserveBatchHash("pricing_manifest_sha256", v.PricingManifestSHA256); err != nil {
		return err
	}
	if v.RemainingMaxCostMicrounits < 0 {
		return fmt.Errorf("remaining_max_cost_microunits must not be negative")
	}
	direct, templated := len(v.Operations) > 0, len(v.Templates) > 0
	if direct == templated {
		return fmt.Errorf("exactly one of operations or templates is required")
	}
	if direct {
		if v.PhaseKey != "" || !v.PhaseExpiresAt.IsZero() {
			return fmt.Errorf("phase fields are only valid with templates")
		}
		if len(v.Operations) > MaxReserveBatchOperations {
			return fmt.Errorf("operations must contain at most %d descriptors", MaxReserveBatchOperations)
		}
		seen := map[string]struct{}{}
		for i, op := range v.Operations {
			if err := op.validate(); err != nil {
				return fmt.Errorf("operations[%d]: %w", i, err)
			}
			if _, ok := seen[op.OperationKey]; ok {
				return fmt.Errorf("duplicate operation_key %q", op.OperationKey)
			}
			seen[op.OperationKey] = struct{}{}
		}
		return nil
	}
	if err := reserveBatchID("phase_key", v.PhaseKey); err != nil {
		return err
	}
	if err := reserveBatchTime("phase_expires_at", v.PhaseExpiresAt); err != nil {
		return err
	}
	if len(v.Templates) > MaxReserveBatchOperations {
		return fmt.Errorf("templates must contain at most %d descriptors", MaxReserveBatchOperations)
	}
	seen, total := map[string]struct{}{}, 0
	for i, t := range v.Templates {
		if err := t.validate(); err != nil {
			return fmt.Errorf("templates[%d]: %w", i, err)
		}
		if _, ok := seen[t.TemplateKey]; ok {
			return fmt.Errorf("duplicate template_key %q", t.TemplateKey)
		}
		seen[t.TemplateKey] = struct{}{}
		total += int(t.Count)
		if total > MaxReserveBatchOperations {
			return fmt.Errorf("template counts exceed %d operations", MaxReserveBatchOperations)
		}
	}
	return nil
}
func (v ReserveBatchRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	fields := map[string]any{"api_version": ReserveBatchAPIVersion, "context": v.Context, "customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "batch_key": v.BatchKey, "pricing_generation_id": v.PricingGenerationID, "pricing_manifest_sha256": v.PricingManifestSHA256, "remaining_max_cost_microunits": v.RemainingMaxCostMicrounits}
	if len(v.Operations) > 0 {
		fields["operations"] = v.Operations
	} else {
		fields["phase_key"] = v.PhaseKey
		fields["phase_expires_at"] = v.PhaseExpiresAt.UTC()
		fields["templates"] = v.Templates
	}
	return marshalObject(fields)
}
func (v *ReserveBatchRequestV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("reserve batch request must be an object")
	}
	if err = checkUnknownFields(f, "api_version", "context", "customer_id", "run_id", "budget_id", "batch_key", "phase_key", "phase_expires_at", "pricing_generation_id", "pricing_manifest_sha256", "remaining_max_cost_microunits", "operations", "templates"); err != nil {
		return err
	}
	var out ReserveBatchRequestV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != ReserveBatchAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", out.APIVersion)
	}
	raw, err := requireField(f, "context")
	if err != nil {
		return err
	}
	if out.Context, err = decodeRequestContext(raw); err != nil {
		return err
	}
	for n, d := range map[string]*string{"customer_id": &out.CustomerID, "run_id": &out.RunID, "budget_id": &out.BudgetID, "batch_key": &out.BatchKey, "pricing_generation_id": &out.PricingGenerationID, "pricing_manifest_sha256": &out.PricingManifestSHA256} {
		if *d, err = requiredString(f, n); err != nil {
			return err
		}
	}
	out.PhaseKey, _, err = optionalString(f, "phase_key")
	if err != nil {
		return err
	}
	if raw, ok := f["phase_expires_at"]; ok {
		if err = json.Unmarshal(raw, &out.PhaseExpiresAt); err != nil {
			return fmt.Errorf("phase_expires_at must be RFC3339")
		}
	}
	raw, err = requireField(f, "remaining_max_cost_microunits")
	if err != nil {
		return err
	}
	if out.RemainingMaxCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	if raw, ok := f["operations"]; ok {
		if err = json.Unmarshal(raw, &out.Operations); err != nil || out.Operations == nil {
			return fmt.Errorf("operations must be an array")
		}
	}
	if raw, ok := f["templates"]; ok {
		if err = json.Unmarshal(raw, &out.Templates); err != nil || out.Templates == nil {
			return fmt.Errorf("templates must be an array")
		}
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}
func (v ReserveBatchRequestV1) RequestSHA256() (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return reserveBatchDigest(b), nil
}

type ReserveBatchGrantV1 struct {
	OperationKey             string    `json:"operation_key"`
	OperationSHA256          string    `json:"operation_sha256"`
	OperationID              string    `json:"operation_id"`
	GrantID                  string    `json:"grant_id"`
	GrantSHA256              string    `json:"grant_sha256"`
	GrantKeyID               string    `json:"grant_key_id"`
	GrantHMACSHA256          string    `json:"grant_hmac_sha256"`
	MaxCostMicrounits        int64     `json:"max_cost_microunits"`
	ReservationGenerationID  string    `json:"reservation_generation_id"`
	ReservationIncarnationID string    `json:"reservation_incarnation_id"`
	ReservationExpiresAt     time.Time `json:"reservation_expires_at"`
}

func (v ReserveBatchGrantV1) validate() error {
	for n, s := range map[string]string{"operation_key": v.OperationKey, "operation_id": v.OperationID, "grant_id": v.GrantID, "grant_key_id": v.GrantKeyID, "reservation_generation_id": v.ReservationGenerationID, "reservation_incarnation_id": v.ReservationIncarnationID} {
		if err := reserveBatchID(n, s); err != nil {
			return err
		}
	}
	for n, s := range map[string]string{"operation_sha256": v.OperationSHA256, "grant_sha256": v.GrantSHA256, "grant_hmac_sha256": v.GrantHMACSHA256} {
		if err := reserveBatchHash(n, s); err != nil {
			return err
		}
	}
	if v.MaxCostMicrounits < 0 {
		return fmt.Errorf("max_cost_microunits must not be negative")
	}
	if v.ReservationExpiresAt.IsZero() {
		return fmt.Errorf("reservation_expires_at is required")
	}
	_, off := v.ReservationExpiresAt.Zone()
	if off != 0 {
		return fmt.Errorf("reservation_expires_at must use UTC")
	}
	return nil
}
func (v ReserveBatchGrantV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"operation_key": v.OperationKey, "operation_sha256": v.OperationSHA256, "operation_id": v.OperationID, "grant_id": v.GrantID, "grant_sha256": v.GrantSHA256, "grant_key_id": v.GrantKeyID, "grant_hmac_sha256": v.GrantHMACSHA256, "max_cost_microunits": v.MaxCostMicrounits, "reservation_generation_id": v.ReservationGenerationID, "reservation_incarnation_id": v.ReservationIncarnationID, "reservation_expires_at": v.ReservationExpiresAt.UTC()})
}
func (v *ReserveBatchGrantV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("reserve batch grant must be an object")
	}
	if err = checkUnknownFields(f, "operation_key", "operation_sha256", "operation_id", "grant_id", "grant_sha256", "grant_key_id", "grant_hmac_sha256", "max_cost_microunits", "reservation_generation_id", "reservation_incarnation_id", "reservation_expires_at"); err != nil {
		return err
	}
	var out ReserveBatchGrantV1
	for n, d := range map[string]*string{"operation_key": &out.OperationKey, "operation_sha256": &out.OperationSHA256, "operation_id": &out.OperationID, "grant_id": &out.GrantID, "grant_sha256": &out.GrantSHA256, "grant_key_id": &out.GrantKeyID, "grant_hmac_sha256": &out.GrantHMACSHA256, "reservation_generation_id": &out.ReservationGenerationID, "reservation_incarnation_id": &out.ReservationIncarnationID} {
		if *d, err = requiredString(f, n); err != nil {
			return err
		}
	}
	raw, err := requireField(f, "max_cost_microunits")
	if err != nil {
		return err
	}
	if out.MaxCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	raw, err = requireField(f, "reservation_expires_at")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.ReservationExpiresAt); err != nil {
		return fmt.Errorf("reservation_expires_at must be RFC3339")
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}

type ReserveBatchResponseV1 struct {
	APIVersion             string                `json:"api_version"`
	BatchID                string                `json:"batch_id"`
	RequestSHA256          string                `json:"request_sha256"`
	BatchSHA256            string                `json:"batch_sha256"`
	Status                 ReserveBatchStatus    `json:"status"`
	EscrowID               string                `json:"escrow_id,omitempty"`
	EscrowSHA256           string                `json:"escrow_sha256,omitempty"`
	ReservedCostMicrounits int64                 `json:"reserved_cost_microunits"`
	Grants                 []ReserveBatchGrantV1 `json:"grants"`
}

func (v ReserveBatchResponseV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != ReserveBatchAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	if err := reserveBatchID("batch_id", v.BatchID); err != nil {
		return err
	}
	if err := reserveBatchHash("request_sha256", v.RequestSHA256); err != nil {
		return err
	}
	if err := reserveBatchHash("batch_sha256", v.BatchSHA256); err != nil {
		return err
	}
	if v.ReservedCostMicrounits < 0 {
		return fmt.Errorf("reserved_cost_microunits must not be negative")
	}
	switch v.Status {
	case ReserveBatchStatusDenied:
		if v.ReservedCostMicrounits != 0 || len(v.Grants) != 0 || v.EscrowID != "" || v.EscrowSHA256 != "" {
			return fmt.Errorf("denied response must have zero cost, no grants, and no escrow")
		}
		return nil
	case ReserveBatchStatusEscrowed:
		if len(v.Grants) != 0 {
			return fmt.Errorf("escrowed response must not contain grants")
		}
		if err := reserveBatchID("escrow_id", v.EscrowID); err != nil {
			return err
		}
		return reserveBatchHash("escrow_sha256", v.EscrowSHA256)
	case ReserveBatchStatusReserved:
		if v.EscrowID != "" || v.EscrowSHA256 != "" {
			return fmt.Errorf("reserved response must not contain escrow identity")
		}
	default:
		return fmt.Errorf("invalid reserve batch status %q", v.Status)
	}
	if len(v.Grants) < 1 || len(v.Grants) > MaxReserveBatchOperations {
		return fmt.Errorf("invalid grant count")
	}
	var total int64
	seen := map[string]struct{}{}
	for _, g := range v.Grants {
		if err := g.validate(); err != nil {
			return err
		}
		if _, ok := seen[g.OperationKey]; ok {
			return fmt.Errorf("duplicate grant operation_key")
		}
		seen[g.OperationKey] = struct{}{}
		if g.MaxCostMicrounits > math.MaxInt64-total {
			return fmt.Errorf("grant cost overflow")
		}
		total += g.MaxCostMicrounits
	}
	if total != v.ReservedCostMicrounits {
		return fmt.Errorf("reserved cost must equal grant sum")
	}
	return nil
}
func (v ReserveBatchResponseV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	g := v.Grants
	if g == nil {
		g = []ReserveBatchGrantV1{}
	}
	fields := map[string]any{"api_version": ReserveBatchAPIVersion, "batch_id": v.BatchID, "request_sha256": v.RequestSHA256, "batch_sha256": v.BatchSHA256, "status": v.Status, "reserved_cost_microunits": v.ReservedCostMicrounits, "grants": g}
	if v.EscrowID != "" {
		fields["escrow_id"] = v.EscrowID
		fields["escrow_sha256"] = v.EscrowSHA256
	}
	return marshalObject(fields)
}
func (v *ReserveBatchResponseV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = checkUnknownFields(f, "api_version", "batch_id", "request_sha256", "batch_sha256", "status", "escrow_id", "escrow_sha256", "reserved_cost_microunits", "grants"); err != nil {
		return err
	}
	var out ReserveBatchResponseV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != ReserveBatchAPIVersion {
		return fmt.Errorf("unsupported api_version")
	}
	if out.BatchID, err = requiredString(f, "batch_id"); err != nil {
		return err
	}
	if out.RequestSHA256, err = requiredString(f, "request_sha256"); err != nil {
		return err
	}
	if out.BatchSHA256, err = requiredString(f, "batch_sha256"); err != nil {
		return err
	}
	s, err := requiredString(f, "status")
	if err != nil {
		return err
	}
	out.Status = ReserveBatchStatus(s)
	out.EscrowID, _, err = optionalString(f, "escrow_id")
	if err != nil {
		return err
	}
	out.EscrowSHA256, _, err = optionalString(f, "escrow_sha256")
	if err != nil {
		return err
	}
	raw, err := requireField(f, "reserved_cost_microunits")
	if err != nil {
		return err
	}
	if out.ReservedCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	raw, err = requireField(f, "grants")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.Grants); err != nil || out.Grants == nil {
		return fmt.Errorf("grants must be an array")
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}

type AllocateBatchGrantsRequestV1 struct {
	APIVersion         string                    `json:"api_version"`
	Context            RequestContext            `json:"context"`
	CustomerID         string                    `json:"customer_id"`
	RunID              string                    `json:"run_id"`
	BudgetID           string                    `json:"budget_id"`
	PhaseKey           string                    `json:"phase_key"`
	WaveKey            string                    `json:"wave_key"`
	AllocationSequence int64                     `json:"allocation_sequence"`
	BatchID            string                    `json:"batch_id"`
	BatchSHA256        string                    `json:"batch_sha256"`
	EscrowID           string                    `json:"escrow_id"`
	EscrowSHA256       string                    `json:"escrow_sha256"`
	GrantExpiresAt     time.Time                 `json:"grant_expires_at"`
	Operations         []ReserveBatchOperationV1 `json:"operations"`
}

func (v AllocateBatchGrantsRequestV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != AllocateBatchGrantsAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	if v.Context.Tenant == "" || v.Context.Project == "" || v.Context.Actor == "" {
		return fmt.Errorf("context requires tenant, project, and actor")
	}
	for n, s := range map[string]string{"customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "phase_key": v.PhaseKey, "wave_key": v.WaveKey, "batch_id": v.BatchID, "escrow_id": v.EscrowID} {
		if err := reserveBatchID(n, s); err != nil {
			return err
		}
	}
	if v.AllocationSequence < 1 {
		return fmt.Errorf("allocation_sequence must be positive")
	}
	if err := reserveBatchHash("batch_sha256", v.BatchSHA256); err != nil {
		return err
	}
	if err := reserveBatchHash("escrow_sha256", v.EscrowSHA256); err != nil {
		return err
	}
	if err := reserveBatchTime("grant_expires_at", v.GrantExpiresAt); err != nil {
		return err
	}
	if len(v.Operations) < 1 || len(v.Operations) > MaxReserveBatchOperations {
		return fmt.Errorf("operations must contain between 1 and %d descriptors", MaxReserveBatchOperations)
	}
	seen := map[string]struct{}{}
	for i, op := range v.Operations {
		if err := op.validate(); err != nil {
			return fmt.Errorf("operations[%d]: %w", i, err)
		}
		if _, ok := seen[op.OperationKey]; ok {
			return fmt.Errorf("duplicate operation_key %q", op.OperationKey)
		}
		seen[op.OperationKey] = struct{}{}
	}
	return nil
}
func (v AllocateBatchGrantsRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"api_version": AllocateBatchGrantsAPIVersion, "context": v.Context, "customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "phase_key": v.PhaseKey, "wave_key": v.WaveKey, "allocation_sequence": v.AllocationSequence, "batch_id": v.BatchID, "batch_sha256": v.BatchSHA256, "escrow_id": v.EscrowID, "escrow_sha256": v.EscrowSHA256, "grant_expires_at": v.GrantExpiresAt.UTC(), "operations": v.Operations})
}
func (v *AllocateBatchGrantsRequestV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("allocate batch grants request must be an object")
	}
	if err = checkUnknownFields(f, "api_version", "context", "customer_id", "run_id", "budget_id", "phase_key", "wave_key", "allocation_sequence", "batch_id", "batch_sha256", "escrow_id", "escrow_sha256", "grant_expires_at", "operations"); err != nil {
		return err
	}
	var out AllocateBatchGrantsRequestV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != AllocateBatchGrantsAPIVersion {
		return fmt.Errorf("unsupported api_version")
	}
	raw, err := requireField(f, "context")
	if err != nil {
		return err
	}
	if out.Context, err = decodeRequestContext(raw); err != nil {
		return err
	}
	for n, d := range map[string]*string{"customer_id": &out.CustomerID, "run_id": &out.RunID, "budget_id": &out.BudgetID, "phase_key": &out.PhaseKey, "wave_key": &out.WaveKey, "batch_id": &out.BatchID, "batch_sha256": &out.BatchSHA256, "escrow_id": &out.EscrowID, "escrow_sha256": &out.EscrowSHA256} {
		if *d, err = requiredString(f, n); err != nil {
			return err
		}
	}
	raw, err = requireField(f, "allocation_sequence")
	if err != nil {
		return err
	}
	if out.AllocationSequence, err = decodeInt64(raw); err != nil {
		return err
	}
	raw, err = requireField(f, "grant_expires_at")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.GrantExpiresAt); err != nil {
		return fmt.Errorf("grant_expires_at must be RFC3339")
	}
	raw, err = requireField(f, "operations")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.Operations); err != nil || out.Operations == nil {
		return fmt.Errorf("operations must be an array")
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}
func (v AllocateBatchGrantsRequestV1) RequestSHA256() (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return reserveBatchDigest(b), nil
}

type AllocateBatchGrantsResponseV1 struct {
	APIVersion                    string                `json:"api_version"`
	BatchID                       string                `json:"batch_id"`
	RequestSHA256                 string                `json:"request_sha256"`
	BatchSHA256                   string                `json:"batch_sha256"`
	EscrowID                      string                `json:"escrow_id"`
	EscrowSHA256                  string                `json:"escrow_sha256"`
	AllocationSequence            int64                 `json:"allocation_sequence"`
	Status                        ReserveBatchStatus    `json:"status"`
	ReservedCostMicrounits        int64                 `json:"reserved_cost_microunits"`
	RemainingEscrowCostMicrounits int64                 `json:"remaining_escrow_cost_microunits"`
	Grants                        []ReserveBatchGrantV1 `json:"grants"`
}

func (v AllocateBatchGrantsResponseV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != AllocateBatchGrantsAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	for n, s := range map[string]string{"batch_id": v.BatchID, "escrow_id": v.EscrowID} {
		if err := reserveBatchID(n, s); err != nil {
			return err
		}
	}
	for n, s := range map[string]string{"request_sha256": v.RequestSHA256, "batch_sha256": v.BatchSHA256, "escrow_sha256": v.EscrowSHA256} {
		if err := reserveBatchHash(n, s); err != nil {
			return err
		}
	}
	if v.AllocationSequence < 1 {
		return fmt.Errorf("allocation_sequence must be positive")
	}
	if v.ReservedCostMicrounits < 0 || v.RemainingEscrowCostMicrounits < 0 {
		return fmt.Errorf("reserved and remaining escrow costs must not be negative")
	}
	if v.Status == ReserveBatchStatusDenied {
		if v.ReservedCostMicrounits != 0 || len(v.Grants) != 0 {
			return fmt.Errorf("denied allocation must have zero reserved cost and no grants")
		}
		return nil
	}
	if v.Status != ReserveBatchStatusReserved {
		return fmt.Errorf("allocation status must be reserved or denied")
	}
	if len(v.Grants) < 1 || len(v.Grants) > MaxReserveBatchOperations {
		return fmt.Errorf("invalid grant count")
	}
	var total int64
	for _, g := range v.Grants {
		if err := g.validate(); err != nil {
			return err
		}
		if g.MaxCostMicrounits > math.MaxInt64-total {
			return fmt.Errorf("grant cost overflow")
		}
		total += g.MaxCostMicrounits
	}
	if total != v.ReservedCostMicrounits {
		return fmt.Errorf("reserved cost must equal grant sum")
	}
	return nil
}
func (v AllocateBatchGrantsResponseV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	g := v.Grants
	if g == nil {
		g = []ReserveBatchGrantV1{}
	}
	return marshalObject(map[string]any{"api_version": AllocateBatchGrantsAPIVersion, "batch_id": v.BatchID, "request_sha256": v.RequestSHA256, "batch_sha256": v.BatchSHA256, "escrow_id": v.EscrowID, "escrow_sha256": v.EscrowSHA256, "allocation_sequence": v.AllocationSequence, "status": v.Status, "reserved_cost_microunits": v.ReservedCostMicrounits, "remaining_escrow_cost_microunits": v.RemainingEscrowCostMicrounits, "grants": g})
}
func (v *AllocateBatchGrantsResponseV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = checkUnknownFields(f, "api_version", "batch_id", "request_sha256", "batch_sha256", "escrow_id", "escrow_sha256", "allocation_sequence", "status", "reserved_cost_microunits", "remaining_escrow_cost_microunits", "grants"); err != nil {
		return err
	}
	var out AllocateBatchGrantsResponseV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != AllocateBatchGrantsAPIVersion {
		return fmt.Errorf("unsupported api_version")
	}
	for n, d := range map[string]*string{"batch_id": &out.BatchID, "request_sha256": &out.RequestSHA256, "batch_sha256": &out.BatchSHA256, "escrow_id": &out.EscrowID, "escrow_sha256": &out.EscrowSHA256} {
		if *d, err = requiredString(f, n); err != nil {
			return err
		}
	}
	raw, err := requireField(f, "allocation_sequence")
	if err != nil {
		return err
	}
	if out.AllocationSequence, err = decodeInt64(raw); err != nil {
		return err
	}
	s, err := requiredString(f, "status")
	if err != nil {
		return err
	}
	out.Status = ReserveBatchStatus(s)
	raw, err = requireField(f, "reserved_cost_microunits")
	if err != nil {
		return err
	}
	if out.ReservedCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	raw, err = requireField(f, "remaining_escrow_cost_microunits")
	if err != nil {
		return err
	}
	if out.RemainingEscrowCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	raw, err = requireField(f, "grants")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &out.Grants); err != nil || out.Grants == nil {
		return fmt.Errorf("grants must be an array")
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}

type CloseBatchReason string

const (
	CloseBatchReasonCompleted          CloseBatchReason = "completed"
	CloseBatchReasonZeroOpUnchanged    CloseBatchReason = "zero_op_unchanged"
	CloseBatchReasonTypedSkip          CloseBatchReason = "typed_skip"
	CloseBatchReasonOperatorCancel     CloseBatchReason = "operator_cancel"
	CloseBatchReasonPreFinalizeFailure CloseBatchReason = "pre_finalize_failure"
)

func (v CloseBatchReason) valid() bool {
	switch v {
	case CloseBatchReasonCompleted, CloseBatchReasonZeroOpUnchanged, CloseBatchReasonTypedSkip, CloseBatchReasonOperatorCancel, CloseBatchReasonPreFinalizeFailure:
		return true
	default:
		return false
	}
}

type CloseBatchRequestV1 struct {
	APIVersion   string           `json:"api_version"`
	Context      RequestContext   `json:"context"`
	CustomerID   string           `json:"customer_id"`
	RunID        string           `json:"run_id"`
	BudgetID     string           `json:"budget_id"`
	PhaseKey     string           `json:"phase_key"`
	CloseKey     string           `json:"close_key"`
	Reason       CloseBatchReason `json:"reason"`
	BatchID      string           `json:"batch_id"`
	BatchSHA256  string           `json:"batch_sha256"`
	EscrowID     string           `json:"escrow_id"`
	EscrowSHA256 string           `json:"escrow_sha256"`
}

func (v CloseBatchRequestV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != CloseBatchAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	if v.Context.Tenant == "" || v.Context.Project == "" || v.Context.Actor == "" {
		return fmt.Errorf("context requires tenant, project, and actor")
	}
	for n, s := range map[string]string{"customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "phase_key": v.PhaseKey, "close_key": v.CloseKey, "batch_id": v.BatchID, "escrow_id": v.EscrowID} {
		if err := reserveBatchID(n, s); err != nil {
			return err
		}
	}
	if !v.Reason.valid() {
		return fmt.Errorf("close reason %q is invalid", v.Reason)
	}
	if err := reserveBatchHash("batch_sha256", v.BatchSHA256); err != nil {
		return err
	}
	return reserveBatchHash("escrow_sha256", v.EscrowSHA256)
}
func (v CloseBatchRequestV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return marshalObject(map[string]any{"api_version": CloseBatchAPIVersion, "context": v.Context, "customer_id": v.CustomerID, "run_id": v.RunID, "budget_id": v.BudgetID, "phase_key": v.PhaseKey, "close_key": v.CloseKey, "reason": v.Reason, "batch_id": v.BatchID, "batch_sha256": v.BatchSHA256, "escrow_id": v.EscrowID, "escrow_sha256": v.EscrowSHA256})
}
func (v *CloseBatchRequestV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return fmt.Errorf("close batch request must be an object")
	}
	if err = checkUnknownFields(f, "api_version", "context", "customer_id", "run_id", "budget_id", "phase_key", "close_key", "reason", "batch_id", "batch_sha256", "escrow_id", "escrow_sha256"); err != nil {
		return err
	}
	var out CloseBatchRequestV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != CloseBatchAPIVersion {
		return fmt.Errorf("unsupported api_version")
	}
	raw, err := requireField(f, "context")
	if err != nil {
		return err
	}
	if out.Context, err = decodeRequestContext(raw); err != nil {
		return err
	}
	for n, d := range map[string]*string{"customer_id": &out.CustomerID, "run_id": &out.RunID, "budget_id": &out.BudgetID, "phase_key": &out.PhaseKey, "close_key": &out.CloseKey, "batch_id": &out.BatchID, "batch_sha256": &out.BatchSHA256, "escrow_id": &out.EscrowID, "escrow_sha256": &out.EscrowSHA256} {
		if *d, err = requiredString(f, n); err != nil {
			return err
		}
	}
	reason, err := requiredString(f, "reason")
	if err != nil {
		return err
	}
	out.Reason = CloseBatchReason(reason)
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}
func (v CloseBatchRequestV1) RequestSHA256() (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return reserveBatchDigest(b), nil
}

type CloseBatchStatus string

const (
	CloseBatchStatusClosed        CloseBatchStatus = "closed"
	CloseBatchStatusAlreadyClosed CloseBatchStatus = "already_closed"
)

type CloseBatchResponseV1 struct {
	APIVersion             string           `json:"api_version"`
	BatchID                string           `json:"batch_id"`
	EscrowID               string           `json:"escrow_id"`
	Status                 CloseBatchStatus `json:"status"`
	RefundedCostMicrounits int64            `json:"refunded_cost_microunits"`
}

func (v CloseBatchResponseV1) validate() error {
	if v.APIVersion != "" && v.APIVersion != CloseBatchAPIVersion {
		return fmt.Errorf("api_version %q is unsupported", v.APIVersion)
	}
	if err := reserveBatchID("batch_id", v.BatchID); err != nil {
		return err
	}
	if err := reserveBatchID("escrow_id", v.EscrowID); err != nil {
		return err
	}
	if v.Status != CloseBatchStatusClosed && v.Status != CloseBatchStatusAlreadyClosed {
		return fmt.Errorf("close status %q is invalid", v.Status)
	}
	if v.RefundedCostMicrounits < 0 {
		return fmt.Errorf("refunded_cost_microunits must not be negative")
	}
	return nil
}
func (v CloseBatchResponseV1) MarshalJSON() ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	type plain CloseBatchResponseV1
	v.APIVersion = CloseBatchAPIVersion
	return json.Marshal(plain(v))
}
func (v *CloseBatchResponseV1) UnmarshalJSON(data []byte) error {
	f, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = checkUnknownFields(f, "api_version", "batch_id", "escrow_id", "status", "refunded_cost_microunits"); err != nil {
		return err
	}
	var out CloseBatchResponseV1
	if out.APIVersion, err = requiredString(f, "api_version"); err != nil || out.APIVersion != CloseBatchAPIVersion {
		return fmt.Errorf("unsupported api_version")
	}
	if out.BatchID, err = requiredString(f, "batch_id"); err != nil {
		return err
	}
	if out.EscrowID, err = requiredString(f, "escrow_id"); err != nil {
		return err
	}
	status, err := requiredString(f, "status")
	if err != nil {
		return err
	}
	out.Status = CloseBatchStatus(status)
	raw, err := requireField(f, "refunded_cost_microunits")
	if err != nil {
		return err
	}
	if out.RefundedCostMicrounits, err = decodeInt64(raw); err != nil {
		return err
	}
	if err = out.validate(); err != nil {
		return err
	}
	*v = out
	return nil
}
func reserveBatchTime(name string, value time.Time) error {
	if value.IsZero() {
		return fmt.Errorf("%s is required", name)
	}
	_, offset := value.Zone()
	if offset != 0 {
		return fmt.Errorf("%s must use UTC", name)
	}
	return nil
}
func reserveBatchID(name, value string) error {
	if value == "" || len(value) > 256 {
		return fmt.Errorf("%s must contain 1 to 256 characters", name)
	}
	return nil
}
func reserveBatchHash(name, value string) error {
	if len(value) != 64 {
		return fmt.Errorf("%s must be lowercase SHA-256", name)
	}
	b, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(b) != value {
		return fmt.Errorf("%s must be lowercase SHA-256", name)
	}
	return nil
}
func reserveBatchDigest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
