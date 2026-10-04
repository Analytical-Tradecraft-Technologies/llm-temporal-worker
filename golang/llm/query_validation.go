package llm

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var regexpDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

func decodeResultPage(data []byte, field string) ([]json.RawMessage, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, field); err != nil {
		return nil, err
	}
	raw, err := requireField(fields, field)
	if err != nil || string(raw) == "null" {
		return nil, fmt.Errorf("%s must be an array", field)
	}
	var values []json.RawMessage
	if err := decodeJSON(raw, &values); err != nil {
		return nil, fmt.Errorf("%s must be an array", field)
	}
	return values, nil
}

func (page *ProviderStatusPage) UnmarshalJSON(data []byte) error {
	values, err := decodeResultPage(data, "routes")
	if err != nil {
		return err
	}
	page.Routes = values
	return validateResultArray(values, "routes", validateProviderRoute)
}

func (page *ModelInventoryPage) UnmarshalJSON(data []byte) error {
	values, err := decodeResultPage(data, "models")
	if err != nil {
		return err
	}
	page.Models = values
	return validateResultArray(values, "models", validateModelInventoryEntry)
}

func (page *CreditStatusPage) UnmarshalJSON(data []byte) error {
	values, err := decodeResultPage(data, "endpoints")
	if err != nil {
		return err
	}
	page.Endpoints = values
	return validateResultArray(values, "endpoints", validateCreditStatusEntry)
}

// BudgetStatus and SpendSummary contain required scalar/page members in
// addition to their open row arrays. Decode their object fields explicitly so
// encoding/json cannot discard an unknown result member before validation.
func (value *BudgetStatus) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "active_at", "generation_id", "manifest_digest", "stream_high_water_mark", "windows"); err != nil {
		return err
	}
	activeAt, err := requiredString(fields, "active_at")
	if err != nil {
		return err
	}
	generationID, err := requiredString(fields, "generation_id")
	if err != nil {
		return err
	}
	digest, err := requiredString(fields, "manifest_digest")
	if err != nil {
		return err
	}
	streamHighWaterMark, err := requiredString(fields, "stream_high_water_mark")
	if err != nil {
		return err
	}
	rawWindows, err := requireField(fields, "windows")
	if err != nil || string(rawWindows) == "null" {
		return fmt.Errorf("windows must be an array")
	}
	var windows []json.RawMessage
	if err := decodeJSON(rawWindows, &windows); err != nil {
		return fmt.Errorf("windows must be an array")
	}
	decoded := BudgetStatus{ActiveAt: activeAt, GenerationID: generationID, ManifestDigest: digest, StreamHighWaterMark: streamHighWaterMark, Windows: windows}
	if err := validateBudgetStatus(decoded); err != nil {
		return err
	}
	*value = decoded
	return nil
}

func (value *SpendSummary) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "start_time", "end_time", "buckets"); err != nil {
		return err
	}
	startTime, err := requiredString(fields, "start_time")
	if err != nil {
		return err
	}
	endTime, err := requiredString(fields, "end_time")
	if err != nil {
		return err
	}
	rawBuckets, err := requireField(fields, "buckets")
	if err != nil || string(rawBuckets) == "null" {
		return fmt.Errorf("buckets must be an array")
	}
	var buckets []json.RawMessage
	if err := decodeJSON(rawBuckets, &buckets); err != nil {
		return fmt.Errorf("buckets must be an array")
	}
	decoded := SpendSummary{StartTime: startTime, EndTime: endTime, Buckets: buckets}
	if err := validateSpendSummary(decoded); err != nil {
		return err
	}
	*value = decoded
	return nil
}

// validateQueryResult applies the same closed-world checks as the query JSON
// schemas to the polymorphic result pages. The page structs intentionally keep
// rows as raw JSON so the public contract can evolve independently of storage;
// that must not turn the result boundary into an unvalidated escape hatch.
func validateQueryResult(kind QueryKind, result QueryResult) error {
	switch kind {
	case QueryProviderStatus:
		page, ok := queryProviderStatusPage(result)
		if !ok {
			return fmt.Errorf("provider status result has unexpected type")
		}
		return validateResultArray(page.Routes, "routes", validateProviderRoute)
	case QueryModelInventory:
		page, ok := queryModelInventoryPage(result)
		if !ok {
			return fmt.Errorf("model inventory result has unexpected type")
		}
		return validateResultArray(page.Models, "models", validateModelInventoryEntry)
	case QueryCreditStatus:
		page, ok := queryCreditStatusPage(result)
		if !ok {
			return fmt.Errorf("credit status result has unexpected type")
		}
		return validateResultArray(page.Endpoints, "endpoints", validateCreditStatusEntry)
	case QueryBudgetStatus:
		budget, ok := queryBudgetStatus(result)
		if !ok {
			return fmt.Errorf("budget status result has unexpected type")
		}
		return validateBudgetStatus(budget)
	case QuerySpendSummary:
		spend, ok := querySpendSummary(result)
		if !ok {
			return fmt.Errorf("spend summary result has unexpected type")
		}
		return validateSpendSummary(spend)
	default:
		return fmt.Errorf("query result kind %q is invalid", kind)
	}
}

func queryProviderStatusPage(result QueryResult) (ProviderStatusPage, bool) {
	switch value := result.(type) {
	case ProviderStatusPage:
		return value, true
	case *ProviderStatusPage:
		return *value, value != nil
	default:
		return ProviderStatusPage{}, false
	}
}

func queryModelInventoryPage(result QueryResult) (ModelInventoryPage, bool) {
	switch value := result.(type) {
	case ModelInventoryPage:
		return value, true
	case *ModelInventoryPage:
		return *value, value != nil
	default:
		return ModelInventoryPage{}, false
	}
}

func queryCreditStatusPage(result QueryResult) (CreditStatusPage, bool) {
	switch value := result.(type) {
	case CreditStatusPage:
		return value, true
	case *CreditStatusPage:
		return *value, value != nil
	default:
		return CreditStatusPage{}, false
	}
}

func queryBudgetStatus(result QueryResult) (BudgetStatus, bool) {
	switch value := result.(type) {
	case BudgetStatus:
		return value, true
	case *BudgetStatus:
		return *value, value != nil
	default:
		return BudgetStatus{}, false
	}
}

func querySpendSummary(result QueryResult) (SpendSummary, bool) {
	switch value := result.(type) {
	case SpendSummary:
		return value, true
	case *SpendSummary:
		return *value, value != nil
	default:
		return SpendSummary{}, false
	}
}

func validateResultArray(values []json.RawMessage, name string, validate func(map[string]json.RawMessage) error) error {
	if values == nil {
		return fmt.Errorf("result.%s must be an array", name)
	}
	for index, raw := range values {
		if string(raw) == "null" {
			return fmt.Errorf("result.%s[%d] must be an object", name, index)
		}
		fields, err := decodeObject(raw)
		if err != nil {
			return fmt.Errorf("result.%s[%d] must be an object", name, index)
		}
		if err := validate(fields); err != nil {
			return fmt.Errorf("result.%s[%d]: %w", name, index, err)
		}
	}
	return nil
}

func validateProviderRoute(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "route_id", "provider", "endpoint", "availability", "credit_state", "billing_state", "circuit_state", "observed_at", "stale_after", "safe_code"); err != nil {
		return err
	}
	for _, name := range []string{"route_id", "provider", "endpoint", "availability", "observed_at", "stale_after"} {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	if err := validateQueryEnum(fields["availability"], "availability", "available", "degraded", "unavailable", "unknown"); err != nil {
		return err
	}
	for _, name := range []string{"observed_at", "stale_after"} {
		value, _ := requiredString(fields, name)
		if err := validateQueryTimestamp(name, value); err != nil {
			return err
		}
	}
	for name, allowed := range map[string][]string{
		"credit_state":  {"ok", "low", "exhausted", "unknown"},
		"billing_state": {"ok", "blocked", "unknown"},
		"circuit_state": {"closed", "open", "half_open"},
	} {
		if raw, ok := fields[name]; ok {
			if err := validateQueryEnum(raw, name, allowed...); err != nil {
				return err
			}
		}
	}
	if raw, ok := fields["safe_code"]; ok {
		if _, err := requiredString(map[string]json.RawMessage{"safe_code": raw}, "safe_code"); err != nil {
			return err
		}
	}
	return nil
}

func validateModelInventoryEntry(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "provider", "endpoint", "provider_model_id", "display_name", "lifecycle", "capabilities", "complete_snapshot"); err != nil {
		return err
	}
	for _, name := range []string{"provider", "endpoint", "provider_model_id"} {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	if err := validateQueryEnum(fields["lifecycle"], "lifecycle", "available", "deprecated", "unavailable", "unknown"); err != nil {
		return err
	}
	if raw, ok := fields["display_name"]; ok && string(raw) != "null" {
		if _, err := requiredString(map[string]json.RawMessage{"display_name": raw}, "display_name"); err != nil {
			return err
		}
	}
	if err := validateStringArray(fields, "capabilities"); err != nil {
		return err
	}
	if _, err := requiredBool(fields, "complete_snapshot"); err != nil {
		return err
	}
	return nil
}

func validateCreditStatusEntry(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "provider", "endpoint", "credit_state", "billing_state", "confirmed_at", "evidence_source", "safe_evidence_code"); err != nil {
		return err
	}
	for _, name := range []string{"provider", "endpoint"} {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	if err := validateQueryEnum(fields["credit_state"], "credit_state", "ok", "low", "exhausted", "unknown"); err != nil {
		return err
	}
	if err := validateQueryEnum(fields["billing_state"], "billing_state", "ok", "blocked", "unknown"); err != nil {
		return err
	}
	if raw, ok := fields["confirmed_at"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"confirmed_at": raw}, "confirmed_at")
		if err != nil {
			return err
		}
		if err := validateQueryTimestamp("confirmed_at", value); err != nil {
			return err
		}
	}
	if err := validateQueryEnum(fields["evidence_source"], "evidence_source", "provider_api", "operator", "unknown"); err != nil {
		return err
	}
	if raw, ok := fields["safe_evidence_code"]; ok && string(raw) != "null" {
		if _, err := requiredString(map[string]json.RawMessage{"safe_evidence_code": raw}, "safe_evidence_code"); err != nil {
			return err
		}
	}
	return nil
}

func validateBudgetStatus(value BudgetStatus) error {
	fields := map[string]json.RawMessage{}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fields, err = decodeObject(raw)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "active_at", "generation_id", "manifest_digest", "stream_high_water_mark", "windows"); err != nil {
		return err
	}
	for _, name := range []string{"active_at", "generation_id", "manifest_digest", "stream_high_water_mark"} {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	activeAt, _ := requiredString(fields, "active_at")
	if err := validateQueryTimestamp("active_at", activeAt); err != nil {
		return err
	}
	digest, _ := requiredString(fields, "manifest_digest")
	if !regexpDigest.MatchString(digest) {
		return fmt.Errorf("manifest_digest must be a lowercase SHA-256 digest")
	}
	return validateBudgetWindows(value.Windows)
}

func validateBudgetWindows(values []json.RawMessage) error {
	return validateResultArray(values, "windows", func(fields map[string]json.RawMessage) error {
		if err := checkUnknownFields(fields, "policy_key", "window_key", "coverage_start", "coverage_end", "limit_usd", "reserved_cost_usd", "accounted_cost_usd", "available_usd", "retry_after_seconds"); err != nil {
			return err
		}
		for _, name := range []string{"policy_key", "window_key", "coverage_start", "coverage_end", "limit_usd", "reserved_cost_usd", "accounted_cost_usd", "available_usd"} {
			if _, err := requiredString(fields, name); err != nil {
				return err
			}
		}
		for _, name := range []string{"coverage_start", "coverage_end"} {
			value, _ := requiredString(fields, name)
			if err := validateQueryTimestamp(name, value); err != nil {
				return err
			}
		}
		for _, name := range []string{"limit_usd", "reserved_cost_usd", "accounted_cost_usd", "available_usd"} {
			value, _ := requiredString(fields, name)
			if !decimalPattern.MatchString(value) {
				return fmt.Errorf("%s must be a nonnegative decimal", name)
			}
		}
		if raw, ok := fields["retry_after_seconds"]; ok && string(raw) != "null" {
			value, err := decodeInt64(raw)
			if err != nil || value < 0 {
				return fmt.Errorf("retry_after_seconds must be a nonnegative integer")
			}
		}
		return nil
	})
}

func validateSpendSummary(value SpendSummary) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fields, err := decodeObject(raw)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "start_time", "end_time", "buckets"); err != nil {
		return err
	}
	for _, name := range []string{"start_time", "end_time"} {
		value, err := requiredString(fields, name)
		if err != nil {
			return err
		}
		if err := validateQueryTimestamp(name, value); err != nil {
			return err
		}
	}
	return validateResultArray(value.Buckets, "buckets", validateSpendBucket)
}

func validateSpendBucket(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "group", "known_actual_cost_usd", "exact_operation_count", "unknown_operation_count", "completeness"); err != nil {
		return err
	}
	for _, name := range []string{"known_actual_cost_usd", "completeness"} {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	known, _ := requiredString(fields, "known_actual_cost_usd")
	if !decimalPattern.MatchString(known) {
		return fmt.Errorf("known_actual_cost_usd must be a nonnegative decimal")
	}
	for _, name := range []string{"exact_operation_count", "unknown_operation_count"} {
		raw, err := requireField(fields, name)
		if err != nil {
			return err
		}
		value, err := decodeInt64(raw)
		if err != nil || value < 0 {
			return fmt.Errorf("%s must be a nonnegative integer", name)
		}
	}
	if err := validateQueryEnum(fields["completeness"], "completeness", "complete", "partial"); err != nil {
		return err
	}
	if raw, ok := fields["group"]; ok && string(raw) != "null" {
		group, err := decodeObject(raw)
		if err != nil {
			return fmt.Errorf("group must be an object or null")
		}
		if err := checkUnknownFields(group, "operation_kind", "provider", "model"); err != nil {
			return err
		}
		if raw, ok := group["operation_kind"]; ok && string(raw) != "null" {
			if err := validateQueryEnum(raw, "operation_kind", "generate", "compact", "query"); err != nil {
				return err
			}
		}
		for _, name := range []string{"provider", "model"} {
			if raw, ok := group[name]; ok && string(raw) != "null" {
				if _, err := requiredString(map[string]json.RawMessage{name: raw}, name); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateStringArray(fields map[string]json.RawMessage, name string) error {
	raw, err := requireField(fields, name)
	if err != nil || string(raw) == "null" {
		return fmt.Errorf("%s must be an array of strings", name)
	}
	var values []string
	if err := decodeJSON(raw, &values); err != nil {
		return fmt.Errorf("%s must be an array of strings", name)
	}
	return nil
}
