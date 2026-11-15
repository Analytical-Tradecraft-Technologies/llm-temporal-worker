package llm

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var regexpDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// decodeResultPage decodes a result page whose only member is the row array
// field into *rows, then validates each row.
func decodeResultPage(data []byte, field string, rows *[]json.RawMessage, validate func(map[string]json.RawMessage) error) error {
	fields, err := decodeClosedObject(data, field)
	if err != nil {
		return err
	}
	values, err := requiredRawArray(fields, field)
	if err != nil {
		return err
	}
	*rows = values
	return validateResultArray(values, field, validate)
}

func (page *ProviderStatusPage) UnmarshalJSON(data []byte) error {
	return decodeResultPage(data, "routes", &page.Routes, validateProviderRoute)
}

func (page *ModelInventoryPage) UnmarshalJSON(data []byte) error {
	return decodeResultPage(data, "models", &page.Models, validateModelInventoryEntry)
}

func (page *CreditStatusPage) UnmarshalJSON(data []byte) error {
	return decodeResultPage(data, "endpoints", &page.Endpoints, validateCreditStatusEntry)
}

// BudgetStatus and SpendSummary contain required scalar/page members in
// addition to their open row arrays. Decode their object fields explicitly so
// encoding/json cannot discard an unknown result member before validation.
func (value *BudgetStatus) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "active_at", "generation_id", "manifest_digest", "stream_high_water_mark", "windows")
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	decoded := BudgetStatus{
		ActiveAt:            reader.requiredString("active_at"),
		GenerationID:        reader.requiredString("generation_id"),
		ManifestDigest:      reader.requiredString("manifest_digest"),
		StreamHighWaterMark: reader.requiredString("stream_high_water_mark"),
	}
	if reader.err != nil {
		return reader.err
	}
	if decoded.Windows, err = requiredRawArray(fields, "windows"); err != nil {
		return err
	}
	return storeValid(value, decoded, validateBudgetStatus(decoded))
}

func (value *SpendSummary) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "start_time", "end_time", "buckets")
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	decoded := SpendSummary{StartTime: reader.requiredString("start_time"), EndTime: reader.requiredString("end_time")}
	if reader.err != nil {
		return reader.err
	}
	if decoded.Buckets, err = requiredRawArray(fields, "buckets"); err != nil {
		return err
	}
	return storeValid(value, decoded, validateSpendSummary(decoded))
}

// validateQueryResult applies the same closed-world checks as the query JSON
// schemas to the polymorphic result pages. The page structs intentionally keep
// rows as raw JSON so the public contract can evolve independently of storage;
// that must not turn the result boundary into an unvalidated escape hatch.
func validateQueryResult(kind QueryKind, result QueryResult) error {
	switch kind {
	case QueryProviderStatus:
		return validateTypedResult(result, "provider status", func(page ProviderStatusPage) error {
			return validateResultArray(page.Routes, "routes", validateProviderRoute)
		})
	case QueryModelInventory:
		return validateTypedResult(result, "model inventory", func(page ModelInventoryPage) error {
			return validateResultArray(page.Models, "models", validateModelInventoryEntry)
		})
	case QueryCreditStatus:
		return validateTypedResult(result, "credit status", func(page CreditStatusPage) error {
			return validateResultArray(page.Endpoints, "endpoints", validateCreditStatusEntry)
		})
	case QueryBudgetStatus:
		return validateTypedResult(result, "budget status", validateBudgetStatus)
	case QuerySpendSummary:
		return validateTypedResult(result, "spend summary", validateSpendSummary)
	default:
		return fmt.Errorf("query result kind %q is invalid", kind)
	}
}

// validateTypedResult applies validate to result as a T, reporting any other
// type as "<label> result has unexpected type".
func validateTypedResult[T QueryResult](result QueryResult, label string, validate func(T) error) error {
	value, ok := derefAs[T](result)
	if !ok {
		return fmt.Errorf("%s result has unexpected type", label)
	}
	return validate(value)
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
	if err := requireStrings(fields, "route_id", "provider", "endpoint", "availability", "observed_at", "stale_after"); err != nil {
		return err
	}
	if err := validateQueryEnum(fields["availability"], "availability", "available", "degraded", "unavailable", "unknown"); err != nil {
		return err
	}
	if err := validateTimestampFields(fields, "observed_at", "stale_after"); err != nil {
		return err
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
		if _, err := decodeStringField("safe_code", raw); err != nil {
			return err
		}
	}
	return nil
}

func validateModelInventoryEntry(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "provider", "endpoint", "provider_model_id", "display_name", "lifecycle", "capabilities", "complete_snapshot"); err != nil {
		return err
	}
	if err := requireStrings(fields, "provider", "endpoint", "provider_model_id"); err != nil {
		return err
	}
	if err := validateQueryEnum(fields["lifecycle"], "lifecycle", "available", "deprecated", "unavailable", "unknown"); err != nil {
		return err
	}
	if err := checkNullableStrings(fields, "display_name"); err != nil {
		return err
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
	if err := requireStrings(fields, "provider", "endpoint"); err != nil {
		return err
	}
	if err := validateQueryEnum(fields["credit_state"], "credit_state", "ok", "low", "exhausted", "unknown"); err != nil {
		return err
	}
	if err := validateQueryEnum(fields["billing_state"], "billing_state", "ok", "blocked", "unknown"); err != nil {
		return err
	}
	if raw, ok := fields["confirmed_at"]; ok && string(raw) != "null" {
		if err := validateTimestampFields(fields, "confirmed_at"); err != nil {
			return err
		}
	}
	if err := validateQueryEnum(fields["evidence_source"], "evidence_source", "provider_api", "operator", "unknown"); err != nil {
		return err
	}
	return checkNullableStrings(fields, "safe_evidence_code")
}

func validateBudgetStatus(value BudgetStatus) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fields, err := decodeClosedObject(raw, "active_at", "generation_id", "manifest_digest", "stream_high_water_mark", "windows")
	if err != nil {
		return err
	}
	if err := requireStrings(fields, "active_at", "generation_id", "manifest_digest", "stream_high_water_mark"); err != nil {
		return err
	}
	if err := validateTimestampFields(fields, "active_at"); err != nil {
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
		if err := requireStrings(fields, "policy_key", "window_key", "coverage_start", "coverage_end", "limit_usd", "reserved_cost_usd", "accounted_cost_usd", "available_usd"); err != nil {
			return err
		}
		if err := validateTimestampFields(fields, "coverage_start", "coverage_end"); err != nil {
			return err
		}
		if err := validateDecimalFields(fields, "limit_usd", "reserved_cost_usd", "accounted_cost_usd", "available_usd"); err != nil {
			return err
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
	fields, err := decodeClosedObject(raw, "start_time", "end_time", "buckets")
	if err != nil {
		return err
	}
	if err := validateTimestampFields(fields, "start_time", "end_time"); err != nil {
		return err
	}
	return validateResultArray(value.Buckets, "buckets", validateSpendBucket)
}

func validateSpendBucket(fields map[string]json.RawMessage) error {
	if err := checkUnknownFields(fields, "group", "known_actual_cost_usd", "exact_operation_count", "unknown_operation_count", "completeness"); err != nil {
		return err
	}
	if err := requireStrings(fields, "known_actual_cost_usd", "completeness"); err != nil {
		return err
	}
	if err := validateDecimalFields(fields, "known_actual_cost_usd"); err != nil {
		return err
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
		return checkNullableStrings(group, "provider", "model")
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

// validateTimestampFields requires each named field, in order, to be an
// RFC 3339 timestamp string.
func validateTimestampFields(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		value, err := requiredString(fields, name)
		if err != nil {
			return err
		}
		if err := validateQueryTimestamp(name, value); err != nil {
			return err
		}
	}
	return nil
}

// validateDecimalFields requires each named string field, in order, to be a
// nonnegative decimal. The caller has already checked they are strings.
func validateDecimalFields(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		value, _ := requiredString(fields, name)
		if !decimalPattern.MatchString(value) {
			return fmt.Errorf("%s must be a nonnegative decimal", name)
		}
	}
	return nil
}

// checkNullableStrings checks that each named field, when present and not
// null, is a string.
func checkNullableStrings(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		if raw, ok := fields[name]; ok && string(raw) != "null" {
			if _, err := decodeStringField(name, raw); err != nil {
				return err
			}
		}
	}
	return nil
}
