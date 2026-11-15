package llm

import (
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

type ResponseStatus string

const (
	ResponseStatusPaused          ResponseStatus = "paused"
	ResponseStatusCompleted       ResponseStatus = "completed"
	ResponseStatusToolCalls       ResponseStatus = "tool_calls"
	ResponseStatusRefused         ResponseStatus = "refused"
	ResponseStatusLength          ResponseStatus = "length"
	ResponseStatusContentFiltered ResponseStatus = "content_filtered"
)

func (status ResponseStatus) Valid() bool {
	switch status {
	case ResponseStatusPaused, ResponseStatusCompleted, ResponseStatusToolCalls, ResponseStatusRefused, ResponseStatusLength, ResponseStatusContentFiltered:
		return true
	default:
		return false
	}
}

type RouteFacts struct {
	RouteID        string
	EndpointID     string
	APIFamily      string
	RequestedModel string
	ResolvedModel  string
}

func (route RouteFacts) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any)
	setIfNotEmpty(fields, "route_id", route.RouteID)
	setIfNotEmpty(fields, "endpoint_id", route.EndpointID)
	setIfNotEmpty(fields, "api_family", route.APIFamily)
	setIfNotEmpty(fields, "requested_model", route.RequestedModel)
	setIfNotEmpty(fields, "resolved_model", route.ResolvedModel)
	return marshalObject(fields)
}

func decodeRouteFacts(data []byte) (RouteFacts, error) {
	fields, err := decodeClosedObject(data, "route_id", "endpoint_id", "api_family", "requested_model", "resolved_model")
	if err != nil {
		return RouteFacts{}, err
	}
	reader := fieldReader{fields: fields}
	route := RouteFacts{
		RouteID:        reader.optionalString("route_id"),
		EndpointID:     reader.optionalString("endpoint_id"),
		APIFamily:      reader.optionalString("api_family"),
		RequestedModel: reader.optionalString("requested_model"),
		ResolvedModel:  reader.optionalString("resolved_model"),
	}
	if reader.err != nil {
		return RouteFacts{}, reader.err
	}
	return route, nil
}

type ServiceFacts struct {
	Requested     ServiceClass
	Attempted     ServiceClass
	Actual        *ServiceClass
	ProviderValue string
	FallbackIndex int
}

func (service ServiceFacts) MarshalJSON() ([]byte, error) {
	requested, err := NormalizeServiceClass(service.Requested)
	if err != nil {
		return nil, err
	}
	attempted, err := NormalizeServiceClass(service.Attempted)
	if err != nil {
		return nil, err
	}
	if service.FallbackIndex < 0 {
		return nil, fmt.Errorf("service fallback_index must not be negative")
	}
	fields := map[string]any{
		"requested":      requested,
		"attempted":      attempted,
		"fallback_index": service.FallbackIndex,
	}
	if service.Actual != nil {
		if !service.Actual.Valid() {
			return nil, fmt.Errorf("service actual class %q is invalid", *service.Actual)
		}
		fields["actual"] = *service.Actual
	}
	if service.ProviderValue != "" {
		fields["provider_value"] = service.ProviderValue
	}
	return marshalObject(fields)
}

func decodeServiceFacts(data []byte) (ServiceFacts, error) {
	return decodeServiceFactsFields(data, false)
}

// decodeServiceFactsStrict decodes the public v1 service object, whose schema
// requires fallback_index and allows actual only as a service class.
func decodeServiceFactsStrict(data []byte) (ServiceFacts, error) {
	return decodeServiceFactsFields(data, true)
}

func decodeServiceFactsFields(data []byte, strict bool) (ServiceFacts, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return ServiceFacts{}, err
	}
	if _, ok := fields["fallback_index"]; strict && !ok {
		return ServiceFacts{}, fmt.Errorf("service fallback_index is required")
	}
	if err := checkUnknownFields(fields, "requested", "attempted", "actual", "provider_value", "fallback_index"); err != nil {
		return ServiceFacts{}, err
	}
	reader := fieldReader{fields: fields}
	requestedValue := reader.requiredString("requested")
	attemptedValue := reader.requiredString("attempted")
	if reader.err != nil {
		return ServiceFacts{}, reader.err
	}
	requested, err := NormalizeServiceClass(ServiceClass(requestedValue))
	if err != nil {
		return ServiceFacts{}, err
	}
	attempted, err := NormalizeServiceClass(ServiceClass(attemptedValue))
	if err != nil {
		return ServiceFacts{}, err
	}
	providerValue, _, err := optionalString(fields, "provider_value")
	if err != nil {
		return ServiceFacts{}, err
	}
	fallbackIndex := 0
	if raw, ok := fields["fallback_index"]; ok {
		fallbackIndex, err = decodeInt(raw)
		if err != nil {
			return ServiceFacts{}, fmt.Errorf("service fallback_index: %w", err)
		}
		if fallbackIndex < 0 {
			return ServiceFacts{}, fmt.Errorf("service fallback_index must not be negative")
		}
	}
	var actual *ServiceClass
	// The public schema allows only a service class; the encoder omits an
	// unknown actual class rather than writing null (#1662).
	if raw, ok := fields["actual"]; ok && string(raw) == "null" && strict {
		return ServiceFacts{}, fmt.Errorf("service actual must be a service class")
	}
	if raw, ok := fields["actual"]; ok && string(raw) != "null" {
		value, err := decodeStringField("actual", raw)
		if err != nil {
			return ServiceFacts{}, err
		}
		parsed := ServiceClass(value)
		if !parsed.Valid() {
			return ServiceFacts{}, fmt.Errorf("service actual class %q is invalid", parsed)
		}
		actual = &parsed
	}
	return ServiceFacts{Requested: requested, Attempted: attempted, Actual: actual, ProviderValue: providerValue, FallbackIndex: fallbackIndex}, nil
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	ReasoningTokens  int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	ProviderRaw      map[string]json.RawMessage
}

func (usage Usage) MarshalJSON() ([]byte, error) {
	values := []int64{usage.InputTokens, usage.OutputTokens, usage.ReasoningTokens, usage.CacheReadTokens, usage.CacheWriteTokens}
	for _, value := range values {
		if value < 0 {
			return nil, fmt.Errorf("usage token counts must not be negative")
		}
	}
	fields := map[string]any{
		"input_tokens":       usage.InputTokens,
		"output_tokens":      usage.OutputTokens,
		"reasoning_tokens":   usage.ReasoningTokens,
		"cache_read_tokens":  usage.CacheReadTokens,
		"cache_write_tokens": usage.CacheWriteTokens,
	}
	if len(usage.ProviderRaw) > 0 {
		fields["provider_raw"] = copyRawMap(usage.ProviderRaw)
	}
	return marshalObject(fields)
}

func decodeUsage(data []byte) (Usage, error) {
	fields, err := decodeClosedObject(data, "input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_write_tokens", "provider_raw")
	if err != nil {
		return Usage{}, err
	}
	usage := Usage{}
	counts := []struct {
		name  string
		value *int64
	}{
		{"input_tokens", &usage.InputTokens},
		{"output_tokens", &usage.OutputTokens},
		{"reasoning_tokens", &usage.ReasoningTokens},
		{"cache_read_tokens", &usage.CacheReadTokens},
		{"cache_write_tokens", &usage.CacheWriteTokens},
	}
	for _, count := range counts {
		if err := decodeOptionalField(fields, count.name, count.value, decodeInt64); err != nil {
			return Usage{}, fmt.Errorf("usage %w", err)
		}
	}
	for _, count := range counts {
		if *count.value < 0 {
			return Usage{}, fmt.Errorf("usage %s must not be negative", count.name)
		}
	}
	if raw, ok := fields["provider_raw"]; ok {
		if err := decodeJSON(raw, &usage.ProviderRaw); err != nil {
			return Usage{}, fmt.Errorf("usage provider_raw: %w", err)
		}
		usage.ProviderRaw = copyRawMap(usage.ProviderRaw)
	}
	return usage, nil
}

type CostStatus string

const (
	CostStatusKnown   CostStatus = "known"
	CostStatusUnknown CostStatus = "unknown"
)

// Unknown-cost reasons a worker records on a Cost, from the v1 unknown_reason
// set. A cost with no reason, such as one stored before the field, reports
// CostUnknownProviderDidNotReport.
const (
	// CostUnknownProviderDidNotReport: the provider omitted the usage, cost or
	// tool charge needed to settle the call.
	CostUnknownProviderDidNotReport = "provider_did_not_report_cost"
	// CostUnknownCatalogIncomplete: the provider reported usage the catalog
	// cannot price (no quote, an unpriced component or an unmodeled billing
	// multiplier).
	CostUnknownCatalogIncomplete = "catalog_incomplete"
)

func (status CostStatus) Valid() bool {
	switch status {
	case CostStatusKnown, CostStatusUnknown:
		return true
	default:
		return false
	}
}

type Cost struct {
	Status CostStatus
	// Exact USD fields are the v1 contract. A nil pointer means unknown; a
	// non-nil zero value is known free.
	ReservedCostUSD *pricing.USD
	ActualCostUSD   *pricing.USD
	Method          string
	CatalogVersion  string
	// UnknownReason says why an unknown cost is unknown; it is empty for a
	// known cost. It is not part of the Cost encoding, which earlier workers
	// decode strictly: storage that keeps it saves it beside the response.
	UnknownReason string
}

func (cost Cost) MarshalJSON() ([]byte, error) {
	if cost.Status != "" && !cost.Status.Valid() {
		return nil, fmt.Errorf("cost status %q is invalid", cost.Status)
	}
	// V1 responses always carry both nullable exact fields. A nil value is
	// meaningful: it records that the cost is unknown rather than zero.
	fields := map[string]any{
		"method":            cost.Method,
		"catalog_version":   cost.CatalogVersion,
		"reserved_cost_usd": cost.ReservedCostUSD,
		"actual_cost_usd":   cost.ActualCostUSD,
	}
	if cost.Status != "" {
		fields["cost_status"] = cost.Status
	}
	return marshalObject(fields)
}

func (cost *Cost) UnmarshalJSON(data []byte) error {
	decoded, err := decodeCost(data)
	if err != nil {
		return err
	}
	*cost = decoded
	return nil
}

func decodeCost(data []byte) (Cost, error) {
	fields, err := decodeClosedObject(data, "cost_status", "reserved_cost_usd", "actual_cost_usd", "method", "catalog_version")
	if err != nil {
		return Cost{}, err
	}
	reader := fieldReader{fields: fields}
	cost := Cost{Status: CostStatus(reader.optionalString("cost_status"))}
	if reader.err != nil {
		return Cost{}, reader.err
	}
	if cost.Status != "" && !cost.Status.Valid() {
		return Cost{}, fmt.Errorf("cost status %q is invalid", cost.Status)
	}
	cost.Method = reader.optionalString("method")
	cost.CatalogVersion = reader.optionalString("catalog_version")
	if reader.err != nil {
		return Cost{}, reader.err
	}
	decodeUSD := pointerTo(unmarshalValue[pricing.USD])
	readNullableField(&reader, "reserved_cost_usd", &cost.ReservedCostUSD, decodeUSD)
	readNullableField(&reader, "actual_cost_usd", &cost.ActualCostUSD, decodeUSD)
	if reader.err != nil {
		return Cost{}, fmt.Errorf("cost %w", reader.err)
	}
	return cost, nil
}

type ProviderFacts struct {
	ResponseID   string
	RequestID    string
	GenerationID string
	FinishReason string
	Raw          map[string]json.RawMessage
}

func (provider ProviderFacts) MarshalJSON() ([]byte, error) {
	fields := map[string]any{}
	setIfNotEmpty(fields, "response_id", provider.ResponseID)
	setIfNotEmpty(fields, "request_id", provider.RequestID)
	setIfNotEmpty(fields, "generation_id", provider.GenerationID)
	setIfNotEmpty(fields, "finish_reason", provider.FinishReason)
	if len(provider.Raw) > 0 {
		fields["raw"] = copyRawMap(provider.Raw)
	}
	return marshalObject(fields)
}

func decodeProviderFacts(data []byte) (ProviderFacts, error) {
	fields, err := decodeClosedObject(data, "response_id", "request_id", "generation_id", "finish_reason", "raw")
	if err != nil {
		return ProviderFacts{}, err
	}
	reader := fieldReader{fields: fields}
	provider := ProviderFacts{
		ResponseID:   reader.optionalString("response_id"),
		RequestID:    reader.optionalString("request_id"),
		GenerationID: reader.optionalString("generation_id"),
		FinishReason: reader.optionalString("finish_reason"),
	}
	if reader.err != nil {
		return ProviderFacts{}, reader.err
	}
	if raw, ok := fields["raw"]; ok {
		if err := decodeJSON(raw, &provider.Raw); err != nil {
			return ProviderFacts{}, fmt.Errorf("provider raw: %w", err)
		}
		provider.Raw = copyRawMap(provider.Raw)
	}
	return provider, nil
}

type Response struct {
	APIVersion   string
	OperationKey string
	OperationID  string
	Status       ResponseStatus
	Output       []Item
	Route        RouteFacts
	Service      ServiceFacts
	Usage        Usage
	Cost         Cost
	Provider     ProviderFacts
	Continuation *Continuation
	Diagnostics  []Diagnostic
}

func (response Response) MarshalJSON() ([]byte, error) {
	if response.APIVersion != "" && response.APIVersion != APIVersion {
		return nil, fmt.Errorf("api_version %q is unsupported", response.APIVersion)
	}
	if response.OperationKey == "" {
		return nil, errorsForField("response", "operation_key")
	}
	if !response.Status.Valid() {
		return nil, fmt.Errorf("response status %q is invalid", response.Status)
	}
	fields := map[string]any{
		"api_version":   APIVersion,
		"operation_key": response.OperationKey,
		"status":        response.Status,
		"output":        emptyIfNil(response.Output),
		"route":         response.Route,
		"service":       response.Service,
		"usage":         response.Usage,
		"cost":          response.Cost,
		"provider":      response.Provider,
		"diagnostics":   emptyIfNil(response.Diagnostics),
	}
	setIfNotEmpty(fields, "operation_id", response.OperationID)
	setIfNotNil(fields, "continuation", response.Continuation)
	return marshalObject(fields)
}

func (response *Response) UnmarshalJSON(data []byte) error {
	fields, err := decodeClosedObject(data, "api_version", "operation_key", "operation_id", "status", "output", "route", "service", "usage", "cost", "provider", "continuation", "diagnostics")
	if err != nil {
		return err
	}
	apiVersion, operationKey, err := decodeEngineHeader(fields)
	if err != nil {
		return err
	}
	reader := fieldReader{fields: fields}
	status := ResponseStatus(reader.requiredString("status"))
	if reader.err != nil {
		return reader.err
	}
	if !status.Valid() {
		return fmt.Errorf("response status %q is invalid", status)
	}
	result := Response{APIVersion: apiVersion, OperationKey: operationKey, Status: status}
	result.OperationID = reader.optionalString("operation_id")
	readOptionalField(&reader, "output", &result.Output, decodeItems)
	readOptionalField(&reader, "route", &result.Route, decodeRouteFacts)
	readOptionalField(&reader, "service", &result.Service, decodeServiceFacts)
	readOptionalField(&reader, "usage", &result.Usage, decodeUsage)
	readOptionalField(&reader, "cost", &result.Cost, decodeCost)
	readOptionalField(&reader, "provider", &result.Provider, decodeProviderFacts)
	readNullableField(&reader, "continuation", &result.Continuation, decodeContinuation)
	readOptionalField(&reader, "diagnostics", &result.Diagnostics, decodeDiagnostics)
	if reader.err != nil {
		return reader.err
	}
	result.Output = emptyIfNil(result.Output)
	result.Diagnostics = emptyIfNil(result.Diagnostics)
	*response = result
	return nil
}
