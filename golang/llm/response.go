package llm

import (
	"encoding/json"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

type ResponseStatus string

const (
	ResponseStatusCompleted       ResponseStatus = "completed"
	ResponseStatusToolCalls       ResponseStatus = "tool_calls"
	ResponseStatusRefused         ResponseStatus = "refused"
	ResponseStatusLength          ResponseStatus = "length"
	ResponseStatusContentFiltered ResponseStatus = "content_filtered"
)

func (status ResponseStatus) Valid() bool {
	switch status {
	case ResponseStatusCompleted, ResponseStatusToolCalls, ResponseStatusRefused, ResponseStatusLength, ResponseStatusContentFiltered:
		return true
	default:
		return false
	}
}

type ModelIdentityBasis string

const (
	ModelIdentityBasisUnknown          ModelIdentityBasis = "unknown"
	ModelIdentityBasisConfiguredRoute  ModelIdentityBasis = "configured_route"
	ModelIdentityBasisProviderReported ModelIdentityBasis = "provider_reported"
)

type RouteFacts struct {
	RouteID        string
	EndpointID     string
	APIFamily      string
	RequestedModel string
	ResolvedModel  string
	// ResolvedModel is the configured routing target, not revision attestation.
	ModelIdentityBasis    ModelIdentityBasis
	ObservedModelRevision string
}

func (route RouteFacts) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any)
	if route.RouteID != "" {
		fields["route_id"] = route.RouteID
	}
	if route.EndpointID != "" {
		fields["endpoint_id"] = route.EndpointID
	}
	if route.APIFamily != "" {
		fields["api_family"] = route.APIFamily
	}
	if route.RequestedModel != "" {
		fields["requested_model"] = route.RequestedModel
	}
	if route.ResolvedModel != "" {
		fields["resolved_model"] = route.ResolvedModel
	}
	if route.ModelIdentityBasis != "" {
		fields["model_identity_basis"] = route.ModelIdentityBasis
	}
	if route.ObservedModelRevision != "" {
		fields["observed_model_revision"] = route.ObservedModelRevision
	}
	return marshalObject(fields)
}

func decodeRouteFacts(data []byte) (RouteFacts, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return RouteFacts{}, err
	}
	if err := checkUnknownFields(fields, "route_id", "endpoint_id", "api_family", "requested_model", "resolved_model", "model_identity_basis", "observed_model_revision"); err != nil {
		return RouteFacts{}, err
	}
	route := RouteFacts{}
	if route.RouteID, _, err = optionalString(fields, "route_id"); err != nil {
		return RouteFacts{}, err
	}
	if route.EndpointID, _, err = optionalString(fields, "endpoint_id"); err != nil {
		return RouteFacts{}, err
	}
	if route.APIFamily, _, err = optionalString(fields, "api_family"); err != nil {
		return RouteFacts{}, err
	}
	if route.RequestedModel, _, err = optionalString(fields, "requested_model"); err != nil {
		return RouteFacts{}, err
	}
	if route.ResolvedModel, _, err = optionalString(fields, "resolved_model"); err != nil {
		return RouteFacts{}, err
	}
	basis, _, err := optionalString(fields, "model_identity_basis")
	if err != nil {
		return RouteFacts{}, err
	}
	route.ModelIdentityBasis = ModelIdentityBasis(basis)
	if route.ObservedModelRevision, _, err = optionalString(fields, "observed_model_revision"); err != nil {
		return RouteFacts{}, err
	}
	switch route.ModelIdentityBasis {
	case "", ModelIdentityBasisUnknown, ModelIdentityBasisConfiguredRoute:
		if route.ObservedModelRevision != "" {
			return RouteFacts{}, fmt.Errorf("observed model revision requires provider_reported identity basis")
		}
	case ModelIdentityBasisProviderReported:
		if route.ObservedModelRevision == "" {
			return RouteFacts{}, fmt.Errorf("provider_reported identity basis requires an observed model revision")
		}
	default:
		return RouteFacts{}, fmt.Errorf("invalid model identity basis %q", route.ModelIdentityBasis)
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
	fields, err := decodeObject(data)
	if err != nil {
		return ServiceFacts{}, err
	}
	if err := checkUnknownFields(fields, "requested", "attempted", "actual", "provider_value", "fallback_index"); err != nil {
		return ServiceFacts{}, err
	}
	requestedValue, err := requiredString(fields, "requested")
	if err != nil {
		return ServiceFacts{}, err
	}
	attemptedValue, err := requiredString(fields, "attempted")
	if err != nil {
		return ServiceFacts{}, err
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
	if raw, ok := fields["actual"]; ok && string(raw) != "null" {
		value, err := requiredString(map[string]json.RawMessage{"actual": raw}, "actual")
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
	fields, err := decodeObject(data)
	if err != nil {
		return Usage{}, err
	}
	if err := checkUnknownFields(fields, "input_tokens", "output_tokens", "reasoning_tokens", "cache_read_tokens", "cache_write_tokens", "provider_raw"); err != nil {
		return Usage{}, err
	}
	usage := Usage{}
	if raw, ok := fields["input_tokens"]; ok {
		usage.InputTokens, err = decodeInt64(raw)
		if err != nil {
			return Usage{}, fmt.Errorf("usage input_tokens: %w", err)
		}
	}
	if raw, ok := fields["output_tokens"]; ok {
		usage.OutputTokens, err = decodeInt64(raw)
		if err != nil {
			return Usage{}, fmt.Errorf("usage output_tokens: %w", err)
		}
	}
	if raw, ok := fields["reasoning_tokens"]; ok {
		usage.ReasoningTokens, err = decodeInt64(raw)
		if err != nil {
			return Usage{}, fmt.Errorf("usage reasoning_tokens: %w", err)
		}
	}
	if raw, ok := fields["cache_read_tokens"]; ok {
		usage.CacheReadTokens, err = decodeInt64(raw)
		if err != nil {
			return Usage{}, fmt.Errorf("usage cache_read_tokens: %w", err)
		}
	}
	if raw, ok := fields["cache_write_tokens"]; ok {
		usage.CacheWriteTokens, err = decodeInt64(raw)
		if err != nil {
			return Usage{}, fmt.Errorf("usage cache_write_tokens: %w", err)
		}
	}
	for name, value := range map[string]int64{
		"input_tokens":       usage.InputTokens,
		"output_tokens":      usage.OutputTokens,
		"reasoning_tokens":   usage.ReasoningTokens,
		"cache_read_tokens":  usage.CacheReadTokens,
		"cache_write_tokens": usage.CacheWriteTokens,
	} {
		if value < 0 {
			return Usage{}, fmt.Errorf("usage %s must not be negative", name)
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
	fields, err := decodeObject(data)
	if err != nil {
		return Cost{}, err
	}
	if err := checkUnknownFields(fields, "cost_status", "reserved_cost_usd", "actual_cost_usd", "method", "catalog_version"); err != nil {
		return Cost{}, err
	}
	cost := Cost{}
	status, _, err := optionalString(fields, "cost_status")
	if err != nil {
		return Cost{}, err
	}
	cost.Status = CostStatus(status)
	if cost.Status != "" && !cost.Status.Valid() {
		return Cost{}, fmt.Errorf("cost status %q is invalid", cost.Status)
	}
	if cost.Method, _, err = optionalString(fields, "method"); err != nil {
		return Cost{}, err
	}
	if cost.CatalogVersion, _, err = optionalString(fields, "catalog_version"); err != nil {
		return Cost{}, err
	}
	if raw, ok := fields["reserved_cost_usd"]; ok && string(raw) != "null" {
		var value pricing.USD
		if err := json.Unmarshal(raw, &value); err != nil {
			return Cost{}, fmt.Errorf("cost reserved_cost_usd: %w", err)
		}
		cost.ReservedCostUSD = &value
	}
	if raw, ok := fields["actual_cost_usd"]; ok && string(raw) != "null" {
		var value pricing.USD
		if err := json.Unmarshal(raw, &value); err != nil {
			return Cost{}, fmt.Errorf("cost actual_cost_usd: %w", err)
		}
		cost.ActualCostUSD = &value
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
	if provider.ResponseID != "" {
		fields["response_id"] = provider.ResponseID
	}
	if provider.RequestID != "" {
		fields["request_id"] = provider.RequestID
	}
	if provider.GenerationID != "" {
		fields["generation_id"] = provider.GenerationID
	}
	if provider.FinishReason != "" {
		fields["finish_reason"] = provider.FinishReason
	}
	if len(provider.Raw) > 0 {
		fields["raw"] = copyRawMap(provider.Raw)
	}
	return marshalObject(fields)
}

func decodeProviderFacts(data []byte) (ProviderFacts, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return ProviderFacts{}, err
	}
	if err := checkUnknownFields(fields, "response_id", "request_id", "generation_id", "finish_reason", "raw"); err != nil {
		return ProviderFacts{}, err
	}
	provider := ProviderFacts{}
	if provider.ResponseID, _, err = optionalString(fields, "response_id"); err != nil {
		return ProviderFacts{}, err
	}
	if provider.RequestID, _, err = optionalString(fields, "request_id"); err != nil {
		return ProviderFacts{}, err
	}
	if provider.GenerationID, _, err = optionalString(fields, "generation_id"); err != nil {
		return ProviderFacts{}, err
	}
	if provider.FinishReason, _, err = optionalString(fields, "finish_reason"); err != nil {
		return ProviderFacts{}, err
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
	output := response.Output
	if output == nil {
		output = []Item{}
	}
	diagnostics := response.Diagnostics
	if diagnostics == nil {
		diagnostics = []Diagnostic{}
	}
	fields := map[string]any{
		"api_version":   APIVersion,
		"operation_key": response.OperationKey,
		"status":        response.Status,
		"output":        output,
		"route":         response.Route,
		"service":       response.Service,
		"usage":         response.Usage,
		"cost":          response.Cost,
		"provider":      response.Provider,
		"diagnostics":   diagnostics,
	}
	if response.OperationID != "" {
		fields["operation_id"] = response.OperationID
	}
	if response.Continuation != nil {
		fields["continuation"] = response.Continuation
	}
	return marshalObject(fields)
}

func (response *Response) UnmarshalJSON(data []byte) error {
	fields, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err := checkUnknownFields(fields, "api_version", "operation_key", "operation_id", "status", "output", "route", "service", "usage", "cost", "provider", "continuation", "diagnostics"); err != nil {
		return err
	}
	apiVersion, err := requiredString(fields, "api_version")
	if err != nil {
		return err
	}
	if apiVersion != APIVersion {
		return fmt.Errorf("api_version %q is unsupported", apiVersion)
	}
	operationKey, err := requiredString(fields, "operation_key")
	if err != nil {
		return err
	}
	statusValue, err := requiredString(fields, "status")
	if err != nil {
		return err
	}
	status := ResponseStatus(statusValue)
	if !status.Valid() {
		return fmt.Errorf("response status %q is invalid", status)
	}
	result := Response{APIVersion: apiVersion, OperationKey: operationKey, Status: status}
	if result.OperationID, _, err = optionalString(fields, "operation_id"); err != nil {
		return err
	}
	if raw, ok := fields["output"]; ok {
		result.Output, err = decodeItems(raw)
		if err != nil {
			return fmt.Errorf("output: %w", err)
		}
	}
	if raw, ok := fields["route"]; ok {
		result.Route, err = decodeRouteFacts(raw)
		if err != nil {
			return fmt.Errorf("route: %w", err)
		}
	}
	if raw, ok := fields["service"]; ok {
		result.Service, err = decodeServiceFacts(raw)
		if err != nil {
			return fmt.Errorf("service: %w", err)
		}
	}
	if raw, ok := fields["usage"]; ok {
		result.Usage, err = decodeUsage(raw)
		if err != nil {
			return fmt.Errorf("usage: %w", err)
		}
	}
	if raw, ok := fields["cost"]; ok {
		result.Cost, err = decodeCost(raw)
		if err != nil {
			return fmt.Errorf("cost: %w", err)
		}
	}
	if raw, ok := fields["provider"]; ok {
		result.Provider, err = decodeProviderFacts(raw)
		if err != nil {
			return fmt.Errorf("provider: %w", err)
		}
	}
	if raw, ok := fields["continuation"]; ok && string(raw) != "null" {
		result.Continuation, err = decodeContinuation(raw)
		if err != nil {
			return fmt.Errorf("continuation: %w", err)
		}
	}
	if raw, ok := fields["diagnostics"]; ok {
		result.Diagnostics, err = decodeDiagnostics(raw)
		if err != nil {
			return fmt.Errorf("diagnostics: %w", err)
		}
	}
	if result.Output == nil {
		result.Output = []Item{}
	}
	if result.Diagnostics == nil {
		result.Diagnostics = []Diagnostic{}
	}
	*response = result
	return nil
}
