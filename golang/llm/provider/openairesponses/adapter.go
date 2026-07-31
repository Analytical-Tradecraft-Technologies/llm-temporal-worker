package openairesponses

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// Adapter owns one official OpenAI Responses client and one configured
// endpoint. SDK unions never cross this package boundary.
type Adapter struct {
	client            *Client
	endpointID        string
	capabilityVersion string
}

// ModelListerAdapter is the direct OpenAI Responses adapter with its optional
// management capability. Azure adapters use Adapter directly and do not
// satisfy provider.ModelLister.
type ModelListerAdapter struct{ *Adapter }

// NewOpenAIAdapter constructs an adapter for the direct OpenAI Responses API.
// The separate constructor keeps the management capability explicit; generic
// New remains a one-shot adapter for compatible endpoints.
func NewOpenAIAdapter(client *Client, endpointID, capabilityVersion string) (*ModelListerAdapter, error) {
	base, err := New(client, endpointID, capabilityVersion)
	if err != nil {
		return nil, err
	}
	return &ModelListerAdapter{Adapter: base}, nil
}

func New(client *Client, endpointID, capabilityVersion string) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("openai responses: client is required")
	}
	if endpointID == "" {
		return nil, fmt.Errorf("openai responses: endpoint ID is required")
	}
	if capabilityVersion == "" {
		capabilityVersion = defaultCapabilityVersion
	}
	return &Adapter{client: client, endpointID: endpointID, capabilityVersion: capabilityVersion}, nil
}

// NewAdapter is an explicit alias used by route construction code.
func NewAdapter(client *Client, endpointID, capabilityVersion string) (*Adapter, error) {
	return New(client, endpointID, capabilityVersion)
}

func (adapter *Adapter) Name() string { return adapterName }

func (adapter *Adapter) Capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if adapter == nil {
		return provider.CapabilitySet{}, fmt.Errorf("openai responses: adapter is nil")
	}
	return adapter.capabilities(ctx, query)
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, compileError("adapter is nil")
	}
	if err := ctx.Err(); err != nil {
		return provider.Call{}, compileContextError(err)
	}
	if err := validateQuery(input.Query, adapter.endpointID); err != nil {
		return provider.Call{}, compileError(err.Error())
	}
	normalized, err := llm.NormalizeRequest(input.Request)
	if err != nil {
		return provider.Call{}, compileError(err.Error())
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		return provider.Call{}, compileError(err.Error())
	}
	if err := llm.ValidateServiceClassFallbacks(serviceClass, normalized.ServiceClassFallbacks); err != nil {
		return provider.Call{}, compileError(err.Error())
	}
	if input.Query.Model != "" && input.Query.Model != normalized.Model {
		return provider.Call{}, compileError(fmt.Sprintf("model %q does not match capability query %q", normalized.Model, input.Query.Model))
	}
	set := input.Capability
	if set.Version == "" && len(set.Features) == 0 {
		set, err = adapter.capabilities(ctx, input.Query)
		if err != nil {
			return provider.Call{}, compileError(err.Error())
		}
	}
	if set.Version == "" {
		set.Version = adapter.capabilityVersion
	}
	for _, feature := range requiredFeatures(normalized) {
		capability, err := set.Resolve(feature, input.Strict)
		if err != nil {
			return provider.Call{}, unsupportedError(feature, err.Error())
		}
		if capability.State != provider.CapabilityNative && capability.State != provider.CapabilityEmulated {
			return provider.Call{}, unsupportedError(feature, fmt.Sprintf("capability is %s", capability.State))
		}
	}
	params, err := lowerRequest(normalized, serviceClass)
	if err != nil {
		return provider.Call{}, compileError(err.Error())
	}
	digest := input.Metadata.SchemaDigest
	if digest == ([32]byte{}) {
		digest, err = llm.RequestDigest(normalized)
		if err != nil {
			return provider.Call{}, compileError(err.Error())
		}
	}
	metadata := input.Metadata
	metadata.SchemaDigest = digest
	metadata.CapabilityVersion = set.Version
	metadata.ProviderTier = string(providerTier(serviceClass))
	metadata.OpaqueStateRequired = normalized.Continuation != nil
	if metadata.EstimatedBytes == 0 {
		canonical, canonicalErr := canonicalRequestBytes(normalized)
		if canonicalErr != nil {
			return provider.Call{}, compileError(canonicalErr.Error())
		}
		metadata.EstimatedBytes = len(canonical)
	}
	return provider.Call{
		EndpointID:   adapter.endpointID,
		Family:       provider.FamilyOpenAIResponses,
		Model:        normalized.Model,
		OperationKey: normalized.OperationKey,
		ServiceClass: serviceClass,
		SDKParams:    params,
		Metadata:     metadata,
	}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, dispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	if err := ctx.Err(); err != nil {
		return provider.Result{}, dispatchContextError(err)
	}
	if call.Family != provider.FamilyOpenAIResponses || call.EndpointID != adapter.endpointID {
		return provider.Result{}, dispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*responses.ResponseNewParams); pointerOK && pointer != nil {
			params = *pointer
			ok = true
		}
	}
	if !ok {
		return provider.Result{}, dispatchError("call SDK parameters have unexpected type", provider.DispatchNotDispatched)
	}
	if observer == nil {
		observer = provider.NopObserver{}
	}
	callContext, egressOutcome := provider.WithEgressOutcome(ctx)
	if err := observer.BeforePossibleWrite(callContext); err != nil {
		return provider.Result{}, dispatchObserverError(err, provider.DispatchNotDispatched)
	}
	var rawResponse *http.Response
	response, err := adapter.client.sdk.Responses.New(callContext, params, option.WithResponseInto(&rawResponse))
	if rawResponse != nil && provider.IsRedirectStatus(rawResponse.StatusCode) {
		return provider.Result{}, provider.WithEndpointID(provider.NewRedirectResponseError(rawResponse.StatusCode), adapter.endpointID)
	}
	if err != nil {
		if mapped := provider.ClassifyEgressOutcome(egressOutcome, err); mapped != nil {
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		return provider.Result{}, provider.WithEndpointID(mapError(err), adapter.endpointID)
	}
	// A custom transport or a future SDK implementation can return a nil
	// response without an error. Treat that as an accepted malformed provider
	// response instead of dereferencing it below and panicking the Activity.
	if response == nil {
		return provider.Result{}, invalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: string(response.ServiceTier)}
	if rawResponse != nil {
		metadata.Status = rawResponse.StatusCode
		metadata.RequestID = rawResponse.Header.Get("x-request-id")
	}
	if err := observer.AfterResponseHeaders(callContext, metadata); err != nil {
		mapped := dispatchObserverError(err, provider.DispatchAccepted)
		mapped.Provider.ResponseID = response.ID
		mapped.Provider.RequestID = metadata.RequestID
		return provider.Result{}, mapped
	}
	observer.OnProgress(callContext, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Output)})
	lifted, err := liftResponse(call, response, metadata.RequestID)
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{Response: lifted}, nil
}

func requiredFeatures(request llm.Request) []provider.Feature {
	features := []provider.Feature{provider.FeatureText, provider.FeatureUsage}
	for _, instruction := range request.Instructions {
		for _, part := range instruction.Content {
			features = append(features, partFeature(part))
		}
	}
	for _, item := range request.Input {
		switch value := item.(type) {
		case llm.Message:
			for _, part := range value.Content {
				features = append(features, partFeature(part))
			}
		case llm.ToolCall, llm.ToolResult:
			features = append(features, provider.FeatureToolCall)
		}
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		features = append(features, provider.FeatureToolCall)
	}
	if request.Output != nil && request.Output.Format.Kind == llm.OutputKindJSONSchema {
		features = append(features, provider.FeatureStructuredOutput)
	}
	if request.Reasoning != nil {
		features = append(features, provider.FeatureReasoning)
	}
	if request.Continuation != nil {
		features = append(features, provider.FeatureContinuation)
	}
	return uniqueFeatures(features)
}

func partFeature(part llm.Part) provider.Feature {
	switch part.PartKind() {
	case llm.PartKindImage:
		return provider.FeatureImage
	case llm.PartKindDocument:
		return provider.FeatureDocument
	default:
		return provider.FeatureText
	}
}

func uniqueFeatures(features []provider.Feature) []provider.Feature {
	seen := make(map[provider.Feature]struct{}, len(features))
	result := make([]provider.Feature, 0, len(features))
	for _, feature := range features {
		if feature == "" {
			continue
		}
		if _, ok := seen[feature]; ok {
			continue
		}
		seen[feature] = struct{}{}
		result = append(result, feature)
	}
	return result
}

func canonicalRequestBytes(request llm.Request) ([]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return llm.CanonicalJSON(encoded)
}

func compileError(message string) *provider.Error {
	return provider.NewError(provider.CodeInvalidArgument, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, message)
}

func unsupportedError(feature provider.Feature, message string) *provider.Error {
	return provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, fmt.Sprintf("%s: %s", feature, message))
}

func compileContextError(err error) *provider.Error {
	if err == context.Canceled {
		return provider.NewError(provider.CodeCanceled, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "compile canceled")
	}
	return provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "compile deadline exceeded")
}

func dispatchError(message string, certainty provider.DispatchCertainty) *provider.Error {
	return provider.NewError(provider.CodeInvalidArgument, provider.PhaseDispatch, certainty, provider.RetryNever, message)
}

func dispatchContextError(err error) *provider.Error {
	if err == context.Canceled {
		return provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "dispatch canceled")
	}
	return provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "dispatch deadline exceeded")
}

func dispatchObserverError(err error, certainty provider.DispatchCertainty) *provider.Error {
	mapped := provider.NewError(provider.CodeInternal, provider.PhaseDispatch, certainty, provider.RetryNever, "observer rejected provider response")
	mapped.Cause = err
	return mapped
}
