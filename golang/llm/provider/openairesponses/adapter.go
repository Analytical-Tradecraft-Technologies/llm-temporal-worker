package openairesponses

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaischema"
)

// Adapter owns one official OpenAI Responses client and one configured
// endpoint. SDK unions never cross this package boundary.
type Adapter struct {
	client            *Client
	endpointID        string
	capabilityVersion string
	storageDenied     bool
	omitServiceTier   bool
	exaAgent          bool
	// allowedExtensions holds the openai.responses extension fields this
	// endpoint accepts. Empty rejects every extension field.
	allowedExtensions map[string]struct{}
}

// WithoutRequestServiceTier keeps service_tier out of the request for APIs
// that do not define the field, such as Azure OpenAI Responses. The public
// service class still selects routing and pricing.
func WithoutRequestServiceTier() AdapterOption {
	return func(adapter *Adapter) { adapter.omitServiceTier = true }
}

// ModelListerAdapter is the direct OpenAI Responses adapter with its optional
// management capability. Azure adapters use Adapter directly and do not
// satisfy provider.ModelLister.
type ModelListerAdapter struct{ *Adapter }

// NewOpenAIAdapter constructs an adapter for the direct OpenAI Responses API.
// The separate constructor keeps the management capability explicit; generic
// New remains a one-shot adapter for compatible endpoints.
func NewOpenAIAdapter(client *Client, endpointID, capabilityVersion string, options ...AdapterOption) (*ModelListerAdapter, error) {
	if client == nil || !client.directOpenAI {
		return nil, fmt.Errorf("openai responses: client is not verified for direct OpenAI management")
	}
	base, err := New(client, endpointID, capabilityVersion, options...)
	if err != nil {
		return nil, err
	}
	return &ModelListerAdapter{Adapter: base}, nil
}

func New(client *Client, endpointID, capabilityVersion string, options ...AdapterOption) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("openai responses: client is required")
	}
	if endpointID == "" {
		return nil, fmt.Errorf("openai responses: endpoint ID is required")
	}
	if capabilityVersion == "" {
		capabilityVersion = defaultCapabilityVersion
	}
	adapter := &Adapter{client: client, endpointID: endpointID, capabilityVersion: capabilityVersion}
	for _, option := range options {
		if option != nil {
			option(adapter)
		}
	}
	return adapter, nil
}

// NewAdapter is an explicit alias used by route construction code.
func NewAdapter(client *Client, endpointID, capabilityVersion string, options ...AdapterOption) (*Adapter, error) {
	return New(client, endpointID, capabilityVersion, options...)
}

func (adapter *Adapter) Name() string { return adapterName }

// lowerRequestMap builds the intended wire body for this endpoint: the common
// lowering with the endpoint's request policy applied to the map itself, so
// the body that is sent is exactly the body that was intended. tier is the
// endpoint's configured provider value for the class; empty keeps the
// canonical Responses tier for the class.
func (adapter *Adapter) lowerRequestMap(request llm.Request, serviceClass llm.ServiceClass, tier string) (map[string]any, loweredToolPolicy, error) {
	requestMap, policy, err := lowerRequestMap(request, serviceClass, adapter.storageDenied, adapter.allowedExtensions)
	if err != nil {
		return nil, loweredToolPolicy{}, err
	}
	if tier != "" {
		requestMap["service_tier"] = tier
	}
	if adapter.omitServiceTier {
		delete(requestMap, "service_tier")
	}
	if adapter.exaAgent {
		if err := exaAgentRequest(requestMap, request); err != nil {
			return nil, loweredToolPolicy{}, err
		}
	}
	// A caller's explicit store value is left for enforceStoragePolicy to
	// accept or reject.
	if _, explicit := requestMap["store"]; adapter.storageDenied && !explicit {
		requestMap["store"] = false
	}
	return requestMap, policy, nil
}

func (adapter *Adapter) lowerRequest(request llm.Request, serviceClass llm.ServiceClass, tier string) (responses.ResponseNewParams, error) {
	requestMap, policy, err := adapter.lowerRequestMap(request, serviceClass, tier)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	return requestParams(requestMap, policy)
}

func (adapter *Adapter) Capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if adapter == nil {
		return provider.CapabilitySet{}, fmt.Errorf("openai responses: adapter is nil")
	}
	return adapter.capabilities(ctx, query)
}

// ValidateRequest rejects structured output the provider documents as an
// error before an engine reserves budget. Compile repeats the check, and any
// other lowering failure is left for it to report.
func (adapter *Adapter) ValidateRequest(ctx context.Context, request llm.Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if adapter == nil {
		return provider.NewCompileError("adapter is nil")
	}
	if request.Output == nil {
		return nil
	}
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		return nil
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		return nil
	}
	if _, _, err := adapter.lowerRequestMap(normalized, serviceClass, ""); errors.As(err, new(*openaischema.UnsupportedError)) {
		return lowerError(err)
	}
	return nil
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, provider.NewCompileError("adapter is nil")
	}
	if err := ctx.Err(); err != nil {
		return provider.Call{}, provider.NewCompileContextError(err)
	}
	if err := provider.ValidateCapabilityQuery(input.Query, "openai responses", provider.FamilyOpenAIResponses, adapter.endpointID); err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	if (input.Request.WebSearch || input.Request.CodeExecution || input.Request.WebFetch) && !adapter.client.directOpenAI {
		return provider.Call{}, provider.NewCompileError("hosted tools require the direct OpenAI Responses transport")
	}
	input.Metadata.WebSearch = input.Request.WebSearch
	input.Metadata.WebFetch = input.Request.WebFetch
	input.Metadata.CodeExecution = input.Request.CodeExecution
	normalized, serviceClass, err := provider.NormalizeCompileRequest(input.Request, input.Query, "")
	if err != nil {
		return provider.Call{}, err
	}
	set, err := provider.CompileCapabilities(input.Capability, func() (provider.CapabilitySet, error) {
		return adapter.capabilities(ctx, input.Query)
	}, adapter.capabilityVersion)
	if err != nil {
		return provider.Call{}, err
	}
	for _, feature := range requiredFeatures(normalized) {
		capability, err := set.Resolve(feature, input.Strict)
		if err != nil {
			return provider.Call{}, provider.NewUnsupportedFeatureError(feature, err.Error())
		}
		if capability.State != provider.CapabilityNative && capability.State != provider.CapabilityEmulated {
			return provider.Call{}, provider.NewUnsupportedFeatureError(feature, fmt.Sprintf("capability is %s", capability.State))
		}
	}
	if callID, reserved := provider.ReservedToolResultPrefix(normalized.Input); input.Strict && reserved {
		return provider.Call{}, provider.NewStrictPortabilityError(fmt.Sprintf("tool result %q output starts with the reserved tool-error prefix and cannot be distinguished from a failed result in strict portability mode", callID))
	}
	// Routing and pricing key the request on the endpoint's configured
	// provider value, so the request must carry the same tier.
	tier := input.Metadata.ProviderTier
	if tier == "" {
		tier = providerTier(serviceClass)
	}
	params, err := adapter.lowerRequest(normalized, serviceClass, tier)
	if err != nil {
		return provider.Call{}, lowerError(err)
	}
	if err := adapter.enforceStoragePolicy(&params); err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	metadata, err := provider.CompileMetadata(input.Metadata, normalized, set.Version, tier)
	if err != nil {
		return provider.Call{}, err
	}
	return provider.Call{
		EndpointID:   adapter.endpointID,
		Family:       provider.FamilyOpenAIResponses,
		Model:        normalized.Model,
		OperationKey: normalized.OperationKey,
		ServiceClass: serviceClass,
		SDKParams:    params,
		OutputSchema: outputSchema(normalized),
		Metadata:     metadata,
	}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, provider.NewDispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	if err := ctx.Err(); err != nil {
		return provider.Result{}, provider.NewDispatchContextError(err)
	}
	if call.Family != provider.FamilyOpenAIResponses || call.EndpointID != adapter.endpointID {
		return provider.Result{}, provider.NewDispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*responses.ResponseNewParams); pointerOK && pointer != nil {
			params = *pointer
			ok = true
		}
	}
	if !ok {
		return provider.Result{}, provider.NewDispatchError("call SDK parameters have unexpected type", provider.DispatchNotDispatched)
	}
	if err := adapter.enforceStoragePolicy(&params); err != nil {
		return provider.Result{}, provider.NewDispatchError(err.Error(), provider.DispatchNotDispatched)
	}
	call.SDKParams = params
	// Encode the ordered schemas before the possible-write boundary, so a
	// failure here is a definite non-dispatch.
	ordered, err := schemaOrderOptions(params)
	if err != nil {
		return provider.Result{}, provider.NewDispatchError("schema property order could not be encoded", provider.DispatchNotDispatched)
	}
	if observer == nil {
		observer = provider.NopObserver{}
	}
	callContext, egressOutcome := provider.WithEgressOutcome(ctx)
	if err := observer.BeforePossibleWrite(callContext); err != nil {
		return provider.Result{}, provider.NewDispatchObserverError(err, provider.DispatchNotDispatched)
	}
	var rawResponse *http.Response
	probe := &provider.DispatchProbe{}
	requestOptions := []option.RequestOption{option.WithResponseInto(&rawResponse), option.WithMiddleware(probe.Middleware)}
	requestOptions = append(requestOptions, ordered...)
	response, panicked, err := provider.CallRecovered(func() (*responses.Response, error) {
		return adapter.client.sdk.Responses.New(callContext, params, requestOptions...)
	})
	if panicked != nil {
		return provider.Result{}, provider.WithEndpointID(probe.PanicError(call.OperationKey, panicked), adapter.endpointID)
	}
	if rawResponse != nil && provider.IsRedirectStatus(rawResponse.StatusCode) {
		return provider.Result{}, provider.WithEndpointID(provider.NewRedirectResponseError(rawResponse.StatusCode), adapter.endpointID)
	}
	if err != nil {
		if mapped := provider.ClassifyEgressOutcome(egressOutcome, err); mapped != nil {
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		// Nothing reached the transport when the error came before the
		// probe, or from the Azure token-credential middleware, which the
		// SDK runs inside the probe but before it sends the request (#1495).
		if mapped := provider.ClassifyUnsentError(err); mapped != nil && (!probe.Sent() || errors.Is(err, provider.ErrProviderCredentials)) {
			mapped.SafeDetails = map[string]string{"provider": adapter.Name()}
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		return provider.Result{}, provider.WithEndpointID(mapError(err), adapter.endpointID)
	}
	// A custom transport or a future SDK implementation can return a nil
	// response without an error. Treat that as an accepted malformed provider
	// response instead of dereferencing it below and panicking the Activity.
	if response == nil {
		return provider.Result{}, provider.NewInvalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: string(response.ServiceTier)}
	if rawResponse != nil {
		metadata.Status = rawResponse.StatusCode
		metadata.RequestID = rawResponse.Header.Get("x-request-id")
	}
	if err := observer.AfterResponseHeaders(callContext, metadata); err != nil {
		mapped := provider.NewDispatchObserverError(err, provider.DispatchAccepted)
		mapped.Provider.ResponseID = response.ID
		mapped.Provider.RequestID = metadata.RequestID
		return provider.Result{}, mapped
	}
	observer.OnProgress(callContext, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Output)})
	lifted, err := liftResponse(call, response, metadata.RequestID)
	if err != nil {
		return provider.Result{}, err
	}
	if adapter.exaAgent {
		cost, costErr := exaAgentCost(response.RawJSON())
		if costErr != nil {
			mapped := provider.NewInvalidResponseError(call, metadata.RequestID, costErr.Error())
			mapped.Provider.ResponseID = response.ID
			return provider.Result{}, mapped
		}
		if cost != nil {
			lifted.Cost = llm.Cost{ActualCostUSD: cost, Method: "exa_reported"}
		}
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
	return provider.UniqueFeatures(features)
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

// lowerError maps a lowering failure to the error contract. A request that
// structured output cannot represent is unsupported, not malformed.
func lowerError(err error) *provider.Error {
	var unsupported *openaischema.UnsupportedError
	if errors.As(err, &unsupported) {
		return provider.NewUnsupportedFeatureError(provider.FeatureStructuredOutput, err.Error())
	}
	return provider.NewCompileError(err.Error())
}
