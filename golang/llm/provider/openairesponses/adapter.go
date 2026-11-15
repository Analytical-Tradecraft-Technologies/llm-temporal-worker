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
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaiwire"
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
	if err := provider.RequireAdapterArgs("openai responses", client != nil, endpointID); err != nil {
		return nil, err
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
		return openaiwire.LowerError(err)
	}
	return nil
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, provider.NewCompileError("adapter is nil")
	}
	if err := provider.StartCompile(ctx, input.Query, "openai responses", provider.FamilyOpenAIResponses, adapter.endpointID); err != nil {
		return provider.Call{}, err
	}
	if (input.Request.WebSearch || input.Request.CodeExecution || input.Request.WebFetch) && !adapter.client.directOpenAI {
		return provider.Call{}, provider.NewCompileError("hosted tools require the direct OpenAI Responses transport")
	}
	input.Metadata.RecordHostedTools(input.Request)
	normalized, serviceClass, err := provider.NormalizeCompileRequest(input.Request, input.Query, "")
	if err != nil {
		return provider.Call{}, err
	}
	if err := provider.RequirePinnedContinuation(normalized, adapter.endpointID); err != nil {
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
		return provider.Call{}, openaiwire.LowerError(err)
	}
	if err := adapter.enforceStoragePolicy(&params); err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	return provider.NewCall(adapter.endpointID, provider.FamilyOpenAIResponses, normalized, serviceClass, params, input.Metadata, set.Version, tier)
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, provider.NewDispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	params, err := provider.StartDispatch[responses.ResponseNewParams](ctx, call, provider.FamilyOpenAIResponses, adapter.endpointID)
	if err != nil {
		return provider.Result{}, err
	}
	if err := adapter.enforceStoragePolicy(&params); err != nil {
		return provider.Result{}, provider.NewDispatchError(err.Error(), provider.DispatchNotDispatched)
	}
	call.SDKParams = params
	// Encode the ordered schemas before the possible-write boundary, so a
	// failure here is a definite non-dispatch.
	ordered, err := openaiwire.SchemaOrderOptions(params, true)
	if err != nil {
		return provider.Result{}, provider.NewDispatchError("schema property order could not be encoded", provider.DispatchNotDispatched)
	}
	// Nothing reached the transport when the error came before the probe, or
	// from the Azure token-credential middleware, which the SDK runs inside
	// the probe but before it sends the request (#1495).
	dispatch, err := provider.BeginDispatch(ctx, observer, provider.DispatchTarget{EndpointID: adapter.endpointID, Provider: adapter.Name(), CredentialUnsent: true, MapError: func(err error, _ string) *provider.Error { return mapError(err) }})
	if err != nil {
		return provider.Result{}, err
	}
	response, rawResponse, err := provider.SendHTTP(dispatch, call.OperationKey, func(probe *provider.DispatchProbe, raw **http.Response) (*responses.Response, error) {
		requestOptions := []option.RequestOption{option.WithResponseInto(raw), option.WithMiddleware(probe.Middleware)}
		requestOptions = append(requestOptions, ordered...)
		return adapter.client.sdk.Responses.New(dispatch.Context, params, requestOptions...)
	})
	if err != nil {
		return provider.Result{}, err
	}
	// A custom transport or a future SDK implementation can return a nil
	// response without an error. Treat that as an accepted malformed provider
	// response instead of dereferencing it below and panicking the Activity.
	if response == nil {
		return provider.Result{}, provider.NewInvalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.HTTPResponseMetadata(response.ID, string(response.ServiceTier), rawResponse, "x-request-id")
	return dispatch.Lift(metadata, len(response.Output), func() (llm.Response, error) {
		lifted, err := liftResponse(call, response, metadata.RequestID)
		if err != nil || !adapter.exaAgent {
			return lifted, err
		}
		cost, costErr := exaAgentCost(response.RawJSON())
		if costErr != nil {
			return llm.Response{}, provider.NewInvalidResponseErrorWithID(call, metadata.RequestID, response.ID, costErr.Error())
		}
		if cost != nil {
			lifted.Cost = llm.Cost{ActualCostUSD: cost, Method: "exa_reported"}
		}
		return lifted, nil
	})
}

func requiredFeatures(request llm.Request) []provider.Feature {
	return provider.RequestFeatures(request, openaiwire.PartFeature, nil, func(kind llm.OutputKind) bool { return kind == llm.OutputKindJSONSchema })
}
