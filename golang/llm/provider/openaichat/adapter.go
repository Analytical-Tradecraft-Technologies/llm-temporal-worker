package openaichat

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaischema"
)

// Adapter owns one official OpenAI SDK client and one immutable compatible
// endpoint profile. SDK unions never cross this package boundary.
type Adapter struct {
	client     *Client
	endpointID string
	profile    Profile
}

// ModelListerAdapter is the direct OpenAI adapter with its optional
// management capability. Compatible adapters use Adapter directly and do not
// satisfy provider.ModelLister.
type ModelListerAdapter struct{ *Adapter }

func NewOpenAIAdapter(client *Client, endpointID string, profile Profile) (*ModelListerAdapter, error) {
	if !profile.directOpenAI {
		return nil, fmt.Errorf("openai chat: profile is not verified for direct OpenAI management")
	}
	base, err := New(client, endpointID, profile)
	if err != nil {
		return nil, err
	}
	return &ModelListerAdapter{Adapter: base}, nil
}

func New(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("openai chat: client is required")
	}
	if endpointID == "" {
		return nil, fmt.Errorf("openai chat: endpoint ID is required")
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	if validated.ExpectedBaseURL != "" && client.baseURL != validated.ExpectedBaseURL {
		return nil, fmt.Errorf("openai chat profile %q requires base URL %q, got %q", validated.ID, validated.ExpectedBaseURL, client.baseURL)
	}
	return &Adapter{client: client, endpointID: endpointID, profile: validated}, nil
}

// NewAdapter is an explicit alias used by route construction code.
func NewAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	return New(client, endpointID, profile)
}

// NewProfileAdapter is a convenience for profiles whose endpoint identity is
// already encoded by the caller's profile registry. The profile ID is used as
// the endpoint ID and remains a logical identifier, not a URL-derived value.
func NewProfileAdapter(client *Client, profile Profile) (*Adapter, error) {
	return New(client, profile.ID, profile)
}

func (adapter *Adapter) Name() string {
	if adapter == nil || adapter.profile.ID == "" {
		return adapterName
	}
	return adapterName + "/" + adapter.profile.ID
}

func (adapter *Adapter) Profile() Profile {
	if adapter == nil {
		return Profile{}
	}
	copy, _ := NewProfile(adapter.profile)
	return copy
}

// PreservesInstructionHierarchy reports whether policy and application
// instructions lower to different Chat roles on this endpoint profile.
func (adapter *Adapter) PreservesInstructionHierarchy() bool {
	return adapter == nil || adapter.profile.applicationInstructionRole() != "system"
}

func (adapter *Adapter) Capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if adapter == nil {
		return provider.CapabilitySet{}, fmt.Errorf("openai chat: adapter is nil")
	}
	return adapter.profile.capabilities(ctx, query, adapter.endpointID)
}

// ValidateRequest checks profile-owned extension controls before an engine
// reserves budget. The compiler independently repeats these checks.
func (adapter *Adapter) ValidateRequest(ctx context.Context, request llm.Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if adapter == nil {
		return provider.NewCompileError("adapter is nil")
	}
	if err := lowerExtensions(adapter.profile, request.Extensions, make(map[string]any)); err != nil {
		return provider.NewCompileError(err.Error())
	}
	// Structured output the provider documents as an error is rejected here
	// too; any other lowering failure is left for Compile to report.
	if request.Output != nil {
		if normalized, err := llm.NormalizeRequest(request); err == nil {
			if _, err := lowerRequestMap(normalized, adapter.profile, ""); errors.As(err, new(*openaischema.UnsupportedError)) {
				return lowerError(err)
			}
		}
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
	if err := provider.ValidateCapabilityQuery(input.Query, "openai chat", provider.FamilyOpenAIChat, adapter.endpointID); err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	normalized, err := llm.NormalizeRequest(input.Request)
	input.Metadata.WebSearch = input.Request.WebSearch
	input.Metadata.WebFetch = input.Request.WebFetch
	input.Metadata.CodeExecution = input.Request.CodeExecution
	if err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	if err := llm.ValidateServiceClassFallbacks(serviceClass, normalized.ServiceClassFallbacks); err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	if input.Query.Model != "" && input.Query.Model != normalized.Model {
		return provider.Call{}, provider.NewCompileError(fmt.Sprintf("model %q does not match capability query %q", normalized.Model, input.Query.Model))
	}
	if adapter.profile.ExpectedModel != "" && adapter.profile.ExpectedModel != normalized.Model {
		return provider.Call{}, provider.NewCompileError(fmt.Sprintf("model %q is not the pinned profile model %q", normalized.Model, adapter.profile.ExpectedModel))
	}
	providerTier, err := adapter.profile.providerTier(serviceClass)
	if err != nil {
		return provider.Call{}, provider.NewUnsupportedCapabilityError(err.Error())
	}
	set := input.Capability
	if set.Version == "" && len(set.Features) == 0 {
		set, err = adapter.profile.capabilities(ctx, input.Query, adapter.endpointID)
		if err != nil {
			return provider.Call{}, provider.NewCompileError(err.Error())
		}
	}
	if set.Version == "" {
		set.Version = adapter.profile.capabilityVersion()
	}
	for _, feature := range requiredFeatures(normalized) {
		capability, resolveErr := set.Resolve(feature, input.Strict)
		if resolveErr != nil {
			return provider.Call{}, provider.NewUnsupportedFeatureError(feature, resolveErr.Error())
		}
		if capability.State != provider.CapabilityNative && capability.State != provider.CapabilityEmulated {
			return provider.Call{}, provider.NewUnsupportedFeatureError(feature, fmt.Sprintf("capability %q is %s", feature, capability.State))
		}
		if capability.State == provider.CapabilityEmulated && feature == provider.FeatureStructuredOutput && adapter.profile.StructuredOutputTransform == "" {
			return provider.Call{}, provider.NewUnsupportedFeatureError(feature, "emulated structured output requires a named profile transform")
		}
	}
	if input.Strict && adapter.profile.applicationInstructionRole() == "system" && mixedInstructionLevels(normalized.Instructions) {
		return provider.Call{}, provider.NewStrictPortabilityError("instruction hierarchy cannot be preserved when application instructions also use the system role in strict portability mode")
	}
	if callID, reserved := provider.ReservedToolResultPrefix(normalized.Input); input.Strict && reserved {
		return provider.Call{}, provider.NewStrictPortabilityError(fmt.Sprintf("tool result %q output starts with the reserved tool-error prefix and cannot be distinguished from a failed result in strict portability mode", callID))
	}
	profile, err := adapter.profile.forRequest(normalized.Model, serviceClass, input.Metadata.PriceCeiling)
	if err != nil {
		return provider.Call{}, provider.NewCompileError(err.Error())
	}
	params, err := lowerRequest(normalized, profile, providerTier)
	if err != nil {
		return provider.Call{}, lowerError(err)
	}
	digest := input.Metadata.SchemaDigest
	if digest == ([32]byte{}) {
		digest, err = llm.RequestDigest(normalized)
		if err != nil {
			return provider.Call{}, provider.NewCompileError(err.Error())
		}
	}
	metadata := input.Metadata
	metadata.SchemaDigest = digest
	metadata.CapabilityVersion = set.Version
	metadata.ProviderTier = providerTier
	metadata.OpaqueStateRequired = normalized.Continuation != nil
	if metadata.EstimatedBytes == 0 {
		canonical, canonicalErr := provider.CanonicalRequestBytes(normalized)
		if canonicalErr != nil {
			return provider.Call{}, provider.NewCompileError(canonicalErr.Error())
		}
		metadata.EstimatedBytes = len(canonical)
	}
	return provider.Call{
		EndpointID:   adapter.endpointID,
		Family:       provider.FamilyOpenAIChat,
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
	if call.Family != provider.FamilyOpenAIChat || call.EndpointID != adapter.endpointID {
		return provider.Result{}, provider.NewDispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(openai.ChatCompletionNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*openai.ChatCompletionNewParams); pointerOK && pointer != nil {
			params = *pointer
			ok = true
		}
	}
	if !ok {
		return provider.Result{}, provider.NewDispatchError("call SDK parameters have unexpected type", provider.DispatchNotDispatched)
	}
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
	requestOptions := adapter.client.options()
	requestOptions = append(requestOptions, option.WithResponseInto(&rawResponse))
	probe := &provider.DispatchProbe{}
	requestOptions = append(requestOptions, option.WithMiddleware(probe.Middleware))
	requestOptions = append(requestOptions, ordered...)
	response, panicked, err := provider.CallRecovered(func() (*openai.ChatCompletion, error) {
		return adapter.client.sdk.Chat.Completions.New(callContext, params, requestOptions...)
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
		// probe, or from a credential middleware the SDK runs inside the
		// probe but before it sends the request (#1495).
		if mapped := provider.ClassifyUnsentError(err); mapped != nil && (!probe.Sent() || errors.Is(err, provider.ErrProviderCredentials)) {
			mapped.SafeDetails = map[string]string{"provider": adapter.Name()}
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		return provider.Result{}, provider.WithEndpointID(mapError(err, adapter.Name()), adapter.endpointID)
	}
	if response == nil {
		return provider.Result{}, liftFailure(call, "", "", "", withLiftCause("response_empty", errors.New("provider returned an empty response")))
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
	observer.OnProgress(callContext, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Choices)})
	lifted, err := adapter.profile.liftResponse(call, response, metadata.RequestID)
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
	if request.Output != nil && request.Output.Format.Kind != "" && request.Output.Format.Kind != llm.OutputKindText {
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

func mixedInstructionLevels(instructions []llm.Instruction) bool {
	application, policy := false, false
	for _, instruction := range instructions {
		if instruction.Level == llm.InstructionLevelPolicy {
			policy = true
		} else {
			application = true
		}
	}
	return application && policy
}
