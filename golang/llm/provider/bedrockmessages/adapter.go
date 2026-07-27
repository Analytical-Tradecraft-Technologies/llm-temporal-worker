package bedrockmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// Adapter owns one official Anthropic SDK client configured with Bedrock's
// request middleware and one immutable Bedrock profile.
type Adapter struct {
	client     *Client
	endpointID string
	profile    Profile
}

func New(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("bedrock messages: client is required")
	}
	if endpointID == "" {
		return nil, fmt.Errorf("bedrock messages: endpoint ID is required")
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	if validated.ExpectedBaseURL != "" && client.baseURL != validated.ExpectedBaseURL {
		return nil, fmt.Errorf("bedrock messages profile %q requires base URL %q, got %q", validated.ID, validated.ExpectedBaseURL, client.baseURL)
	}
	return &Adapter{client: client, endpointID: endpointID, profile: validated}, nil
}

func NewAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	return New(client, endpointID, profile)
}

func NewProfileAdapter(client *Client, profile Profile) (*Adapter, error) {
	return New(client, profile.ID, profile)
}

func NewBedrockAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	return New(client, endpointID, profile)
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

func (adapter *Adapter) Capabilities(ctx context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if adapter == nil {
		return provider.CapabilitySet{}, fmt.Errorf("bedrock messages: adapter is nil")
	}
	return adapter.profile.capabilities(ctx, query, adapter.endpointID)
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
	if adapter.profile.ExpectedModel != "" && adapter.profile.ExpectedModel != normalized.Model {
		return provider.Call{}, compileError(fmt.Sprintf("model %q is not the pinned profile model %q", normalized.Model, adapter.profile.ExpectedModel))
	}
	if continuation := normalized.Continuation; continuation != nil && continuation.Pinned && continuation.EndpointID != "" && continuation.EndpointID != adapter.endpointID {
		return provider.Call{}, compileError(fmt.Sprintf("continuation endpoint %q is not pinned endpoint %q", continuation.EndpointID, adapter.endpointID))
	}
	providerTier, err := adapter.profile.providerTier(serviceClass)
	if err != nil {
		return provider.Call{}, unsupportedServiceError(err.Error())
	}
	set := input.Capability
	if set.Version == "" && len(set.Features) == 0 {
		set, err = adapter.profile.capabilities(ctx, input.Query, adapter.endpointID)
		if err != nil {
			return provider.Call{}, compileError(err.Error())
		}
	}
	if set.Version == "" {
		set.Version = adapter.profile.capabilityVersion()
	}
	for _, feature := range requiredFeatures(normalized) {
		capability, resolveErr := set.Resolve(feature, input.Strict)
		if resolveErr != nil {
			return provider.Call{}, unsupportedError(feature, resolveErr.Error())
		}
		if capability.State != provider.CapabilityNative && capability.State != provider.CapabilityEmulated {
			return provider.Call{}, unsupportedError(feature, fmt.Sprintf("capability %q is %s", feature, capability.State))
		}
	}
	params, err := lowerRequestWithStrict(normalized, adapter.profile, providerTier, input.Strict)
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
	metadata.ProviderTier = providerTier
	metadata.OpaqueStateRequired = normalized.Continuation != nil
	if metadata.EstimatedBytes == 0 {
		canonical, canonicalErr := canonicalRequestBytes(normalized)
		if canonicalErr != nil {
			return provider.Call{}, compileError(canonicalErr.Error())
		}
		metadata.EstimatedBytes = len(canonical)
	}
	return provider.Call{EndpointID: adapter.endpointID, Family: provider.FamilyBedrockMessages, Model: normalized.Model, OperationKey: normalized.OperationKey, ServiceClass: serviceClass, SDKParams: params, Metadata: metadata}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, dispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	if err := ctx.Err(); err != nil {
		return provider.Result{}, dispatchContextError(err)
	}
	if call.Family != provider.FamilyBedrockMessages || call.EndpointID != adapter.endpointID {
		return provider.Result{}, dispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(anthropic.MessageNewParams)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*anthropic.MessageNewParams); pointerOK && pointer != nil {
			params, ok = *pointer, true
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
	messages := adapter.client.messages
	if messages == nil {
		messages = &adapter.client.sdk.Messages
	}
	response, err := messages.New(callContext, params, option.WithResponseInto(&rawResponse))
	if rawResponse != nil && provider.IsRedirectStatus(rawResponse.StatusCode) {
		return provider.Result{}, provider.WithEndpointID(provider.NewRedirectResponseError(rawResponse.StatusCode), adapter.endpointID)
	}
	if err != nil {
		if mapped := provider.ClassifyEgressOutcome(egressOutcome, err); mapped != nil {
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		return provider.Result{}, provider.WithEndpointID(mapError(err, adapter.Name()), adapter.endpointID)
	}
	if response == nil {
		return provider.Result{}, invalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: string(response.Usage.ServiceTier)}
	if rawResponse != nil {
		metadata.Status = rawResponse.StatusCode
		for _, header := range []string{"x-amzn-requestid", "x-amzn-request-id", "request-id", "x-request-id"} {
			metadata.RequestID = rawResponse.Header.Get(header)
			if metadata.RequestID != "" {
				break
			}
		}
	}
	if metadata.RequestID == "" {
		metadata.RequestID = responseRequestID(response)
	}
	if err := observer.AfterResponseHeaders(callContext, metadata); err != nil {
		mapped := dispatchObserverError(err, provider.DispatchAccepted)
		mapped.Provider.ResponseID = response.ID
		mapped.Provider.RequestID = metadata.RequestID
		return provider.Result{}, mapped
	}
	observer.OnProgress(callContext, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(response.Content)})
	lifted, err := adapter.profile.liftResponse(call, response, metadata.RequestID)
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{Response: lifted}, nil
}

func responseRequestID(response *anthropic.Message) string {
	if response == nil {
		return ""
	}
	var envelope struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal([]byte(response.RawJSON()), &envelope); err == nil {
		return envelope.RequestID
	}
	return ""
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

func unsupportedServiceError(message string) *provider.Error {
	return provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, message)
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
		return provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "provider request canceled")
	}
	return provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetryNever, "provider request deadline exceeded")
}

func dispatchObserverError(err error, certainty provider.DispatchCertainty) *provider.Error {
	mapped := provider.NewError(provider.CodeInternal, provider.PhaseDispatch, certainty, provider.RetryNever, "observer rejected provider response")
	mapped.Cause = err
	return mapped
}
