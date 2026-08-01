package bedrockconverse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// Adapter compiles provider-neutral requests into AWS Bedrock Converse input
// and invokes exactly one non-streaming Converse operation.
type Adapter struct {
	client     *Client
	endpointID string
	profile    Profile
}

func New(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("bedrock converse: client is required")
	}
	if endpointID == "" {
		return nil, fmt.Errorf("bedrock converse: endpoint ID is required")
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	return &Adapter{client: client, endpointID: endpointID, profile: validated}, nil
}

func NewAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
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
		return provider.CapabilitySet{}, fmt.Errorf("bedrock converse: adapter is nil")
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
	if input.Query.Family != "" && input.Query.Family != provider.FamilyBedrockConverse {
		return provider.Call{}, compileError(fmt.Sprintf("capability family %q does not match %q", input.Query.Family, provider.FamilyBedrockConverse))
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
	if normalized.Continuation != nil {
		return provider.Call{}, unsupportedError(provider.FeatureContinuation, "Bedrock Converse has no provider-hosted continuation handle")
	}
	capabilities := input.Capability
	if capabilities.Version == "" && len(capabilities.Features) == 0 {
		capabilities, err = adapter.profile.capabilities(ctx, input.Query, adapter.endpointID)
		if err != nil {
			return provider.Call{}, compileError(err.Error())
		}
	}
	if capabilities.Version == "" {
		capabilities.Version = adapter.profile.CapabilityVersion
	}
	for _, feature := range requiredFeatures(normalized) {
		capability, resolveErr := capabilities.Resolve(feature, input.Strict)
		if resolveErr != nil {
			return provider.Call{}, unsupportedError(feature, resolveErr.Error())
		}
		if capability.State != provider.CapabilityNative && capability.State != provider.CapabilityEmulated {
			return provider.Call{}, unsupportedError(feature, fmt.Sprintf("capability %q is %s", feature, capability.State))
		}
	}
	tier, err := adapter.profile.providerTier(serviceClass)
	if err != nil {
		return provider.Call{}, unsupportedServiceError(err.Error())
	}
	params, err := lowerRequest(normalized, adapter.profile, tier, input.Strict)
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
	metadata.CapabilityVersion = capabilities.Version
	metadata.ProviderTier = tier
	if metadata.EstimatedBytes == 0 {
		encoded, marshalErr := json.Marshal(params)
		if marshalErr == nil {
			metadata.EstimatedBytes = len(encoded)
		}
	}
	return provider.Call{EndpointID: adapter.endpointID, Family: provider.FamilyBedrockConverse, Model: normalized.Model, OperationKey: normalized.OperationKey, ServiceClass: serviceClass, SDKParams: params, Metadata: metadata}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, dispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	if err := ctx.Err(); err != nil {
		return provider.Result{}, dispatchContextError(err)
	}
	if call.Family != provider.FamilyBedrockConverse || call.EndpointID != adapter.endpointID {
		return provider.Result{}, dispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(bedrockruntime.ConverseInput)
	if !ok {
		if pointer, pointerOK := call.SDKParams.(*bedrockruntime.ConverseInput); pointerOK && pointer != nil {
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
	response, err := adapter.client.converse.Converse(callContext, &params)
	if err != nil {
		if mapped := provider.ClassifyEgressOutcome(egressOutcome, err); mapped != nil {
			return provider.Result{}, provider.WithEndpointID(mapped, adapter.endpointID)
		}
		return provider.Result{}, provider.WithEndpointID(mapError(err, adapter.Name()), adapter.endpointID)
	}
	if response == nil {
		return provider.Result{}, invalidResponseError(call, "", "provider returned an empty response")
	}
	requestID, _ := awsmiddleware.GetRequestIDMetadata(response.ResultMetadata)
	metadata := provider.ResponseMetadata{RequestID: requestID}
	if response.ServiceTier != nil {
		metadata.ProviderTier = string(response.ServiceTier.Type)
	}
	if err := observer.AfterResponseHeaders(callContext, metadata); err != nil {
		mapped := dispatchObserverError(err, provider.DispatchAccepted)
		mapped.Provider.RequestID = requestID
		return provider.Result{}, mapped
	}
	lifted, err := adapter.liftResponse(call, response, requestID)
	if err != nil {
		return provider.Result{}, err
	}
	observer.OnProgress(callContext, provider.Progress{Phase: string(provider.PhaseLift), OutputItems: len(lifted.Output)})
	return provider.Result{Response: lifted}, nil
}

func (adapter *Adapter) liftResponse(call provider.Call, response *bedrockruntime.ConverseOutput, requestID string) (llm.Response, error) {
	actualTier := ""
	if response.ServiceTier != nil {
		actualTier = string(response.ServiceTier.Type)
	}
	actual, err := adapter.profile.actualClass(actualTier)
	if err != nil {
		return llm.Response{}, invalidResponseError(call, requestID, err.Error())
	}
	output, hasToolCalls, err := liftOutput(response.Output)
	if err != nil {
		return llm.Response{}, invalidResponseError(call, requestID, err.Error())
	}
	status, err := liftStatus(response.StopReason, hasToolCalls)
	if err != nil {
		return llm.Response{}, invalidResponseError(call, requestID, err.Error())
	}
	usage := llm.Usage{}
	if response.Usage != nil {
		usage.InputTokens = int64Value(response.Usage.InputTokens)
		usage.OutputTokens = int64Value(response.Usage.OutputTokens)
	}
	return llm.Response{APIVersion: llm.APIVersion, OperationKey: call.OperationKey, Status: status, Output: output,
		Route:   llm.RouteFacts{EndpointID: call.EndpointID, APIFamily: string(provider.FamilyBedrockConverse), RequestedModel: call.Model, ResolvedModel: call.Model},
		Service: llm.ServiceFacts{Requested: call.ServiceClass, Attempted: call.ServiceClass, Actual: actual, ProviderValue: actualTier},
		Usage:   usage, Provider: llm.ProviderFacts{RequestID: requestID, FinishReason: string(response.StopReason)}}, nil
}

func liftOutput(output types.ConverseOutput) ([]llm.Item, bool, error) {
	message, ok := output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return nil, false, fmt.Errorf("provider response omitted a message output")
	}
	items := make([]llm.Item, 0, len(message.Value.Content))
	hasToolCalls := false
	var text strings.Builder
	flushText := func() {
		if text.Len() > 0 {
			items = append(items, llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: text.String()}}})
			text.Reset()
		}
	}
	for index, block := range message.Value.Content {
		switch value := block.(type) {
		case *types.ContentBlockMemberText:
			text.WriteString(value.Value)
		case *types.ContentBlockMemberToolUse:
			flushText()
			arguments, err := value.Value.Input.MarshalSmithyDocument()
			if err != nil || !json.Valid(arguments) || value.Value.ToolUseId == nil || value.Value.Name == nil {
				return nil, false, fmt.Errorf("content block %d tool use is invalid", index)
			}
			hasToolCalls = true
			items = append(items, llm.ToolCall{ID: *value.Value.ToolUseId, Name: *value.Value.Name, Arguments: arguments})
		default:
			return nil, false, fmt.Errorf("content block %d has unsupported type %T", index, block)
		}
	}
	flushText()
	return items, hasToolCalls, nil
}

func liftStatus(reason types.StopReason, hasToolCalls bool) (llm.ResponseStatus, error) {
	switch reason {
	case types.StopReasonEndTurn, types.StopReasonStopSequence:
		if hasToolCalls {
			return llm.ResponseStatusToolCalls, nil
		}
		return llm.ResponseStatusCompleted, nil
	case types.StopReasonToolUse, types.StopReasonMalformedToolUse:
		if !hasToolCalls {
			return "", fmt.Errorf("provider stop reason %q did not contain a tool call", reason)
		}
		return llm.ResponseStatusToolCalls, nil
	case types.StopReasonMaxTokens, types.StopReasonModelContextWindowExceeded:
		return llm.ResponseStatusLength, nil
	case types.StopReasonGuardrailIntervened, types.StopReasonContentFiltered:
		return llm.ResponseStatusRefused, nil
	default:
		return "", fmt.Errorf("provider returned unknown stop reason %q", reason)
	}
}

func requiredFeatures(request llm.Request) []provider.Feature {
	features := []provider.Feature{provider.FeatureText, provider.FeatureUsage}
	for _, item := range request.Input {
		switch item.(type) {
		case llm.ToolCall, llm.ToolResult:
			features = append(features, provider.FeatureToolCall)
		case llm.Message:
			features = append(features, provider.FeatureText)
		}
	}
	for _, instruction := range request.Instructions {
		for _, part := range instruction.Content {
			if _, ok := part.(llm.TextPart); !ok {
				if _, ok := part.(llm.JSONPart); !ok {
					features = append(features, provider.FeatureImage)
				}
			}
		}
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		features = append(features, provider.FeatureToolCall)
	}
	return uniqueFeatures(features)
}

func uniqueFeatures(features []provider.Feature) []provider.Feature {
	seen := map[provider.Feature]bool{}
	result := make([]provider.Feature, 0, len(features))
	for _, feature := range features {
		if feature != "" && !seen[feature] {
			seen[feature] = true
			result = append(result, feature)
		}
	}
	return result
}

func int64Value(value *int32) int64 {
	if value == nil {
		return 0
	}
	return int64(*value)
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
func invalidResponseError(call provider.Call, requestID, message string) *provider.Error {
	mapped := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, message)
	mapped.Provider.RequestID = requestID
	mapped.OperationID = call.OperationKey
	return mapped
}
