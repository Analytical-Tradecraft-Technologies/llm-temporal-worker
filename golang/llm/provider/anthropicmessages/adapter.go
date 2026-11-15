package anthropicmessages

import (
	"context"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicwire"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// Adapter owns one official Anthropic Messages SDK client and one immutable
// endpoint profile. SDK unions never cross the provider-neutral boundary.
type Adapter struct {
	client     *Client
	endpointID string
	profile    Profile
}

func New(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	if err := provider.RequireAdapterArgs("anthropic messages", client != nil, endpointID); err != nil {
		return nil, err
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	if err := provider.RequireExpectedBaseURL("anthropic messages", validated.ID, validated.ExpectedBaseURL, client.baseURL); err != nil {
		return nil, err
	}
	return &Adapter{client: client, endpointID: endpointID, profile: validated}, nil
}

// NewAdapter is an explicit alias used by route construction code.
func NewAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	return New(client, endpointID, profile)
}

// NewProfileAdapter uses the profile ID as a logical endpoint ID.
func NewProfileAdapter(client *Client, profile Profile) (*Adapter, error) {
	return New(client, profile.ID, profile)
}

// NewAnthropicAdapter is a descriptive alias for callers that register
// multiple Anthropic endpoint families.
func NewAnthropicAdapter(client *Client, endpointID string, profile Profile) (*Adapter, error) {
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
		return provider.CapabilitySet{}, fmt.Errorf("anthropic messages: adapter is nil")
	}
	return adapter.profile.capabilities(ctx, query, adapter.endpointID)
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, provider.NewCompileError("adapter is nil")
	}
	if err := provider.StartCompile(ctx, input.Query, "anthropic messages", provider.FamilyAnthropicMessages, adapter.endpointID); err != nil {
		return provider.Call{}, err
	}
	input.Metadata.RecordHostedTools(input.Request)
	normalized, serviceClass, err := provider.NormalizeCompileRequest(input.Request, input.Query, adapter.profile.ExpectedModel)
	if err != nil {
		return provider.Call{}, err
	}
	if err := provider.RequirePinnedContinuation(normalized, adapter.endpointID); err != nil {
		return provider.Call{}, err
	}
	providerTier, err := adapter.profile.providerTier(serviceClass)
	if err != nil {
		return provider.Call{}, provider.NewUnsupportedCapabilityError(err.Error())
	}
	set, err := provider.CompileCapabilities(input.Capability, func() (provider.CapabilitySet, error) {
		return adapter.profile.capabilities(ctx, input.Query, adapter.endpointID)
	}, adapter.profile.capabilityVersion())
	if err != nil {
		return provider.Call{}, err
	}
	if err := provider.RequireFeatures(set, anthropicwire.RequiredFeatures(normalized), input.Strict, nil); err != nil {
		return provider.Call{}, err
	}
	params, err := lowerRequestWithStrict(normalized, adapter.profile, providerTier, input.Strict)
	if err != nil {
		return provider.Call{}, anthropicwire.LowerError(err)
	}
	metadata, err := provider.CompileMetadata(input.Metadata, normalized, set.Version, providerTier)
	if err != nil {
		return provider.Call{}, err
	}
	return provider.Call{
		EndpointID:   adapter.endpointID,
		Family:       provider.FamilyAnthropicMessages,
		Model:        normalized.Model,
		OperationKey: normalized.OperationKey,
		ServiceClass: serviceClass,
		SDKParams:    params,
		OutputSchema: provider.OutputSchema(normalized),
		Metadata:     metadata,
	}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, provider.NewDispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	params, err := provider.StartDispatch[anthropic.MessageNewParams](ctx, call, provider.FamilyAnthropicMessages, adapter.endpointID)
	if err != nil {
		return provider.Result{}, err
	}
	// Providers generate structured output in schema order; write the schemas
	// with properties in "required" order instead of the SDK's sorted map
	// encoding (#1096). Encoded before the possible-write boundary, so a
	// failure here is a definite non-dispatch.
	overrides, err := schemaorder.MessagesOverrides(params)
	if err != nil {
		return provider.Result{}, provider.NewDispatchError("schema property order could not be encoded", provider.DispatchNotDispatched)
	}
	// An error raised before the probe, such as an AWS gateway credential
	// failure while signing, means nothing reached the transport.
	dispatch, err := provider.BeginDispatch(ctx, observer, provider.DispatchTarget{EndpointID: adapter.endpointID, Provider: adapter.Name(), MapError: mapError})
	if err != nil {
		return provider.Result{}, err
	}
	messages := adapter.client.messages
	if messages == nil {
		messages = &adapter.client.sdk.Messages
	}
	response, rawResponse, err := provider.SendHTTP(dispatch, call.OperationKey, func(probe *provider.DispatchProbe, raw **http.Response) (*anthropic.Message, error) {
		requestOptions := []option.RequestOption{option.WithResponseInto(raw), option.WithMiddleware(probe.Middleware)}
		if call.Metadata.ProviderTier == FastTier {
			requestOptions = append(requestOptions, option.WithHeaderAdd("anthropic-beta", fastModeBeta))
		}
		for _, override := range overrides {
			requestOptions = append(requestOptions, option.WithJSONSet(override.Path, override.Value))
		}
		return messages.New(dispatch.Context, params, requestOptions...)
	})
	if err != nil {
		return provider.Result{}, err
	}
	if response == nil {
		return provider.Result{}, provider.NewInvalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: responseTier(response)}
	if rawResponse != nil {
		metadata.Status = rawResponse.StatusCode
		metadata.RequestID = rawResponse.Header.Get("request-id")
		if metadata.RequestID == "" {
			metadata.RequestID = rawResponse.Header.Get("x-request-id")
		}
	}
	if metadata.RequestID == "" {
		metadata.RequestID = anthropicwire.ResponseRequestID(response)
	}
	if err := dispatch.AfterResponseHeaders(metadata); err != nil {
		return provider.Result{}, err
	}
	dispatch.LiftProgress(len(response.Content))
	lifted, err := adapter.profile.liftResponse(call, response, metadata.RequestID)
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{Response: lifted}, nil
}
