package anthropicmessages

import (
	"context"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicwire"
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
	return provider.NewCall(adapter.endpointID, provider.FamilyAnthropicMessages, normalized, serviceClass, params, input.Metadata, set.Version, providerTier)
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, provider.NewDispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	dispatch, response, rawResponse, err := anthropicwire.SendMessage(ctx, call, observer, anthropicwire.MessagesCall{
		Family:   provider.FamilyAnthropicMessages,
		Target:   provider.DispatchTarget{EndpointID: adapter.endpointID, Provider: adapter.Name(), MapError: mapError},
		Messages: adapter.client.messages,
		SDK:      &adapter.client.sdk,
		Options: func(probe *provider.DispatchProbe) []option.RequestOption {
			options := []option.RequestOption{option.WithMiddleware(probe.Middleware)}
			if call.Metadata.ProviderTier == FastTier {
				options = append(options, option.WithHeaderAdd("anthropic-beta", fastModeBeta))
			}
			return options
		},
	})
	if err != nil {
		return provider.Result{}, err
	}
	metadata := anthropicwire.ResponseMetadata(response, rawResponse, responseTier(response), "request-id", "x-request-id")
	return dispatch.Lift(metadata, len(response.Content), func() (llm.Response, error) {
		return adapter.profile.liftResponse(call, response, metadata.RequestID)
	})
}
