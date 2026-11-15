package bedrockmessages

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/anthropicwire"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// Adapter owns one official Anthropic SDK client configured with Bedrock's
// request middleware and one immutable Bedrock profile.
type Adapter struct {
	client     *Client
	endpointID string
	profile    Profile
}

func New(client *Client, endpointID string, profile Profile) (*Adapter, error) {
	if err := provider.RequireAdapterArgs("bedrock messages", client != nil, endpointID); err != nil {
		return nil, err
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	if err := provider.RequireExpectedBaseURL("bedrock messages", validated.ID, validated.ExpectedBaseURL, client.baseURL); err != nil {
		return nil, err
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
		return provider.Call{}, provider.NewCompileError("adapter is nil")
	}
	if err := provider.StartCompile(ctx, input.Query, "bedrock messages", provider.FamilyBedrockMessages, adapter.endpointID); err != nil {
		return provider.Call{}, err
	}
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
	params, err := lowerRequestWithStrict(normalized, adapter.profile, input.Strict)
	if err != nil {
		// Both unsupported errors are leaves, so checking media first
		// cannot shadow an unsupported output schema.
		var unsupportedMedia *unsupportedMediaError
		if errors.As(err, &unsupportedMedia) {
			return provider.Call{}, provider.NewUnsupportedFeatureError(unsupportedMedia.feature, err.Error())
		}
		return provider.Call{}, anthropicwire.LowerError(err)
	}
	metadata, err := provider.CompileMetadata(input.Metadata, normalized, set.Version, providerTier)
	if err != nil {
		return provider.Call{}, err
	}
	metadata.ExtensionsDropped = !input.Strict && len(normalized.Extensions) > 0
	return provider.Call{EndpointID: adapter.endpointID, Family: provider.FamilyBedrockMessages, Model: normalized.Model, OperationKey: normalized.OperationKey, ServiceClass: serviceClass, SDKParams: params, OutputSchema: provider.OutputSchema(normalized), Metadata: metadata}, nil
}

func (adapter *Adapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	if adapter == nil {
		return provider.Result{}, provider.NewDispatchError("adapter is nil", provider.DispatchNotDispatched)
	}
	params, err := provider.StartDispatch[anthropic.MessageNewParams](ctx, call, provider.FamilyBedrockMessages, adapter.endpointID)
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
	// An error raised before the probe, such as an AWS credential failure
	// while signing, means nothing reached the transport.
	dispatch, err := provider.BeginDispatch(ctx, observer, provider.DispatchTarget{EndpointID: adapter.endpointID, Provider: adapter.Name(), MapError: mapError})
	if err != nil {
		return provider.Result{}, err
	}
	messages := adapter.client.messages
	if messages == nil {
		messages = &adapter.client.sdk.Messages
	}
	response, rawResponse, err := provider.SendHTTP(dispatch, call.OperationKey, func(probe *provider.DispatchProbe, raw **http.Response) (*anthropic.Message, error) {
		// InvokeModel takes the requested tier as a header, not as an
		// Anthropic body field. It is set before the Bedrock middleware signs
		// the request.
		options := []option.RequestOption{option.WithResponseInto(raw)}
		if call.Metadata.ProviderTier != "" {
			options = append(options, option.WithHeader(serviceTierHeader, call.Metadata.ProviderTier))
		}
		options = append(options, option.WithMiddleware(probe.Middleware))
		for _, override := range overrides {
			options = append(options, option.WithJSONSet(override.Path, override.Value))
		}
		return messages.New(dispatch.Context, params, options...)
	})
	if err != nil {
		return provider.Result{}, err
	}
	if response == nil {
		return provider.Result{}, provider.NewInvalidResponseError(call, "", "provider returned an empty response")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: string(response.Usage.ServiceTier)}
	headerTier := ""
	if rawResponse != nil {
		metadata.Status = rawResponse.StatusCode
		headerTier = rawResponse.Header.Get(serviceTierHeader)
		if metadata.ProviderTier == "" {
			metadata.ProviderTier = headerTier
		}
		for _, header := range []string{"x-amzn-requestid", "x-amzn-request-id", "request-id", "x-request-id"} {
			metadata.RequestID = rawResponse.Header.Get(header)
			if metadata.RequestID != "" {
				break
			}
		}
	}
	if metadata.RequestID == "" {
		metadata.RequestID = anthropicwire.ResponseRequestID(response)
	}
	if err := dispatch.AfterResponseHeaders(metadata); err != nil {
		return provider.Result{}, err
	}
	dispatch.LiftProgress(len(response.Content))
	lifted, err := adapter.profile.liftResponseWithTier(call, response, metadata.RequestID, headerTier)
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{Response: lifted}, nil
}
