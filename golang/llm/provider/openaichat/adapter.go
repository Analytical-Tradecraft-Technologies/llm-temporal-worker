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
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaiwire"
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
	if err := provider.RequireAdapterArgs("openai chat", client != nil, endpointID); err != nil {
		return nil, err
	}
	validated, err := NewProfile(profile)
	if err != nil {
		return nil, err
	}
	if err := provider.RequireExpectedBaseURL("openai chat", validated.ID, validated.ExpectedBaseURL, client.baseURL); err != nil {
		return nil, err
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
				return openaiwire.LowerError(err)
			}
		}
	}
	return nil
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, provider.NewCompileError("adapter is nil")
	}
	if err := provider.StartCompile(ctx, input.Query, "openai chat", provider.FamilyOpenAIChat, adapter.endpointID); err != nil {
		return provider.Call{}, err
	}
	input.Metadata.RecordHostedTools(input.Request)
	normalized, serviceClass, err := provider.NormalizeCompileRequest(input.Request, input.Query, adapter.profile.ExpectedModel)
	if err != nil {
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
	if err := provider.RequireFeatures(set, requiredFeatures(normalized), input.Strict, func(feature provider.Feature, capability provider.Capability) error {
		if capability.State == provider.CapabilityEmulated && feature == provider.FeatureStructuredOutput && adapter.profile.StructuredOutputTransform == "" {
			return provider.NewUnsupportedFeatureError(feature, "emulated structured output requires a named profile transform")
		}
		return nil
	}); err != nil {
		return provider.Call{}, err
	}
	if input.Strict && adapter.profile.applicationInstructionRole() == "system" && provider.MixedInstructionLevels(normalized.Instructions) {
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
		return provider.Call{}, openaiwire.LowerError(err)
	}
	metadata, err := provider.CompileMetadata(input.Metadata, normalized, set.Version, providerTier)
	if err != nil {
		return provider.Call{}, err
	}
	return provider.Call{
		EndpointID:   adapter.endpointID,
		Family:       provider.FamilyOpenAIChat,
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
	params, err := provider.StartDispatch[openai.ChatCompletionNewParams](ctx, call, provider.FamilyOpenAIChat, adapter.endpointID)
	if err != nil {
		return provider.Result{}, err
	}
	// Encode the ordered schemas before the possible-write boundary, so a
	// failure here is a definite non-dispatch.
	ordered, err := openaiwire.SchemaOrderOptions(params, false)
	if err != nil {
		return provider.Result{}, provider.NewDispatchError("schema property order could not be encoded", provider.DispatchNotDispatched)
	}
	// Nothing reached the transport when the error came before the probe, or
	// from a credential middleware the SDK runs inside the probe but before it
	// sends the request (#1495).
	dispatch, err := provider.BeginDispatch(ctx, observer, provider.DispatchTarget{EndpointID: adapter.endpointID, Provider: adapter.Name(), CredentialUnsent: true, MapError: mapError})
	if err != nil {
		return provider.Result{}, err
	}
	response, rawResponse, err := provider.SendHTTP(dispatch, call.OperationKey, func(probe *provider.DispatchProbe, raw **http.Response) (*openai.ChatCompletion, error) {
		requestOptions := adapter.client.options()
		requestOptions = append(requestOptions, option.WithResponseInto(raw), option.WithMiddleware(probe.Middleware))
		requestOptions = append(requestOptions, ordered...)
		return adapter.client.sdk.Chat.Completions.New(dispatch.Context, params, requestOptions...)
	})
	if err != nil {
		return provider.Result{}, err
	}
	if response == nil {
		return provider.Result{}, liftFailure(call, "", "", "", withLiftCause("response_empty", errors.New("provider returned an empty response")))
	}
	metadata := provider.HTTPResponseMetadata(response.ID, string(response.ServiceTier), rawResponse, "x-request-id")
	return dispatch.Lift(metadata, len(response.Choices), func() (llm.Response, error) {
		return adapter.profile.liftResponse(call, response, metadata.RequestID)
	})
}

func requiredFeatures(request llm.Request) []provider.Feature {
	return provider.RequestFeatures(request, openaiwire.PartFeature, nil, provider.StructuredOutputKind)
}
