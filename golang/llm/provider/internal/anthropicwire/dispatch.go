package anthropicwire

import (
	"context"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/schemaorder"
)

// MessageService is the Messages SDK surface both Messages adapters call.
type MessageService interface {
	New(context.Context, anthropic.MessageNewParams, ...option.RequestOption) (*anthropic.Message, error)
}

// MessagesCall describes one Messages SDK call.
type MessagesCall struct {
	Family provider.Family
	Target provider.DispatchTarget
	// Messages sends the request; nil selects SDK.Messages.
	Messages MessageService
	SDK      *anthropic.Client
	// Options returns the request options placed after the response recorder
	// and before the schema-order overrides. They must install
	// probe.Middleware.
	Options func(probe *provider.DispatchProbe) []option.RequestOption
}

// SendMessage dispatches one non-streaming Messages call and returns the
// dispatch, the non-nil SDK response and the HTTP response.
func SendMessage(ctx context.Context, call provider.Call, observer provider.Observer, spec MessagesCall) (*provider.Dispatch, *anthropic.Message, *http.Response, error) {
	params, err := provider.StartDispatch[anthropic.MessageNewParams](ctx, call, spec.Family, spec.Target.EndpointID)
	if err != nil {
		return nil, nil, nil, err
	}
	// Providers generate structured output in schema order; write the schemas
	// with properties in "required" order instead of the SDK's sorted map
	// encoding (#1096). Encoded before the possible-write boundary, so a
	// failure here is a definite non-dispatch.
	overrides, err := schemaorder.MessagesOverrides(params)
	if err != nil {
		return nil, nil, nil, provider.NewDispatchError("schema property order could not be encoded", provider.DispatchNotDispatched)
	}
	// An error raised before the probe, such as an AWS credential failure
	// while signing, means nothing reached the transport.
	dispatch, err := provider.BeginDispatch(ctx, observer, spec.Target)
	if err != nil {
		return nil, nil, nil, err
	}
	messages := spec.Messages
	if messages == nil {
		messages = &spec.SDK.Messages
	}
	response, rawResponse, err := provider.SendHTTP(dispatch, call.OperationKey, func(probe *provider.DispatchProbe, raw **http.Response) (*anthropic.Message, error) {
		options := append([]option.RequestOption{option.WithResponseInto(raw)}, spec.Options(probe)...)
		for _, override := range overrides {
			options = append(options, option.WithJSONSet(override.Path, override.Value))
		}
		return messages.New(dispatch.Context, params, options...)
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if response == nil {
		return nil, nil, nil, provider.NewInvalidResponseError(call, "", "provider returned an empty response")
	}
	return dispatch, response, rawResponse, nil
}

// ResponseMetadata returns provider.HTTPResponseMetadata for response, with
// the body's request ID when no requestIDHeaders header carries one.
func ResponseMetadata(response *anthropic.Message, raw *http.Response, providerTier string, requestIDHeaders ...string) provider.ResponseMetadata {
	metadata := provider.HTTPResponseMetadata(response.ID, providerTier, raw, requestIDHeaders...)
	if metadata.RequestID == "" {
		metadata.RequestID = ResponseRequestID(response)
	}
	return metadata
}
