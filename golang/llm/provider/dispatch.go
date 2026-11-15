package provider

import (
	"context"
	"errors"
	"net/http"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The helpers in this file are the steps every adapter's non-streaming Invoke
// shares: the possible-write boundary, the SDK call and the response-headers
// hook. Sharing them keeps the dispatch-certainty contract each step reports
// from drifting between endpoint families.

// StartDispatch checks the caller context, then returns the call's SDK
// parameters as DispatchParams does.
func StartDispatch[P any](ctx context.Context, call Call, family Family, endpointID string) (P, error) {
	if err := ctx.Err(); err != nil {
		var zero P
		return zero, NewDispatchContextError(err)
	}
	return DispatchParams[P](call, family, endpointID)
}

// DispatchTarget names the adapter endpoint one call is dispatched to.
type DispatchTarget struct {
	// EndpointID is attached to every classified SDK failure.
	EndpointID string
	// Provider names the adapter in the safe details of an unsent failure and
	// is passed to MapError.
	Provider string
	// CredentialUnsent classifies a credential error as unsent even after the
	// probe saw the request: the SDK runs a credential middleware inside the
	// probe but before it sends anything (#1495).
	CredentialUnsent bool
	// MapError classifies an SDK error that carries no pre-dispatch evidence.
	MapError func(err error, provider string) *Error
}

// Dispatch is one call past its possible-write boundary.
type Dispatch struct {
	// Context is the call context to pass to the SDK. It carries the call's
	// egress outcome.
	Context  context.Context
	Observer Observer
	target   DispatchTarget
	outcome  *EgressOutcome
}

// BeginDispatch defaults a nil observer, attaches an egress outcome to ctx
// and runs the observer's BeforePossibleWrite hook. A rejection there is a
// definite non-dispatch.
func BeginDispatch(ctx context.Context, observer Observer, target DispatchTarget) (*Dispatch, error) {
	if observer == nil {
		observer = NopObserver{}
	}
	callContext, outcome := WithEgressOutcome(ctx)
	if err := observer.BeforePossibleWrite(callContext); err != nil {
		return nil, NewDispatchObserverError(err, DispatchNotDispatched)
	}
	return &Dispatch{Context: callContext, Observer: observer, target: target, outcome: outcome}, nil
}

// SDKError classifies a non-nil error the SDK call returned. A certified
// egress outcome comes first; then an error raised before the request reached
// the transport (sent is false) is a pre-dispatch failure; anything else is
// left to the target's MapError. The result carries the endpoint ID.
func (dispatch *Dispatch) SDKError(err error, sent bool) error {
	endpointID := dispatch.target.EndpointID
	if mapped := ClassifyEgressOutcome(dispatch.outcome, err); mapped != nil {
		return WithEndpointID(mapped, endpointID)
	}
	if mapped := ClassifyUnsentError(err); mapped != nil && (!sent || dispatch.target.CredentialUnsent && errors.Is(err, ErrProviderCredentials)) {
		mapped.SafeDetails = map[string]string{"provider": dispatch.target.Provider}
		return WithEndpointID(mapped, endpointID)
	}
	return WithEndpointID(dispatch.target.MapError(err, dispatch.target.Provider), endpointID)
}

// SendHTTP runs one probed call through an HTTP provider SDK. send must
// install probe.Middleware as the call's innermost request middleware and
// record the HTTP response into raw. A recovered panic, a redirect response
// and an SDK error each come back classified and carrying the endpoint ID;
// otherwise SendHTTP returns the SDK's response and the HTTP response.
func SendHTTP[R any](dispatch *Dispatch, operationKey string, send func(probe *DispatchProbe, raw **http.Response) (R, error)) (R, *http.Response, error) {
	var rawResponse *http.Response
	probe := &DispatchProbe{}
	response, panicked, err := CallRecovered(func() (R, error) { return send(probe, &rawResponse) })
	var zero R
	switch {
	case panicked != nil:
		return zero, nil, WithEndpointID(probe.PanicError(operationKey, panicked), dispatch.target.EndpointID)
	case rawResponse != nil && IsRedirectStatus(rawResponse.StatusCode):
		return zero, nil, WithEndpointID(NewRedirectResponseError(rawResponse.StatusCode), dispatch.target.EndpointID)
	case err != nil:
		return zero, nil, dispatch.SDKError(err, probe.Sent())
	}
	return response, rawResponse, nil
}

// AfterResponseHeaders runs the observer's AfterResponseHeaders hook. A
// rejection is reported as an accepted dispatch carrying the response's IDs.
func (dispatch *Dispatch) AfterResponseHeaders(metadata ResponseMetadata) error {
	if err := dispatch.Observer.AfterResponseHeaders(dispatch.Context, metadata); err != nil {
		mapped := NewDispatchObserverError(err, DispatchAccepted)
		mapped.Provider.ResponseID = metadata.ResponseID
		mapped.Provider.RequestID = metadata.RequestID
		return mapped
	}
	return nil
}

// LiftProgress reports to the observer that the response, with outputItems
// output items, is being lifted.
func (dispatch *Dispatch) LiftProgress(outputItems int) {
	dispatch.Observer.OnProgress(dispatch.Context, Progress{Phase: string(PhaseLift), OutputItems: outputItems})
}

// HTTPResponseMetadata returns the response metadata of one HTTP call: the
// response ID and provider tier, plus, when raw is set, the HTTP status and
// the first non-empty of the requestIDHeaders.
func HTTPResponseMetadata(responseID, providerTier string, raw *http.Response, requestIDHeaders ...string) ResponseMetadata {
	metadata := ResponseMetadata{ResponseID: responseID, ProviderTier: providerTier}
	if raw != nil {
		metadata.Status = raw.StatusCode
		for _, header := range requestIDHeaders {
			if metadata.RequestID = raw.Header.Get(header); metadata.RequestID != "" {
				break
			}
		}
	}
	return metadata
}

// Lift runs AfterResponseHeaders, reports LiftProgress for outputItems and
// then returns the response lift produces.
func (dispatch *Dispatch) Lift(metadata ResponseMetadata, outputItems int, lift func() (llm.Response, error)) (Result, error) {
	if err := dispatch.AfterResponseHeaders(metadata); err != nil {
		return Result{}, err
	}
	dispatch.LiftProgress(outputItems)
	lifted, err := lift()
	if err != nil {
		return Result{}, err
	}
	return Result{Response: lifted}, nil
}
