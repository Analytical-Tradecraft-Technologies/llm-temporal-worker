package openairesponses

import (
	"context"
	"net/http"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
)

// BackgroundAdapter is only constructed for provider profiles documenting
// background Responses. Generic compatible endpoints keep the synchronous Adapter.
type BackgroundAdapter struct{ *Adapter }

var _ provider.ResumableAdapter = (*BackgroundAdapter)(nil)
var _ provider.ResumableAdapter = (*ModelListerAdapter)(nil)

func (a *ModelListerAdapter) Submit(ctx context.Context, c provider.Call, o provider.Observer) (provider.ResumableResult, error) {
	return (&BackgroundAdapter{a.Adapter}).Submit(ctx, c, o)
}
func (a *ModelListerAdapter) Poll(ctx context.Context, c provider.Call, id string, o provider.Observer) (provider.ResumableResult, error) {
	return (&BackgroundAdapter{a.Adapter}).Poll(ctx, c, id, o)
}

func (a *BackgroundAdapter) Submit(ctx context.Context, call provider.Call, observer provider.Observer) (provider.ResumableResult, error) {
	if a == nil || a.Adapter == nil || call.Family != provider.FamilyOpenAIResponses || call.EndpointID != a.endpointID {
		return provider.ResumableResult{}, dispatchError("call does not belong to this adapter", provider.DispatchNotDispatched)
	}
	params, ok := call.SDKParams.(responses.ResponseNewParams)
	if !ok {
		if p, yes := call.SDKParams.(*responses.ResponseNewParams); yes && p != nil {
			params = *p
			ok = true
		}
	}
	if !ok {
		return provider.ResumableResult{}, dispatchError("invalid Responses parameters", provider.DispatchNotDispatched)
	}
	params.Background = openai.Bool(true)
	if observer == nil {
		observer = provider.NopObserver{}
	}
	ctx, egress := provider.WithEgressOutcome(ctx)
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.ResumableResult{}, dispatchObserverError(err, provider.DispatchNotDispatched)
	}
	var raw *http.Response
	response, err := a.client.sdk.Responses.New(ctx, params, option.WithResponseInto(&raw), option.WithMaxRetries(0))
	if raw != nil && provider.IsRedirectStatus(raw.StatusCode) {
		return provider.ResumableResult{}, provider.NewRedirectResponseError(raw.StatusCode)
	}
	if err != nil {
		if mapped := provider.ClassifyEgressOutcome(egress, err); mapped != nil {
			return provider.ResumableResult{}, mapped
		}
		return provider.ResumableResult{}, mapError(err)
	}
	return a.backgroundResult(ctx, call, response, raw, observer)
}

func (a *BackgroundAdapter) Poll(ctx context.Context, call provider.Call, id string, observer provider.Observer) (provider.ResumableResult, error) {
	if a == nil || a.Adapter == nil || call.Family != provider.FamilyOpenAIResponses || call.EndpointID != a.endpointID || id == "" || len(id) > 512 || strings.ContainsAny(id, "/\\?#\x00\r\n") {
		return provider.ResumableResult{}, dispatchError("invalid poll identity", provider.DispatchNotDispatched)
	}
	if observer == nil {
		observer = provider.NopObserver{}
	}
	var raw *http.Response
	response, err := a.client.sdk.Responses.Get(ctx, id, responses.ResponseGetParams{}, option.WithResponseInto(&raw), option.WithMaxRetries(0))
	if raw != nil && provider.IsRedirectStatus(raw.StatusCode) {
		return provider.ResumableResult{}, provider.NewRedirectResponseError(raw.StatusCode)
	}
	if err != nil {
		if raw != nil && raw.StatusCode == http.StatusNotFound {
			return provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}, nil
		}
		mapped := mapError(err)
		mapped.Phase = provider.PhasePoll
		mapped.Dispatch = provider.DispatchAccepted
		if mapped.Code == provider.CodeDeadlineExceeded {
			mapped.Retry = provider.RetrySameOperation
		}
		return provider.ResumableResult{}, mapped
	}
	if response != nil && response.ID != id {
		return provider.ResumableResult{}, invalidResponseError(call, "", "provider poll identity changed")
	}
	// GET does not mark another dispatch or create a new budget reservation.
	return a.backgroundResult(ctx, call, response, raw, observer)
}
func (a *BackgroundAdapter) backgroundResult(ctx context.Context, call provider.Call, response *responses.Response, raw *http.Response, observer provider.Observer) (provider.ResumableResult, error) {
	if response == nil || response.ID == "" {
		return provider.ResumableResult{}, invalidResponseError(call, "", "provider returned no operation identity")
	}
	metadata := provider.ResponseMetadata{ResponseID: response.ID, ProviderTier: string(response.ServiceTier)}
	if raw != nil {
		metadata.Status = raw.StatusCode
		metadata.RequestID = raw.Header.Get("x-request-id")
	}
	if err := observer.AfterResponseHeaders(ctx, metadata); err != nil {
		return provider.ResumableResult{}, dispatchObserverError(err, provider.DispatchAccepted)
	}
	result := provider.ResumableResult{ProviderOperationID: response.ID, Dispatch: provider.DispatchAccepted, Metadata: metadata}
	switch string(response.Status) {
	case "queued", "in_progress":
		result.State = provider.ResumablePending
	case "failed", "cancelled":
		result.State = provider.ResumableFailed
		result.Failure = provider.NewError(provider.CodeProviderInvalidResponse, provider.PhasePoll, provider.DispatchAccepted, provider.RetryNever, "provider background operation failed")
	case "completed", "incomplete":
		lifted, err := liftResponse(call, response, metadata.RequestID)
		if err != nil {
			return provider.ResumableResult{}, err
		}
		result.State = provider.ResumableCompleted
		result.Result = provider.Result{Response: lifted}
	default:
		return provider.ResumableResult{}, invalidResponseError(call, metadata.RequestID, "unknown background operation status")
	}
	return result, result.ValidateForCall(call)
}
