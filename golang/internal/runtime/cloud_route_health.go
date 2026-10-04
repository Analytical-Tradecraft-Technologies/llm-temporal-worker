package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// ProviderRouteStatusReader reads one snapshot-bound shared route projection.
type ProviderRouteStatusReader interface {
	GetRouteStatus(context.Context, [32]byte, string) (control.RouteStatus, error)
}

func (p *ProviderPlanning) routeBlocked(ctx context.Context, candidate routing.Candidate) (bool, error) {
	if isNilCapability(p.routeStatus) {
		return false, nil
	}
	status, err := p.routeStatus.GetRouteStatus(ctx, p.configDigest, candidate.RouteID)
	if errors.Is(err, control.ErrProviderStatusNotFound) {
		return false, nil
	}
	if err != nil {
		return false, providerPlanningError(provider.CodeStateUnavailable, provider.PhasePlan, provider.RetrySameOperation)
	}
	if status.ConfigDigest != p.configDigest || status.RouteID != candidate.RouteID || status.EndpointID != candidate.EndpointID || status.EndpointAccountHMAC != candidate.EndpointAccountHMAC || status.Provider != candidate.Provider || status.EndpointFamily != candidate.Family {
		return false, providerPlanningError(provider.CodeStateCorrupt, provider.PhasePlan, provider.RetryNever)
	}
	if status.Credit == control.CreditExhausted || status.Billing == control.BillingIssue {
		return true, nil
	}
	return status.Circuit == control.CircuitOpen && p.clock().Before(status.StaleAfter), nil
}

// Stable completion timestamps make activity replay idempotent at the Redis
// projection. Health is best effort and cannot prevent budget settlement.
func (e *CloudProviderExecution) recordRouteStatus(ctx context.Context, saved cloudstate.SavedProviderExecution) {
	if isNilCapability(e.statusRecorder) {
		return
	}
	x, plan := saved.Execution, saved.Plan
	if x.Stage != cloudstate.ExecutionSucceeded && x.Stage != cloudstate.ExecutionFailed {
		return
	}
	if x.Failure != nil && x.Failure.Dispatch == provider.DispatchAmbiguous {
		return
	}
	account, err := hex.DecodeString(string(plan.Route.CacheIdentity.Account))
	if err != nil || len(account) != 32 {
		return
	}
	var accountID [32]byte
	copy(accountID[:], account)
	o := control.StatusObservation{ConfigDigest: plan.ConfigDigest, ConfigEpoch: plan.ConfigEpoch, RouteID: plan.Route.RouteID, EndpointID: plan.Route.EndpointID, EndpointAccountHMAC: accountID, Provider: plan.Route.Provider, EndpointFamily: plan.Family, ObservedAt: x.CompletedAt, ExpiresAt: x.CompletedAt.Add(30 * time.Second), Source: control.SourceInference, Availability: control.AvailabilityAvailable, Credit: control.CreditOK, Billing: control.BillingOK, EvidenceDigest: sha256.Sum256([]byte(plan.Route.OperationID))}
	if x.Failure != nil {
		o.SafeErrorCode = string(x.Failure.Code)
		switch x.Failure.Code {
		case provider.CodeProviderUnavailable, provider.CodeAuthentication, provider.CodePermissionDenied, provider.CodeConfiguration:
			o.Availability = control.AvailabilityUnavailable
		case provider.CodeProviderRateLimited:
			o.Availability = control.AvailabilityDegraded
		default:
			return
		}
	}
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = e.statusRecorder.RecordProviderStatus(finalCtx, o)
}
