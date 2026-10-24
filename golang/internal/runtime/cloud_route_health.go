package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// ProviderRouteStatusReader reads one snapshot-bound shared route projection.
// Health is scoped to the provider model and tier: model sync gives every
// synced model on an endpoint the same route ID, and provider rate limits,
// overloads and model denials are per model and tier.
type ProviderRouteStatusReader interface {
	GetRouteStatus(ctx context.Context, configDigest [32]byte, routeID, endpointID, model, providerTier string) (control.RouteStatus, error)
}

func (p *ProviderPlanning) routeBlocked(ctx context.Context, candidate routing.Candidate) (bool, error) {
	reason, err := p.routeHold(ctx, candidate)
	return reason != "", err
}

// routeHold returns why the candidate's shared health record holds it, or ""
// when nothing does.
func (p *ProviderPlanning) routeHold(ctx context.Context, candidate routing.Candidate) (string, error) {
	if isNilCapability(p.routeStatus) {
		return "", nil
	}
	status, err := p.routeStatus.GetRouteStatus(ctx, p.configDigest, candidate.RouteID, candidate.EndpointID, candidate.Model, candidate.ProviderTier)
	if errors.Is(err, control.ErrProviderStatusNotFound) {
		return "", nil
	}
	if err != nil {
		return "", providerPlanningError(provider.CodeStateUnavailable, provider.PhasePlan, provider.RetrySameOperation)
	}
	if status.ConfigDigest != p.configDigest || status.RouteID != candidate.RouteID || status.EndpointID != candidate.EndpointID || status.EndpointAccountHMAC != candidate.EndpointAccountHMAC || status.Provider != candidate.Provider || status.EndpointFamily != candidate.Family || status.Model != candidate.Model || status.ProviderTier != candidate.ProviderTier {
		// A record for another route identity is not evidence about this
		// candidate. Health is advisory, so it must not fail the selection.
		return "", nil
	}
	switch {
	case status.Credit == control.CreditExhausted:
		return observability.RouteHeldCreditExhausted, nil
	case status.Billing == control.BillingIssue:
		return observability.RouteHeldBillingIssue, nil
	case status.Circuit == control.CircuitOpen && p.clock().Before(status.StaleAfter):
		return observability.RouteHeldCircuitOpen, nil
	}
	return "", nil
}

// routeHealthCooldown is how long an ordinary definite failure keeps counting
// toward, or holding open, a route circuit before a later request may probe it.
const routeHealthCooldown = 30 * time.Second

// credentialFailureHold keeps a route open after the provider rejects it with
// an authentication or configuration failure. Health records are keyed by the configuration digest,
// so publishing a new snapshot clears the hold at once; the bound only lets a
// credential rotated in place, without a snapshot change, recover eventually.
const credentialFailureHold = 24 * time.Hour

// creditFailureHold keeps a route open after the provider reports its account
// out of credit. A new snapshot also clears it; the short bound lets a top-up
// made in place recover after one probe request, which itself fails over.
const creditFailureHold = 15 * time.Minute

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
	o := control.StatusObservation{ConfigDigest: plan.ConfigDigest, ConfigEpoch: plan.ConfigEpoch, RouteID: plan.Route.RouteID, EndpointID: plan.Route.EndpointID, EndpointAccountHMAC: accountID, Provider: plan.Route.Provider, EndpointFamily: plan.Family, Model: plan.Route.Model, ProviderTier: plan.ProviderTier, ObservedAt: x.CompletedAt, ExpiresAt: x.CompletedAt.Add(routeHealthCooldown), Source: control.SourceInference, Availability: control.AvailabilityAvailable, Credit: control.CreditOK, Billing: control.BillingOK, EvidenceDigest: sha256.Sum256([]byte(plan.Route.OperationID))}
	if x.Failure != nil {
		o.SafeErrorCode = string(x.Failure.Code)
		switch x.Failure.Code {
		case provider.CodeAuthentication, provider.CodeConfiguration:
			o.Availability = control.AvailabilityUnavailable
			// Only a provider rejection proves the credentials are bad. A
			// local credential lookup that failed before dispatch, such as an
			// unreachable IMDS, can recover without a snapshot change.
			if x.Failure.Dispatch == provider.DispatchRejected {
				o.ExpiresAt = x.CompletedAt.Add(credentialFailureHold)
			}
		case provider.CodePermissionDenied:
			// A 403 can be a request-level rejection such as a moderation
			// flag, so it counts toward the threshold instead of opening the
			// shared circuit for every caller.
			o.Availability = control.AvailabilityUnavailable
			if x.Failure.CreditExhausted {
				// A 402 is account-wide, so it holds the route at once. It is
				// not a sticky credit incident: credit is usually topped up in
				// place, and an inference observation could never clear one.
				o.SafeErrorCode, o.ExpiresAt = control.SafeCodeInsufficientCredit, x.CompletedAt.Add(creditFailureHold)
			}
		case provider.CodeProviderUnavailable:
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
