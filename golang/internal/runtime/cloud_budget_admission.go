package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type cloudAdmissionStore interface {
	CloudBudgetPlanStore
	Read(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.Record, error)
}

// CloudBudgetAdmission connects original request/plan recovery to Redis. Scope
// must already be authorized by the caller. Preparation is separate from
// Reserve so cache lookup/fill ownership can precede budget acquisition. Claim
// belongs immediately before submission, never in a budget-waiting activity.
type CloudBudgetAdmission struct {
	store    cloudAdmissionStore
	planning *BudgetPlanning
	recovery *ProviderRecovery
	admit    BudgetAdmission
}

// CloudBudgetCall is invocation-local. Private bindings prevent a caller from
// replacing the persisted reservation between preparation, Reserve and Claim.
// It is not serializable workflow state or proof of dispatch permission.
type CloudBudgetCall struct {
	owner    *CloudBudgetAdmission
	scope    cloudstate.Scope
	id       cloudstate.RequestID
	plan     cloudstate.BudgetPlan
	provider PlannedProviderCall
	// transcript is the Generate input the provider output must extend.
	transcript []llm.Item
}

// Provider returns the invocation-local compiled call. Only a successful Claim
// permits paid submission; this value alone is not a grant.
func (call *CloudBudgetCall) Provider() PlannedProviderCall { return call.provider }

// Plan returns a detached copy; mutations cannot affect later admission.
func (call *CloudBudgetCall) Plan() cloudstate.BudgetPlan {
	plan := call.plan
	plan.Reservation.Reservations = append(plan.Reservation.Reservations[:0:0], plan.Reservation.Reservations...)
	plan.Quote.Entry.UnknownComponents = append(plan.Quote.Entry.UnknownComponents[:0:0], plan.Quote.Entry.UnknownComponents...)
	return plan
}

func (capabilities V1RuntimeCapabilities) NewCloudBudgetAdmission(ctx context.Context) (*CloudBudgetAdmission, error) {
	store, ok := capabilities.Requests.(cloudAdmissionStore)
	boundary, err := capabilities.cloudBudgetBoundary()
	if !ok || isNilCapability(store) || err != nil {
		return nil, budgetPlanningError(provider.CodeConfiguration)
	}
	planning, err := capabilities.NewBudgetPlanning(ctx)
	if err != nil {
		return nil, err
	}
	// Use one capture for initial planning and reconstruction after a save race.
	return &CloudBudgetAdmission{store: store, planning: planning,
		recovery: &ProviderRecovery{providers: planning.providers},
		admit:    BudgetAdmission{boundary: boundary}}, nil
}

func (admission *CloudBudgetAdmission) PrepareGenerate(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, replay durable.GenerateReplay, attempt BudgetAttempt) (*CloudBudgetCall, error) {
	record, err := admission.original(ctx, scope, id, "generate")
	if err != nil {
		return nil, err
	}
	var request llm.GenerateRequestV1
	if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	prepared, err := PrepareGenerateInput(ctx, request, replay)
	if err != nil {
		return nil, err
	}
	key, err := cloudProviderOperationKey(record, request.OperationKey)
	if err != nil {
		return nil, err
	}
	prepared.Request.OperationKey = key
	call, err := admission.prepare(ctx, record, key, attempt,
		func() (PlannedBudgetCall, error) { return admission.planning.Generate(ctx, prepared, attempt) },
		func(binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
			return admission.recovery.Generate(ctx, prepared, binding)
		})
	if err != nil {
		return nil, err
	}
	call.transcript = prepared.Request.Input
	return call, nil
}

func (admission *CloudBudgetAdmission) PrepareCompact(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, replay durable.CompactReplay, attempt BudgetAttempt) (*CloudBudgetCall, error) {
	record, err := admission.original(ctx, scope, id, "compact")
	if err != nil {
		return nil, err
	}
	var request llm.CompactRequestV1
	if json.Unmarshal(record.Request.Manifest, &request) != nil || request.Context.Tenant != scope.Tenant || request.Context.Project != scope.Project {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	prepared, err := PrepareCompactInput(ctx, request, replay)
	if err != nil {
		return nil, err
	}
	key, err := cloudProviderOperationKey(record, request.OperationKey)
	if err != nil {
		return nil, err
	}
	if prepared.Request != nil {
		prepared.Request.OperationKey = key
	}
	return admission.prepare(ctx, record, key, attempt,
		func() (PlannedBudgetCall, error) { return admission.planning.Compact(ctx, prepared, attempt) },
		func(binding ProviderRecoveryBinding) (PlannedProviderCall, error) {
			return admission.recovery.Compact(ctx, prepared, binding)
		})
}

func (admission *CloudBudgetAdmission) original(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, kind string) (cloudstate.Record, error) {
	if ctx == nil || admission == nil || isNilCapability(admission.store) || admission.planning == nil || admission.recovery == nil {
		return cloudstate.Record{}, budgetPlanningError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return cloudstate.Record{}, err
	}
	record, err := admission.store.Read(ctx, scope, id)
	if err != nil {
		return cloudstate.Record{}, cloudRuntimeError(err, false)
	}
	if record.Request.ID != id || record.Request.Scope != scope || record.Request.Kind != kind {
		return cloudstate.Record{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if record.Status != cloudstate.StatusRunning {
		return cloudstate.Record{}, cloudRuntimeError(contracts.ErrConflict, false)
	}
	return record, nil
}

func (admission *CloudBudgetAdmission) prepare(ctx context.Context, record cloudstate.Record, key string, attempt BudgetAttempt,
	planNew func() (PlannedBudgetCall, error), reconstruct func(ProviderRecoveryBinding) (PlannedProviderCall, error),
) (*CloudBudgetCall, error) {
	scope, id := record.Request.Scope, record.Request.ID
	linked, err := cloudRequestAttempt(record)
	if err != nil {
		return nil, err
	}
	plan, err := admission.store.LoadBudgetPlan(ctx, scope, id)
	if errors.Is(err, cloudstate.ErrBudgetPlanMissing) {
		if attempt.QuotedAt.Before(record.Request.CreatedAt) || (linked != nil && attempt.OperationID != durable.OperationID(linked.ID)) {
			return nil, budgetPlanningError(provider.CodeInvalidArgument)
		}
		planned, planErr := planNew()
		if planErr != nil {
			return nil, planErr
		}
		plan, planErr = planned.BudgetPlan(record.Request.Kind)
		if planErr != nil {
			return nil, planErr
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		saveErr := admission.store.SaveBudgetPlan(ctx, scope, id, plan, attempt.QuotedAt)
		if saveErr != nil && !errors.Is(saveErr, contracts.ErrConflict) {
			// A lost acknowledgement may have committed. No Redis mutation until
			// a later invocation has read and verified the durable winner.
			return nil, cloudRuntimeError(saveErr, false)
		}
		// Even our successful write is read back: the store repairs discovery,
		// fences finalization, and supplies the same authority as restart.
		plan, err = admission.store.LoadBudgetPlan(ctx, scope, id)
	}
	if err != nil {
		return nil, cloudRuntimeError(err, false)
	}
	if plan.Validate() != nil || plan.Kind != record.Request.Kind || plan.QuotedAt.Before(record.Request.CreatedAt) {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	if linked != nil && plan.Route.OperationID != durable.OperationID(linked.ID) {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	call, err := reconstruct(ProviderRecoveryBinding{ConfigDigest: plan.ConfigDigest, ConfigEpoch: plan.ConfigEpoch, EndpointDigest: planEndpointDigest(plan),
		RequestDigest: plan.RequestDigest, OperationKeyDigest: ProviderRecoveryOperationKeyDigest(key),
		CandidateID: plan.Estimate.CandidateID, Route: plan.Route, Family: plan.Family,
		CapabilityVersion: plan.CapabilityVersion, ProviderTier: plan.ProviderTier,
		RequestedClass: plan.RequestedClass, AttemptedClass: plan.AttemptedClass})
	if err != nil {
		return nil, err
	}
	return &CloudBudgetCall{owner: admission, scope: scope, id: id, plan: plan, provider: call}, nil
}

// Reserve performs one acquired/wait decision with the stored quote. Calling it
// again never moves a bucket, changes an operation ID or extends an expiry.
func (admission *CloudBudgetAdmission) Reserve(ctx context.Context, call *CloudBudgetCall) (durable.ReserveResult, error) {
	if err := admission.verify(ctx, call); err != nil {
		return durable.ReserveResult{}, err
	}
	if !call.plan.RequiresReservation() {
		return durable.ReserveResult{}, nil
	}
	return admission.admit.reserve(ctx, call.plan.Route, call.Plan().Reservation, nil)
}

// Claim rechecks durable eligibility before consuming Redis's single-use grant.
// A duplicate or uncertain claim remains charged and cannot submit again.
func (admission *CloudBudgetAdmission) Claim(ctx context.Context, call *CloudBudgetCall, reservation durable.ReserveResult) (durable.ClaimReceipt, error) {
	if err := admission.verify(ctx, call); err != nil {
		return durable.ClaimReceipt{}, err
	}
	if !call.plan.RequiresReservation() {
		return durable.ClaimReceipt{}, budgetPlanningError(provider.CodeInvalidArgument)
	}
	return admission.admit.claim(ctx, call.plan.Route, reservation)
}

func (admission *CloudBudgetAdmission) verify(ctx context.Context, call *CloudBudgetCall) error {
	if ctx == nil || admission == nil || call == nil || call.owner != admission || isNilCapability(admission.store) {
		return budgetPlanningError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := admission.store.LoadBudgetPlan(ctx, call.scope, call.id)
	if err != nil {
		return cloudRuntimeError(err, false)
	}
	if plan.Validate() != nil || cloudPlanDigest(plan) != cloudPlanDigest(call.plan) {
		return cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	return ctx.Err()
}

func cloudPlanDigest(plan cloudstate.BudgetPlan) [32]byte {
	encoded, _ := json.Marshal(plan)
	return sha256.Sum256(encoded)
}
