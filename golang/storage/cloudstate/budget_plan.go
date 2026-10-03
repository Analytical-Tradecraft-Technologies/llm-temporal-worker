package cloudstate

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// ErrBudgetPlanMissing alone permits initial planning. Missing request rows,
// referenced blobs, and invalid plans must never be treated as a planning miss.
var ErrBudgetPlanMissing = fmt.Errorf("budget plan missing: %w", contracts.ErrNotFound)

type BudgetMode string

const (
	BudgetReserved  BudgetMode = "reserved"
	BudgetFree      BudgetMode = "free"
	BudgetUnmatched BudgetMode = "unmatched"
)

// BudgetPlan is an attempt's immutable preparation, including whether its
// captured policy requires Redis acceptance. It contains no SDK parameters, credentials or dispatch
// permission. QuotedAt and Reservation.ExpiresAt are reused across retries.
// A new paid attempt after an unknown outcome requires separate orchestration;
// it must never replace this plan or reuse its operation ID/consumed claim.
type BudgetPlan struct {
	Mode              BudgetMode             `json:"mode"`
	Unpriced          bool                   `json:"unpriced,omitempty"`
	Version           int                    `json:"version"`
	Kind              string                 `json:"kind"`
	ConfigDigest      [32]byte               `json:"config_digest"`
	ConfigEpoch       string                 `json:"config_epoch"`
	RequestDigest     [32]byte               `json:"request_digest"`
	CapabilityVersion string                 `json:"capability_version"`
	CompilerVersion   string                 `json:"compiler_version"`
	Family            string                 `json:"family"`
	ProviderTier      string                 `json:"provider_tier"`
	RequestedClass    llm.ServiceClass       `json:"requested_class"`
	AttemptedClass    llm.ServiceClass       `json:"attempted_class"`
	Route             durable.RoutePlan      `json:"route"`
	Quote             pricing.Quote          `json:"quote"`
	Estimate          budget.Estimate        `json:"estimate"`
	Reservation       durable.ReserveRequest `json:"reservation"`
	QuotedAt          time.Time              `json:"quoted_at"`
}

func (plan BudgetPlan) RequiresReservation() bool { return plan.Mode == BudgetReserved }

// Validate checks durable bindings without consulting current prices, settings,
// budget balances or clocks. Exact USD values are authoritative.
func (plan BudgetPlan) Validate() error {
	if plan.Mode != BudgetReserved && plan.Mode != BudgetFree && plan.Mode != BudgetUnmatched {
		return ErrInvalid
	}
	if plan.RequiresReservation() != (len(plan.Reservation.Reservations) > 0) || (plan.Unpriced && plan.Mode != BudgetUnmatched) {
		return ErrInvalid
	}
	if plan.Version != 1 || (plan.Kind != "generate" && plan.Kind != "compact") ||
		plan.ConfigDigest == ([32]byte{}) || plan.RequestDigest == ([32]byte{}) ||
		!safeText(plan.ConfigEpoch, 256) || !safeText(plan.CapabilityVersion, 256) ||
		!safeText(plan.CompilerVersion, 128) || !safeText(plan.Family, 128) || !safeText(plan.ProviderTier, 128) ||
		!plan.RequestedClass.Valid() || !plan.AttemptedClass.Valid() || plan.Route.Validate() != nil ||
		!budgetPlanTime(plan.QuotedAt) || !budgetPlanTime(plan.Reservation.ExpiresAt) || !plan.Reservation.ExpiresAt.After(plan.QuotedAt) ||
		plan.Reservation.OperationID != plan.Route.OperationID || plan.Reservation.GenerationID != plan.Route.GenerationID {
		return ErrInvalid
	}
	identity, entry := plan.Route.CacheIdentity, plan.Quote.Entry
	if string(identity.Provider) != plan.Route.Provider ||
		string(identity.Endpoint) != plan.Route.EndpointID || string(identity.Model) != plan.Route.Model ||
		!safeText(string(identity.Revision), 256) || string(identity.Compiler) != plan.Family+"/"+plan.CompilerVersion ||
		(identity.Account != "" && !hexDigest(string(identity.Account))) {
		return ErrInvalid
	}
	for _, value := range []string{string(identity.Provider), plan.Route.RouteID, plan.Route.EndpointID, plan.Route.Provider, plan.Route.Model,
		string(plan.Route.OperationID), string(plan.Route.GenerationID), plan.Estimate.CandidateID} {
		if !safeText(value, 512) {
			return ErrInvalid
		}
	}
	if plan.Unpriced {
		// An absent price is allowed only by the captured unmatched-policy path.
		// No synthetic zero quote may turn this into known-free work.
		if !equalExecutionJSON(plan.Quote, pricing.Quote{}) || !equalExecutionJSON(plan.Estimate, budget.Estimate{CandidateID: plan.Estimate.CandidateID}) {
			return ErrInvalid
		}
		return nil
	}
	if !safeText(plan.Route.PriceVersion, 512) || !safeText(plan.Quote.CatalogVersion, 512) {
		return ErrInvalid
	}
	digest, err := hex.DecodeString(plan.Quote.CatalogDigest)
	if err != nil || len(digest) != 32 || [32]byte(digest) == ([32]byte{}) || !entry.Active(plan.QuotedAt) ||
		entry.Provider != plan.Route.Provider || entry.Family != plan.Family || entry.EndpointID != plan.Route.EndpointID ||
		entry.Region != string(identity.Region) || entry.Model != plan.Route.Model || entry.ProviderTier != plan.ProviderTier ||
		entry.Version != plan.Route.PriceVersion || plan.Estimate.CatalogVersion != entry.Version {
		return ErrInvalid
	}
	if _, err := pricing.CompileUSD(plan.Quote.CatalogVersion, []pricing.Entry{entry}); err != nil {
		return ErrInvalid
	}
	if plan.Estimate.InputTokens < 0 || plan.Estimate.OutputTokens < 0 || plan.Estimate.ReasoningTokens < 0 || plan.Estimate.CacheWriteTokens < 0 ||
		plan.Estimate.MicroUSD < 0 || plan.Estimate.CostUSD.Validate() != nil ||
		(plan.RequiresReservation() && plan.Estimate.CostUSD.IsZero()) || (plan.Mode == BudgetFree && !plan.Estimate.CostUSD.IsZero()) {
		return ErrInvalid
	}
	for _, component := range []struct {
		price pricing.PriceComponent
		units int64
	}{
		{pricing.PriceComponentInput, plan.Estimate.InputTokens},
		{pricing.PriceComponentOutput, plan.Estimate.OutputTokens},
		{pricing.PriceComponentReasoning, plan.Estimate.ReasoningTokens},
		{pricing.PriceComponentCacheWrite, plan.Estimate.CacheWriteTokens},
		{pricing.PriceComponentPerRequest, 1},
	} {
		if component.units > 0 && entry.ComponentUnknown(component.price) {
			return ErrInvalid
		}
	}
	if plan.Mode == BudgetFree {
		cost, err := pricing.CostFromUsage(entry, pricing.Usage{InputTokens: plan.Estimate.InputTokens, OutputTokens: plan.Estimate.OutputTokens,
			ReasoningTokens: plan.Estimate.ReasoningTokens, CacheWriteTokens: plan.Estimate.CacheWriteTokens})
		if err != nil || !cost.USD.IsZero() || plan.Estimate.MicroUSD != 0 {
			return ErrInvalid
		}
	}
	if _, err := pricing.CeilNanoUSD(plan.Estimate.CostUSD); err != nil {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(plan.Reservation.Reservations))
	for _, window := range plan.Reservation.Reservations {
		if !safeText(window.PolicyID, 128) || !safeText(window.WindowID, 128) || seen[window.WindowID] ||
			window.Bucket < 0 || window.BucketNanos <= 0 || window.DurationNanos < window.BucketNanos || window.DurationNanos%window.BucketNanos != 0 ||
			window.Bucket != plan.QuotedAt.UnixNano()/window.BucketNanos || window.AmountUSD.Cmp(plan.Estimate.CostUSD) != 0 ||
			window.Amount != plan.Estimate.MicroUSD || window.Limit < 0 || window.LimitUSD.Validate() != nil || window.LimitUSD.IsZero() {
			return ErrInvalid
		}
		seen[window.WindowID] = true
		start := window.Bucket * window.BucketNanos // bound by QuotedAt above
		if window.DurationNanos > (1<<62)-start-window.BucketNanos {
			return ErrInvalid
		}
		if limit, err := pricing.FloorNanoUSD(window.LimitUSD); err != nil || limit == 0 {
			return ErrInvalid
		}
	}
	return nil
}

func budgetPlanTime(at time.Time) bool {
	return !at.IsZero() && at.Equal(time.Unix(0, at.UnixNano())) && at.UnixNano() >= 0 && at.UnixNano() < 1<<62
}

// SaveBudgetPlan saves one immutable initial plan in the request's encrypted
// progress. A healthy identical replay is read-only; uncertain event/index
// writes are repaired by retrying the same plan. Different plans conflict.
func (r *Repository) SaveBudgetPlan(ctx context.Context, scope Scope, id RequestID, plan BudgetPlan, now time.Time) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if plan.Validate() != nil || !validTime(now) {
		return ErrInvalid
	}
	encoded, err := canonicalBudgetPlan(plan)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 16; attempt++ {
		record, err := r.Read(ctx, scope, id)
		if err != nil {
			return err
		}
		if record.Status != StatusRunning || record.Request.Kind != plan.Kind || plan.QuotedAt.Before(record.Request.CreatedAt) {
			return contracts.ErrConflict
		}
		progress, existing, err := budgetPlanProgress(record.Progress)
		if err != nil {
			return err
		}
		if _, finalizing := progress["checkpoint_finalization"]; finalizing {
			return contracts.ErrConflict
		}
		if _, finalizing := progress["finalization_handoff"]; finalizing {
			return contracts.ErrConflict
		}
		if _, executing := progress["provider_execution"]; executing {
			return contracts.ErrConflict
		}
		if existing != nil {
			previous, _ := canonicalBudgetPlan(*existing)
			if !bytes.Equal(previous, encoded) {
				return contracts.ErrConflict
			}
			return r.repairBudgetPlanIndex(ctx, record)
		}
		progress["budget_plan"] = encoded
		data, err := json.Marshal(progress)
		if err != nil || len(data) > maxPayloadBytes {
			return ErrInvalid
		}
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		_, err = r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: "budget-plan", Status: StatusRunning, Progress: data, UpdatedAt: now})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

// LoadBudgetPlan returns the original plan, including prices no longer active
// today. Repairing discovery is required before returning it for admission.
// Finalizing/terminal/unknown-outcome requests cannot resume initial admission.
func (r *Repository) LoadBudgetPlan(ctx context.Context, scope Scope, id RequestID) (BudgetPlan, error) {
	record, err := r.Read(ctx, scope, id)
	if err != nil {
		return BudgetPlan{}, err
	}
	progress, plan, err := budgetPlanProgress(record.Progress)
	if err != nil {
		return BudgetPlan{}, err
	}
	_, checkpoint := progress["checkpoint_finalization"]
	_, handoff := progress["finalization_handoff"]
	_, executing := progress["provider_execution"]
	if record.Status != StatusRunning || checkpoint || handoff || executing {
		return BudgetPlan{}, contracts.ErrConflict
	}
	if plan == nil {
		return BudgetPlan{}, ErrBudgetPlanMissing
	}
	if plan.Kind != record.Request.Kind || plan.QuotedAt.Before(record.Request.CreatedAt) {
		return BudgetPlan{}, ErrCorrupt
	}
	if err := r.repairBudgetPlanIndex(ctx, record); err != nil {
		return BudgetPlan{}, err
	}
	return *plan, nil
}

func (r *Repository) repairBudgetPlanIndex(ctx context.Context, record Record) error {
	if err := r.advanceIndex(ctx, record); err != nil {
		return fmt.Errorf("%w: %w", ErrIndexPending, err)
	}
	return nil
}

func canonicalBudgetPlan(plan BudgetPlan) (json.RawMessage, error) {
	plan.QuotedAt, plan.Reservation.ExpiresAt = plan.QuotedAt.UTC(), plan.Reservation.ExpiresAt.UTC()
	if !plan.Quote.Entry.EffectiveFrom.IsZero() {
		plan.Quote.Entry.EffectiveFrom = plan.Quote.Entry.EffectiveFrom.UTC()
	}
	if !plan.Quote.Entry.EffectiveUntil.IsZero() {
		plan.Quote.Entry.EffectiveUntil = plan.Quote.Entry.EffectiveUntil.UTC()
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return nil, ErrInvalid
	}
	return objectJSON(data)
}

func budgetPlanProgress(data json.RawMessage) (map[string]json.RawMessage, *BudgetPlan, error) {
	var progress map[string]json.RawMessage
	if json.Unmarshal(data, &progress) != nil || progress == nil || string(progress["version"]) != "1" {
		return nil, nil, ErrCorrupt
	}
	encoded, exists := progress["budget_plan"]
	if !exists {
		return progress, nil, nil
	}
	var plan BudgetPlan
	if json.Unmarshal(encoded, &plan) != nil || plan.Validate() != nil {
		return nil, nil, ErrCorrupt
	}
	return progress, &plan, nil
}
