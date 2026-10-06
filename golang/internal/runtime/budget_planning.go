package runtime

import (
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// BudgetAttempt comes from the durable attempt, not a clock read during each
// retry. Persist these values and the resulting quote before Redis acceptance.
// ExpiresAt bounds unused admission; Redis owns the separate 15-minute start
// deadline and retains consumed reservations until settlement/window expiry.
type BudgetAttempt struct {
	PriorCandidates []string
	OperationID     durable.OperationID
	GenerationID    durable.GenerationID
	QuotedAt        time.Time
	ExpiresAt       time.Time
}

// PlannedBudgetCall is invocation-local data, not a dispatch grant. The price,
// route and reservation are the non-secret facts to persist for recovery; the
// Provider's adapter/SDK parameters must never be serialized or logged.
type PlannedBudgetCall struct {
	mode        cloudstate.BudgetMode
	unpriced    bool
	Provider    PlannedProviderCall
	Route       durable.RoutePlan
	Quote       *pricing.Quote // nil only for an explicitly permitted unpriced, unbudgeted call
	Estimate    budget.Estimate
	Reservation durable.ReserveRequest
	QuotedAt    time.Time
	// ClassEntries price the route's other service classes at QuotedAt.
	ClassEntries map[llm.ServiceClass]pricing.Entry
}

// RequiresReservation distinguishes a positive matched quote from known-free
// or unmatched requests. Empty vectors must not be submitted to Redis, and are
// not permission to dispatch; composition must explicitly support that path.
func (planned PlannedBudgetCall) RequiresReservation() bool {
	return len(planned.Reservation.Reservations) != 0
}

// BudgetPlanning selects a compilable, priceable route using the exact snapshot
// captured by ProviderPlanning. It neither reads budget balances nor reserves,
// claims, submits, polls, or persists anything.
type BudgetPlanning struct {
	providers *ProviderPlanning
	estimator budget.Estimator
}

// NewBudgetPlanning uses only snapshot-owned capabilities. It performs the
// snapshot capture once and never constructs separate clients or a leaser.
func (capabilities V1RuntimeCapabilities) NewBudgetPlanning(ctx context.Context) (*BudgetPlanning, error) {
	providers, err := capabilities.NewProviderPlanning(ctx)
	if err != nil {
		return nil, err
	}
	return providers.NewBudgetPlanning(capabilities.BudgetEstimator, capabilities.MaxBudgetBucketsPerWindow)
}

func (planning *ProviderPlanning) NewBudgetPlanning(estimator budget.Estimator, maxBuckets int) (*BudgetPlanning, error) {
	if planning == nil || isNilCapability(planning.budgetSnapshot.Prices) || maxBuckets <= 0 ||
		estimator.MaxOutput <= 0 || estimator.MaxReasoning < 0 || (estimator.SafetyRatio != nil && estimator.SafetyRatio.Sign() <= 0) {
		return nil, budgetPlanningError(provider.CodeConfiguration)
	}
	if catalog, ok := planning.budgetSnapshot.Prices.(pricing.Catalog); ok && catalog.Validate() != nil {
		return nil, budgetPlanningError(provider.CodeConfiguration)
	}
	seenPolicies, seenWindows := make(map[string]bool), make(map[string]bool)
	for _, policy := range planning.budgetSnapshot.BudgetPolicies {
		if policy.Validate(maxBuckets) != nil || len(policy.ID) > 128 || seenPolicies[policy.ID] {
			return nil, budgetPlanningError(provider.CodeConfiguration)
		}
		seenPolicies[policy.ID] = true
		for _, window := range policy.Windows {
			// Redis completion identities name window/bucket, independently of
			// policy. Aliased windows would duplicate a debit or its settlement.
			if window.ID == "" || len(window.ID) > 128 || seenWindows[window.ID] {
				return nil, budgetPlanningError(provider.CodeConfiguration)
			}
			seenWindows[window.ID] = true
			limit, err := exactWindowLimit(window)
			if err != nil {
				return nil, budgetPlanningError(provider.CodeConfiguration)
			}
			materialized, err := pricing.FloorNanoUSD(limit)
			if err != nil || materialized == 0 {
				return nil, budgetPlanningError(provider.CodeConfiguration)
			}
		}
	}
	providers := *planning
	providers.outputLimit = estimator.MaxOutput
	providers.contextEstimator = copyBudgetEstimator(estimator)
	return &BudgetPlanning{providers: &providers, estimator: copyBudgetEstimator(estimator)}, nil
}

func copyBudgetEstimator(estimator budget.Estimator) budget.Estimator {
	if estimator.SafetyRatio != nil {
		estimator.SafetyRatio = new(big.Rat).Set(estimator.SafetyRatio)
	}
	return estimator
}

func (planning *BudgetPlanning) Generate(ctx context.Context, prepared PreparedGenerateInput, attempt BudgetAttempt) (PlannedBudgetCall, error) {
	return planning.plan(ctx, prepared.Request, prepared.pins, attempt)
}

func (planning *BudgetPlanning) Compact(ctx context.Context, prepared PreparedCompactInput, attempt BudgetAttempt) (PlannedBudgetCall, error) {
	if prepared.Request == nil {
		return PlannedBudgetCall{}, budgetPlanningError(provider.CodeInvalidArgument)
	}
	return planning.plan(ctx, *prepared.Request, providerStatePins{}, attempt)
}

func (planning *BudgetPlanning) plan(ctx context.Context, request llm.Request, pins providerStatePins, attempt BudgetAttempt) (PlannedBudgetCall, error) {
	if ctx == nil || planning == nil || planning.providers == nil {
		return PlannedBudgetCall{}, budgetPlanningError(provider.CodeConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return PlannedBudgetCall{}, err
	}
	if attempt.OperationID.Validate() != nil || attempt.GenerationID.Validate() != nil ||
		!safeQuoteTime(attempt.QuotedAt) || !safeQuoteTime(attempt.ExpiresAt) || !attempt.ExpiresAt.After(attempt.QuotedAt) {
		return PlannedBudgetCall{}, budgetPlanningError(provider.CodeInvalidArgument)
	}
	semantic, err := planning.estimator.PrepareRequest(request)
	if err != nil || semantic.Continuation != nil {
		return PlannedBudgetCall{}, budgetPlanningError(provider.CodeInvalidArgument)
	}
	var result PlannedBudgetCall
	oversized := false
	_, err = planning.providers.selectCall(ctx, semantic, pins, func(call PlannedProviderCall) (bool, error) {
		quoted, usable, err := planning.quote(ctx, semantic, pins, call, attempt, &oversized)
		if err == nil && usable {
			result = quoted
		}
		return usable, err
	}, attempt.PriorCandidates...)
	if err != nil {
		// A terminal no-route result after a candidate whose reservation
		// exceeds a matched window limit can never be admitted; waiting for
		// capacity would only run out the workflow deadline, so say so. Any
		// other outcome, such as a retryable health block on a candidate that
		// would fit, is kept.
		var mapped *provider.Error
		if oversized && ctx.Err() == nil && errors.As(err, &mapped) && mapped.Code == provider.CodeNoRoute {
			return PlannedBudgetCall{}, provider.NewError(provider.CodeBudgetDenied, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "request reservation exceeds a budget window limit")
		}
		return PlannedBudgetCall{}, err
	}
	return result, nil
}

func (planning *BudgetPlanning) quote(ctx context.Context, semantic llm.Request, pins providerStatePins, call PlannedProviderCall, attempt BudgetAttempt, oversized *bool) (PlannedBudgetCall, bool, error) {
	snapshot, candidate := planning.providers.budgetSnapshot, call.Candidate
	route, err := call.Route(attempt.OperationID, attempt.GenerationID)
	if err != nil {
		return PlannedBudgetCall{}, false, err
	}
	// Match the application's logical model and the actually attempted class.
	matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(semantic, candidate, snapshot.Environment))
	if snapshot.RequireBudgetMatch && len(matches) == 0 {
		return PlannedBudgetCall{}, false, nil
	}
	result := PlannedBudgetCall{mode: cloudstate.BudgetReserved, Provider: call, Route: route, QuotedAt: attempt.QuotedAt.UTC(), Estimate: budget.Estimate{CandidateID: candidate.ID},
		Reservation: durable.ReserveRequest{OperationID: attempt.OperationID, GenerationID: attempt.GenerationID, ExpiresAt: attempt.ExpiresAt.UTC()}}
	query := pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID,
		Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: attempt.QuotedAt.UTC()}
	quote, err := snapshot.Prices.Resolve(query)
	if ctx.Err() != nil {
		return PlannedBudgetCall{}, false, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, pricing.ErrNoActivePrice) {
			// Preserve the existing opt-in rule for unknown-cost unbudgeted work.
			result.mode, result.unpriced = cloudstate.BudgetUnmatched, true
			return result, len(matches) == 0 && snapshot.RequirePriceWhenBudgeted, nil
		}
		return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
	}
	entry := quote.Entry
	if entry.Version == "" {
		entry.Version = quote.CatalogVersion
	}
	if !validBudgetQuote(quote, entry, query) || (candidate.PriceVersion != "" && entry.Version != candidate.PriceVersion) {
		return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
	}
	quote.Entry = entry
	quote.Entry.UnknownComponents = append([]pricing.PriceComponent(nil), entry.UnknownComponents...)
	result.Route.PriceVersion = entry.Version
	result.ClassEntries = planning.classEntries(semantic, candidate, query)
	// Estimate the provider model, attempted tier and exact compiled input.
	resolved := resolveCandidateRequest(semantic, pins, candidate, call.Adapter)
	digest, err := llm.RequestDigest(resolved)
	if err != nil || digest != call.Call.Metadata.SchemaDigest {
		return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
	}
	estimate, err := planning.estimator.EstimateCandidate(resolved, candidate, entry)
	if ctx.Err() != nil {
		return PlannedBudgetCall{}, false, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, budget.ErrUnusablePrice) || errors.Is(err, budget.ErrContextLimit) {
			return PlannedBudgetCall{}, false, nil
		}
		return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeInvalidArgument)
	}
	result.Quote, result.Estimate = &quote, estimate
	if len(matches) == 0 || estimate.CostUSD.IsZero() {
		result.mode = cloudstate.BudgetUnmatched
		if estimate.CostUSD.IsZero() {
			result.mode = cloudstate.BudgetFree
		}
		return result, true, nil
	}
	if _, err := pricing.CeilNanoUSD(estimate.CostUSD); err != nil {
		return PlannedBudgetCall{}, false, nil // unusable Redis quote; try an authorized fallback
	}
	for _, match := range matches {
		window := match.Window
		_, bucket := window.Range(attempt.QuotedAt)
		// Match Redis's timestamp bounds without overflowing expiry arithmetic.
		if bucket < 0 || bucket > (1<<62)/window.Bucket.Nanoseconds() {
			return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
		}
		start := bucket * window.Bucket.Nanoseconds()
		if window.Duration.Nanoseconds() > (1<<62)-start-window.Bucket.Nanoseconds() {
			return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
		}
		limit, _ := exactWindowLimit(window) // validated when capturing the planner
		if estimate.CostUSD.Cmp(limit) > 0 {
			// This candidate can never fit the window; another may.
			*oversized = true
			return PlannedBudgetCall{}, false, nil
		}
		legacyLimit, err := compatibilityBudgetLimit(limit, window.Limit)
		if err != nil {
			return PlannedBudgetCall{}, false, budgetPlanningError(provider.CodeConfiguration)
		}
		result.Reservation.Reservations = append(result.Reservation.Reservations, admission.WindowReservation{
			PolicyID: match.PolicyID, WindowID: window.ID, Bucket: bucket,
			BucketNanos: window.Bucket.Nanoseconds(), DurationNanos: window.Duration.Nanoseconds(),
			Amount: estimate.MicroUSD, AmountUSD: estimate.CostUSD, Limit: legacyLimit, LimitUSD: limit})
	}
	return result, true, nil
}

func exactWindowLimit(window budget.Window) (pricing.USD, error) {
	if !window.LimitUSD.IsZero() {
		return window.LimitUSD, window.LimitUSD.Validate()
	}
	return pricing.USDFromMicro(window.Limit)
}

func validBudgetQuote(quote pricing.Quote, entry pricing.Entry, query pricing.Query) bool {
	digest, err := hex.DecodeString(quote.CatalogDigest)
	if err != nil || len(digest) != 32 || quote.CatalogVersion == "" || entry.Version == "" ||
		entry.Provider != query.Provider || entry.Family != query.Family || entry.EndpointID != query.EndpointID || entry.Region != query.Region ||
		entry.Model != query.Model || entry.ProviderTier != query.ProviderTier || !entry.Active(query.At) {
		return false
	}
	if [32]byte(digest) == ([32]byte{}) {
		return false
	}
	_, err = pricing.CompileUSD(quote.CatalogVersion, []pricing.Entry{entry})
	return err == nil
}

func safeQuoteTime(at time.Time) bool {
	return !at.IsZero() && at.Equal(time.Unix(0, at.UnixNano())) && at.UnixNano() >= 0 && at.UnixNano() < 1<<62
}

func budgetPlanningError(code provider.Code) error {
	return provider.NewError(code, provider.PhasePrice, provider.DispatchNotDispatched, provider.RetryNever, "budget planning failed")
}

// classEntries resolves, at the quote time, the price entry of every other
// service class the candidate's route offers. A provider may serve a response
// at another class than attempted (for example a priority request served at
// standard), and that response is priced at the class it was served at. A
// class without an active price is left out and priced at the attempted entry.
func (planning *BudgetPlanning) classEntries(semantic llm.Request, candidate routing.Candidate, query pricing.Query) map[llm.ServiceClass]pricing.Entry {
	model := planning.providers.catalog.Models[semantic.Model]
	if candidate.RouteIndex < 0 || candidate.RouteIndex >= len(model.Routes) || model.Routes[candidate.RouteIndex].ID != candidate.RouteID {
		return nil
	}
	var entries map[llm.ServiceClass]pricing.Entry
	for class, tier := range model.Routes[candidate.RouteIndex].ProviderTiers {
		if class == candidate.AttemptedClass || tier == "" {
			continue
		}
		classQuery := query
		classQuery.ProviderTier = tier
		quote, err := planning.providers.budgetSnapshot.Prices.Resolve(classQuery)
		if err != nil {
			continue
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		if !validBudgetQuote(quote, entry, classQuery) {
			continue
		}
		entry.UnknownComponents = append([]pricing.PriceComponent(nil), entry.UnknownComponents...)
		if entries == nil {
			entries = make(map[llm.ServiceClass]pricing.Entry)
		}
		entries[class] = entry
	}
	return entries
}
