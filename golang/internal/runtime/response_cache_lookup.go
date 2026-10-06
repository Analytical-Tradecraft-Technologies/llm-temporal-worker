package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// Cache planners authorize the opaque scope and complete route, compute the
// semantic fingerprint, and persist a stable fill lease before returning it.
// Replay must resolve earlier paid work first. Every lease field must remain
// identical on an uncertain acquisition retry; Attempt is the budget generation.
type GenerateCachePlanner func(context.Context, llm.GenerateRequestV1, PreparedGenerateInput) (cache.FillLease, error)
type CompactCachePlanner func(context.Context, llm.CompactRequestV1, PreparedCompactInput) (cache.FillLease, error)

// ResponseCacheLookup supplies the two CacheLookup phase ports from one
// snapshot. It owns no invocation state, provider clients or budget authority.
type ResponseCacheLookup struct {
	cache    *durable.ResponseCache
	generate GenerateCachePlanner
	compact  CompactCachePlanner
}

// NewResponseCacheLookup binds the configured repositories and clock after the
// complete runtime builder has validated its composition. It creates no clients
// and does not invoke the composition factory again.
func (capabilities V1RuntimeCapabilities) NewResponseCacheLookup(generate GenerateCachePlanner, compact CompactCachePlanner) (*ResponseCacheLookup, error) {
	composition, ok := capabilities.DurableComposition()
	if !ok {
		return nil, errors.New("response cache lookup requires a bound durable composition")
	}
	if err := capabilities.validateDurableComposition(composition); err != nil {
		return nil, err
	}
	if generate == nil || compact == nil {
		return nil, errors.New("Generate and Compact cache planners are required")
	}
	responseCache, err := durable.NewResponseCache(capabilities.Responses, capabilities.ResponseFills, capabilities.Clock)
	if err != nil {
		return nil, err
	}
	return &ResponseCacheLookup{cache: responseCache, generate: generate, compact: compact}, nil
}

// Generate preserves the existing v1 policy: omission disables cache access,
// while an enabled policy binds both successful-completion age and sample index.
func (lookup *ResponseCacheLookup) Generate(ctx context.Context, request llm.GenerateRequestV1, replay durable.GenerateReplay) (durable.CacheDecision, error) {
	_, requestErr := request.MarshalJSON()
	if err := lookup.validate(ctx, requestErr, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return durable.CacheDecision{}, err
	}
	if request.Cache == nil {
		return durable.CacheDecision{Disposition: durable.CacheDisabled}, nil
	}
	prepared, err := PrepareGenerateInput(ctx, request, replay)
	if err != nil {
		return durable.CacheDecision{}, err
	}
	lease, err := lookup.generate(ctx, request, prepared)
	if err := validateCacheLookupPlan(ctx, lease, cache.OperationGenerate, int64(request.Cache.Variant), err); err != nil {
		return durable.CacheDecision{}, err
	}
	maxAge := cacheMaximumAge(request.Cache)
	decision, err := lookup.cache.PrepareGenerate(ctx, lease, maxAge)
	return decision, responseCacheLookupError(err)
}

// Compact uses a distinct cache domain and independent sample index. Its planner fingerprints
// the source content and policy versions rather than the caller's operation key.
func (lookup *ResponseCacheLookup) Compact(ctx context.Context, request llm.CompactRequestV1, replay durable.CompactReplay) (durable.CompactCacheDecision, error) {
	_, requestErr := request.MarshalJSON()
	if err := lookup.validate(ctx, requestErr, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return durable.CompactCacheDecision{}, err
	}
	if request.Cache == nil {
		return durable.CompactCacheDecision{Disposition: durable.CacheDisabled}, nil
	}
	prepared, err := PrepareCompactInput(ctx, request, replay)
	if err != nil {
		return durable.CompactCacheDecision{}, err
	}
	if prepared.Request == nil {
		// No artifact can be produced for an empty/safely retained prefix.
		// Do not acquire a fill lease which could later reach paid admission.
		return durable.CompactCacheDecision{}, preparationError(provider.CodeInvalidArgument)
	}
	lease, err := lookup.compact(ctx, request, prepared)
	if err := validateCacheLookupPlan(ctx, lease, cache.OperationCompact, request.Cache.SampleIndex(), err); err != nil {
		return durable.CompactCacheDecision{}, err
	}
	maxAge := cacheMaximumAge(request.Cache)
	decision, err := lookup.cache.PrepareCompact(ctx, lease, maxAge)
	return decision, responseCacheLookupError(err)
}

func (lookup *ResponseCacheLookup) validate(ctx context.Context, requestErr error, alreadyResolved bool) error {
	if ctx == nil || lookup == nil || lookup.cache == nil || lookup.generate == nil || lookup.compact == nil || alreadyResolved {
		return cacheLookupError(provider.CodeConfiguration, provider.RetryNever)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if requestErr != nil {
		return cacheLookupError(provider.CodeInvalidArgument, provider.RetryNever)
	}
	return nil
}

func validateCacheLookupPlan(ctx context.Context, lease cache.FillLease, kind cache.OperationKind, index int64, planErr error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if planErr != nil || lease.Key.Operation != kind || lease.Key.RequestIndex != index ||
		durable.OperationID(lease.OperationID).Validate() != nil || durable.GenerationID(lease.Attempt).Validate() != nil {
		return cacheLookupError(provider.CodeConfiguration, provider.RetryNever)
	}
	return nil
}

func responseCacheLookupError(err error) error {
	if err == nil {
		return nil
	}
	code, retry := provider.CodeStateUnavailable, provider.RetrySameOperation
	if errors.Is(err, durable.ErrResponseCacheInvalid) {
		code, retry = provider.CodeConfiguration, provider.RetryNever
	} else if errors.Is(err, durable.ErrResponseCacheCorrupt) {
		code, retry = provider.CodeStateCorrupt, provider.RetryNever
	}
	// A failed lookup/acquisition is never a miss. An unknown acquisition can
	// be retried only with the original lease; the fill start gate still applies.
	return cacheLookupError(code, retry)
}

func cacheLookupError(code provider.Code, retry provider.RetryDisposition) error {
	return provider.NewError(code, provider.PhaseStateLoad, provider.DispatchNotDispatched, retry, "response cache lookup failed")
}

func cacheMaximumAge(policy *llm.CachePolicyV1) *time.Duration {
	if policy == nil || policy.MaxAgeSeconds == 0 {
		return nil
	}
	age := time.Duration(policy.MaxAgeSeconds) * time.Second
	return &age
}
