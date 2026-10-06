package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// redisBudgetGeneration is stable across reloads, like the Redis materializer.
// Changing configuration must not replace the accounting authority's identity.
const redisBudgetGeneration durable.GenerationID = "redis-budget-v1"

// CloudV1RuntimeOptions contains deployment policy, never snapshot-owned keys
// or clients. ResolveScope must authorize the caller before returning the same
// opaque scope used to sign and materialize that caller's checkpoints.
type CloudV1RuntimeOptions struct {
	ResolveScope  CheckpointScopeResolver
	Limits        state.MaterializeLimits
	CheckpointTTL time.Duration
}

// NewCloudV1RuntimeBuilder composes bounded workflow activities from the exact
// cloud, Redis, provider and signing capabilities owned by each config snapshot.
// Query authorization remains independently configured by QueryServiceBuilder.
// No authorization is inferred from caller-supplied tenant/project fields.
func NewCloudV1RuntimeBuilder(options CloudV1RuntimeOptions) (V1RuntimeBuilder, error) {
	limits := options.Limits
	if options.ResolveScope == nil || options.CheckpointTTL <= 0 || limits.MaxDepth < 0 || limits.MaxRows < 0 || limits.MaxItems < 0 || limits.MaxBytes < 0 {
		return nil, fmt.Errorf("%w: cloud authorization, retention and limits are required", ErrDurableV1Composition)
	}
	return func(ctx context.Context, snapshot *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
		if ctx == nil || snapshot == nil || isNilCapability(clients) {
			return nil, fmt.Errorf("%w: cloud snapshot and clients are required", ErrDurableV1Composition)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		source, ok := clients.(V1RuntimeCapabilitiesSource)
		if !ok || isNilCapability(source) {
			return nil, fmt.Errorf("%w: cloud capabilities are unavailable", ErrDurableV1Composition)
		}
		capabilities := source.V1RuntimeCapabilities()
		value := snapshot.Config()
		identity, err := cloudRequestIdentity(value.State.Requests)
		if err != nil || value.State.Kind != config.StateKindDurable || value.State.Requests == nil || identity != capabilities.CloudIdentity || snapshot.Digest() == ([32]byte{}) || snapshot.Digest() != capabilities.ConfigDigest {
			return nil, fmt.Errorf("%w: cloud capabilities do not match the snapshot", ErrDurableV1Composition)
		}
		redisIdentity := durable.RedisIdentity{KeyPrefix: value.State.Redis.KeyPrefix, HashTag: value.State.Redis.AdmissionHashTag}
		if (durable.StateIdentity{Cloud: identity, Redis: redisIdentity, ConfigDigest: capabilities.ConfigDigest}).Validate() != nil || redisIdentity != capabilities.RedisIdentity {
			return nil, fmt.Errorf("%w: Redis capabilities do not match the snapshot", ErrDurableV1Composition)
		}
		execution, err := capabilities.NewCloudExecutionRuntime(ctx, CloudExecutionOptions{
			ResolveScope: options.ResolveScope, Limits: limits, CheckpointTTL: options.CheckpointTTL,
			Keyring: capabilities.CheckpointKeyring, BudgetGeneration: redisBudgetGeneration, MaxAttempts: value.Limits.RouteAttempts,
			RequestLimits: cloudRequestLimitsFromConfig(value.Limits), FinalizationTimeout: time.Duration(value.Server.FinalizationTimeout),
		})
		if err != nil {
			return nil, fmt.Errorf("%w: construct cloud execution: %w", ErrDurableV1Composition, err)
		}
		return &cloudV1Runtime{CloudExecutionRuntime: execution, query: queryServiceFromClients(clients)}, nil
	}, nil
}

// The compatibility methods fail closed. Only the bounded methods are used by
// the registered generation and compaction activities; no legacy engine runs.
type cloudV1Runtime struct {
	activity.UnconfiguredV1Runtime
	*CloudExecutionRuntime
	query activity.QueryService
}

var _ activity.V1Runtime = (*cloudV1Runtime)(nil)
var _ activity.ExecutionRuntime = (*cloudV1Runtime)(nil)
var _ activity.GenerationPlanningRuntime = (*cloudV1Runtime)(nil)

func (r *cloudV1Runtime) QueryV1(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if r == nil || isNilCapability(r.query) {
		return activity.UnconfiguredV1Runtime{}.QueryV1(ctx, request)
	}
	return r.query.Execute(ctx, request)
}
