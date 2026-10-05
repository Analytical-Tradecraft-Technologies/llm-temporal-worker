package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// Gate both initial compilation and reload before resolving any secret. The
// engine factory repeats the check for callers that supply a compiled snapshot.
func newCLIReferenceResolver(resolver secrets.Resolver) config.ReferenceResolver {
	return config.ReferenceResolverFunc(func(ctx context.Context, value *config.Config) error {
		if value == nil {
			return fmt.Errorf("%w: configuration is required", ErrDurableV1Composition)
		}
		if !isCLIReadinessFixture(*value) {
			if _, err := trustedTemporalCloudOptions(*value); err != nil {
				return err
			}
		}
		return (secrets.ConfigResolver{Resolver: resolver}).Resolve(ctx, value)
	})
}

func isCLIReadinessFixture(value config.Config) bool {
	return !config.IsProductionEnvironment(value.Environment) && value.State.Kind != config.StateKindDurable && value.Authorization == nil
}

// newCLIEngineFactory binds policy and clients to the same immutable snapshot.
// Validate policy before building clients, and build a new resolver on every
// reload. Copy the factory rather than mutating one shared by active snapshots.
func newCLIEngineFactory(options ProductionFactoryOptions) (EngineFactory, error) {
	factory, err := NewProductionEngineFactory(options)
	if err != nil {
		return nil, err
	}
	return EngineFactoryFunc(func(ctx context.Context, snapshot *config.Snapshot) (llm.Engine, app.ClientSet, error) {
		if ctx == nil || snapshot == nil {
			return nil, nil, fmt.Errorf("%w: context and snapshot are required", ErrDurableV1Composition)
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		value := snapshot.Config()
		// Retain the existing development-only readiness fixture. It has no
		// cloud runtime and registers no generation or compaction activities.
		if isCLIReadinessFixture(value) {
			return factory.Build(ctx, snapshot)
		}
		cloudOptions, err := trustedTemporalCloudOptions(value)
		if err != nil {
			return nil, nil, err
		}
		builder, err := NewCloudV1RuntimeBuilder(cloudOptions)
		if err != nil {
			return nil, nil, err
		}
		next := *factory
		next.options.V1RuntimeBuilder = builder
		next.automaticV1RuntimeBuilder = false
		return next.Build(ctx, snapshot)
	}), nil
}

func trustedTemporalCloudOptions(value config.Config) (CloudV1RuntimeOptions, error) {
	if value.State.Kind != config.StateKindDurable || value.Authorization == nil {
		return CloudV1RuntimeOptions{}, fmt.Errorf("%w: CLI requires durable storage and explicit authorization policy", ErrDurableV1Composition)
	}
	if err := value.Authorization.Validate(); err != nil {
		return CloudV1RuntimeOptions{}, fmt.Errorf("%w: %w", ErrDurableV1Composition, err)
	}
	if value.Temporal.Namespace == "" || value.Environment == "" || value.Limits.ContinuationDepth <= 0 || value.Limits.ContinuationDepth > math.MaxInt32 {
		return CloudV1RuntimeOptions{}, fmt.Errorf("%w: invalid scope namespace or continuation depth", ErrDurableV1Composition)
	}
	allowed := make(map[config.AuthorizedScope]string, len(value.Authorization.AllowedScopes))
	for _, scope := range value.Authorization.AllowedScopes {
		// JSON tuple encoding prevents delimiter collisions. Identity excludes
		// actor, tags, policy order and config digest so unchanged scopes retain
		// their handles and cache across reloads. This hash is not a credential.
		data, err := json.Marshal([5]string{"llmtw:trusted-temporal-scope:v1", value.Environment, value.Temporal.Namespace, scope.Tenant, scope.Project})
		if err != nil {
			return CloudV1RuntimeOptions{}, fmt.Errorf("%w: encode caller scope", ErrDurableV1Composition)
		}
		digest := sha256.Sum256(data)
		allowed[scope] = "llmtw_scope_v1_" + hex.EncodeToString(digest[:])
	}
	return CloudV1RuntimeOptions{
		ResolveScope: func(ctx context.Context, caller llm.RequestContext) (string, error) {
			if ctx == nil {
				return "", executionError(provider.CodePermissionDenied)
			}
			if err := ctx.Err(); err != nil {
				return "", err
			}
			scope, ok := allowed[config.AuthorizedScope{Tenant: caller.Tenant, Project: caller.Project}]
			if !ok {
				return "", executionError(provider.CodePermissionDenied)
			}
			return scope, nil
		},
		CheckpointTTL: time.Duration(value.State.ContinuationRetention),
		Limits:        state.MaterializeLimits{MaxDepth: int32(value.Limits.ContinuationDepth)},
	}, nil
}
