package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
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
		// Control queries use the same trusted-Temporal allowlist as
		// generation and a cursor key derived from the primary continuation
		// signing key, so production exposes them without extra secrets.
		if next.options.QueryServiceBuilder == nil {
			next.options.QueryServiceBuilder = trustedTemporalQueryBuilder(&next, value)
		}
		// budget_status reads the active Redis budget generation through the
		// separately provisioned Function. The reader checks for the Function
		// and a published generation per query and stays typed unsupported
		// without them.
		if next.options.BudgetStatusReaderFactory == nil {
			next.options.BudgetStatusReaderFactory = cliBudgetStatusReaderFactory()
		}
		return next.Build(ctx, snapshot)
	}), nil
}

// queryCursorKeyDomain separates the query-cursor HMAC key from every other
// use of the continuation signing key.
const queryCursorKeyDomain = "llmtw:query-cursor:v1"

// trustedTemporalQueryBuilder exposes the persisted control queries under the
// trusted-Temporal policy: a caller may query exactly the tenant/project pairs
// it may generate for. Cursors are signed with a key derived from the primary
// continuation handle key and the configuration snapshot digest. Query families without a configured reader stay
// explicitly unsupported.
func trustedTemporalQueryBuilder(factory *ProductionEngineFactory, value config.Config) QueryServiceBuilder {
	allowed := make(map[config.AuthorizedScope]struct{})
	if value.Authorization != nil {
		for _, scope := range value.Authorization.AllowedScopes {
			allowed[scope] = struct{}{}
		}
	}
	return func(ctx context.Context, snapshot *config.Snapshot, repositories QueryRepositories) (activity.QueryService, error) {
		keys, err := factory.continuationKeys(ctx, snapshot.Config())
		if err != nil {
			return nil, err
		}
		var primary []byte
		for _, key := range keys {
			if key.Primary {
				primary = key.Secret
			}
		}
		if len(primary) == 0 {
			return nil, fmt.Errorf("%w: query cursors need a primary continuation handle key", ErrDurableV1Composition)
		}
		mac := hmac.New(sha256.New, primary)
		mac.Write([]byte(queryCursorKeyDomain))
		// Bind cursors to this snapshot so a reload fails closed instead of
		// resuming a page position computed under another configuration.
		digest := snapshot.Digest()
		mac.Write(digest[:])
		builder, err := NewPersistedQueryServiceBuilder(PersistedQueryBuilderOptions{
			Authorize: func(ctx context.Context, request control.Authorization) error {
				if ctx == nil {
					return control.ErrQueryAuthorization
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if _, ok := allowed[config.AuthorizedScope{Tenant: request.Tenant, Project: request.Project}]; !ok {
					return control.ErrQueryAuthorization
				}
				return nil
			},
			Cursor: &control.CursorCodec{Key: mac.Sum(nil)},
			Clock:  factory.options.Clock,
		})
		if err != nil {
			return nil, err
		}
		return builder(ctx, snapshot, repositories)
	}
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
		// A lineage of the configured depth holds depth+1 rows. Deriving the row
		// bound keeps a continuation_depth above the 512-row default effective.
		Limits: state.MaterializeLimits{MaxDepth: int32(value.Limits.ContinuationDepth), MaxRows: value.Limits.ContinuationDepth + 1},
	}, nil
}
