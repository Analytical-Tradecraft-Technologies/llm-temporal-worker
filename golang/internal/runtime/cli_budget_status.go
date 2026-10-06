package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

// budgetFunctionLister is the read-only Redis capability the CLI uses to
// detect whether the separately provisioned budget status Function exists.
// The worker never loads or replaces Redis code itself.
type budgetFunctionLister interface {
	FunctionList(context.Context, redisclient.FunctionListQuery) *redisclient.FunctionListCmd
}

// budgetFunctionNodes calls fn once for every Redis node that could serve the
// budget FCALL. FUNCTION LIST is keyless, so a cluster client routes it to an
// arbitrary master while FCALL goes to the master that owns the budget key's
// slot. Checking every master (or every ring shard) means a passing check holds
// for whichever node owns the slot, including after a resharding moves it.
type budgetFunctionNodes func(context.Context, func(context.Context, budgetFunctionLister) error) error

func singleBudgetFunctionNode(lister budgetFunctionLister) budgetFunctionNodes {
	return func(ctx context.Context, fn func(context.Context, budgetFunctionLister) error) error {
		return fn(ctx, lister)
	}
}

// budgetFunctionNodesFor selects the node fan-out for a snapshot's Redis
// client. A nil result means the client cannot list Functions.
func budgetFunctionNodesFor(client redisclient.Scripter) budgetFunctionNodes {
	switch value := client.(type) {
	case *redisclient.ClusterClient:
		return func(ctx context.Context, fn func(context.Context, budgetFunctionLister) error) error {
			return value.ForEachMaster(ctx, func(ctx context.Context, node *redisclient.Client) error { return fn(ctx, node) })
		}
	case *redisclient.Ring:
		return func(ctx context.Context, fn func(context.Context, budgetFunctionLister) error) error {
			return value.ForEachShard(ctx, func(ctx context.Context, node *redisclient.Client) error { return fn(ctx, node) })
		}
	case budgetFunctionLister:
		return singleBudgetFunctionNode(value)
	default:
		return nil
	}
}

// cliBudgetStatusReaderFactory is the production CLI's budget_status
// composition. It binds the versioned Redis reader only in Function admission
// mode, because nothing provisions the equivalent Lua script by SHA; any other
// mode, or a client that cannot list Functions, leaves budget_status typed
// unsupported.
func cliBudgetStatusReaderFactory() BudgetStatusReaderFactory {
	return func(_ context.Context, _ *config.Snapshot, options redisstore.BudgetStatusReaderOptions) (BudgetStatusReader, error) {
		if options.Mode != redisstore.AdmissionModeFunction {
			return nil, nil
		}
		nodes := budgetFunctionNodesFor(options.Client)
		if nodes == nil {
			return nil, nil
		}
		return newCLIBudgetStatusReader(nodes, options)
	}
}

func newCLIBudgetStatusReader(nodes budgetFunctionNodes, options redisstore.BudgetStatusReaderOptions) (BudgetStatusReader, error) {
	if nodes == nil || options.Generation == nil {
		return nil, nil
	}
	reader, err := redisstore.NewRedisBudgetStatusReader(options)
	if err != nil {
		return nil, err
	}
	return &provisionedBudgetStatusReader{nodes: nodes, generation: options.Generation, reader: reader}, nil
}

var errBudgetFunctionAbsent = errors.New("budget status Function is not provisioned on a Redis node")

// provisionedBudgetStatusReader checks, on every query, that the exact
// budget status Function library is loaded and that a budget generation has
// been published, before delegating to the coherent Redis read. Checking per
// query rather than at snapshot build lets an operator load the Function
// without a configuration reload, and a removed library fails closed.
//
// An absent or different library and an unpublished generation are
// deployment states, not transient outages, so they return the typed
// unsupported-query error rather than a retryable unavailable error. A Redis
// failure while checking stays retryable. No value is ever synthesized.
//
// In Redis Cluster every master must hold the exact library; one that lacks
// it makes the whole check fail closed, as does a topology with no masters.
type provisionedBudgetStatusReader struct {
	nodes      budgetFunctionNodes
	generation redisstore.BudgetGenerationPort
	reader     BudgetStatusReader
}

func (reader *provisionedBudgetStatusReader) ReadBudgetStatus(ctx context.Context, query control.BudgetStatusQuery, activeAt time.Time) (control.BudgetStatusResult, error) {
	// Cluster fan-out calls fn concurrently, so count checked nodes atomically.
	var checked atomic.Int64
	err := reader.nodes(ctx, func(ctx context.Context, node budgetFunctionLister) error {
		libraries, err := node.FunctionList(ctx, redisclient.FunctionListQuery{LibraryNamePattern: redisstore.BudgetStatusFunctionLibrary, WithCode: true}).Result()
		if err != nil {
			return fmt.Errorf("%w: list budget status Function: %v", redisstore.ErrBudgetStatusUnavailable, err)
		}
		if !budgetStatusFunctionProvisioned(libraries) {
			return errBudgetFunctionAbsent
		}
		checked.Add(1)
		return nil
	})
	if errors.Is(err, errBudgetFunctionAbsent) {
		return control.BudgetStatusResult{}, unsupportedQuery(llm.QueryBudgetStatus, "Redis budget status Function "+redisstore.BudgetStatusFunctionLibrary+" is not provisioned on every Redis master")
	}
	if err != nil {
		if !errors.Is(err, redisstore.ErrBudgetStatusUnavailable) {
			err = fmt.Errorf("%w: list budget status Function: %v", redisstore.ErrBudgetStatusUnavailable, err)
		}
		return control.BudgetStatusResult{}, err
	}
	if checked.Load() == 0 {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: no Redis node was checked for the budget status Function", redisstore.ErrBudgetStatusUnavailable)
	}
	if _, err := reader.generation.ActiveGeneration(ctx); err != nil {
		if errors.Is(err, redisstore.ErrBudgetActiveGenerationMissing) {
			return control.BudgetStatusResult{}, unsupportedQuery(llm.QueryBudgetStatus, "no Redis budget generation is published")
		}
		return control.BudgetStatusResult{}, fmt.Errorf("%w: active generation: %v", redisstore.ErrBudgetStatusUnavailable, err)
	}
	return reader.reader.ReadBudgetStatus(ctx, query, activeAt)
}

// budgetStatusFunctionProvisioned requires the exact library code this
// worker was built against, so a stale or foreign library with the same name
// is treated as absent rather than trusted.
func budgetStatusFunctionProvisioned(libraries []redisclient.Library) bool {
	for _, library := range libraries {
		if library.Name != redisstore.BudgetStatusFunctionLibrary {
			continue
		}
		digest := sha256.Sum256([]byte(library.Code))
		if hex.EncodeToString(digest[:]) != redisstore.BudgetStatusFunctionDigest() {
			return false
		}
		for _, function := range library.Functions {
			if function.Name == redisstore.BudgetStatusFunctionVersion {
				return true
			}
		}
		return false
	}
	return false
}
