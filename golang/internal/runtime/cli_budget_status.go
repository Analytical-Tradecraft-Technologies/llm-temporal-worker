package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

// budgetFunctionLister is the read-only Redis capability the CLI uses to
// detect whether the separately provisioned budget status Function exists.
// The worker never loads or replaces Redis code itself.
type budgetFunctionLister interface {
	FunctionList(context.Context, redisclient.FunctionListQuery) *redisclient.FunctionListCmd
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
		lister, ok := options.Client.(budgetFunctionLister)
		if !ok {
			return nil, nil
		}
		return newCLIBudgetStatusReader(lister, options)
	}
}

func newCLIBudgetStatusReader(lister budgetFunctionLister, options redisstore.BudgetStatusReaderOptions) (BudgetStatusReader, error) {
	if lister == nil || options.Generation == nil {
		return nil, nil
	}
	reader, err := redisstore.NewRedisBudgetStatusReader(options)
	if err != nil {
		return nil, err
	}
	return &provisionedBudgetStatusReader{functions: lister, generation: options.Generation, reader: reader}, nil
}

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
type provisionedBudgetStatusReader struct {
	functions  budgetFunctionLister
	generation redisstore.BudgetGenerationPort
	reader     BudgetStatusReader
}

func (reader *provisionedBudgetStatusReader) ReadBudgetStatus(ctx context.Context, query control.BudgetStatusQuery, activeAt time.Time) (control.BudgetStatusResult, error) {
	libraries, err := reader.functions.FunctionList(ctx, redisclient.FunctionListQuery{LibraryNamePattern: redisstore.BudgetStatusFunctionLibrary, WithCode: true}).Result()
	if err != nil {
		return control.BudgetStatusResult{}, fmt.Errorf("%w: list budget status Function: %v", redisstore.ErrBudgetStatusUnavailable, err)
	}
	if !budgetStatusFunctionProvisioned(libraries) {
		return control.BudgetStatusResult{}, unsupportedQuery(llm.QueryBudgetStatus, "Redis budget status Function "+redisstore.BudgetStatusFunctionLibrary+" is not provisioned")
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
