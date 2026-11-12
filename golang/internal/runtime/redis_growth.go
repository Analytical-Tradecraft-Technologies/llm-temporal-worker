package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

// RedisGrowthMaintainer runs the snapshot's low-frequency Redis growth work
// on the readiness monitor's cadence (#1543): it samples durable operation
// records and the budget Stream for metrics, reports this snapshot's provider
// configuration digest as loaded, and drops provider state for digests no
// worker has reported for redisstore.ProviderStateRetireAfter.
type RedisGrowthMaintainer struct {
	sampler      *redisstore.BudgetGrowthSampler
	providers    *redisstore.ProviderStateStore
	configDigest [32]byte
	retireAfter  time.Duration
}

type redisGrowthSource interface {
	RedisGrowth() *RedisGrowthMaintainer
}

// Run performs one pass and publishes its results to metrics. Each step runs
// even when an earlier one failed; the first error is returned.
func (maintainer *RedisGrowthMaintainer) Run(ctx context.Context, metrics *observability.Metrics) error {
	if maintainer == nil {
		return nil
	}
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	if maintainer.sampler != nil {
		growth, err := maintainer.sampler.Sample(ctx)
		if err == nil {
			metrics.SetBudgetGrowth(growth.OperationRecords, growth.PersistentOperationRecords, growth.StreamLength)
		}
		keep(err)
	}
	if maintainer.providers != nil {
		keep(maintainer.providers.MarkConfigLoaded(ctx, maintainer.configDigest))
		started := time.Now()
		dropped, err := maintainer.providers.DropRetiredConfigs(ctx, maintainer.configDigest, maintainer.retireAfter)
		if err != nil {
			metrics.RecordMaintenanceFailure("status")
		} else if dropped > 0 {
			metrics.RecordMaintenance("status", "deleted", dropped, time.Since(started))
		}
		keep(err)
	}
	return first
}

func (set *productionClientSet) RedisGrowth() *RedisGrowthMaintainer {
	if set == nil {
		return nil
	}
	return set.redisGrowth
}

func (clients *snapshotClients) RedisGrowth() *RedisGrowthMaintainer {
	if clients == nil {
		return nil
	}
	return clients.redisGrowth
}

// syncRedisGrowth runs the active snapshot's growth pass under a lease, so a
// reload cannot close its clients mid-pass.
func (runtime *Runtime) syncRedisGrowth(ctx context.Context) error {
	if runtime == nil || runtime.App == nil {
		return nil
	}
	lease, err := runtime.App.Acquire()
	if err != nil {
		return nil
	}
	defer lease.Release()
	source, ok := lease.Snapshot().Clients.(redisGrowthSource)
	if !ok || source.RedisGrowth() == nil {
		return nil
	}
	timeout := runtime.readinessProbeTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	passCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = source.RedisGrowth().Run(passCtx, runtime.Metrics)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
