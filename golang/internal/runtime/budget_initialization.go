package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

var ErrBudgetInitializationRequired = errors.New("budget initialization is missing, incomplete or mismatched; run budget-initialize to inspect it")

type budgetAuthorityInitializer interface {
	Identity() budget.InitializationIdentity
	Check(context.Context, budget.Initialization) error
	Initialize(context.Context, budget.InitializationPreparation) error
}

func readBudgetInitialization(ctx context.Context, store budget.InitializationStore, identity budget.InitializationIdentity) (budget.Initialization, error) {
	value, err := store.ReadBudgetInitialization(ctx, identity.Namespace)
	if err != nil {
		return budget.Initialization{}, err
	}
	if value.Validate() != nil || value.Identity != identity {
		return budget.Initialization{}, ErrBudgetInitializationRequired
	}
	return value, nil
}

func withBudgetAuthorityProbe(base DependencyProbe, initializer budgetAuthorityInitializer, receipt budget.Initialization) DependencyProbe {
	return identifyDependencyProbe(DependencyRedis, DependencyProbeFunc(func(ctx context.Context) ProbeResult {
		result := base.Probe(ctx)
		if result.Status != ProbeStatusReady {
			return result
		}
		if err := initializer.Check(ctx, receipt); err != nil {
			if errors.Is(err, redisstore.ErrBudgetAuthorityUnavailable) {
				return redisPolicyMismatch()
			}
			return redisProbeFailure(ctx, err)
		}
		return result
	}))
}

type budgetInitializationReport struct {
	Status  string `json:"status"`
	Applied bool   `json:"applied"`
}

// No path resets a receipt, a marker, or a balance. A lost cloud/Redis reply is
// resolved by another invocation, which has no permission to recreate a marker.
func initializeBudgetAuthority(ctx context.Context, store budget.InitializationStore, initializer budgetAuthorityInitializer, apply bool, now time.Time) (budgetInitializationReport, error) {
	value, err := readBudgetInitialization(ctx, store, initializer.Identity())
	fresh := errors.Is(err, contracts.ErrNotFound)
	if err != nil && !fresh {
		return budgetInitializationReport{}, err
	}
	if !fresh && value.Ready {
		if err := initializer.Check(ctx, value); err != nil {
			return budgetInitializationReport{}, err
		}
		return budgetInitializationReport{Status: "ready"}, nil
	}
	if !apply {
		status := "incomplete"
		if fresh {
			status = "initialization_required"
		}
		return budgetInitializationReport{Status: status}, nil
	}
	preparation := budget.InitializationPreparation{Receipt: value}
	if fresh {
		preparation, err = store.PrepareBudgetInitialization(ctx, initializer.Identity(), now)
		if err != nil {
			return budgetInitializationReport{}, err
		}
	}
	if err := initializer.Initialize(ctx, preparation); err != nil {
		return budgetInitializationReport{}, err
	}
	if err := store.CompleteBudgetInitialization(ctx, preparation.Receipt); err != nil {
		return budgetInitializationReport{}, err
	}
	completed, err := readBudgetInitialization(ctx, store, initializer.Identity())
	if err != nil {
		return budgetInitializationReport{}, err
	}
	if !completed.Ready || completed.Marker() != preparation.Receipt.Marker() {
		return budgetInitializationReport{}, ErrBudgetInitializationRequired
	}
	if err := initializer.Check(ctx, completed); err != nil {
		return budgetInitializationReport{}, err
	}
	return budgetInitializationReport{Status: "ready", Applied: true}, nil
}

// RunBudgetInitialize is the explicit installation command. Without --apply it
// performs reads only. It resolves only Redis/cloud storage references, using
// the worker's existing file/environment SecretRef and verified TLS support.
func RunBudgetInitialize(ctx context.Context, data []byte, apply bool, output io.Writer) error {
	snapshot, err := config.Compile(ctx, data, nil)
	if err != nil {
		return err
	}
	value := snapshot.Config()
	if value.State.Kind != config.StateKindDurable || value.State.Requests == nil {
		return errors.New("budget-initialize requires durable cloud storage")
	}
	if _, err := trustedTemporalCloudOptions(value); err != nil {
		return err
	}
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.New(secrets.Options{}), RedisFactory: defaultRedisFactory}}
	client, owned, err := factory.buildRedis(ctx, value)
	if err != nil {
		return errors.New("open Redis for budget initialization failed")
	}
	if owned {
		defer client.Close()
	}
	key, err := factory.redisKeySecret(ctx, value)
	if err != nil {
		return errors.New("resolve budget initialization references failed")
	}
	defer clear(key)
	keys, err := redisKeyOptions(value, key)
	if err != nil {
		return err
	}
	initializer, err := redisstore.NewRedisBudgetInitializer(client, keys, redisstore.AdmissionMode(value.State.Redis.AdmissionMode), value.State.Redis.AdmissionVersion)
	if err != nil {
		return err
	}
	// Check persistence, noeviction, time, and the pinned library before any
	// write. The Stream is created by initialization, so it cannot gate this
	// first-install preflight. Normal worker readiness still requires it.
	preflight := value.State.Redis
	disabled := false
	preflight.CoordinationStreamEnabled = &disabled
	probe, err := NewRedisDependencyProbe(client, preflight)
	if err != nil || probe.Probe(ctx).Status != ProbeStatusReady {
		return errors.New("Redis initialization preflight failed")
	}
	repository, err := factory.buildCloudRequests(ctx, value.State.Requests)
	if err != nil {
		return err
	}
	store, ok := repository.(budget.InitializationStore)
	if !ok || isNilCapability(store) {
		return ErrBudgetInitializationRequired
	}
	report, err := initializeBudgetAuthority(ctx, store, initializer, apply, time.Now().UTC())
	if err != nil {
		// Never expose SDK errors or connection details through the CLI.
		return errors.New("budget initialization could not establish authority; inspect the receipt and Redis before retrying, never clear them to reset budgets")
	}
	if report.Status == "ready" {
		// A receipt/marker alone does not prove an enabled Stream is healthy.
		// Run the same read-only Redis checks as a worker before reporting ready,
		// including on a replay where an operator may have removed the Stream.
		space, err := redisstore.NewBudgetKeySpace(keys)
		if err != nil {
			return err
		}
		probe, err := NewRedisDependencyProbeWithStream(client, value.State.Redis, space.EventsKey())
		if err != nil || probe.Probe(ctx).Status != ProbeStatusReady {
			return errors.New("Redis budget initialization completed but worker readiness checks failed")
		}
	}
	if err := json.NewEncoder(output).Encode(report); err != nil {
		return errors.New("write budget initialization result failed")
	}
	return nil
}
