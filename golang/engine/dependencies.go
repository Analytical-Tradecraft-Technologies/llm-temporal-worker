package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// Snapshot is the immutable, non-secret view consumed by one Generate call.
// A caller captures one value before planning; reloads never change an
// in-flight request's route, capability, or price decision.
type Snapshot struct {
	Version                  string
	ConfigDigest             [32]byte
	ConfigEpoch              string
	Routes                   routing.Catalog
	EndpointCapabilities     map[string]provider.CapabilitySet
	Health                   routing.HealthView
	Prices                   pricing.Resolver
	BudgetPolicies           []budget.Policy
	RequireBudgetMatch       bool
	RequirePriceWhenBudgeted bool
	Environment              string
	ReservationLease         time.Duration
	OperationRetention       time.Duration
	ContinuationRetention    time.Duration
}

// SnapshotSource publishes complete immutable snapshots.
type SnapshotSource interface {
	Current(context.Context) (Snapshot, error)
}

// StaticSnapshot is useful for tests, embedded library callers, and a process
// that has already built its immutable configuration snapshot.
type StaticSnapshot struct{ Value Snapshot }

func (source StaticSnapshot) Current(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return source.Value, nil
}

// AdapterRegistry resolves a planned route to exactly one provider adapter.
// The registry owns clients; the engine only invokes the provider-neutral port.
type AdapterRegistry interface {
	Adapter(context.Context, routing.Candidate) (provider.Adapter, error)
}

// AdapterMap is a deterministic endpoint-ID registry for simple deployments.
type AdapterMap map[string]provider.Adapter

func (adapters AdapterMap) Adapter(ctx context.Context, candidate routing.Candidate) (provider.Adapter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	adapter := adapters[candidate.EndpointID]
	if adapter == nil {
		return nil, fmt.Errorf("no adapter configured for endpoint %q", candidate.EndpointID)
	}
	return adapter, nil
}

// ResultStore durably stores the normalized response before the admission
// ledger is completed. Implementations must be immutable/idempotent by
// operation ID and must never log response content.
type ResultStore interface {
	Get(context.Context, string) (llm.Response, error)
	Put(context.Context, string, llm.Response) (state.BlobRef, error)
}

// ErrResultNotFound is the optional sentinel result stores should return when
// no response has been committed for an operation.
var ErrResultNotFound = errors.New("result not found")

// RootContinuationStore is the additional operation needed to create the
// first secure continuation handle. Child writes use state.ContinuationStore.
type RootContinuationStore interface {
	CreateRoot(context.Context, state.Continuation) (state.Handle, error)
}

// Heartbeat receives redacted engine progress. It is intentionally tiny so a
// Temporal adapter can impose its own jitter and rate limit.
type Heartbeat interface {
	Beat(context.Context, Progress) error
}

// ProviderStatusRecorder receives bounded, snapshot-scoped provider
// observations. Implementations may persist the observation durably, but a
// recorder outage must never change the provider result returned to a caller.
// Implementations must not retain prompts, outputs, credentials, or raw
// provider response bodies.
type ProviderStatusRecorder interface {
	RecordProviderStatus(context.Context, control.StatusObservation) error
}

type Progress struct {
	OperationID string
	Phase       string
	RouteIndex  int
	ClassIndex  int
	OutputItems int
	At          time.Time
}

// Dependencies are injected so engine tests do not need Temporal, provider
// credentials, Redis, or wall-clock sleeps.
type Dependencies struct {
	Snapshots           SnapshotSource
	Planner             routing.Planner
	Adapters            AdapterRegistry
	Admission           admission.AdmissionStore
	Continuations       state.ContinuationStore
	Results             ResultStore
	Clock               func() time.Time
	Estimator           budget.Estimator
	MaxAttempts         int
	FinalizationTimeout time.Duration
	Heartbeat           Heartbeat
	ProviderControl     ProviderStatusRecorder
}

func (dependencies Dependencies) validate() error {
	if dependencies.Snapshots == nil {
		return fmt.Errorf("engine snapshots are required")
	}
	if dependencies.Planner == nil {
		return fmt.Errorf("engine planner is required")
	}
	if dependencies.Adapters == nil {
		return fmt.Errorf("engine adapters are required")
	}
	if dependencies.Admission == nil {
		return fmt.Errorf("engine admission store is required")
	}
	if dependencies.Results == nil {
		return fmt.Errorf("engine result store is required")
	}
	if dependencies.Clock == nil {
		return fmt.Errorf("engine clock is required")
	}
	return nil
}

// Engine composes normalization, immutable snapshot capture, routing,
// pricing/admission, provider compilation/dispatch, continuation persistence,
// and result finalization.
type Engine struct{ dependencies Dependencies }

func New(dependencies Dependencies) (*Engine, error) {
	if dependencies.Clock == nil {
		dependencies.Clock = time.Now
	}
	if dependencies.MaxAttempts <= 0 {
		dependencies.MaxAttempts = 8
	}
	if dependencies.FinalizationTimeout <= 0 {
		dependencies.FinalizationTimeout = 5 * time.Second
	}
	if err := dependencies.validate(); err != nil {
		return nil, err
	}
	return &Engine{dependencies: dependencies}, nil
}

var _ llm.Engine = (*Engine)(nil)
var _ llm.StreamingEngine = (*Engine)(nil)
