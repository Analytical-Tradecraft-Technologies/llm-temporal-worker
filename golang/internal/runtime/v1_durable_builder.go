package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// ErrDurableV1Composition is returned when a snapshot cannot prove that both
// one-shot durable phases are backed by the same immutable capability bundle.
var ErrDurableV1Composition = errors.New("durable v1 composition is unavailable")

// NewDurableV1RuntimeBuilder returns the complete one-shot v1 composition.
// Both phase factories run against the same copied capability bundle, so a
// reload cannot mix Generate dependencies from one snapshot with Compact
// dependencies from another. The factories own all PostgreSQL, Redis,
// provider, checkpoint, and result-port wiring; this helper only validates
// their contracts and adapts the resulting ports to the Activity boundary.
// Query remains an independent authorization seam on Activities.QueryService.
//
// A missing capability, either phase factory, or phase port fails closed
// before the worker can poll Temporal. The legacy llm.Engine is never used as
// a fallback and no storage or provider work occurs in this constructor.
func NewDurableV1RuntimeBuilder() V1RuntimeBuilder {
	return func(ctx context.Context, snapshot *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
		if ctx == nil {
			return nil, fmt.Errorf("%w: context is nil", ErrDurableV1Composition)
		}
		if snapshot == nil {
			return nil, fmt.Errorf("%w: configuration snapshot is nil", ErrDurableV1Composition)
		}
		if isNilCapability(clients) {
			return nil, fmt.Errorf("%w: snapshot client set is nil", ErrDurableV1Composition)
		}
		source, ok := clients.(V1RuntimeCapabilitiesSource)
		if !ok || isNilCapability(source) {
			return nil, fmt.Errorf("%w: snapshot client set does not expose V1RuntimeCapabilitiesSource", ErrDurableV1Composition)
		}
		capabilities := source.V1RuntimeCapabilities()
		if err := capabilities.ValidateGenerate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDurableV1Composition, err)
		}
		if err := capabilities.ValidateCompact(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDurableV1Composition, err)
		}
		// Bind one validated composition to both phase factories. This keeps
		// PostgreSQL operation state, the write-only journal, and Redis active
		// budgets on the same snapshot identity across Generate and Compact.
		// A missing factory remains valid for contract-only tests and lets the
		// deployment-owned phase factories use their direct capabilities; when
		// configured, however, an invalid result fails before either phase
		// factory can construct ports.
		phaseCapabilities := capabilities
		if capabilities.CompositionFactory != nil {
			composition, err := capabilities.BuildDurableComposition(ctx)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrDurableV1Composition, err)
			}
			phaseCapabilities.composition = &composition
		}
		generate, err := phaseCapabilities.GeneratePortsFactory(ctx, phaseCapabilities)
		if err != nil {
			return nil, fmt.Errorf("%w: construct Generate ports: %v", ErrDurableV1Composition, err)
		}
		compact, err := phaseCapabilities.CompactPortsFactory(ctx, phaseCapabilities)
		if err != nil {
			return nil, fmt.Errorf("%w: construct Compact ports: %v", ErrDurableV1Composition, err)
		}
		runtime, err := activity.NewDurableV1Runtime(generate, compact, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: validate durable ports: %v", ErrDurableV1Composition, err)
		}
		return runtime, nil
	}
}
