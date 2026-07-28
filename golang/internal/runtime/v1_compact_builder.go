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

var ErrCompactV1Composition = errors.New("v1 Compact composition is unavailable")

// NewCompactV1RuntimeBuilder validates and constructs the Compact phase from
// one immutable snapshot. It returns a Compact-only runtime deliberately;
// Generate and Query are never inferred from the legacy engine.
func NewCompactV1RuntimeBuilder() V1RuntimeBuilder {
	return func(ctx context.Context, snapshot *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
		if ctx == nil {
			return nil, fmt.Errorf("%w: context is nil", ErrCompactV1Composition)
		}
		if snapshot == nil {
			return nil, fmt.Errorf("%w: configuration snapshot is nil", ErrCompactV1Composition)
		}
		if isNilCapability(clients) {
			return nil, fmt.Errorf("%w: snapshot client set is nil", ErrCompactV1Composition)
		}
		source, ok := clients.(V1RuntimeCapabilitiesSource)
		if !ok || isNilCapability(source) {
			return nil, fmt.Errorf("%w: snapshot client set does not expose V1RuntimeCapabilitiesSource", ErrCompactV1Composition)
		}
		capabilities := source.V1RuntimeCapabilities()
		if err := capabilities.ValidateCompact(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrCompactV1Composition, err)
		}
		ports, err := capabilities.CompactPortsFactory(ctx, capabilities)
		if err != nil {
			return nil, fmt.Errorf("%w: construct Compact ports: %v", ErrCompactV1Composition, err)
		}
		runtime, err := activity.NewCompactOnlyV1Runtime(ports)
		if err != nil {
			return nil, fmt.Errorf("%w: validate Compact ports: %v", ErrCompactV1Composition, err)
		}
		return runtime, nil
	}
}
