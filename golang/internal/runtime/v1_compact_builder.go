package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

var ErrCompactV1Composition = errors.New("v1 Compact composition is unavailable")

// NewCompactV1RuntimeBuilder validates and constructs the Compact phase from
// one immutable snapshot. It returns a Compact-only runtime deliberately;
// Generate and Query are never inferred from the legacy engine.
func NewCompactV1RuntimeBuilder() V1RuntimeBuilder {
	return func(ctx context.Context, snapshot *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
		capabilities, err := snapshotV1RuntimeCapabilities(ctx, snapshot, clients, ErrCompactV1Composition)
		if err != nil {
			return nil, err
		}
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
