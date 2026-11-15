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

var (
	// ErrGenerateV1Composition is returned when a snapshot cannot prove that
	// every Generate dependency is owned by the same immutable client set.
	ErrGenerateV1Composition = errors.New("v1 Generate composition is unavailable")
)

// NewGenerateV1RuntimeBuilder returns the bounded production composition for
// the one-shot Generate Activity. The builder requires the client set to
// expose V1RuntimeCapabilitiesSource and validates that source before asking
// its per-snapshot GeneratePortsFactory for ports. It never adapts the legacy
// llm.Engine, creates clients, or supplies Compact/Query implementations.
//
// The returned builder is safe to install in ProductionFactoryOptions. A
// missing factory or any incomplete capability causes Build to fail closed,
// and the factory then drains the rejected snapshot clients before returning.
func NewGenerateV1RuntimeBuilder() V1RuntimeBuilder {
	return func(ctx context.Context, snapshot *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
		capabilities, err := snapshotV1RuntimeCapabilities(ctx, snapshot, clients, ErrGenerateV1Composition)
		if err != nil {
			return nil, err
		}
		if err := capabilities.ValidateGenerate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrGenerateV1Composition, err)
		}
		ports, err := capabilities.GeneratePortsFactory(ctx, capabilities)
		if err != nil {
			return nil, fmt.Errorf("%w: construct Generate ports: %v", ErrGenerateV1Composition, err)
		}
		runtime, err := activity.NewGenerateOnlyV1Runtime(ports)
		if err != nil {
			return nil, fmt.Errorf("%w: validate Generate ports: %v", ErrGenerateV1Composition, err)
		}
		return runtime, nil
	}
}

// snapshotV1RuntimeCapabilities checks the inputs every V1RuntimeBuilder
// receives and returns the snapshot client set's capability bundle. Every
// error wraps composition, the calling builder's sentinel.
func snapshotV1RuntimeCapabilities(ctx context.Context, snapshot *config.Snapshot, clients app.ClientSet, composition error) (V1RuntimeCapabilities, error) {
	if ctx == nil {
		return V1RuntimeCapabilities{}, fmt.Errorf("%w: context is nil", composition)
	}
	if snapshot == nil {
		return V1RuntimeCapabilities{}, fmt.Errorf("%w: configuration snapshot is nil", composition)
	}
	if isNilCapability(clients) {
		return V1RuntimeCapabilities{}, fmt.Errorf("%w: snapshot client set is nil", composition)
	}
	source, ok := clients.(V1RuntimeCapabilitiesSource)
	if !ok || isNilCapability(source) {
		return V1RuntimeCapabilities{}, fmt.Errorf("%w: snapshot client set does not expose V1RuntimeCapabilitiesSource", composition)
	}
	return source.V1RuntimeCapabilities(), nil
}
