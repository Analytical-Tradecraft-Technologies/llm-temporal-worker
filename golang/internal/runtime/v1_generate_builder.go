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
		if ctx == nil {
			return nil, fmt.Errorf("%w: context is nil", ErrGenerateV1Composition)
		}
		if snapshot == nil {
			return nil, fmt.Errorf("%w: configuration snapshot is nil", ErrGenerateV1Composition)
		}
		if isNilCapability(clients) {
			return nil, fmt.Errorf("%w: snapshot client set is nil", ErrGenerateV1Composition)
		}
		source, ok := clients.(V1RuntimeCapabilitiesSource)
		if !ok || isNilCapability(source) {
			return nil, fmt.Errorf("%w: snapshot client set does not expose V1RuntimeCapabilitiesSource", ErrGenerateV1Composition)
		}
		capabilities := source.V1RuntimeCapabilities()
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
