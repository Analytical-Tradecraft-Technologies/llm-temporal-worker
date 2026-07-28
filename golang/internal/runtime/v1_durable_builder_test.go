package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func completeDurableBuilderCapabilities(generate GeneratePortsFactory, compact CompactPortsFactory) V1RuntimeCapabilities {
	capabilities := completeGenerateCapabilities(generate)
	capabilities.CompactPortsFactory = compact
	return capabilities
}

func TestDurableV1RuntimeBuilderComposesBothPhasesFromOneSnapshot(t *testing.T) {
	var seen []string
	clients := &generateBuilderClientSet{}
	clients.capabilities = completeDurableBuilderCapabilities(
		func(ctx context.Context, capabilities V1RuntimeCapabilities) (durable.GeneratePorts, error) {
			value, err := capabilities.Snapshot.Current(ctx)
			if err != nil {
				return durable.GeneratePorts{}, err
			}
			seen = append(seen, "generate:"+value.Version)
			return validBuilderGeneratePorts(nil), nil
		},
		func(ctx context.Context, capabilities V1RuntimeCapabilities) (durable.CompactPorts, error) {
			value, err := capabilities.Snapshot.Current(ctx)
			if err != nil {
				return durable.CompactPorts{}, err
			}
			seen = append(seen, "compact:"+value.Version)
			return validCompactPorts(), nil
		},
	)

	runtimeValue, err := NewDurableV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err != nil {
		t.Fatalf("builder error = %v", err)
	}
	if _, ok := runtimeValue.(*activity.DurableV1Runtime); !ok {
		t.Fatalf("runtime type = %T, want *activity.DurableV1Runtime", runtimeValue)
	}
	if got, want := strings.Join(seen, ","), "generate:snapshot-1,compact:snapshot-1"; got != want {
		t.Fatalf("factory snapshot observations = %q, want %q", got, want)
	}
}

func TestDurableV1RuntimeBuilderRequiresBothPhaseFactories(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*V1RuntimeCapabilities)
		want   string
	}{
		{name: "generate", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.GeneratePortsFactory = nil }, want: "Generate ports factory"},
		{name: "compact", mutate: func(capabilities *V1RuntimeCapabilities) { capabilities.CompactPortsFactory = nil }, want: "Compact ports factory"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			capabilities := completeDurableBuilderCapabilities(
				func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
					return validBuilderGeneratePorts(nil), nil
				},
				func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
					return validCompactPorts(), nil
				},
			)
			test.mutate(&capabilities)
			clients := &generateBuilderClientSet{capabilities: capabilities}
			_, err := NewDurableV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
			if err == nil || !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("builder error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDurableV1RuntimeBuilderFailsClosedOnPhaseFactoryError(t *testing.T) {
	compactCalled := false
	clients := &generateBuilderClientSet{capabilities: completeDurableBuilderCapabilities(
		func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
			return durable.GeneratePorts{}, errors.New("postgres operation store unavailable")
		},
		func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
			compactCalled = true
			return validCompactPorts(), nil
		},
	)}
	_, err := NewDurableV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err == nil || !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), "construct Generate ports") {
		t.Fatalf("builder error = %v, want Generate factory failure", err)
	}
	if compactCalled {
		t.Fatal("Compact factory ran after Generate composition failed")
	}
}

func TestDurableV1RuntimeBuilderRejectsIncompletePhasePorts(t *testing.T) {
	clients := &generateBuilderClientSet{capabilities: completeDurableBuilderCapabilities(
		func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
			return durable.GeneratePorts{}, nil
		},
		func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
			return validCompactPorts(), nil
		},
	)}
	_, err := NewDurableV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err == nil || !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), "validate durable ports") {
		t.Fatalf("builder error = %v, want invalid Generate port failure", err)
	}
}

func TestDurableV1RuntimeBuilderRequiresCapabilitySource(t *testing.T) {
	_, err := NewDurableV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, generateBuilderPlainClientSet{})
	if err == nil || !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), "V1RuntimeCapabilitiesSource") {
		t.Fatalf("builder error = %v, want missing capability source", err)
	}
}

func TestNewProductionEngineFactoryInstallsCompleteBuilderWhenBothFactoriesExist(t *testing.T) {
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
			return validCompactPorts(), nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionEngineFactory() error = %v", err)
	}
	if factory.options.V1RuntimeBuilder == nil {
		t.Fatal("complete phase factories did not install durable v1 builder")
	}
}

func TestNewProductionEngineFactoryLeavesPartialPhaseCompositionUnconfigured(t *testing.T) {
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durable.GeneratePorts, error) {
			return validBuilderGeneratePorts(nil), nil
		},
	})
	if err != nil {
		t.Fatalf("NewProductionEngineFactory() error = %v", err)
	}
	if factory.options.V1RuntimeBuilder != nil {
		t.Fatal("partial phase factories installed a durable v1 builder")
	}
}
