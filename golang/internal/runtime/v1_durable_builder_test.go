package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/state"
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

func TestV1RuntimeCapabilitiesBuildDurableCompositionFailsClosedWithoutFactory(t *testing.T) {
	capabilities := completeGenerateCapabilities(nil)
	if _, err := capabilities.BuildDurableComposition(context.Background()); err == nil || !strings.Contains(err.Error(), "factory is not configured") {
		t.Fatalf("missing composition factory error = %v", err)
	}
}

func TestV1RuntimeCapabilitiesBuildDurableCompositionValidatesFactoryResult(t *testing.T) {
	capabilities := completeGenerateCapabilities(nil)
	called := false
	capabilities.CompositionFactory = func(_ context.Context, _ V1RuntimeCapabilities) (durable.Composition, error) {
		called = true
		return durable.Composition{}, nil
	}
	if _, err := capabilities.BuildDurableComposition(context.Background()); err == nil || !strings.Contains(err.Error(), "validate durable composition") {
		t.Fatalf("invalid composition error = %v", err)
	}
	if !called {
		t.Fatal("composition factory was not called")
	}
}

func TestV1RuntimeCapabilitiesBuildDurableCompositionUsesSnapshotOwnedPorts(t *testing.T) {
	capabilities := completeGenerateCapabilities(nil)
	capabilities.CompositionFactory = func(_ context.Context, received V1RuntimeCapabilities) (durable.Composition, error) {
		if received.CompositionFactory == nil {
			t.Fatal("factory did not receive the snapshot capability bundle")
		}
		return validCapabilityComposition(), nil
	}
	composition, err := capabilities.BuildDurableComposition(context.Background())
	if err != nil {
		t.Fatalf("BuildDurableComposition() error = %v", err)
	}
	if err := composition.Validate(); err != nil {
		t.Fatalf("returned composition invalid after construction: %v", err)
	}
}

// The capability stubs intentionally embed the narrow interfaces. The
// composition builder validates their presence without invoking a client,
// which keeps this factory test independent of Redis, PostgreSQL, and any
// provider credentials.
type capabilityAdmissionStub struct{ admission.AdmissionStore }
type capabilityContinuationStub struct{ state.ContinuationStore }
type capabilityResultStub struct{ durable.ResultStore }
type capabilityJournalStub struct{ durable.Journal }
type capabilityMaterializerStub struct{ durable.BudgetMaterializer }

func validCapabilityComposition() durable.Composition {
	return durable.Composition{
		Identity: durable.StateIdentity{
			Postgres:     durable.PostgresIdentity{Database: "llmtw", Schema: "worker", TablePrefix: "prod_"},
			Redis:        durable.RedisIdentity{KeyPrefix: "llmtw", HashTag: "admission"},
			ConfigDigest: [32]byte{1},
		},
		Operations:    capabilityAdmissionStub{},
		Continuations: capabilityContinuationStub{},
		Results:       capabilityResultStub{},
		Journal:       capabilityJournalStub{},
		Materializer:  capabilityMaterializerStub{},
	}
}

var (
	_ admission.AdmissionStore   = capabilityAdmissionStub{}
	_ state.ContinuationStore    = capabilityContinuationStub{}
	_ durable.ResultStore        = capabilityResultStub{}
	_ durable.Journal            = capabilityJournalStub{}
	_ durable.BudgetMaterializer = capabilityMaterializerStub{}
)
