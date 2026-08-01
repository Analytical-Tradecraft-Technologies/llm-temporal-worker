package runtime

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func TestRequireDurableV1RuntimeBuilderRejectsProductionDurableWithoutBuilder(t *testing.T) {
	err := requireDurableV1RuntimeBuilder(config.Config{
		Environment: "production",
		State:       config.StateConfig{Kind: config.StateKindDurable},
	}, nil)
	if !errors.Is(err, ErrDurableV1Composition) {
		t.Fatalf("error = %v, want ErrDurableV1Composition", err)
	}
}

func TestRequireDurableV1RuntimeBuilderAllowsDevelopmentFixture(t *testing.T) {
	if err := requireDurableV1RuntimeBuilder(config.Config{
		Environment: "development",
		State:       config.StateConfig{Kind: config.StateKindDurable},
	}, nil); err != nil {
		t.Fatalf("development durable fixture rejected: %v", err)
	}
}

func TestRequireDurableV1RuntimeBuilderAllowsExplicitBuilder(t *testing.T) {
	builder := NewGenerateV1RuntimeBuilder()
	if err := requireDurableV1RuntimeBuilder(config.Config{
		Environment: "production",
		State:       config.StateConfig{Kind: config.StateKindDurable},
	}, builder); err != nil {
		t.Fatalf("explicit durable builder rejected: %v", err)
	}
}

func TestRequireDurableV1RuntimeBuilderIgnoresNonDurableState(t *testing.T) {
	if err := requireDurableV1RuntimeBuilder(config.Config{
		Environment: "production",
		State:       config.StateConfig{Kind: config.StateKindRedis},
	}, nil); err != nil {
		t.Fatalf("non-durable state rejected: %v", err)
	}
}

func TestProductionFactoryRejectsUnconfiguredDurableSnapshotBeforeLoadingEngine(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile example config: %v", err)
	}
	loaded := false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			loaded = true
			return engine.Snapshot{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = factory.Build(context.Background(), snapshot)
	if !errors.Is(err, ErrDurableV1Composition) {
		t.Fatalf("Build() error = %v, want ErrDurableV1Composition", err)
	}
	if loaded {
		t.Fatal("Build() loaded the engine snapshot before rejecting unconfigured durable composition")
	}
}

func TestProductionFactoryDoesNotInferDurableBuilderFromCompositionFactory(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile example config: %v", err)
	}
	loaded := false
	compositionCalled := false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			loaded = true
			return engine.Snapshot{}, nil
		}),
		DurableCompositionFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.Composition, error) {
			compositionCalled = true
			return durablestore.Composition{}, errors.New("durable composition factory must be invoked by an explicit V1RuntimeBuilder")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = factory.Build(context.Background(), snapshot)
	if !errors.Is(err, ErrDurableV1Composition) {
		t.Fatalf("Build() error = %v, want ErrDurableV1Composition", err)
	}
	if loaded {
		t.Fatal("Build() loaded the engine snapshot before rejecting unconfigured durable composition")
	}
	if compositionCalled {
		t.Fatal("Build() invoked DurableCompositionFactory without an explicit V1RuntimeBuilder")
	}
}
