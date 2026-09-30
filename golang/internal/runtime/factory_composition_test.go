package runtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	"github.com/redis/go-redis/v9"
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

func TestProductionFactoryRejectsPhaseFactoriesWithoutCompositionBeforeLoadingEngine(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile example config: %v", err)
	}
	loaded := false
	resolverCalled, redisCalled := false, false
	generateCalled, compactCalled := false, false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			resolverCalled = true
			return nil, nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			loaded = true
			return engine.Snapshot{}, nil
		}),
		RedisFactory: func(context.Context, config.RedisConfig, string, string) (redis.UniversalClient, error) {
			redisCalled = true
			return nil, errors.New("Redis must not be constructed")
		},
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
			generateCalled = true
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
			compactCalled = true
			return validCompactPorts(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = factory.Build(context.Background(), snapshot)
	if !errors.Is(err, ErrDurableV1Composition) {
		t.Fatalf("Build() error = %v, want ErrDurableV1Composition", err)
	}
	if loaded || resolverCalled || redisCalled || generateCalled || compactCalled {
		t.Fatalf("missing composition reached external work: snapshot=%t provider=%t redis=%t generate=%t compact=%t", loaded, resolverCalled, redisCalled, generateCalled, compactCalled)
	}
}

func TestProductionFactoryRejectsInvalidAutomaticCompositionBeforeLoadingEngine(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile example config: %v", err)
	}
	loaded := false
	resolverCalled, redisCalled := false, false
	compositionCalled := false
	generateCalled, compactCalled := false, false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			resolverCalled = true
			return nil, nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			loaded = true
			return engine.Snapshot{}, nil
		}),
		RedisFactory: func(context.Context, config.RedisConfig, string, string) (redis.UniversalClient, error) {
			redisCalled = true
			return nil, errors.New("Redis must not be constructed")
		},
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
			generateCalled = true
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
			compactCalled = true
			return validCompactPorts(), nil
		},
		DurableCompositionFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.Composition, error) {
			compositionCalled = true
			return durablestore.Composition{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = factory.Build(context.Background(), snapshot)
	if !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), "validate durable composition") {
		t.Fatalf("Build() error = %v, want invalid automatic-composition failure", err)
	}
	if !compositionCalled || loaded || resolverCalled || redisCalled || generateCalled || compactCalled {
		t.Fatalf("invalid composition reached external work: composition=%t snapshot=%t provider=%t redis=%t generate=%t compact=%t", compositionCalled, loaded, resolverCalled, redisCalled, generateCalled, compactCalled)
	}
}

func TestProductionFactoryRejectsMismatchedAutomaticCompositionBeforeLoadingEngine(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile example config: %v", err)
	}
	loaded := false
	resolverCalled, redisCalled := false, false
	compositionCalled := false
	generateCalled, compactCalled := false, false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			resolverCalled = true
			return nil, nil
		}),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
			loaded = true
			return engine.Snapshot{}, nil
		}),
		RedisFactory: func(context.Context, config.RedisConfig, string, string) (redis.UniversalClient, error) {
			redisCalled = true
			return nil, errors.New("Redis must not be constructed")
		},
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
			generateCalled = true
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
			compactCalled = true
			return validCompactPorts(), nil
		},
		DurableCompositionFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.Composition, error) {
			compositionCalled = true
			return validCapabilityComposition(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = factory.Build(context.Background(), snapshot)
	if !errors.Is(err, ErrDurableV1Composition) || !strings.Contains(err.Error(), "config digest does not match") {
		t.Fatalf("Build() error = %v, want mismatched automatic-composition failure", err)
	}
	if !compositionCalled || loaded || resolverCalled || redisCalled || generateCalled || compactCalled {
		t.Fatalf("mismatched composition reached external work: composition=%t snapshot=%t provider=%t redis=%t generate=%t compact=%t", compositionCalled, loaded, resolverCalled, redisCalled, generateCalled, compactCalled)
	}
}

func TestProductionFactoryPreflightsAutomaticCompositionForEachReloadIdentity(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	first, err := config.Compile(context.Background(), data, nil)
	if err != nil {
		t.Fatalf("compile first config: %v", err)
	}
	secondData := []byte(strings.Replace(string(data), "route_attempts: 6", "route_attempts: 5", 1))
	second, err := config.Compile(context.Background(), secondData, nil)
	if err != nil {
		t.Fatalf("compile second config: %v", err)
	}
	if first.Digest() == second.Digest() {
		t.Fatal("distinct compiled snapshots share a config digest")
	}

	seen := make(map[[32]byte]int)
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
			return validCompactPorts(), nil
		},
		DurableCompositionFactory: func(_ context.Context, capabilities V1RuntimeCapabilities) (durablestore.Composition, error) {
			if capabilities.ConfigDigest == ([32]byte{}) {
				t.Fatal("automatic preflight omitted the current config digest")
			}
			seen[capabilities.ConfigDigest]++
			composition := validCapabilityComposition()
			composition.Identity.ConfigDigest = capabilities.ConfigDigest
			return composition, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []*config.Snapshot{first, second} {
		composition, err := factory.preflightAutomaticDurableComposition(context.Background(), snapshot)
		if err != nil {
			t.Fatalf("preflight snapshot %x: %v", snapshot.Digest(), err)
		}
		if composition.Identity.ConfigDigest != snapshot.Digest() {
			t.Fatalf("preflight composition digest = %x, want %x", composition.Identity.ConfigDigest, snapshot.Digest())
		}
	}
	if seen[first.Digest()] != 1 || seen[second.Digest()] != 1 {
		t.Fatalf("composition factory reload digests = %#v, want one invocation per snapshot", seen)
	}
}

func TestProductionFactoryDoesNotPreflightCompositionForExplicitBuilder(t *testing.T) {
	compositionCalled := false
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:         secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return nil, nil }),
		SnapshotLoader:   SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		V1RuntimeBuilder: NewGenerateV1RuntimeBuilder(),
		GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
			return validBuilderGeneratePorts(nil), nil
		},
		CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
			return validCompactPorts(), nil
		},
		DurableCompositionFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.Composition, error) {
			compositionCalled = true
			return durablestore.Composition{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.preflightAutomaticDurableComposition(context.Background(), &config.Snapshot{}); err != nil {
		t.Fatalf("explicit builder preflight error = %v", err)
	}
	if compositionCalled {
		t.Fatal("production factory preflighted a composition for an explicit builder")
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

func TestCloudRequestIdentityBindsMappingsAndProviderConfiguration(t *testing.T) {
	original := testCloudConfig()
	want, err := cloudRequestIdentity(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*config.CloudRequestConfig)
	}{
		{"namespace", func(c *config.CloudRequestConfig) { c.Namespace = "other" }},
		{"table alias", func(c *config.CloudRequestConfig) { c.RequestTable = "another" }},
		{"bucket alias", func(c *config.CloudRequestConfig) { c.PayloadStore = "another" }},
		{"physical table", func(c *config.CloudRequestConfig) { c.Provider.KeyValueStores["requests"] = "other-table" }},
		{"physical bucket", func(c *config.CloudRequestConfig) { c.Provider.BlobStores["payloads"] = "other-bucket" }},
		{"region", func(c *config.CloudRequestConfig) { c.Provider.AWS.Region = "us-east-1" }},
		{"profile", func(c *config.CloudRequestConfig) { c.Provider.AWS.Profile = "other-account" }},
		{"provider", func(c *config.CloudRequestConfig) { c.Provider.Type = "azure" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := testCloudConfig()
			test.change(c)
			got, err := cloudRequestIdentity(c)
			if err != nil || got == want {
				t.Fatalf("identity did not change: error=%v", err)
			}
		})
	}
	// encoding/json sorts map keys, regardless of insertion order.
	original.Provider.KeyValueStores["extra"] = "another-table"
	first, err := cloudRequestIdentity(original)
	if err != nil {
		t.Fatal(err)
	}
	original.Provider.KeyValueStores = map[string]string{"extra": "another-table", "requests": "physical-table"}
	second, err := cloudRequestIdentity(original)
	if err != nil || first != second {
		t.Fatal("unstable map digest", err)
	}
	original.Secret.Name = "different-secret-reference"
	third, err := cloudRequestIdentity(original)
	if err != nil || second != third {
		t.Fatal("secret reference entered provider identity", err)
	}
}

func TestCloudCompositionPreflightValidatesBackendBeforeExternalWork(t *testing.T) {
	snapshot := cloudSnapshot(t, "preflight")
	for _, test := range []struct {
		name   string
		modify func(*durablestore.Composition)
		valid  bool
	}{
		{"cloud without SQL", func(*durablestore.Composition) {}, true},
		{"SQL instead of cloud", func(c *durablestore.Composition) {
			c.Identity.Cloud = durablestore.CloudIdentity{}
			c.Identity.Postgres = validCapabilityComposition().Identity.Postgres
		}, false},
		{"mixed backends", func(c *durablestore.Composition) {
			c.Identity.Postgres = validCapabilityComposition().Identity.Postgres
		}, false},
		{"stale namespace", func(c *durablestore.Composition) { c.Identity.Cloud.Namespace = "previous" }, false},
		{"stale provider mapping", func(c *durablestore.Composition) { c.Identity.Cloud.ProviderDigest[0] ^= 1 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					t.Fatal("resolved secret before identity validation")
					return nil, nil
				}),
				SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
					t.Fatal("loaded snapshot before identity validation")
					return engine.Snapshot{}, nil
				}),
				GeneratePortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
					t.Fatal("constructed Generate before identity validation")
					return durablestore.GeneratePorts{}, nil
				},
				CompactPortsFactory: func(context.Context, V1RuntimeCapabilities) (durablestore.CompactPorts, error) {
					t.Fatal("constructed Compact before identity validation")
					return durablestore.CompactPorts{}, nil
				},
				DurableCompositionFactory: func(_ context.Context, cap V1RuntimeCapabilities) (durablestore.Composition, error) {
					calls++
					c := validCapabilityComposition()
					c.Identity.Postgres = durablestore.PostgresIdentity{}
					c.Identity.Cloud = cap.CloudIdentity
					c.Identity.ConfigDigest = cap.ConfigDigest
					test.modify(&c)
					return c, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.valid {
				c, err := factory.preflightAutomaticDurableComposition(context.Background(), snapshot)
				if err != nil || c == nil || c.Identity.Postgres != (durablestore.PostgresIdentity{}) {
					t.Fatalf("cloud preflight: %v", err)
				}
			} else {
				_, _, err := factory.Build(context.Background(), snapshot)
				if !errors.Is(err, ErrDurableV1Composition) {
					t.Fatalf("invalid cloud preflight: %v", err)
				}
			}
			if calls != 1 {
				t.Fatalf("composition calls=%d", calls)
			}
		})
	}
}

func TestBoundCloudCompositionRevalidatesIdentity(t *testing.T) {
	expected, err := cloudRequestIdentity(testCloudConfig())
	if err != nil {
		t.Fatal(err)
	}
	c := validCapabilityComposition()
	c.Identity.Postgres = durablestore.PostgresIdentity{}
	c.Identity.Cloud = expected
	capabilities := V1RuntimeCapabilities{CloudIdentity: expected, ConfigDigest: c.Identity.ConfigDigest, composition: &c}
	if _, err := capabilities.BuildDurableComposition(context.Background()); err != nil {
		t.Fatal(err)
	}
	capabilities.CloudIdentity.Namespace = "other"
	if _, err := capabilities.BuildDurableComposition(context.Background()); err == nil {
		t.Fatal("reused mismatched cloud composition")
	}
	capabilities.CloudIdentity = durablestore.CloudIdentity{}
	if _, err := capabilities.BuildDurableComposition(context.Background()); err == nil {
		t.Fatal("legacy SQL bundle reused cloud composition")
	}
}
