package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	redisclient "github.com/redis/go-redis/v9"
)

func testCloudConfig() *config.CloudRequestConfig {
	return &config.CloudRequestConfig{
		Provider:     config.CloudStorageProviderConfig{Type: "aws", AWS: config.CloudStorageAWSConfig{Region: "ap-southeast-2"}, KeyValueStores: map[string]string{"requests": "physical-table"}, BlobStores: map[string]string{"payloads": "physical-bucket"}},
		RequestTable: "requests", PayloadStore: "payloads", Namespace: "test-requests", Secret: config.SecretRef{Kind: config.SecretEnv, Name: "REQUEST_KEY"},
	}
}

func cloudSnapshot(t *testing.T, namespace string) *config.Snapshot {
	t.Helper()
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, config.ReferenceResolverFunc(func(_ context.Context, c *config.Config) error {
		c.State.Requests = testCloudConfig()
		c.State.Requests.Namespace = namespace
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestCloudRepositoryFactoryUsesAliasesAndIndependentSecret(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	var handedKey []byte
	var opened cloudstate.Config
	repository := &recordingCloudRequests{}
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
			if ref.Name != "REQUEST_KEY" {
				t.Fatal("used another storage secret")
			}
			return []byte(base64.StdEncoding.EncodeToString(key)), nil
		}),
		CloudRequestFactory: func(_ context.Context, c cloudstate.Config, secret []byte) (CloudRequestRepository, error) {
			opened, handedKey = c, secret
			if !bytes.Equal(key, secret) {
				t.Fatal("wrong key")
			}
			return repository, nil
		},
	}}
	result, err := factory.buildCloudRequests(context.Background(), testCloudConfig())
	if err != nil || result != repository {
		t.Fatal(err)
	}
	if opened.Namespace != "test-requests" || opened.RequestTable != "requests" || opened.PayloadStore != "payloads" || opened.Provider["key_value_stores"].(map[string]any)["requests"] != "physical-table" {
		t.Fatal("lost portable config")
	}
	if !bytes.Equal(handedKey, make([]byte, 32)) {
		t.Fatal("resolved key not cleared after repository copied it")
	}
}

func TestCloudRepositoryFactoryFailsClosedWithoutSecretLeak(t *testing.T) {
	for _, test := range []string{"resolver", "invalid-base64", "wrong-size", "factory", "typed-nil"} {
		t.Run(test, func(t *testing.T) {
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					if test == "resolver" {
						return nil, errors.New("sensitive-cause")
					}
					if test == "invalid-base64" {
						return []byte("sensitive-cause"), nil
					}
					if test == "wrong-size" {
						return []byte(base64.StdEncoding.EncodeToString([]byte("sensitive-cause"))), nil
					}
					return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))), nil
				}),
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					if test == "typed-nil" {
						return (*recordingCloudRequests)(nil), nil
					}
					if test != "factory" {
						t.Fatal("opened cloud before key validation")
					}
					return nil, errors.New("sensitive-cause")
				},
			}}
			if _, err := factory.buildCloudRequests(context.Background(), testCloudConfig()); err == nil || strings.Contains(err.Error(), "sensitive-cause") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}

func TestCloudRequestsAttachedOncePerSnapshotAndDrainedOnFailure(t *testing.T) {
	verifier, err := state.NewKeyring([]state.Key{{ID: "test", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var repositories []*recordingCloudRequests
	var namespaces []string
	var fail bool
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Clock: time.Now,
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
		}),
		CloudRequestFactory: func(_ context.Context, c cloudstate.Config, _ []byte) (CloudRequestRepository, error) {
			r := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: &cloudFillTestStore{}}
			repositories = append(repositories, r)
			namespaces = append(namespaces, c.Namespace)
			return r, nil
		},
		V1RuntimeBuilder: func(_ context.Context, _ *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
			got := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().Requests
			if got != repositories[len(repositories)-1] {
				t.Fatal("builder did not receive snapshot-owned repository")
			}
			responses := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().Responses
			if responses == nil {
				t.Fatal("response cache not attached")
			}
			if err := responses.Publish(context.Background(), cache.ResponseEntry{}); err != nil {
				t.Fatal(err)
			}
			if repositories[len(repositories)-1].responseStore.(*cloudResponseTestStore).calls != 1 {
				t.Fatal("mixed cache snapshots")
			}
			fills := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().ResponseFills
			if fills == nil {
				t.Fatal("response fills not attached")
			}
			if _, err := fills.Acquire(context.Background(), cache.FillLease{}, time.Time{}); err != nil {
				t.Fatal(err)
			}
			if repositories[len(repositories)-1].fillStore.(*cloudFillTestStore).calls != 1 {
				t.Fatal("mixed fill snapshots")
			}
			finalizer := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().Finalizer
			if finalizer == nil || finalizer.requests != repositories[len(repositories)-1] {
				t.Fatal("finalizer not bound to snapshot request stores")
			}
			checkpoints := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().Checkpoints
			if err := checkpoints.RequireMaterializer(); err != nil {
				t.Fatal(err)
			}
			if checkpoints.BlobWriter == nil {
				t.Fatal("cloud checkpoint writer missing")
			}
			if _, err := checkpoints.BlobWriter.Write(context.Background(), "scope", []byte("data"), "text/plain"); err != nil {
				t.Fatal(err)
			}
			if repositories[len(repositories)-1].checkpointStore.(*cloudCheckpointTestStore).writes != 1 {
				t.Fatal("writer did not use snapshot cloud store")
			}
			materializer := checkpoints.Materializer.(snapshotCheckpointMaterializer).delegate.(*state.DurableCheckpointMaterializer)
			if materializer.HandleVerifier != verifier {
				t.Fatal("lost snapshot verifier")
			}
			if clients.(CheckpointCapabilitiesSource).CheckpointCapabilities().Repository != checkpoints.Repository {
				t.Fatal("different checkpoint bundles exposed")
			}
			if fail {
				return nil, errors.New("builder failed")
			}
			return &cloudInnerRuntime{}, nil
		},
	}}
	for _, namespace := range []string{"snapshot-one", "snapshot-two"} {
		closed := false
		clients := &productionClientSet{probes: cloudBaseTestProbes(), checkpointVerifier: verifier, checkpoints: CheckpointCapabilities{Repository: builderCheckpointRepository{}, Blobs: builderCheckpointBlobReader{}, Materializer: builderCheckpointMaterializer{}}, close: func(context.Context) error { closed = true; return nil }}
		_, built, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, namespace), nil, clients)
		if err != nil || built != clients || closed {
			t.Fatalf("attach: %v", err)
		}
		if _, ok := clients.V1Runtime().(*cloudRequestRuntime); !ok {
			t.Fatal("repository not applied to actual V1 runtime")
		}
		if len(clients.DependencyProbes()) != 3 || clients.DependencyProbes()[2].Probe(context.Background()).Dependency != DependencyCloudRequests {
			t.Fatal("missing cloud readiness")
		}
	}
	if repositories[0] == repositories[1] || namespaces[0] == namespaces[1] {
		t.Fatal("mixed snapshots")
	}
	fail = true
	closed := false
	clients := &productionClientSet{probes: cloudBaseTestProbes(), checkpointVerifier: verifier, close: func(context.Context) error { closed = true; return nil }}
	if _, _, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, "rejected"), nil, clients); err == nil || !closed {
		t.Fatal("failed build did not drain clients")
	}
	if err := requireDurableV1RuntimeBuilder(config.Config{Environment: "development", State: config.StateConfig{Kind: config.StateKindDurable, Requests: testCloudConfig()}}, nil); err == nil {
		t.Fatal("silently ignored requests without runtime")
	}
}

type cloudCheckpointTestStore struct {
	builderCheckpointRepository
	builderCheckpointBlobReader
	writes int
}

func (s *cloudCheckpointTestStore) Write(context.Context, string, []byte, string) (state.CheckpointBlobReference, error) {
	s.writes++
	return state.CheckpointBlobReference{}, nil
}

func TestCloudCheckpointCapabilitiesRejectIncompleteStores(t *testing.T) {
	verifier, err := state.NewKeyring([]state.Key{{ID: "test", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		repository CloudRequestRepository
		verifier   state.CheckpointHandleVerifier
	}{
		{"no source", struct{ CloudRequestRepository }{&recordingCloudRequests{}}, verifier},
		{"nil store", &recordingCloudRequests{}, verifier},
		{"typed nil", &recordingCloudRequests{checkpointStore: (*cloudCheckpointTestStore)(nil)}, verifier},
		{"no verifier", &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := cloudCheckpointCapabilities(test.repository, test.verifier, time.Now); !errors.Is(err, ErrProductionFactoryInvalid) {
				t.Fatalf("missing capability accepted: %v", err)
			}
			closed, built := false, false
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
				}),
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					return test.repository, nil
				},
				V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
					built = true
					return &cloudInnerRuntime{}, nil
				},
			}}
			clients := &productionClientSet{checkpointVerifier: test.verifier, close: func(context.Context) error { closed = true; return nil }}
			if _, _, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, "incomplete"), nil, clients); err == nil || !closed || built {
				t.Fatalf("incomplete checkpoint snapshot not rejected: closed=%t built=%t err=%v", closed, built, err)
			}
		})
	}
}

func TestCloudReadinessBlocksUnavailableStorageAndSanitizesErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		status ProbeStatus
	}{{nil, ProbeStatusReady}, {errors.New("private endpoint"), ProbeStatusUnavailable}, {context.DeadlineExceeded, ProbeStatusTimeout}} {
		p := cloudRequestProbe(&recordingCloudRequests{probeErr: test.err})
		if got := p.Probe(context.Background()); got.Status != test.status || got.Dependency != DependencyCloudRequests {
			t.Fatalf("probe: %+v", got)
		}
		err := CheckDependencyProbes(context.Background(), []DependencyProbe{p}, time.Second)
		if (err == nil) != (test.err == nil) {
			t.Fatalf("readiness gate: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), "private endpoint") {
			t.Fatal("leaked endpoint")
		}
	}
}

// Cloud workers retain Redis and the existing result/blob-store dependency.
func cloudBaseTestProbes() []DependencyProbe {
	var probes []DependencyProbe
	for _, id := range []DependencyID{DependencyRedis, DependencyBlobStore} {
		probes = append(probes, identifyDependencyProbe(id, DependencyProbeFunc(func(context.Context) ProbeResult {
			return ProbeResult{Dependency: id, Status: ProbeStatusReady, Reason: ProbeReasonReady}
		})))
	}
	return probes
}

// Construction exercises the real factory with in-process storage doubles;
// the resolver rejects any attempt to request credentials for the removed backend.
func TestProductionFactoryBuildsCloudSnapshotWithoutPostgres(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := config.Compile(context.Background(), data, config.ReferenceResolverFunc(func(_ context.Context, c *config.Config) error {
		c.State.Requests = testCloudConfig()
		c.Endpoints = map[string]config.EndpointConfig{"openai-prod": c.Endpoints["openai-prod"]}
		for name, model := range c.Models {
			model.Routes = model.Routes[:1]
			c.Models[name] = model
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		failOpen bool
		invalid  string
	}{
		{name: "successful cloud build"}, {name: "failed cloud open", failOpen: true},
		{name: "missing initialization capability", invalid: "capability"},
		{name: "missing initialization receipt", invalid: "missing"},
		{name: "incomplete initialization receipt", invalid: "incomplete"},
		{name: "wrong Redis identity", invalid: "identity"},
	} {
		t.Run(test.name, func(t *testing.T) {
			redis := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
			defer redis.Close()
			receipt := testInitialization(testBudgetIdentity(t, snapshot.Config()), time.Now())
			receipt.Ready = true
			switch test.invalid {
			case "missing":
				receipt = budget.Initialization{}
			case "incomplete":
				receipt.Ready = false
			case "identity":
				receipt.Identity.KeyFingerprint = strings.Repeat("0", 64)
			}
			base := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: &cloudFillTestStore{}}
			var initializationCalls []string
			var repository CloudRequestRepository = &testInitializedRequests{recordingCloudRequests: base, InitializationStore: &initializationTestStore{value: receipt, trace: &initializationCalls}}
			if test.invalid == "capability" {
				repository = base
			}
			opened := 0
			built := false
			closed := false
			factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
				SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
					return engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{"test": {Routes: []routing.Route{{EndpointID: "openai-prod", Capabilities: routing.CapabilitySet{Version: "test-v1"}}}}}}}, nil
				}),
				Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
					if strings.Contains(strings.ToLower(ref.Name+ref.Path), "postgres") {
						t.Fatal("resolved PostgreSQL secret")
					}
					if ref.Name == "REQUEST_KEY" {
						return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
					}
					return bytes.Repeat([]byte{7}, 32), nil
				}),
				RedisClient: redis, RedisKeySecret: bytes.Repeat([]byte{8}, 32),
				BlobFactory: func(context.Context, config.Config) (blob.Store, io.Closer, error) {
					return &cloudBootstrapBlobStore{newTestBlobStore()}, cloudBootstrapCloser{func() { closed = true }}, nil
				},
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					opened++
					if test.failOpen {
						return nil, errors.New("cloud open failed")
					}
					return repository, nil
				},
				V1RuntimeBuilder: func(_ context.Context, _ *config.Snapshot, _ llm.Engine, set app.ClientSet) (activity.V1Runtime, error) {
					built = true
					capabilities := set.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities()
					if capabilities.Requests != repository || capabilities.Budgets == nil || capabilities.Finalizer == nil || capabilities.Checkpoints.Repository == nil || capabilities.Responses == nil || capabilities.ResponseFills == nil || capabilities.CheckpointKeyring == nil || capabilities.CheckpointKeyring != set.(*productionClientSet).checkpointVerifier {
						t.Fatal("incomplete cloud capabilities")
					}
					queries := set.(QueryRepositoriesSource).QueryRepositories()
					if queries.SpendSummary != nil || queries.ProviderStatus == nil || queries.Inventory == nil {
						t.Fatal("incorrect query backend capabilities")
					}
					return &cloudInnerRuntime{}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, clients, err := factory.Build(context.Background(), snapshot)
			if opened != 1 {
				t.Fatalf("opened cloud repository %d times", opened)
			}
			for _, call := range initializationCalls {
				if call != "read" {
					t.Fatalf("worker startup wrote initialization: %v", initializationCalls)
				}
			}
			if test.failOpen || test.invalid != "" {
				if err == nil || clients != nil || built || !closed {
					t.Fatalf("failed cloud open did not reject and drain: err=%v built=%v closed=%v", err, built, closed)
				}
				if test.invalid != "" && !errors.Is(err, ErrBudgetInitializationRequired) {
					t.Fatalf("initialization error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !built || closed {
				t.Fatal("cloud runtime not constructed")
			}
			probes := clients.(dependencyProbeSource).DependencyProbes()
			if len(probes) != 3 {
				t.Fatalf("probes=%d", len(probes))
			}
			if err := validateRequiredDependencyProbeSet(snapshot.Config().State, probes); err != nil {
				t.Fatal(err)
			}
			if err := clients.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !closed {
				t.Fatal("cloud snapshot leaked blob client")
			}
		})
	}
}

type cloudBootstrapBlobStore struct{ *testBlobStore }

func (*cloudBootstrapBlobStore) ProbeBucket(context.Context) error { return nil }

type cloudBootstrapCloser struct{ close func() }

func (c cloudBootstrapCloser) Close() error { c.close(); return nil }

func TestCloudReadinessRejectsIncompleteClientsBeforeRuntimeBuilder(t *testing.T) {
	verifier, err := state.NewKeyring([]state.Key{{ID: "test", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		probes []DependencyProbe
	}{
		{"missing Redis", cloudBaseTestProbes()[1:]},
		{"missing blob store", cloudBaseTestProbes()[:1]},
		{"unexpected SQL", append(cloudBaseTestProbes(), identifyDependencyProbe(DependencyID("postgres"), DependencyProbeFunc(func(context.Context) ProbeResult { return ProbeResult{} })))},
	} {
		t.Run(test.name, func(t *testing.T) {
			closed := false
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				Clock: time.Now,
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
				}),
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					return &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: &cloudFillTestStore{}}, nil
				},
				V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
					t.Fatal("invoked builder with invalid readiness set")
					return nil, nil
				},
			}}
			clients := &productionClientSet{probes: test.probes, checkpointVerifier: verifier, close: func(context.Context) error { closed = true; return nil }}
			_, set, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, "invalid-readiness"), nil, clients)
			if err == nil || set != nil || !closed {
				t.Fatalf("readiness rejection: error=%v set=%T closed=%v", err, set, closed)
			}
		})
	}
}

// The budget-authority composition (cloud request repository, initialization
// receipt and authority probe) is production hardening. It must apply to every
// environment name except development, not only the exact string "production".
func TestProductionFactoryRequiresBudgetInitializationOutsideDevelopment(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, environment := range []string{"Production", "prod", "staging"} {
		t.Run(environment, func(t *testing.T) {
			// Budget matchers follow the renamed environment.
			renamed := strings.ReplaceAll(string(data), "environment: production", "environment: "+environment)
			snapshot, err := config.Compile(context.Background(), []byte(renamed), config.ReferenceResolverFunc(func(_ context.Context, c *config.Config) error {
				c.State.Requests = testCloudConfig()
				c.Endpoints = map[string]config.EndpointConfig{"openai-prod": c.Endpoints["openai-prod"]}
				for name, model := range c.Models {
					model.Routes = model.Routes[:1]
					c.Models[name] = model
				}
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			redis := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
			defer redis.Close()
			receipt := testInitialization(testBudgetIdentity(t, snapshot.Config()), time.Now())
			base := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: &cloudFillTestStore{}}
			var calls []string
			opened, built := 0, false
			factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
				SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) {
					return engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{"test": {Routes: []routing.Route{{EndpointID: "openai-prod", Capabilities: routing.CapabilitySet{Version: "test-v1"}}}}}}}, nil
				}),
				Resolver: secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
					if ref.Name == "REQUEST_KEY" {
						return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))), nil
					}
					return bytes.Repeat([]byte{7}, 32), nil
				}),
				RedisClient: redis, RedisKeySecret: bytes.Repeat([]byte{8}, 32),
				BlobFactory: func(context.Context, config.Config) (blob.Store, io.Closer, error) {
					return &cloudBootstrapBlobStore{newTestBlobStore()}, cloudBootstrapCloser{func() {}}, nil
				},
				CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
					opened++
					// The receipt is present but incomplete: startup must refuse it.
					return &testInitializedRequests{recordingCloudRequests: base, InitializationStore: &initializationTestStore{value: receipt, trace: &calls}}, nil
				},
				V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
					built = true
					return &cloudInnerRuntime{}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, clients, err := factory.Build(context.Background(), snapshot)
			if !errors.Is(err, ErrBudgetInitializationRequired) || clients != nil || built || opened != 1 || len(calls) == 0 {
				t.Fatalf("environment %q skipped the budget initialization check: err=%v built=%v opened=%d reads=%v", environment, err, built, opened, calls)
			}
		})
	}
}

func TestCloudRepositoryFactoryForwardsMRSCOptIn(t *testing.T) {
	for _, allow := range []bool{false, true} {
		for _, region := range []string{"region-primary", "region-fallback-a", "region-fallback-b"} {
			t.Run(region+"/"+fmt.Sprint(allow), func(t *testing.T) {
				called := false
				factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
					Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
						return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))), nil
					}),
					CloudRequestFactory: func(_ context.Context, c cloudstate.Config, _ []byte) (CloudRequestRepository, error) {
						called = true
						aws := c.Provider["aws"].(map[string]any)
						value, exists := aws["allow_mrsc"]
						if aws["region"] != region || (allow && value != true) || (!allow && exists) {
							t.Fatalf("wrong AWS configuration: %+v", aws)
						}
						// Validate the actual pinned cloud-storage factory accepts
						// the forwarded option without opening any AWS resources.
						if _, err := providers.FromJSON(context.Background(), c.Provider); err != nil {
							t.Fatal(err)
						}
						return &recordingCloudRequests{}, nil
					},
				}}
				t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/absent")
				t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/absent")
				t.Setenv("AWS_PROFILE", "")
				t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
				cfg := testCloudConfig()
				cfg.Provider.AWS.Region, cfg.Provider.AWS.AllowMRSC = region, allow
				if _, err := factory.buildCloudRequests(context.Background(), cfg); err != nil || !called {
					t.Fatalf("called=%t err=%v", called, err)
				}
			})
		}
	}
}
