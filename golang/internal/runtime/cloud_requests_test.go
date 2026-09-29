package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
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
			r := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}}
			repositories = append(repositories, r)
			namespaces = append(namespaces, c.Namespace)
			return r, nil
		},
		V1RuntimeBuilder: func(_ context.Context, _ *config.Snapshot, _ llm.Engine, clients app.ClientSet) (activity.V1Runtime, error) {
			got := clients.(V1RuntimeCapabilitiesSource).V1RuntimeCapabilities().Requests
			if got != repositories[len(repositories)-1] {
				t.Fatal("builder did not receive snapshot-owned repository")
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
		clients := &productionClientSet{checkpointVerifier: verifier, checkpoints: CheckpointCapabilities{Repository: builderCheckpointRepository{}, Blobs: builderCheckpointBlobReader{}, Materializer: builderCheckpointMaterializer{}}, close: func(context.Context) error { closed = true; return nil }}
		_, built, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, namespace), nil, clients)
		if err != nil || built != clients || closed {
			t.Fatalf("attach: %v", err)
		}
		if _, ok := clients.V1Runtime().(*cloudRequestRuntime); !ok {
			t.Fatal("repository not applied to actual V1 runtime")
		}
		if len(clients.DependencyProbes()) != 1 || clients.DependencyProbes()[0].Probe(context.Background()).Dependency != DependencyCloudRequests {
			t.Fatal("missing cloud readiness")
		}
	}
	if repositories[0] == repositories[1] || namespaces[0] == namespaces[1] {
		t.Fatal("mixed snapshots")
	}
	fail = true
	closed := false
	clients := &productionClientSet{checkpointVerifier: verifier, close: func(context.Context) error { closed = true; return nil }}
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
		{"no verifier", &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}}, nil},
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
