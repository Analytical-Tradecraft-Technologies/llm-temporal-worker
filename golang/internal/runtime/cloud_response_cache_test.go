package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

type cloudResponseTestStore struct{ calls int }

func (s *cloudResponseTestStore) Publish(context.Context, cache.ResponseEntry) error {
	s.calls++
	return nil
}
func (s *cloudResponseTestStore) Lookup(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
	s.calls++
	return nil, nil
}
func (s *cloudResponseTestStore) RecordUse(context.Context, cache.ResponseUse) error {
	s.calls++
	return nil
}
func (s *cloudResponseTestStore) ReadUse(context.Context, string, state.OperationID) (cache.ResponseUse, error) {
	s.calls++
	return cache.ResponseUse{}, nil
}

func TestCloudResponseCapabilitiesDelegateAllMethods(t *testing.T) {
	store := &cloudResponseTestStore{}
	capability, err := cloudResponseCache(&recordingCloudRequests{responseStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := capability.(*cloudResponseTestStore); ok {
		t.Fatal("concrete store exposed")
	}
	ctx := context.Background()
	if err := capability.Publish(ctx, cache.ResponseEntry{}); err != nil {
		t.Fatal(err)
	}
	if _, err := capability.Lookup(ctx, cache.ResponseLookup{}); err != nil {
		t.Fatal(err)
	}
	if err := capability.RecordUse(ctx, cache.ResponseUse{}); err != nil {
		t.Fatal(err)
	}
	if _, err := capability.ReadUse(ctx, "scope", "operation"); err != nil {
		t.Fatal(err)
	}
	if store.calls != 4 {
		t.Fatalf("delegations: %d", store.calls)
	}
}

// Hide only the response capability while keeping the valid checkpoint source.
type cloudWithoutResponses struct {
	CloudRequestRepository
	CloudCheckpointSource
}

func TestCloudResponseCapabilitiesRejectIncompleteAndDrainSnapshot(t *testing.T) {
	verifier, err := state.NewKeyring([]state.Key{{ID: "key", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, repository := range []CloudRequestRepository{
		&cloudWithoutResponses{CloudRequestRepository: &recordingCloudRequests{}, CloudCheckpointSource: &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}}},
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}},
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: (*cloudResponseTestStore)(nil)},
	} {
		if _, err := cloudResponseCache(repository); !errors.Is(err, ErrProductionFactoryInvalid) {
			t.Fatal(err)
		}
		closed, built := false, false
		factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
			Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
				return []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))), nil
			}),
			CloudRequestFactory: func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error) {
				return repository, nil
			},
			V1RuntimeBuilder: func(context.Context, *config.Snapshot, llm.Engine, app.ClientSet) (activity.V1Runtime, error) {
				built = true
				return &cloudInnerRuntime{}, nil
			},
		}}
		clients := &productionClientSet{checkpointVerifier: verifier, close: func(context.Context) error { closed = true; return nil }}
		if _, _, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, "cache-test"), nil, clients); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing cache accepted: %v %t %t", err, closed, built)
		}
	}
}
