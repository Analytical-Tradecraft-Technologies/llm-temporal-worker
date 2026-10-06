package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

type cloudFillTestStore struct{ calls int }

func (s *cloudFillTestStore) Acquire(context.Context, cache.FillLease, time.Time) (cache.FillDecision, error) {
	s.calls++
	return cache.FillDecision{}, nil
}
func (s *cloudFillTestStore) Start(context.Context, cache.FillLease, time.Time) (bool, error) {
	s.calls++
	return false, nil
}
func (s *cloudFillTestStore) Release(context.Context, cache.FillLease, time.Time) error {
	s.calls++
	return nil
}
func (s *cloudFillTestStore) Complete(context.Context, cache.FillLease, cache.FillCompletion) error {
	s.calls++
	return nil
}

func TestCloudFillCapabilitiesDelegate(t *testing.T) {
	store := &cloudFillTestStore{}
	capability, err := cloudResponseFills(&recordingCloudRequests{fillStore: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := capability.(*cloudFillTestStore); ok {
		t.Fatal("concrete store exposed")
	}
	ctx := context.Background()
	if _, err := capability.Acquire(ctx, cache.FillLease{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := capability.Start(ctx, cache.FillLease{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(ctx, cache.FillLease{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := capability.Complete(ctx, cache.FillLease{}, cache.FillCompletion{}); err != nil {
		t.Fatal(err)
	}
	if store.calls != 4 {
		t.Fatalf("delegations: %d", store.calls)
	}
}

type cloudWithoutFills struct {
	CloudRequestRepository
	CloudCheckpointSource
	CloudResponseCacheSource
}

func TestCloudFillCapabilitiesRejectIncompleteAndDrainSnapshot(t *testing.T) {
	verifier, err := state.NewKeyring([]state.Key{{ID: "key", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	complete := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}}
	for _, repository := range []CloudRequestRepository{
		&cloudWithoutFills{complete, complete, complete}, complete,
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: (*cloudFillTestStore)(nil)},
	} {
		if _, err := cloudResponseFills(repository); !errors.Is(err, ErrProductionFactoryInvalid) {
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
		if _, _, err := factory.attachV1Runtime(context.Background(), cloudSnapshot(t, "fill-test"), nil, clients); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing fill accepted: %v %t %t", err, closed, built)
		}
	}
}
