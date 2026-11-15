package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
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
		if closed, built, err := attachCloudRuntime(t, repository, verifier, "fill-test", 7); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing fill accepted: %v %t %t", err, closed, built)
		}
	}
}
