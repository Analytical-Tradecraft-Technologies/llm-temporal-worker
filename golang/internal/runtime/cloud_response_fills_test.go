package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	testassert.NoError(t, err)
	if _, ok := capability.(*cloudFillTestStore); ok {
		t.Fatal("concrete store exposed")
	}
	ctx := context.Background()
	testassert.NoError(t, testassert.Err(capability.Acquire(ctx, cache.FillLease{}, time.Time{})))
	testassert.NoError(t, testassert.Err(capability.Start(ctx, cache.FillLease{}, time.Time{})))
	testassert.NoError(t, capability.Release(ctx, cache.FillLease{}, time.Time{}))
	testassert.NoError(t, capability.Complete(ctx, cache.FillLease{}, cache.FillCompletion{}))
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
	verifier := testKeyring(t, state.Key{ID: "key", Secret: bytes.Repeat([]byte{7}, 32)})
	complete := &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}}
	for _, repository := range []CloudRequestRepository{
		&cloudWithoutFills{complete, complete, complete}, complete,
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: &cloudResponseTestStore{}, fillStore: (*cloudFillTestStore)(nil)},
	} {
		testassert.ErrorIs(t, testassert.Err(cloudResponseFills(repository)), ErrProductionFactoryInvalid)
		if closed, built, err := attachCloudRuntime(t, repository, verifier, "fill-test", 7); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing fill accepted: %v %t %t", err, closed, built)
		}
	}
}
