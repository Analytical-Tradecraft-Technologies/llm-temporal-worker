package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
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
	testassert.NoError(t, err)
	if _, ok := capability.(*cloudResponseTestStore); ok {
		t.Fatal("concrete store exposed")
	}
	ctx := context.Background()
	testassert.NoError(t, capability.Publish(ctx, cache.ResponseEntry{}))
	testassert.NoError(t, testassert.Err(capability.Lookup(ctx, cache.ResponseLookup{})))
	testassert.NoError(t, capability.RecordUse(ctx, cache.ResponseUse{}))
	testassert.NoError(t, testassert.Err(capability.ReadUse(ctx, "scope", "operation")))
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
	verifier := testKeyring(t, state.Key{ID: "key", Secret: bytes.Repeat([]byte{7}, 32)})
	for _, repository := range []CloudRequestRepository{
		&cloudWithoutResponses{CloudRequestRepository: &recordingCloudRequests{}, CloudCheckpointSource: &recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}}},
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}},
		&recordingCloudRequests{checkpointStore: &cloudCheckpointTestStore{}, responseStore: (*cloudResponseTestStore)(nil)},
	} {
		testassert.ErrorIs(t, testassert.Err(cloudResponseCache(repository)), ErrProductionFactoryInvalid)
		if closed, built, err := attachCloudRuntime(t, repository, verifier, "cache-test", 7); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing cache accepted: %v %t %t", err, closed, built)
		}
	}
}
