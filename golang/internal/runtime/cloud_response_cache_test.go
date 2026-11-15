package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
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
		if closed, built, err := attachCloudRuntime(t, repository, verifier, "cache-test", 7); !errors.Is(err, ErrProductionFactoryInvalid) || !closed || built {
			t.Fatalf("missing cache accepted: %v %t %t", err, closed, built)
		}
	}
}
