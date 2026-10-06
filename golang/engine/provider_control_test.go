package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

type providerStatusRecorderStub struct {
	observations []control.StatusObservation
	err          error
}

func (stub *providerStatusRecorderStub) RecordProviderStatus(_ context.Context, observation control.StatusObservation) error {
	stub.observations = append(stub.observations, observation)
	return stub.err
}

func TestRecordProviderStatusSuccessIsBoundedToSnapshot(t *testing.T) {
	recorder := &providerStatusRecorderStub{}
	engineValue := &Engine{dependencies: Dependencies{Clock: func() time.Time { return time.Unix(100, 0).UTC() }, ProviderControl: recorder}}
	snapshot := Snapshot{Version: "epoch-a", ConfigEpoch: "epoch-a", ConfigDigest: [32]byte{1}}
	candidate := routing.Candidate{RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{2}, Provider: "openai", Family: "openai_responses", Model: "gpt-test"}
	engineValue.recordProviderStatus(context.Background(), snapshot, admission.Operation{ID: "operation-a"}, candidate, provider.Result{}, nil)
	if len(recorder.observations) != 1 {
		t.Fatalf("recorded observations = %d, want 1", len(recorder.observations))
	}
	observation := recorder.observations[0]
	if observation.Source != control.SourceInference || observation.Availability != control.AvailabilityAvailable || observation.Credit != control.CreditOK || observation.Billing != control.BillingOK {
		t.Fatalf("success observation = %#v", observation)
	}
	if observation.ConfigDigest != snapshot.ConfigDigest || observation.ConfigEpoch != snapshot.ConfigEpoch || observation.RouteID != candidate.RouteID || observation.EndpointAccountHMAC != candidate.EndpointAccountHMAC {
		t.Fatalf("snapshot/route binding = %#v", observation)
	}
	if observation.EvidenceDigest == ([32]byte{}) || !observation.ExpiresAt.After(observation.ObservedAt) {
		t.Fatalf("observation evidence/expiry missing = %#v", observation)
	}
}

func TestRecordProviderStatusClassifiesCreditEvidenceButNotGenericRateLimit(t *testing.T) {
	tests := []struct {
		name        string
		safeDetails map[string]string
		code        provider.Code
		credit      control.CreditState
		billing     control.BillingState
	}{
		{name: "quota", safeDetails: map[string]string{"provider_code": "insufficient_quota"}, code: provider.CodeProviderUnavailable, credit: control.CreditExhausted, billing: control.BillingIssue},
		{name: "rate limit", code: provider.CodeProviderRateLimited, credit: control.CreditUnknown, billing: control.BillingUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &providerStatusRecorderStub{}
			engineValue := &Engine{dependencies: Dependencies{Clock: time.Now, ProviderControl: recorder}}
			failure := provider.NewError(test.code, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "provider failure")
			failure.SafeDetails = test.safeDetails
			engineValue.recordProviderStatus(context.Background(), Snapshot{Version: "epoch-a", ConfigDigest: [32]byte{1}}, admission.Operation{ID: "operation-a"}, routing.Candidate{RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{2}, Provider: "openai", Family: "openai_responses"}, provider.Result{}, failure)
			if len(recorder.observations) != 1 {
				t.Fatalf("recorded observations = %d, want 1", len(recorder.observations))
			}
			observation := recorder.observations[0]
			if observation.Credit != test.credit || observation.Billing != test.billing {
				t.Fatalf("credit/billing = %q/%q, want %q/%q", observation.Credit, observation.Billing, test.credit, test.billing)
			}
		})
	}
}

func TestRecordProviderStatusRecorderFailureIsBestEffort(t *testing.T) {
	recorder := &providerStatusRecorderStub{err: errors.New("database unavailable")}
	engineValue := &Engine{dependencies: Dependencies{Clock: time.Now, ProviderControl: recorder}}
	engineValue.recordProviderStatus(context.Background(), Snapshot{Version: "epoch-a", ConfigDigest: [32]byte{1}}, admission.Operation{ID: "operation-a"}, routing.Candidate{RouteID: "route-a", EndpointID: "endpoint-a", EndpointAccountHMAC: [32]byte{2}, Provider: "openai", Family: "openai_responses"}, provider.Result{}, nil)
	if len(recorder.observations) != 1 {
		t.Fatalf("recorded observations = %d, want 1", len(recorder.observations))
	}
}
