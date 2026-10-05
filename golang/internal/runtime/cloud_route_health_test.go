package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/mfow/llm-temporal-worker/golang/control"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"sync"
	"testing"
	"time"
)

type sharedHealthFixture struct {
	mu           sync.Mutex
	status       control.RouteStatus
	observations int
}

func (h *sharedHealthFixture) RecordProviderStatus(_ context.Context, o control.StatusObservation) error {
	event, err := control.NewStatusEvent(o)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status.Apply(event) {
		h.observations++
	}
	return nil
}
func (h *sharedHealthFixture) GetRouteStatus(_ context.Context, digest [32]byte, route string) (control.RouteStatus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status.RouteID != route || h.status.ConfigDigest != digest {
		return control.RouteStatus{}, control.ErrProviderStatusNotFound
	}
	return h.status, nil
}

func TestCloudRouteHealthAcrossWorkersAndCooldown(t *testing.T) {
	f := boundedCloud(t, false)
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	success := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
	}
	for i := 0; i < 3; i++ {
		f.request.OperationKey = fmt.Sprintf("failure-%d", i)
		result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
		boundedState(t, result, err, llm.ExecutionFailed)
		f.now = f.now.Add(time.Second)
		f.restart(t)
	}
	if shared.status.Circuit != control.CircuitOpen || shared.observations != 3 {
		t.Fatalf("shared failures missing: %+v events=%d", shared.status, shared.observations)
	}
	f.request.OperationKey = "blocked"
	_, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeProviderUnavailable || classified.Retry == provider.RetryNever {
		t.Fatal("open circuit not retryable", err)
	}
	if f.submits.Load() != 3 {
		t.Fatal("open circuit dispatched")
	}
	f.now = f.now.Add(time.Minute)
	f.adapter.invoke = success
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if shared.status.Circuit != control.CircuitClosed || shared.status.ConsecutiveDefiniteFailures != 0 {
		t.Fatal("successful probe did not close circuit")
	}
	before := shared.observations
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	_, err = f.runtime.PollExecutionV1(context.Background(), ref)
	if err != nil || shared.observations != before {
		t.Fatal("replay duplicated health observation", err)
	}
}

func TestCloudUnknownOutcomeDoesNotOpenCircuit(t *testing.T) {
	f := boundedCloud(t, false)
	shared := &sharedHealthFixture{}
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("response lost")
	}
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	if shared.observations != 0 {
		t.Fatal("unknown outcome counted as endpoint failure")
	}
}

func TestCloudOpenCircuitDoesNotAbandonPendingProviderWork(t *testing.T) {
	f := boundedCloud(t, true)
	shared := &sharedHealthFixture{}
	f.cap.ProviderRouteStatus = shared
	f.cap.ProviderStatusRecorder = shared
	f.restart(t)
	result, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, result, err, llm.ExecutionPending)
	snapshot, err := f.cap.Snapshot.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	route := snapshot.Routes.Models["alias"].Routes[0]
	shared.status = control.RouteStatus{ConfigDigest: snapshot.ConfigDigest, ConfigEpoch: snapshot.ConfigEpoch, RouteID: route.ID, EndpointID: route.EndpointID, EndpointAccountHMAC: route.EndpointAccountHMAC, Provider: route.Provider, EndpointFamily: route.Family, Circuit: control.CircuitOpen, ObservedAt: f.now, StaleAfter: f.now.Add(time.Minute), Credit: control.CreditOK, Billing: control.BillingOK}
	f.now = f.now.Add(2 * time.Second)
	f.restart(t)
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context}
	result, err = f.runtime.PollExecutionV1(context.Background(), ref)
	boundedState(t, result, err, llm.ExecutionProviderCompleted)
	if f.submits.Load() != 1 || f.polls.Load() != 1 {
		t.Fatal("paid recovery abandoned or resubmitted")
	}
}
