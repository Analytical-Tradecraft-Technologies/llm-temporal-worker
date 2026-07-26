package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// timeoutAfterMutationFunctionInvoker models the only safe response to a
// transport timeout: the server-side function has already run, but its reply
// was lost. The mutation is retained by inner and the caller must resolve it
// with a read instead of retrying blindly.
type timeoutAfterMutationFunctionInvoker struct {
	inner FunctionInvoker
	once  atomic.Bool
}

func (invoker *timeoutAfterMutationFunctionInvoker) Run(ctx context.Context, name string, keys []string, args ...string) ([]any, error) {
	result, err := invoker.inner.Run(ctx, name, keys, args...)
	if err != nil {
		return nil, err
	}
	if invoker.once.CompareAndSwap(false, true) {
		return nil, context.DeadlineExceeded
	}
	return result, nil
}

// timeoutAfterMutationContinuationInvoker applies the same lost-reply model
// to immutable continuation writes. PutChild's operation index is the read
// resolution path for a committed child.
type timeoutAfterMutationContinuationInvoker struct {
	inner ContinuationInvoker
	once  atomic.Bool
}

func (invoker *timeoutAfterMutationContinuationInvoker) Put(ctx context.Context, keys []string, value, handle, ttl string) (string, error) {
	result, err := invoker.inner.Put(ctx, keys, value, handle, ttl)
	if err != nil {
		return "", err
	}
	if invoker.once.CompareAndSwap(false, true) {
		return "", context.DeadlineExceeded
	}
	return result, nil
}

func TestAdmissionMutationTimeoutsResolveByRead(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AdmissionStore, admission.Operation, time.Time) error
		assert func(t *testing.T, operation admission.Operation)
	}{
		{
			name: "complete",
			mutate: func(store *AdmissionStore, operation admission.Operation, now time.Time) error {
				return store.Complete(context.Background(), admission.CompleteRequest{
					OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: 3,
					Attempt: admission.AttemptFacts{RouteID: "timeout-complete"},
				})
			},
			assert: func(t *testing.T, operation admission.Operation) {
				if operation.State != admission.StateCompleted || operation.FinalMicroUSD != 3 || operation.ReservedMicroUSD != 0 {
					t.Fatalf("resolved complete = %#v", operation)
				}
			},
		},
		{
			name: "definite failure",
			mutate: func(store *AdmissionStore, operation admission.Operation, now time.Time) error {
				return store.Fail(context.Background(), admission.FailRequest{
					OperationID: operation.ID, DispatchToken: operation.DispatchToken,
					Certainty: admission.Rejected, Incurred: 2,
					Attempt: admission.AttemptFacts{RouteID: "timeout-fail"},
				})
			},
			assert: func(t *testing.T, operation admission.Operation) {
				if operation.State != admission.StateDefiniteFailed || operation.FinalMicroUSD != 2 || operation.ReservedMicroUSD != 0 {
					t.Fatalf("resolved failure = %#v", operation)
				}
			},
		},
		{
			name: "continue",
			mutate: func(store *AdmissionStore, operation admission.Operation, now time.Time) error {
				reservation := testReservation(8, 100)
				return func() error {
					_, err := store.Continue(context.Background(), admission.ContinueRequest{
						OperationID: operation.ID, DispatchToken: operation.DispatchToken,
						Outcome: admission.AttemptOutcome{
							Certainty: admission.Rejected, Incurred: 2,
							Attempt: admission.AttemptFacts{RouteID: "timeout-continue", AttemptNumber: 1},
						},
						Remaining: 8, Reservations: []admission.WindowReservation{reservation},
						LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour),
					})
					return err
				}()
			},
			assert: func(t *testing.T, operation admission.Operation) {
				if operation.State != admission.StateReserved || operation.ReservedMicroUSD != 8 || operation.DispatchToken == "" {
					t.Fatalf("resolved continuation = %#v", operation)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			harness := newAdmissionHarness(now)
			store, err := NewAdmissionStore(AdmissionOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			reservation := testReservation(10, 100)
			result, err := store.Begin(context.Background(), admission.BeginRequest{
				ID: "timeout-" + test.name, ScopeKey: "tenant/timeout-" + test.name,
				RequestDigest: admission.Digest([]byte(test.name)), Reservation: 10,
				Reservations: []admission.WindowReservation{reservation}, ExpiresAt: now.Add(time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{
				OperationID: result.Operation.ID, DispatchToken: result.Operation.DispatchToken,
				Attempt: admission.AttemptFacts{RouteID: "before-timeout"}, LeaseUntil: now.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			store.invoke = &timeoutAfterMutationFunctionInvoker{inner: harness}
			if err := test.mutate(store, result.Operation, now); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("mutation error = %v, want unresolved shared state", err)
			}
			resolved, err := store.Get(context.Background(), result.Operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			test.assert(t, resolved)
		})
	}
}

func TestContinuationPutChildTimeoutResolvesByOperationIndex(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	harness := newContinuationHarness()
	store, err := NewContinuationStore(ContinuationOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Keyring: testKeyring(t), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	child := testContinuation(t, now)
	child.ParentID = parent.String()
	child.Depth = 1
	const operationKey = "timeout-child-operation"
	store.invoke = &timeoutAfterMutationContinuationInvoker{inner: harness}
	if _, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child, OperationKey: operationKey}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("child mutation error = %v, want unresolved shared state", err)
	}
	resolved, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child, OperationKey: operationKey})
	if err != nil {
		t.Fatalf("child retry resolution = %v", err)
	}
	loaded, err := store.Get(context.Background(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ParentID != parent.String() || loaded.Depth != 1 {
		t.Fatalf("resolved child = %#v", loaded)
	}
}
