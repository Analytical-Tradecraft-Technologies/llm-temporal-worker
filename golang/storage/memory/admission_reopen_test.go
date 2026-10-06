package memory

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
)

// Only a retryable definite failure reopens on an identical Begin; it gets a
// fresh reservation and dispatch token. A non-retryable one stays terminal and
// a different request digest still conflicts.
func TestAdmissionReopensOnlyARetryableDefiniteFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100, 0)
	store := NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }})
	reservation := admission.WindowReservation{PolicyID: "p", WindowID: "w", Bucket: 100, Amount: 10, Limit: 10, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	begin := func(id string, digest string) admission.BeginRequest {
		return admission.BeginRequest{ID: id, ScopeKey: "tenant/" + id, RequestDigest: admission.Digest([]byte(digest)), Reservation: 10, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	}
	fail := func(request admission.BeginRequest, retryable bool) admission.Operation {
		t.Helper()
		started, err := store.Begin(ctx, request)
		if err != nil || started.Existing || started.Denied != nil {
			t.Fatalf("begin = %#v %v", started, err)
		}
		token := started.Operation.DispatchToken
		if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: request.ID, DispatchToken: token, LeaseUntil: now.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if err := store.Fail(ctx, admission.FailRequest{OperationID: request.ID, DispatchToken: token, Certainty: admission.Rejected, Retryable: retryable}); err != nil {
			t.Fatal(err)
		}
		return started.Operation
	}

	retryable := begin("retryable", "request")
	first := fail(retryable, true)
	conflicting := retryable
	conflicting.RequestDigest = admission.Digest([]byte("different"))
	if _, err := store.Begin(ctx, conflicting); err != admission.ErrOperationConflict {
		t.Fatalf("different digest = %v, want conflict", err)
	}
	reopened, err := store.Begin(ctx, retryable)
	if err != nil || reopened.Existing || reopened.Operation.State != admission.StateReserved || reopened.Operation.ReservedMicroUSD != 10 || reopened.Operation.Retryable {
		t.Fatalf("reopen = %#v %v", reopened, err)
	}
	if reopened.Operation.DispatchToken == first.DispatchToken || !reopened.Operation.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("reopened token %q (first %q), created %v (first %v)", reopened.Operation.DispatchToken, first.DispatchToken, reopened.Operation.CreatedAt, first.CreatedAt)
	}
	// The stale token of the failed attempt cannot drive the reopened one.
	if err := store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: "retryable", DispatchToken: first.DispatchToken, LeaseUntil: now.Add(time.Minute)}); err != admission.ErrInvalidToken {
		t.Fatalf("stale token = %v, want invalid token", err)
	}
	// The reopened attempt holds the whole window, so release it before the
	// next case reserves.
	if err := store.Fail(ctx, admission.FailRequest{OperationID: "retryable", DispatchToken: reopened.Operation.DispatchToken, Certainty: admission.NotDispatched}); err != nil {
		t.Fatal(err)
	}

	terminal := begin("terminal", "request")
	fail(terminal, false)
	existing, err := store.Begin(ctx, terminal)
	if err != nil || !existing.Existing || existing.Operation.State != admission.StateDefiniteFailed {
		t.Fatalf("non-retryable begin = %#v %v, want the terminal operation", existing, err)
	}
}
