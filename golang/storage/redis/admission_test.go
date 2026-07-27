package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	memory "github.com/mfow/llm-temporal-worker/golang/storage/memory"
	"github.com/redis/go-redis/v9"
)

func testKeyOptions() KeyOptions {
	return KeyOptions{Prefix: "test", HashTag: "admission", KeySecret: []byte("01234567890123456789012345678901")}
}

type admissionHarness struct {
	mu      sync.Mutex
	store   *memory.AdmissionStore
	records map[string][]byte
	indices map[string]string
}

type fixedFunctionResultInvoker struct{ result []any }

func (invoker fixedFunctionResultInvoker) Run(_ context.Context, _ string, _ []string, _ ...string) ([]any, error) {
	return append([]any(nil), invoker.result...), nil
}

func newAdmissionHarness(now time.Time) *admissionHarness {
	return &admissionHarness{store: memory.NewAdmissionStore(memory.AdmissionOptions{Clock: func() time.Time { return now }}), records: make(map[string][]byte), indices: make(map[string]string)}
}

func (h *admissionHarness) Get(_ context.Context, key string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if value, ok := h.indices[key]; ok {
		return value, nil
	}
	if value, ok := h.records[key]; ok {
		return string(value), nil
	}
	return "", redis.Nil
}

func (h *admissionHarness) Run(ctx context.Context, _ string, keys []string, args ...string) ([]any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 || len(keys) < 2 {
		return nil, errors.New("invalid fake function call")
	}
	action := args[0]
	var current admission.Operation
	var err error
	if action != "begin" {
		data, ok := h.records[keys[1]]
		if !ok {
			return []any{"not_found", ""}, nil
		}
		current, err = decodeOperation(data)
		if err != nil {
			return []any{"state_unavailable", ""}, nil
		}
	}
	switch action {
	case "begin":
		incoming, err := decodeOperation([]byte(args[1]))
		if err != nil {
			return []any{"invalid_request", ""}, nil
		}
		result, err := h.store.Begin(ctx, admission.BeginRequest{ID: incoming.ID, ScopeKey: incoming.ScopeKey, RequestDigest: incoming.RequestDigest, Reservation: incoming.ReservedMicroUSD, Reservations: incoming.Reservations, ConfigVersion: incoming.ConfigVersion, PriceVersion: incoming.PriceVersion, LeaseUntil: incoming.LeaseUntil, ExpiresAt: incoming.ExpiresAt})
		if err != nil {
			if errors.Is(err, admission.ErrOperationConflict) {
				return []any{"conflict", ""}, nil
			}
			return nil, err
		}
		if result.Denied != nil {
			denial, _ := json.Marshal(denialWire{Policy: result.Denied.PolicyID, Window: result.Denied.WindowID, Limit: int64(result.Denied.Limit), Active: int64(result.Denied.Active), Requested: int64(result.Denied.Requested)})
			return []any{"denied", "", string(denial)}, nil
		}
		encoded, _ := encodeOperation(result.Operation)
		h.records[keys[2]] = encoded
		h.indices[keys[0]] = keys[2]
		h.indices[keys[1]] = keys[2]
		status := "created"
		if result.Existing {
			status = "existing"
		}
		return []any{status, string(encoded)}, nil
	case "mark_dispatching":
		attempt, err := decodeAttempt([]byte(args[2]))
		if err != nil {
			return []any{"invalid_request", ""}, nil
		}
		err = h.store.MarkDispatching(ctx, admission.DispatchRequest{OperationID: current.ID, DispatchToken: args[1], Attempt: attempt})
	case "continue":
		outcome, err := decodeOutcome([]byte(args[2]))
		if err != nil {
			return []any{"invalid_request", ""}, nil
		}
		var reservations []reservationWire
		if err := json.Unmarshal([]byte(args[4]), &reservations); err != nil {
			return []any{"invalid_request", ""}, nil
		}
		converted := make([]admission.WindowReservation, len(reservations))
		for i, value := range reservations {
			converted[i] = admission.WindowReservation{PolicyID: value.Policy, WindowID: value.Window, Bucket: value.Bucket, Amount: pricing.MicroUSD(value.Amount), Limit: pricing.MicroUSD(value.Limit), BucketNanos: value.BucketNS, DurationNanos: value.DurationNS}
		}
		remaining, _ := parseMicro(args[3])
		result, continueErr := h.store.Continue(ctx, admission.ContinueRequest{OperationID: current.ID, DispatchToken: args[1], Outcome: outcome, Remaining: remaining, Reservations: converted})
		if continueErr != nil {
			err = continueErr
			break
		}
		encoded, _ := encodeOperation(result.Operation)
		h.records[keys[1]] = encoded
		if result.Denied != nil {
			denial, _ := json.Marshal(denialWire{Policy: result.Denied.PolicyID, Window: result.Denied.WindowID, Limit: int64(result.Denied.Limit), Active: int64(result.Denied.Active), Requested: int64(result.Denied.Requested)})
			return []any{"denied", string(encoded), string(denial)}, nil
		}
		return []any{"ok", string(encoded)}, nil
	case "complete":
		actual, _ := parseMicro(args[2])
		attempt, _ := decodeAttempt([]byte(args[4]))
		err = h.store.Complete(ctx, admission.CompleteRequest{OperationID: current.ID, DispatchToken: args[1], Actual: actual, Attempt: attempt})
	case "fail":
		incurred, _ := parseMicro(args[3])
		attempt, _ := decodeAttempt([]byte(args[4]))
		err = h.store.Fail(ctx, admission.FailRequest{OperationID: current.ID, DispatchToken: args[1], Certainty: admission.DispatchCertainty(args[2]), Incurred: incurred, Attempt: attempt})
	default:
		return []any{"invalid_request", ""}, nil
	}
	if err != nil {
		switch {
		case errors.Is(err, admission.ErrOperationNotFound):
			return []any{"not_found", ""}, nil
		case errors.Is(err, admission.ErrInvalidToken):
			return []any{"invalid_token", ""}, nil
		case errors.Is(err, admission.ErrInvalidTransition):
			return []any{"invalid_transition", ""}, nil
		default:
			return nil, err
		}
	}
	encoded, _ := encodeOperation(current)
	if updated, ok := h.store.Get(ctx, current.ID); ok == nil {
		encoded, _ = encodeOperation(updated)
		h.records[keys[1]] = encoded
	}
	return []any{"ok", string(encoded)}, nil
}

func testReservation(amount, limit pricing.MicroUSD) admission.WindowReservation {
	return admission.WindowReservation{PolicyID: "policy", WindowID: "window", Bucket: 10, Amount: amount, Limit: limit, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour)}
}

func TestContinuationReservationEnvelopeIsBoundedBeforeInvocation(t *testing.T) {
	for _, test := range []struct {
		name     string
		existing int
		next     int
		wantErr  bool
	}{
		{name: "maximum supported vectors", existing: maxRedisReservations, next: maxRedisReservations, wantErr: false},
		{name: "existing vector too large", existing: maxRedisReservations + 1, next: 1, wantErr: true},
		{name: "next vector too large", existing: 1, next: maxRedisReservations + 1, wantErr: true},
		{name: "negative existing", existing: -1, next: 1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateContinuationReservationEnvelope(test.existing, test.next)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateContinuationReservationEnvelope(%d,%d) error = %v, wantErr %v", test.existing, test.next, err, test.wantErr)
			}
		})
	}
}

func TestMarkDispatchingReservesAttemptIncrementHeadroom(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	harness := newAdmissionHarness(now)
	store, err := NewAdmissionStore(AdmissionOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	err = store.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: "op", DispatchToken: "token",
		Attempt: admission.AttemptFacts{AttemptNumber: maxAttemptNumber},
	})
	if err == nil {
		t.Fatal("dispatch accepted an attempt number that would overflow the Function increment")
	}
}

func TestAdmissionKeysUseOneHashSlotAndOpaqueDigests(t *testing.T) {
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		space.scopeKey("tenant/secret"),
		space.operationIndexKey("operation"),
		space.operationKey("tenant/secret", "operation"),
		space.budgetKey("policy", "window"),
		space.continuationIndexKey("handle"),
		space.continuationKey("tenant/secret", "handle"),
	}
	for _, key := range keys {
		if key == "" || key == "tenant/secret" || key == "operation" {
			t.Fatalf("key leaked raw identifier: %q", key)
		}
		if got := key[0 : strings.IndexByte(key, '}')+1]; got != "test:{admission}" {
			t.Fatalf("key missing configured hash tag: %q", key)
		}
	}
	if space.operationKey("tenant/secret", "operation") == space.operationKey("tenant/secret", "different") {
		t.Fatal("operation HMAC did not distinguish identifiers")
	}
}

func TestAdmissionConformanceReplayConflictAndAmbiguity(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	harness := newAdmissionHarness(now)
	store, err := NewAdmissionStore(AdmissionOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{PolicyID: "policy", WindowID: "window", Bucket: 100, Amount: 6, Limit: 10, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	request := admission.BeginRequest{ID: "op", ScopeKey: "tenant/op", RequestDigest: admission.Digest([]byte("request")), Reservation: 6, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	first, err := store.Begin(context.Background(), request)
	if err != nil || first.Existing || first.Operation.DispatchToken != "op" {
		t.Fatalf("first begin = %#v, %v", first, err)
	}
	replay, err := store.Begin(context.Background(), request)
	if err != nil || !replay.Existing || replay.Operation.ID != "op" {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	request.RequestDigest = admission.Digest([]byte("different"))
	if _, err := store.Begin(context.Background(), request); !errors.Is(err, admission.ErrOperationConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{OperationID: "op", DispatchToken: first.Operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: "route"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), admission.FailRequest{OperationID: "op", DispatchToken: "op", Certainty: admission.Ambiguous, Incurred: 0}); err != nil {
		t.Fatal(err)
	}
	operation, err := store.Get(context.Background(), "op")
	if err != nil || operation.State != admission.StateAmbiguous || operation.ReservedMicroUSD != 6 {
		t.Fatalf("ambiguous operation = %#v, %v", operation, err)
	}
}

func TestAdmissionContinueWireAndCompletion(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	harness := newAdmissionHarness(now)
	store, err := NewAdmissionStore(AdmissionOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{PolicyID: "policy", WindowID: "window", Bucket: 100, Amount: 10, Limit: 100, BucketNanos: int64(time.Second), DurationNanos: int64(time.Minute)}
	first, err := store.Begin(context.Background(), admission.BeginRequest{
		ID: "continue-op", ScopeKey: "tenant/continue", RequestDigest: admission.Digest([]byte("continue")),
		Reservation: 10, Reservations: []admission.WindowReservation{reservation}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken,
		Attempt: admission.AttemptFacts{RouteID: "initial"}, LeaseUntil: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	next := reservation
	next.Amount = 8
	continued, err := store.Continue(context.Background(), admission.ContinueRequest{
		OperationID: first.Operation.ID, DispatchToken: first.Operation.DispatchToken,
		Outcome: admission.AttemptOutcome{
			Certainty: admission.Rejected, Incurred: 2, ProviderRequestID: "provider-retry",
			Attempt: admission.AttemptFacts{RouteID: "retry", ProviderRequestID: "provider-retry", AttemptNumber: 1},
		},
		Remaining: 8, Reservations: []admission.WindowReservation{next}, LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour),
	})
	if err != nil || continued.Operation.State != admission.StateReserved {
		t.Fatalf("continue = %#v, %v", continued, err)
	}
	if continued.Operation.DispatchToken == first.Operation.DispatchToken {
		t.Fatal("continue did not rotate dispatch token")
	}
	if err := store.MarkDispatching(context.Background(), admission.DispatchRequest{
		OperationID: first.Operation.ID, DispatchToken: continued.Operation.DispatchToken,
		Attempt: continued.Operation.Attempt, LeaseUntil: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), admission.CompleteRequest{
		OperationID: first.Operation.ID, DispatchToken: continued.Operation.DispatchToken,
		Actual: 8, Attempt: admission.AttemptFacts{RouteID: "retry", ProviderRequestID: "provider-retry"},
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Get(context.Background(), first.Operation.ID)
	if err != nil || completed.State != admission.StateCompleted || completed.FinalMicroUSD != 8 || completed.Attempt.Dispatch != admission.Accepted {
		t.Fatalf("completed continuation = %#v, %v", completed, err)
	}
}

func TestAdmissionDeniesOverlappingReservationAtomically(t *testing.T) {
	now := time.Unix(100, 0)
	harness := newAdmissionHarness(now)
	store, err := NewAdmissionStore(AdmissionOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions(), Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	reservation := admission.WindowReservation{PolicyID: "policy", WindowID: "window", Bucket: 100, Amount: 6, Limit: 10, BucketNanos: int64(time.Second), DurationNanos: int64(10 * time.Second)}
	first, err := store.Begin(context.Background(), admission.BeginRequest{ID: "one", ScopeKey: "one", RequestDigest: admission.Digest([]byte("one")), Reservation: 6, Reservations: []admission.WindowReservation{reservation}, ExpiresAt: now.Add(time.Hour)})
	if err != nil || first.Denied != nil {
		t.Fatalf("first begin = %#v, %v", first, err)
	}
	reservation.Amount = 5
	second, err := store.Begin(context.Background(), admission.BeginRequest{ID: "two", ScopeKey: "two", RequestDigest: admission.Digest([]byte("two")), Reservation: 5, Reservations: []admission.WindowReservation{reservation}, ExpiresAt: now.Add(time.Hour)})
	if err != nil || second.Denied == nil || second.Denied.Active != 6 {
		t.Fatalf("second begin = %#v, %v", second, err)
	}
}

func TestAdmissionBeginDecodesTwoFieldDeniedFunctionResult(t *testing.T) {
	now := time.Unix(100, 0)
	denialData, err := json.Marshal(denialWire{Policy: "policy", Window: "window", Limit: 10, Active: 6, Requested: 5})
	if err != nil {
		t.Fatal(err)
	}
	reader := newAdmissionHarness(now)
	store, err := NewAdmissionStore(AdmissionOptions{
		Invoker: fixedFunctionResultInvoker{result: []any{"denied", string(denialData)}},
		Reader:  reader,
		Keys:    testKeyOptions(),
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Begin(context.Background(), admission.BeginRequest{
		ID:            "two-field-denial",
		ScopeKey:      "tenant/two-field-denial",
		RequestDigest: admission.Digest([]byte("two-field-denial")),
		Reservation:   5,
		Reservations:  []admission.WindowReservation{testReservation(5, 10)},
		ExpiresAt:     now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("two-field denial = %v", err)
	}
	if result.Denied == nil || result.Denied.PolicyID != "policy" || result.Denied.WindowID != "window" || result.Denied.Active != 6 || result.Denied.Requested != 5 {
		t.Fatalf("two-field denial = %#v", result)
	}
}
