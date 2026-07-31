package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func testBlobEvent(t *testing.T, id string, at time.Time) Event {
	t.Helper()
	event, err := NewDeleteBlobEvent(id, "blob-"+id, at, at)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestNormalizeSafePayloadBoundsAndCanonicalizes(t *testing.T) {
	canonical, err := NormalizeSafePayload(json.RawMessage(`{"z":2,"a":{"b":1,"a":0}}`))
	if err != nil || string(canonical) != `{"a":{"a":0,"b":1},"z":2}` {
		t.Fatalf("canonical payload=%s err=%v", canonical, err)
	}
	for _, raw := range []string{`{"duplicate":1,"duplicate":2}`, `[1]`, `null`, `{"a":`, `{"a":1} trailing`} {
		if _, err := NormalizeSafePayload(json.RawMessage(raw)); err == nil {
			t.Fatalf("payload %q unexpectedly accepted", raw)
		}
	}
	if _, err := NormalizeSafePayload(json.RawMessage(`{"payload":"` + strings.Repeat("x", MaxSafePayloadBytes) + `"}`)); err == nil {
		t.Fatal("oversized payload unexpectedly accepted")
	}
}

func TestOutboxLeaseBoundsAreValidatedBeforeClaim(t *testing.T) {
	at := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	claim := ClaimOptions{Now: at, Limit: 1, Lease: MaxOutboxLease}
	if err := claim.Validate(); err != nil {
		t.Fatalf("maximum claim lease rejected: %v", err)
	}
	claim.Lease = MaxOutboxLease + time.Nanosecond
	if err := claim.Validate(); err == nil {
		t.Fatal("claim lease beyond maximum was accepted")
	}

	dispatch := DispatchOptions{Now: at, Limit: 1, Lease: MaxOutboxLease, RetryDelay: time.Minute}
	if err := dispatch.Validate(); err != nil {
		t.Fatalf("maximum dispatch lease rejected: %v", err)
	}
	dispatch.Lease = MaxOutboxLease + time.Nanosecond
	if err := dispatch.Validate(); err == nil {
		t.Fatal("dispatch lease beyond maximum was accepted")
	}
}

func TestDeleteBlobPayloadIsTypedAndBoundToAggregate(t *testing.T) {
	at := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	event := testBlobEvent(t, "typed", at)

	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{name: "unknown field", payload: `{"blob_id":"blob-typed","prompt":"never persist this"}`, wantErr: "unknown field"},
		{name: "mismatched aggregate", payload: `{"blob_id":"other-blob"}`, wantErr: "match aggregate"},
		{name: "missing identifier", payload: `{}`, wantErr: "blob_id is required"},
		{name: "trailing value", payload: `{"blob_id":"blob-typed"} false`, wantErr: "trailing"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := event
			candidate.SafePayload = json.RawMessage(test.payload)
			err := candidate.Validate()
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate(%s) error=%v, want substring %q", test.payload, err, test.wantErr)
			}
		})
	}

	// Non-canonical key order is still accepted: validation checks the typed
	// contract while Publish/Claim canonicalize the representation for dedupe.
	valid := event
	valid.SafePayload = json.RawMessage(`{ "blob_id": "blob-typed" }`)
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid reordered delete-blob payload rejected: %v", err)
	}

	wrongAggregateType := event
	wrongAggregateType.AggregateType = "provider_state"
	if err := wrongAggregateType.Validate(); err == nil || !strings.Contains(err.Error(), "aggregate type") {
		t.Fatalf("wrong aggregate type accepted: %v", err)
	}

	reserved := event
	reserved.Kind = EventRefreshInventory
	if err := reserved.Validate(); err == nil || !strings.Contains(err.Error(), "typed payload contract") {
		t.Fatalf("untyped reserved event accepted: %v", err)
	}
}

func TestInMemoryOutboxDedupeAndBoundedConcurrentClaim(t *testing.T) {
	at := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	event := testBlobEvent(t, "event-1", at)
	store, err := NewInMemoryOutbox([]Event{event})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(context.Background(), event); err != nil {
		t.Fatalf("idempotent publish failed: %v", err)
	}
	// JSON object ordering is not part of the dedupe identity. The store keeps
	// one canonical representation so retries from independently encoded
	// callers remain idempotent.
	reordered := event
	reordered.SafePayload = json.RawMessage(`{ "blob_id": "blob-event-1" }`)
	if err := store.Publish(context.Background(), reordered); err != nil {
		t.Fatalf("equivalent payload replay was not idempotent: %v", err)
	}
	conflict := event
	conflict.ID = "event-2"
	conflict.AggregateID = "different"
	conflict.SafePayload = json.RawMessage(`{"blob_id":"different"}`)
	if err := store.Publish(context.Background(), conflict); !errors.Is(err, ErrOutboxConflict) {
		t.Fatalf("expected dedupe conflict, got %v", err)
	}

	options := ClaimOptions{Now: at, Limit: 1, Lease: time.Minute}
	var wg sync.WaitGroup
	claimed := make(chan []Event, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, claimErr := store.Claim(context.Background(), options)
			if claimErr != nil {
				t.Errorf("claim failed: %v", claimErr)
				return
			}
			claimed <- items
		}()
	}
	wg.Wait()
	close(claimed)
	total := 0
	for items := range claimed {
		total += len(items)
	}
	if total != 1 {
		t.Fatalf("concurrent claims returned %d rows, want exactly one", total)
	}
	items, err := store.Claim(context.Background(), ClaimOptions{Now: at.Add(2 * time.Minute), Limit: 1, Lease: time.Minute})
	if err != nil || len(items) != 1 {
		t.Fatalf("claim for completion failed: items=%+v err=%v", items, err)
	}
	if err := store.Complete(context.Background(), event.ID, items[0].LeaseToken, at.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), event.ID, items[0].LeaseToken, at.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatalf("duplicate completion was not idempotent: %v", err)
	}
}

func TestDispatcherMakesMissingObjectSuccessAndRetriesFailures(t *testing.T) {
	at := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	missing := testBlobEvent(t, "missing", at)
	failing := testBlobEvent(t, "failing", at)
	store, err := NewInMemoryOutbox([]Event{missing, failing})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := Dispatcher{Store: store, Delete: func(_ context.Context, event Event) error {
		if event.ID == missing.ID {
			return ErrObjectNotFound
		}
		return errors.New("object store unavailable")
	}}
	result, err := dispatcher.RunOnce(context.Background(), DispatchOptions{Now: at, Limit: 10, Lease: time.Minute, RetryDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Completed != 1 || result.MissingObject != 1 || result.Retried != 1 {
		t.Fatalf("unexpected dispatch result: %+v", result)
	}
	for _, event := range store.Snapshot() {
		switch event.ID {
		case missing.ID:
			if event.State != EventCompleted {
				t.Errorf("missing object did not complete: %+v", event)
			}
		case failing.ID:
			if event.State != EventFailed || event.AttemptCount != 1 || !event.AvailableAt.Equal(at.Add(time.Minute)) {
				t.Errorf("failed object was not retryable: %+v", event)
			}
		}
	}
	claimed, err := store.Claim(context.Background(), ClaimOptions{Now: at.Add(time.Minute), Limit: 1, Lease: time.Minute})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim failed-row recovery failed: items=%+v err=%v", claimed, err)
	}
	if err := store.Retry(context.Background(), claimed[0].ID, claimed[0].LeaseToken, at.Add(time.Minute), at.Add(2*time.Minute)); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if err := store.Retry(context.Background(), claimed[0].ID, claimed[0].LeaseToken, at.Add(time.Minute+time.Second), at.Add(3*time.Minute)); err != nil {
		t.Fatalf("duplicate retry was not idempotent: %v", err)
	}
	for _, event := range store.Snapshot() {
		if event.ID == claimed[0].ID && !event.AvailableAt.Equal(at.Add(2*time.Minute)) {
			t.Fatalf("duplicate retry changed the original retry schedule: %+v", event)
		}
	}
}

func TestOutboxLeaseRecovery(t *testing.T) {
	at := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	event := testBlobEvent(t, "lease", at)
	store, err := NewInMemoryOutbox([]Event{event})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Claim(context.Background(), ClaimOptions{Now: at, Limit: 1, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if items, err := store.Claim(context.Background(), ClaimOptions{Now: at.Add(30 * time.Second), Limit: 1, Lease: time.Minute}); err != nil {
		t.Fatal(err)
	} else if len(items) != 0 {
		t.Fatal("live lease was claimed twice")
	}
	items, err := store.Claim(context.Background(), ClaimOptions{Now: at.Add(2 * time.Minute), Limit: 1, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].AttemptCount != 2 {
		t.Fatalf("expired lease was not recovered: %+v", items)
	}
	if first[0].LeaseToken == items[0].LeaseToken {
		t.Fatal("reclaimed outbox item reused the old lease token")
	}
	if err := store.Complete(context.Background(), event.ID, first[0].LeaseToken, at.Add(2*time.Minute+time.Second)); !errors.Is(err, ErrOutboxNotClaimed) {
		t.Fatalf("stale claimant completed reclaimed item: %v", err)
	}
	if err := store.Retry(context.Background(), event.ID, first[0].LeaseToken, at.Add(2*time.Minute+time.Second), at.Add(3*time.Minute)); !errors.Is(err, ErrOutboxNotClaimed) {
		t.Fatalf("stale claimant retried reclaimed item: %v", err)
	}
	if err := store.Complete(context.Background(), event.ID, items[0].LeaseToken, at.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatalf("current claimant could not complete item: %v", err)
	}
}
