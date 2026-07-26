package postgres

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

func TestOutboxStateUpdateUsesCallerClockForLeaseFence(t *testing.T) {
	completed := outboxStateUpdateSQL(`"llm_worker"."maintenance_outbox"`, true)
	if strings.Contains(completed, "clock_timestamp()") || !strings.Contains(completed, "lease_expires_at > $3") {
		t.Fatalf("completion update does not use caller timestamp: %s", completed)
	}
	retried := outboxStateUpdateSQL(`"llm_worker"."maintenance_outbox"`, false)
	if strings.Contains(retried, "clock_timestamp()") || !strings.Contains(retried, "lease_expires_at > $4") {
		t.Fatalf("retry update does not use caller timestamp: %s", retried)
	}
}

func TestNewDeleteBlobOutboxEventUsesTypedContract(t *testing.T) {
	now := time.Date(2026, 7, 26, 4, 0, 0, 0, time.UTC)
	eventID := uuid.MustParse("018f0b0e-4d5c-7d4a-8c3b-6e7f8a9b0c1e")
	blobID := uuid.MustParse("018f0b0e-4d5c-7d4a-8c3b-6e7f8a9b0c1f")
	event, err := newDeleteBlobOutboxEvent(eventID, blobID, now)
	if err != nil {
		t.Fatal(err)
	}
	if event.Kind != maintenance.EventDeleteBlob || event.AggregateType != "blob" || event.AggregateID != blobID.String() {
		t.Fatalf("unexpected event identity: %+v", event)
	}
	if !bytes.Equal(event.SafePayload, []byte(`{"blob_id":"018f0b0e-4d5c-7d4a-8c3b-6e7f8a9b0c1f"}`)) {
		t.Fatalf("unexpected typed payload: %s", event.SafePayload)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("typed event failed validation: %v", err)
	}
}

func TestNewDeleteBlobOutboxEventRejectsMissingIDsOrTime(t *testing.T) {
	now := time.Date(2026, 7, 26, 4, 0, 0, 0, time.UTC)
	eventID := uuid.New()
	blobID := uuid.New()
	tests := []struct {
		name    string
		eventID uuid.UUID
		blobID  uuid.UUID
		now     time.Time
	}{
		{name: "missing event", blobID: blobID, now: now},
		{name: "missing blob", eventID: eventID, now: now},
		{name: "missing time", eventID: eventID, blobID: blobID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newDeleteBlobOutboxEvent(test.eventID, test.blobID, test.now); err == nil {
				t.Fatal("missing cache deletion event input was accepted")
			}
		})
	}
}
