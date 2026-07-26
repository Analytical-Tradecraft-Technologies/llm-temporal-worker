package postgres

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/maintenance"
)

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
