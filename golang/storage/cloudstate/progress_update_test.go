package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEncodeProgressRejectsOversizedPayload(t *testing.T) {
	data, err := encodeProgress(map[string]json.RawMessage{"version": json.RawMessage(`1`)})
	if err != nil || string(data) != `{"version":1}` {
		t.Fatalf("encode = %s, %v", data, err)
	}
	large := json.RawMessage(`"` + strings.Repeat("x", maxPayloadBytes) + `"`)
	if _, err := encodeProgress(map[string]json.RawMessage{"large": large}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized encode error = %v, want ErrInvalid", err)
	}
}

func TestTryProgressUpdateClampsTimeAndReportsRaces(t *testing.T) {
	r, _, _, request := fixture(t)
	ctx := context.Background()
	record := mustCreate(t, r, request)
	progress := json.RawMessage(`{"version":1}`)
	earlier := record.UpdatedAt.Add(-time.Hour)
	if retry, err := r.tryProgressUpdate(ctx, request.Scope, request.ID, record, "first", StatusRunning, progress, earlier); retry || err != nil {
		t.Fatalf("first update = %v, %v", retry, err)
	}
	updated, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil || updated.Revision != record.Revision+1 || !updated.UpdatedAt.Equal(record.UpdatedAt) || updated.Status != StatusRunning {
		t.Fatalf("updated record = %+v, %v; want clamped UpdatedAt %v", updated, err, record.UpdatedAt)
	}
	// record is now stale: the lost race is a retry, not a final error.
	if retry, err := r.tryProgressUpdate(ctx, request.Scope, request.ID, record, "stale", StatusRunning, progress, earlier); !retry || err == nil {
		t.Fatalf("stale update = %v, %v; want retry", retry, err)
	}
	if retry, err := r.tryProgressUpdate(ctx, request.Scope, request.ID, updated, "invalid", StatusPending, progress, earlier); retry || !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid transition = %v, %v; want final ErrInvalid", retry, err)
	}
}
