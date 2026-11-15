package cloudstate

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// TestProviderResponseLimitFitsRecord checks the configuration bound that
// keeps every provider response the worker reads savable (#1319) against the
// record encoding it relies on: the record limit it mirrors, the six-byte
// escape it assumes, and a worst-case save beside the largest request.
func TestProviderResponseLimitFitsRecord(t *testing.T) {
	if config.DurableRecordBytes != MaxRecordBytes {
		t.Fatalf("config.DurableRecordBytes = %d, but the record limit is %d", config.DurableRecordBytes, MaxRecordBytes)
	}
	escaped, err := json.Marshal("<>&")
	if err != nil || int64(len(escaped)-2) != 3*config.SavedResponseExpansion {
		t.Fatalf("encoding/json writes %q, not %d bytes per character", escaped, config.SavedResponseExpansion)
	}

	inline := config.TemporalBlobLimitBytes
	limit := config.MaxSavableProviderResponseBytes(inline)
	if limit < config.DefaultProviderResponseBytes {
		t.Fatalf("default provider response limit %d exceeds %d, the most a record can save beside a %d-byte request", config.DefaultProviderResponseBytes, limit, inline)
	}
	r, _, _, op := operationFixture(t)
	const manifestFraming = len(`{"pad":""}`)
	op.Manifest = json.RawMessage(`{"pad":"` + strings.Repeat("x", inline-manifestFraming) + `"}`)
	record, plan := budgetPlanForOperation(t, r, op)
	plan, reservation := reserveExecutionPlan(t, r, record, plan)
	saved := startExecution(t, r, record, plan, reservation)
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(reservation) })
	// No body of limit bytes holds more response text than this, and every
	// '<' is saved as a six-byte escape.
	text := strings.Repeat("<", int(limit))
	advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionSucceeded
		e.CompletedAt = e.UpdatedAt.Add(time.Second)
		e.Response = &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ModelText(text)}}
	})

	current, err := r.Read(context.Background(), record.Request.Scope, record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeRecord(current)
	if err != nil {
		t.Fatal(err)
	}
	response := config.SavedResponseExpansion * limit
	bookkeeping := int64(len(encoded)) - response - int64(len(op.Manifest))
	if bookkeeping < 0 {
		t.Fatalf("record of %d bytes did not save the response at %d bytes per body byte", len(encoded), config.SavedResponseExpansion)
	}
	if bookkeeping > config.DurableRecordBookkeepingBytes {
		t.Fatalf("record bookkeeping is %d bytes, more than the %d bytes the bound reserves", bookkeeping, config.DurableRecordBookkeepingBytes)
	}
}
