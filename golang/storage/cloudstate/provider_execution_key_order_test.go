package cloudstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// Saved progress is canonical, so the terminal result reloaded from storage
// spells tool arguments differently from the response still in memory. Key
// order alone must not refuse the settlement acknowledgement.
func TestProviderExecutionSettlementIgnoresArgumentKeyOrder(t *testing.T) {
	r, _, _, record, plan, reservation := executionFixture(t)
	saved := startExecution(t, r, record, plan, reservation)
	ctx := context.Background()
	response := func(arguments string) *llm.Response {
		return &llm.Response{OperationKey: "operation", Status: llm.ResponseStatusToolCalls, Output: []llm.Item{llm.ToolCall{ID: "call", Name: "search", Arguments: json.RawMessage(arguments)}}}
	}
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) { e.Stage = ExecutionSubmitting; e.Claim = executionClaim(reservation) })
	saved = advanceExecution(t, r, record, saved, func(e *ProviderExecution) {
		e.Stage = ExecutionSucceeded
		e.CompletedAt = e.UpdatedAt.Add(time.Second)
		e.Response = response(`{"query":"x","limit":5}`)
	})
	loaded, err := r.LoadProviderExecution(ctx, record.Request.Scope, record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if call, ok := loaded.Execution.Response.Output[0].(llm.ToolCall); !ok || string(call.Arguments) != `{"limit":5,"query":"x"}` {
		t.Fatalf("stored output = %+v", loaded.Execution.Response.Output)
	}
	// A genuinely different result is still refused, in either spelling.
	for _, arguments := range []string{`{"query":"y","limit":5}`, `{"limit":5,"query":"y"}`, `{"query":"x"}`} {
		different := saved.Execution
		different.Revision++
		different.Settled = true
		different.Response = response(arguments)
		if validExecutionTransition(loaded.Execution, different) {
			t.Fatalf("terminal response replaced by %s", arguments)
		}
	}
	// The value still in memory keeps provider key order.
	next := saved.Execution
	next.Revision++
	next.Settled = true
	if !validExecutionTransition(loaded.Execution, next) {
		t.Fatal("settlement acknowledgement refused for key order alone")
	}
	// Nothing but the acknowledgement may change on a terminal result.
	next.Response.Status = llm.ResponseStatusCompleted
	if validExecutionTransition(loaded.Execution, next) {
		t.Fatal("terminal response status replaced")
	}
}
