package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func TestUnknownCostQueueIsScopedStableAndExcludesExactOperations(t *testing.T) {
	operations, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()

	// Completed rows must not predate their creation time. Use future timestamps
	// only to make the queue ordering deterministic without weakening that
	// database invariant or relying on scheduler timing.
	base := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	create := func(scope, suffix string, completed time.Time, unknown bool) uuid.UUID {
		t.Helper()
		id := "unknown-cost-queue-" + suffix + "-" + uuid.NewString()
		started, err := operations.Begin(ctx, admission.BeginRequest{
			ID: id, ScopeKey: scope, RequestDigest: admission.Digest([]byte(id)),
			ReservationUSD: pricing.MustUSD("0"), ExpiresAt: base.Add(time.Hour),
			RequestManifest: []byte(`{"model":"fixture"}`),
		})
		if err != nil {
			t.Fatalf("begin %s: %v", suffix, err)
		}
		if err := operations.MarkDispatching(ctx, admission.DispatchRequest{OperationID: id, DispatchToken: started.Operation.DispatchToken}); err != nil {
			t.Fatalf("dispatch %s: %v", suffix, err)
		}
		request := admission.CompleteRequest{
			OperationID: id, DispatchToken: started.Operation.DispatchToken,
			ResultRef:     &state.BlobRef{Digest: admission.Digest([]byte("result-" + suffix)), Size: 1, Media: "application/json"},
			ActualCostUSD: pricing.MustUSD("0"),
		}
		if unknown {
			request.CostStatus = "unknown"
			request.UnknownReason = "provider_charge_unavailable"
		}
		if err := operations.Complete(ctx, request); err != nil {
			t.Fatalf("complete %s: %v", suffix, err)
		}
		opID := operationUUID(id)
		relation, err := operations.Namespace.Render("operations")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := operations.Pool.Exec(ctx, "UPDATE "+relation+" SET completed_at=$2 WHERE operation_id=$1", opID, completed); err != nil {
			t.Fatalf("set completed time %s: %v", suffix, err)
		}
		return opID
	}

	oldest := create("unknown-queue/project", "oldest", base, true)
	middle := create("unknown-queue/project", "middle", base.Add(time.Second), true)
	newest := create("unknown-queue/project", "newest", base.Add(2*time.Second), true)
	_ = create("unknown-queue/project", "exact", base.Add(3*time.Second), false)
	_ = create("other-queue/project", "other-scope", base.Add(4*time.Second), true)

	scope, err := operations.Scopes.Ensure(ctx, "unknown-queue", "project")
	if err != nil {
		t.Fatal(err)
	}
	repository := UnknownCostRepository{Pool: operations.Pool, Namespace: operations.Namespace}
	first, err := repository.List(ctx, UnknownCostListOptions{ScopeID: scope.ID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].OperationID != newest || first[1].OperationID != middle {
		t.Fatalf("first unknown-cost page = %#v, want newest then middle", first)
	}
	if first[0].UnknownReasonCode != "provider_charge_unavailable" {
		t.Fatalf("unknown-cost reason = %q", first[0].UnknownReasonCode)
	}
	second, err := repository.List(ctx, UnknownCostListOptions{
		ScopeID: scope.ID, Limit: 2,
		After: &UnknownCostCursor{CompletedAt: first[1].CompletedAt, OperationID: first[1].OperationID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].OperationID != oldest {
		t.Fatalf("second unknown-cost page = %#v, want oldest only", second)
	}
}
