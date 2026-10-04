package cloudstate

import (
	"context"
	"errors"
	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"strings"
	"testing"
	"time"
)

func TestRequestExhaustionRequiresSettledFailureAndLimit(t *testing.T) {
	r, table, blobs, root, a, failure, now := failedRequestFixture(t, true)
	ctx, scope := context.Background(), root.Request.Scope
	if _, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("unfinalized child exhausted", err)
	}
	if err := r.FinishRequestFailure(ctx, scope, root.Request.ID, a.ID, failure, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 2, now); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("unused attempt capacity discarded", err)
	}
	result, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now)
	if err != nil || result.Retryable || result.Validate() != nil {
		t.Fatal("invalid terminal failure", result, err)
	}
	replay, err := reopen(t, table, blobs).FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now.Add(time.Second))
	if err != nil || !equalExecutionJSON(replay, result) {
		t.Fatal("exhaustion not durable", err)
	}
	if _, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, a.ID, now.Add(time.Second)); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("terminal root renewed", err)
	}
}

func TestRequestExhaustionCannotCloseNewerAttempt(t *testing.T) {
	r, _, _, root, a, failure, now := failedRequestFixture(t, true)
	ctx, scope := context.Background(), root.Request.Scope
	if err := r.FinishRequestFailure(ctx, scope, root.Request.ID, a.ID, failure, now); err != nil {
		t.Fatal(err)
	}
	next, err := r.BeginRequestAttempt(ctx, scope, root.Request.ID, a.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("old attempt closed newer root", err)
	}
	if _, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, next.ID, 1, now); err == nil {
		t.Fatal("unused child exhausted")
	}
	record, err := r.Read(ctx, scope, root.Request.ID)
	if err != nil || record.Status != StatusRunning {
		t.Fatal("root closed", err)
	}
}

func TestRequestExhaustionRepairsLostWrites(t *testing.T) {
	for _, event := range []bool{false, true} {
		t.Run(map[bool]string{false: "index", true: "event"}[event], func(t *testing.T) {
			r, table, blobs, root, a, failure, now := failedRequestFixture(t, true)
			ctx, scope := context.Background(), root.Request.Scope
			if err := r.FinishRequestFailure(ctx, scope, root.Request.ID, a.ID, failure, now); err != nil {
				t.Fatal(err)
			}
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				if event && action == "create" && strings.Contains(item.PartitionKey, "/request/") {
					return nil, contracts.ErrOutcomeUnknown
				}
				if !event && action == "replace" && strings.Contains(item.PartitionKey, "/pending/") {
					return contracts.ErrUnavailable, nil
				}
				return nil, nil
			}
			if _, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now); err == nil {
				t.Fatal("fault ignored")
			}
			table.hook = nil
			r = reopen(t, table, blobs)
			result, err := r.FinishRequestExhausted(ctx, scope, root.Request.ID, a.ID, 1, now.Add(time.Second))
			if err != nil || result.Retryable || result.Validate() != nil {
				t.Fatal("exhaustion recovery failed", err)
			}
			shard, _ := PendingShard(root.Request.ID)
			page, err := r.ListPending(ctx, shard, 100, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range page.Requests {
				if entry.ID == root.Request.ID {
					t.Fatal("failed root remains pending")
				}
			}
		})
	}
}
