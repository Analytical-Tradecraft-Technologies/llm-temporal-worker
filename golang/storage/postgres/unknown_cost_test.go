package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestUnknownCostListOptionsNormalize(t *testing.T) {
	now := time.Date(2026, time.July, 28, 2, 3, 4, 0, time.FixedZone("test", 3600))
	options := UnknownCostListOptions{
		ScopeID: uuid.New(), Limit: 3,
		After: &UnknownCostCursor{CompletedAt: now, OperationID: uuid.New()},
	}
	if err := options.normalize(); err != nil {
		t.Fatal(err)
	}
	if options.After.CompletedAt.Location() != time.UTC {
		t.Fatalf("cursor time location = %v, want UTC", options.After.CompletedAt.Location())
	}
	for _, test := range []struct {
		name    string
		options UnknownCostListOptions
	}{
		{name: "missing scope", options: UnknownCostListOptions{Limit: 1}},
		{name: "zero limit", options: UnknownCostListOptions{ScopeID: uuid.New()}},
		{name: "excessive limit", options: UnknownCostListOptions{ScopeID: uuid.New(), Limit: maxMaintenanceBatch + 1}},
		{name: "cursor missing time", options: UnknownCostListOptions{ScopeID: uuid.New(), Limit: 1, After: &UnknownCostCursor{OperationID: uuid.New()}}},
		{name: "cursor missing operation", options: UnknownCostListOptions{ScopeID: uuid.New(), Limit: 1, After: &UnknownCostCursor{CompletedAt: time.Now()}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.normalize(); err == nil {
				t.Fatal("invalid unknown-cost page options were accepted")
			}
		})
	}
}

func TestUnknownCostListQueryUsesScopedPartialIndexShape(t *testing.T) {
	query := unknownCostListQuery(`"worker"."operations"`)
	for _, expected := range []string{
		`FROM "worker"."operations"`,
		`scope_id=$1 AND state='completed' AND cost_status='unknown'`,
		`(completed_at, operation_id) < ($2::timestamptz, $3::uuid)`,
		`ORDER BY completed_at DESC, operation_id DESC LIMIT $4`,
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("unknown-cost query missing %q: %s", expected, query)
		}
	}
	if strings.Contains(strings.ToUpper(query), "UPDATE") || strings.Contains(strings.ToUpper(query), "INSERT") || strings.Contains(strings.ToUpper(query), "DELETE") {
		t.Fatalf("unknown-cost queue must remain read-only: %s", query)
	}
}

func TestUnknownCostRepositoryRejectsMissingPool(t *testing.T) {
	repository := UnknownCostRepository{}
	if _, err := repository.List(t.Context(), UnknownCostListOptions{}); err == nil {
		t.Fatal("repository accepted nil pool")
	}
}
