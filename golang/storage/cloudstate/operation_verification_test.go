package cloudstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestVerifyOperationNeverStartsOrRepairsRequests(t *testing.T) {
	for _, state := range []Status{StatusPending, StatusRunning, StatusCompleted} {
		t.Run(string(state), func(t *testing.T) {
			r, table, blobs, op := operationFixture(t)
			var before Record
			var err error
			if state == StatusPending {
				before, err = r.Create(context.Background(), CreateRequest{ID: r.operationID(op.Scope, op.Kind, op.Key), Scope: op.Scope, Kind: op.Kind, Manifest: op.Manifest, CreatedAt: op.Now})
			} else {
				before, err = r.BeginOperation(context.Background(), op)
			}
			testassert.NoError(t, err)
			if state == StatusCompleted {
				before = mustCompleteOperation(t, r, op.Scope, before.Request.ID, json.RawMessage(`{"version":1}`), op.Now)
			}
			forbidWrites(t, table, blobs, "verification")
			op.Now = op.Now.Add(time.Hour)
			got, err := reopen(t, table, blobs).VerifyOperation(context.Background(), op)
			if err != nil || got.Status != before.Status || got.Revision != before.Revision || got.Request.ID != before.Request.ID {
				t.Fatalf("verification changed request: %v", err)
			}
			for _, mutate := range []func(*Operation){
				func(o *Operation) { o.Manifest = json.RawMessage(`{"changed":true}`) },
				func(o *Operation) { o.RequestIndex++ },
			} {
				changed := op
				mutate(&changed)
				testassert.ErrorIs(t, testassert.Err(r.VerifyOperation(context.Background(), changed)), contracts.ErrConflict, "changed input accepted")
			}
			for _, mutate := range []func(*Operation){
				func(o *Operation) { o.Key += "-missing" },
				func(o *Operation) { o.Scope.Project += "-other" },
				func(o *Operation) { o.Kind = "compact" },
			} {
				changed := op
				mutate(&changed)
				testassert.ErrorIs(t, testassert.Err(r.VerifyOperation(context.Background(), changed)), contracts.ErrNotFound, "missing identity accepted")
			}
		})
	}
}

func TestVerifyOperationRequiresMatchingDiscoveryBinding(t *testing.T) {
	for _, mode := range []string{"missing", "binding", "scope", "corrupt", "lagging"} {
		t.Run(mode, func(t *testing.T) {
			r, table, blobs, op := operationFixture(t)
			record := mustBeginOperation(t, r, op)
			key := r.indexKey(record.Request.ID)
			row, err := table.Get(context.Background(), key)
			testassert.NoError(t, err)
			index, err := r.decodeIndex(row)
			testassert.NoError(t, err)
			switch mode {
			case "missing":
				testassert.NoError(t, table.Delete(context.Background(), key, row.Version))
			case "binding":
				index.Binding = r.digest("different", []byte("input"))
			case "scope":
				index.ScopeTag = r.scopeTag(Scope{Tenant: op.Scope.Tenant, Project: "other"})
			case "corrupt":
				index.Schema = 0
			case "lagging":
				index.Revision = 0
				index.Status = StatusPending
				index.UpdatedAt = record.Request.CreatedAt
			}
			if mode != "missing" {
				testassert.NoError(t, testassert.Err(table.Replace(context.Background(), r.indexItem(index), row.Version)))
			}
			table.hook = func(string, kv.KeyValueItem) (error, error) {
				t.Error("verification repaired discovery index")
				return nil, nil
			}
			forbidWrites(t, nil, blobs, "verification")
			_, err = r.VerifyOperation(context.Background(), op)
			switch mode {
			case "missing":
				testassert.ErrorIs(t, err, contracts.ErrNotFound, "missing index")
			case "binding", "scope":
				testassert.ErrorIs(t, err, contracts.ErrConflict, "changed index")
			case "corrupt":
				testassert.ErrorIs(t, err, ErrCorrupt, "corrupt index")
			case "lagging":
				testassert.NoError(t, err, "valid lagging index")
			}
		})
	}
}
