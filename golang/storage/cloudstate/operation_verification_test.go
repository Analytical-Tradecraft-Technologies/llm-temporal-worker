package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
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
			if err != nil {
				t.Fatal(err)
			}
			if state == StatusCompleted {
				before, err = r.CompleteOperation(context.Background(), op.Scope, before.Request.ID, json.RawMessage(`{"version":1}`), op.Now)
				if err != nil {
					t.Fatal(err)
				}
			}
			table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("verification wrote to table"); return nil, nil }
			blobs.hook = func(blob.BlobKey) (error, error) { t.Error("verification wrote a blob"); return nil, nil }
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
				if _, err := r.VerifyOperation(context.Background(), changed); !errors.Is(err, contracts.ErrConflict) {
					t.Fatalf("changed input accepted: %v", err)
				}
			}
			for _, mutate := range []func(*Operation){
				func(o *Operation) { o.Key += "-missing" },
				func(o *Operation) { o.Scope.Project += "-other" },
				func(o *Operation) { o.Kind = "compact" },
			} {
				changed := op
				mutate(&changed)
				if _, err := r.VerifyOperation(context.Background(), changed); !errors.Is(err, contracts.ErrNotFound) {
					t.Fatalf("missing identity accepted: %v", err)
				}
			}
		})
	}
}

func TestVerifyOperationRequiresMatchingDiscoveryBinding(t *testing.T) {
	for _, mode := range []string{"missing", "binding", "scope", "corrupt", "lagging"} {
		t.Run(mode, func(t *testing.T) {
			r, table, blobs, op := operationFixture(t)
			record, err := r.BeginOperation(context.Background(), op)
			if err != nil {
				t.Fatal(err)
			}
			key := r.indexKey(record.Request.ID)
			row, err := table.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			index, err := r.decodeIndex(row)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				if err := table.Delete(context.Background(), key, row.Version); err != nil {
					t.Fatal(err)
				}
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
				if _, err := table.Replace(context.Background(), r.indexItem(index), row.Version); err != nil {
					t.Fatal(err)
				}
			}
			table.hook = func(string, kv.KeyValueItem) (error, error) {
				t.Error("verification repaired discovery index")
				return nil, nil
			}
			blobs.hook = func(blob.BlobKey) (error, error) { t.Error("verification wrote blob"); return nil, nil }
			_, err = r.VerifyOperation(context.Background(), op)
			switch mode {
			case "missing":
				if !errors.Is(err, contracts.ErrNotFound) {
					t.Fatalf("missing index: %v", err)
				}
			case "binding", "scope":
				if !errors.Is(err, contracts.ErrConflict) {
					t.Fatalf("changed index: %v", err)
				}
			case "corrupt":
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("corrupt index: %v", err)
				}
			case "lagging":
				if err != nil {
					t.Fatalf("valid lagging index: %v", err)
				}
			}
		})
	}
}
