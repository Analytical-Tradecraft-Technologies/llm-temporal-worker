package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func operationFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Operation) {
	r, table, blobs, input := fixture(t)
	return r, table, blobs, Operation{Scope: input.Scope, Kind: input.Kind, Key: "operation-private", Manifest: input.Manifest, Now: input.CreatedAt}
}

func TestOperationRestartReplayAndInputBinding(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	first, err := r.BeginOperation(context.Background(), op)
	if err != nil || first.Status != StatusRunning {
		t.Fatalf("begin: %v %v", first.Status, err)
	}
	op.Now = op.Now.Add(time.Hour)
	second, err := reopen(t, table, blobs).BeginOperation(context.Background(), op)
	if err != nil || first.Request.ID != second.Request.ID || !first.Request.CreatedAt.Equal(second.Request.CreatedAt) || first.Revision != second.Revision {
		t.Fatalf("retry changed identity/revision: %v", err)
	}
	progress := json.RawMessage(`{"version":1,"response":{"text":"paid response"}}`)
	complete, err := r.CompleteOperation(context.Background(), op.Scope, first.Request.ID, progress, op.Now)
	if err != nil {
		t.Fatal(err)
	}
	// A healthy completed replay is read-only, including for object storage.
	table.hook = func(string, kv.KeyValueItem) (error, error) {
		t.Error("replay attempted a table write")
		return nil, nil
	}
	blobs.hook = func(blob.BlobKey) (error, error) { t.Error("replay attempted a blob write"); return nil, nil }
	replayed, err := reopen(t, table, blobs).BeginOperation(context.Background(), op)
	if err != nil || replayed.Status != StatusCompleted || string(replayed.Progress) != string(complete.Progress) {
		t.Fatalf("replay: %v", err)
	}
	table.hook, blobs.hook = nil, nil
	if _, err := r.CompleteOperation(context.Background(), op.Scope, first.Request.ID, progress, op.Now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CompleteOperation(context.Background(), op.Scope, first.Request.ID, json.RawMessage(`{"version":1,"different":true}`), op.Now); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("replaced completion: %v", err)
	}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Manifest = json.RawMessage(`{"different":true}`) },
		func(o *Operation) { o.RequestIndex++ },
	} {
		changed := op
		mutate(&changed)
		if _, err := r.BeginOperation(context.Background(), changed); !errors.Is(err, contracts.ErrConflict) {
			t.Fatalf("input change accepted: %v", err)
		}
	}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Key += "-new" },
		func(o *Operation) { o.Scope.Project += "-new" },
		func(o *Operation) { o.Kind = "compact" },
	} {
		changed := op
		mutate(&changed)
		different, err := r.BeginOperation(context.Background(), changed)
		if err != nil || different.Request.ID == first.Request.ID {
			t.Fatalf("identities collided: %v", err)
		}
	}
}

func TestOperationConcurrentInitializersUseOneIdentity(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	const workers = 16
	results := make(chan Record, workers)
	errorsFound := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		repository := reopen(t, table, blobs)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			input := op
			input.Now = input.Now.Add(time.Duration(i) * time.Second)
			record, err := repository.BeginOperation(context.Background(), input)
			if err != nil {
				errorsFound <- err
			} else {
				results <- record
			}
		}(i)
	}
	wg.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent initializer: %v", err)
	}
	var id RequestID
	for record := range results {
		if id != "" && id != record.Request.ID {
			t.Fatal("multiple request IDs")
		}
		id = record.Request.ID
	}
	record, err := r.Read(context.Background(), op.Scope, id)
	if err != nil || record.Revision != 2 {
		t.Fatalf("duplicate start events: %d %v", record.Revision, err)
	}
}

func TestOperationRecoversInterruptedAndUnacknowledgedWrites(t *testing.T) {
	for _, stage := range []string{"index", "blob", "start-index", "complete-index", "complete-event"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, op := operationFixture(t)
			fault := &contracts.StorageError{Kind: contracts.ErrOutcomeUnknown}
			fired := false
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				match := stage == "index" && action == "create" && strings.Contains(item.PartitionKey, "/pending/")
				if stage == "start-index" && action == "replace" {
					data := item.Fields["request"].(kv.KeyValueBytes)
					match = strings.Contains(string(data), `"status":"running"`)
				}
				if match && !fired {
					fired = true
					return nil, fault
				}
				return nil, nil
			}
			if stage == "blob" {
				blobs.hook = func(blob.BlobKey) (error, error) {
					if !fired {
						fired = true
						return fault, nil
					}
					return nil, nil
				}
			}
			record, err := r.BeginOperation(context.Background(), op)
			if stage == "index" || stage == "blob" || stage == "start-index" {
				if err == nil || !fired {
					t.Fatalf("did not inject %s: %v", stage, err)
				}
				op.Now = op.Now.Add(time.Hour)
				record, err = reopen(t, table, blobs).BeginOperation(context.Background(), op)
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(stage, "complete-") {
				table.hook = func(action string, item kv.KeyValueItem) (error, error) {
					match := stage == "complete-index" && action == "replace"
					match = match || stage == "complete-event" && action == "create" && strings.Contains(item.PartitionKey, "/request/")
					if match && !fired {
						fired = true
						return nil, fault
					}
					return nil, nil
				}
				progress := json.RawMessage(`{"version":1,"response":{}}`)
				if _, err := r.CompleteOperation(context.Background(), op.Scope, record.Request.ID, progress, op.Now); err == nil || !fired {
					t.Fatal("missing completion failure")
				}
				if _, err := reopen(t, table, blobs).CompleteOperation(context.Background(), op.Scope, record.Request.ID, progress, op.Now.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				shard, _ := PendingShard(record.Request.ID)
				page, err := r.ListPending(context.Background(), shard, 100, "")
				if err != nil || len(page.Requests) != 0 {
					t.Fatalf("index not repaired: %v", err)
				}
			}
		})
	}
}

func TestOperationValidationAndReadiness(t *testing.T) {
	r, _, _, op := operationFixture(t)
	if err := r.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Probe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Operation){func(o *Operation) { o.Key = "" }, func(o *Operation) { o.Kind = "query" }, func(o *Operation) { o.Now = time.Time{} }, func(o *Operation) { o.Manifest = json.RawMessage(`[]`) }, func(o *Operation) { o.RequestIndex = -1 }} {
		bad := op
		mutate(&bad)
		if _, err := r.BeginOperation(context.Background(), bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}
