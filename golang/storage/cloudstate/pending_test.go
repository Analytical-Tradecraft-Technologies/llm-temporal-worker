package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func TestPendingPaginationAcrossEveryShard(t *testing.T) {
	r, _, _, template := fixture(t)
	ctx := context.Background()
	var byShard [PendingShards][]CreateRequest
	// Deterministic UUIDs, independent of the random fixture, exercise every shard.
	for n, populated := 1, 0; populated < PendingShards; n++ {
		request := template
		request.ID = RequestID(fmt.Sprintf("%s00000000-0000-4000-8000-%012d", RequestIDPrefix, n))
		shard, err := PendingShard(request.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(byShard[shard]) == 3 {
			continue
		}
		byShard[shard] = append(byShard[shard], request)
		if len(byShard[shard]) == 3 {
			populated++
		}
	}
	for shard, requests := range byShard {
		sort.Slice(requests, func(i, j int) bool { return requests[i].ID < requests[j].ID })
		for i, request := range requests {
			record, err := r.Create(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				if _, err := r.TryUpdate(ctx, request.Scope, request.ID, change(record, StatusCompleted, "cache-hit")); err != nil {
					t.Fatal(err)
				}
			}
		}
		// A page containing only terminal rows is empty but still has a cursor.
		page, err := r.ListPending(ctx, shard, 1, "")
		if err != nil || len(page.Requests) != 0 || page.NextPageToken == "" {
			t.Fatalf("terminal page: %+v %v", page, err)
		}
		if _, err := r.ListPending(ctx, (shard+1)%PendingShards, 1, page.NextPageToken); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("cross-shard token: %v", err)
		}
		if _, err := r.ListPending(ctx, shard, 2, page.NextPageToken); !errors.Is(err, contracts.ErrInvalidArgument) {
			t.Fatalf("resized token: %v", err)
		}
		var ids []RequestID
		for page.NextPageToken != "" {
			page, err = r.ListPending(ctx, shard, 1, page.NextPageToken)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range page.Requests {
				ids = append(ids, item.ID)
			}
		}
		if want := []RequestID{requests[1].ID, requests[2].ID}; !reflect.DeepEqual(ids, want) {
			t.Fatalf("shard %d: %v, want %v", shard, ids, want)
		}
	}
	if _, err := PendingShard("not-an-id"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestInitializingAndLaggingIndexRemainDiscoverable(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	request, err := normalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ensureIndex(ctx, request); err != nil {
		t.Fatal(err)
	}
	shard, _ := PendingShard(request.ID)
	r = reopen(t, table, blobs)
	page, err := r.ListPending(ctx, shard, 0, "")
	if err != nil || len(page.Requests) != 1 || !page.Requests[0].Initializing {
		t.Fatalf("initializing: %+v %v", page, err)
	}
	if _, err := r.ReadForRecovery(ctx, request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("unpublished state: %v", err)
	}
	record, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	table.hook = func(op string, _ kv.KeyValueItem) (error, error) {
		if op == "replace" {
			return contracts.ErrUnavailable, nil
		}
		return nil, nil
	}
	update := change(record, StatusCompleted, "complete")
	if _, err := r.TryUpdate(ctx, request.Scope, request.ID, update); !errors.Is(err, ErrIndexPending) {
		t.Fatalf("lost committed event: %v", err)
	}
	page, err = r.ListPending(ctx, shard, 0, "")
	if err != nil || len(page.Requests) != 1 || page.Requests[0].Revision != 1 {
		t.Fatalf("lagging index: %+v %v", page, err)
	}
	recovered, err := r.ReadForRecovery(ctx, request.ID)
	if err != nil || recovered.Status != StatusCompleted {
		t.Fatalf("authoritative recovery: %+v %v", recovered, err)
	}
	table.hook = nil
	if _, err := r.TryUpdate(ctx, request.Scope, request.ID, update); err != nil {
		t.Fatal(err)
	}
	page, err = r.ListPending(ctx, shard, 0, "")
	if err != nil || len(page.Requests) != 0 {
		t.Fatalf("repair: %+v %v", page, err)
	}
}

type queryTable struct {
	kv.KeyValueStore
	query func(kv.KeyValueQuery) (kv.KeyValueQueryPage, error)
}

func (s queryTable) QueryPartition(_ context.Context, q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
	return s.query(q)
}

func TestPendingRejectsMalformedBackendPages(t *testing.T) {
	for _, name := range []string{"schema", "version", "partition", "order", "overflow", "loop", "initial-state", "field-type"} {
		t.Run(name, func(t *testing.T) {
			r, table, blobs, request := fixture(t)
			ctx := context.Background()
			if _, err := r.Create(ctx, request); err != nil {
				t.Fatal(err)
			}
			row, err := table.Get(ctx, r.indexKey(request.ID))
			if err != nil {
				t.Fatal(err)
			}
			value, err := r.decodeIndex(row)
			if err != nil {
				t.Fatal(err)
			}
			page := kv.KeyValueQueryPage{Records: []kv.KeyValueRecord{row}}
			switch name {
			case "schema":
				value.Schema = 2
			case "initial-state":
				value.Status, value.Revision = StatusRunning, 0
			case "version":
				page.Records[0].Version = ""
			case "partition":
				page.Records[0].Item.PartitionKey = r.partition(9)
			case "order":
				page.Records = append(page.Records, row)
			case "overflow":
				page.Records = append(page.Records, row, row)
			case "loop":
				page.NextPageToken = "repeated"
			case "field-type":
				page.Records[0].Item.Fields["request"] = kv.String("unexpected")
			}
			if name == "schema" || name == "initial-state" {
				data, _ := json.Marshal(value)
				page.Records[0].Item.Fields["request"] = kv.Bytes(data)
			}
			r = reopen(t, queryTable{KeyValueStore: table, query: func(kv.KeyValueQuery) (kv.KeyValueQueryPage, error) { return page, nil }}, blobs)
			shard, _ := PendingShard(request.ID)
			if _, err := r.ListPending(ctx, shard, 2, "repeated"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted bad page: %v", err)
			}
		})
	}
}

func TestIndexContentionIsBoundedAndUncertaintyIsNotRetried(t *testing.T) {
	for _, failure := range []error{contracts.ErrConflict, contracts.ErrOutcomeUnknown} {
		r, table, _, request := fixture(t)
		ctx := context.Background()
		record, err := r.Create(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		attempts := 0
		table.hook = func(op string, _ kv.KeyValueItem) (error, error) {
			if op == "replace" {
				attempts++
				return failure, nil
			}
			return nil, nil
		}
		if _, err := r.TryUpdate(ctx, request.Scope, request.ID, change(record, StatusRunning, "claim")); !errors.Is(err, ErrIndexPending) || !errors.Is(err, failure) {
			t.Fatalf("error kind: %v", err)
		}
		want := 16
		if errors.Is(failure, contracts.ErrOutcomeUnknown) {
			want = 1
		}
		if attempts != want {
			t.Fatalf("%v: %d writes, want %d", failure, attempts, want)
		}
	}
}
