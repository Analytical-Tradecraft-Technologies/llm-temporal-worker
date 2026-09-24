package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func TestRequestLifecycleRestartAndScopedReads(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	record, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if record.Revision != 1 || record.Status != StatusPending || !bytes.Contains(record.Request.Manifest, []byte("9007199254740993")) {
		t.Fatalf("bad initial record: %+v", record)
	}
	for _, status := range []Status{StatusRunning, StatusProviderPending, StatusProviderPending, StatusCompleted} {
		r = reopen(t, table, blobs)
		record, err = r.TryUpdate(ctx, request.Scope, request.ID, change(record, status, fmt.Sprint(record.Revision)))
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := reopen(t, table, blobs).Read(ctx, request.Scope, request.ID)
		if err != nil || !reflect.DeepEqual(loaded, record) {
			t.Fatalf("restart read differs: %v", err)
		}
	}
	if _, err := r.Read(ctx, Scope{Tenant: request.Scope.Tenant, Project: "other"}, request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope read: %v", err)
	}
	if _, err := r.TryUpdate(ctx, request.Scope, request.ID, change(record, StatusRunning, "terminal")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("terminal changed: %v", err)
	}
	shard, _ := PendingShard(request.ID)
	page, err := r.ListPending(ctx, shard, 100, "")
	if err != nil || len(page.Requests) != 0 {
		t.Fatalf("completed request still pending: %+v %v", page, err)
	}
	if len(table.rows) != 6 {
		t.Fatalf("history/index unexpectedly deleted: %d", len(table.rows))
	}
}

func TestRequestWriteOrderAndEncryption(t *testing.T) {
	r, table, blobs, request := fixture(t)
	var order []string
	table.trace = func(s string) { order = append(order, s) }
	blobs.trace = table.trace
	record, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 4 || !strings.Contains(order[0], "/pending/") || order[1] != "blob" || !strings.Contains(order[2], "/request/") || !strings.HasPrefix(order[3], "replace:") {
		t.Fatalf("unsafe write order: %v", order)
	}
	if _, err := r.TryUpdate(context.Background(), request.Scope, request.ID, change(record, StatusRunning, "private-update-token")); err != nil {
		t.Fatal(err)
	}
	var stored [][]byte
	for key, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		stored = append(stored, []byte(key.PartitionKey+key.SortKey), data)
	}
	for key, data := range blobs.values {
		stored = append(stored, []byte(key), data)
	}
	for _, data := range stored {
		for _, secret := range []string{"tenant-secret", "project-secret", "private prompt", "private-job-id", "private-update-token", "lease-1", "9007199254740993"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("plaintext %q leaked to storage", secret)
			}
		}
	}
}

func TestFingerprintIncludesSampleIndexButNotRequestID(t *testing.T) {
	r, _, _, request := fixture(t)
	first, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.ID, _ = NewRequestID()
	request.CreatedAt = request.CreatedAt.Add(time.Hour)
	request.Manifest = json.RawMessage(`{"prompt":"private prompt", "account":9007199254740993,"version":1}`)
	second, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Fatal("equivalent independently submitted requests differ")
	}
	request.ID, _ = NewRequestID()
	request.RequestIndex++
	third, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint == third.Fingerprint {
		t.Fatal("sample index did not break identity")
	}
	request.ID, _ = NewRequestID()
	request.RequestIndex = 0
	request.Scope.Project = "different"
	fourth, err := r.Create(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint == fourth.Fingerprint {
		t.Fatal("scope did not break identity")
	}
}

func TestConcurrentCreatesAndUpdates(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	const workers = 24
	var wg sync.WaitGroup
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		client := reopen(t, table, blobs)
		wg.Add(1)
		go func() { defer wg.Done(); _, err := client.Create(ctx, request); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("identical concurrent creation: %v", err)
		}
	}
	initial, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	results = make(chan error, workers)
	for i := 0; i < workers; i++ {
		client := reopen(t, table, blobs)
		update := change(initial, StatusRunning, fmt.Sprint(i))
		update.Progress = json.RawMessage(fmt.Sprintf(`{"version":1,"worker":%d}`, i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.TryUpdate(ctx, request.Scope, request.ID, update)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, contracts.ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("got %d concurrent winners", winners)
	}
	latest, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil || latest.Revision != 2 {
		t.Fatalf("read after race: %+v %v", latest, err)
	}
	changed := request
	changed.Manifest = json.RawMessage(`{"other":true}`)
	if _, err := r.Create(ctx, changed); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("conflicting create accepted: %v", err)
	}
}

func TestIdenticalUpdateRetryAfterLaterCompletionDoesNotRegressIndex(t *testing.T) {
	r, _, _, request := fixture(t)
	ctx := context.Background()
	initial, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	update := change(initial, StatusRunning, "dispatch")
	running, err := r.TryUpdate(ctx, request.Scope, request.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := r.TryUpdate(ctx, request.Scope, request.ID, change(running, StatusCompleted, "done"))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := r.TryUpdate(ctx, request.Scope, request.ID, update)
	if err != nil || !reflect.DeepEqual(retry, running) {
		t.Fatalf("old update replay: %v", err)
	}
	if _, err := r.Create(ctx, request); err != nil {
		t.Fatal(err)
	}
	stored, err := r.table.Get(ctx, r.indexKey(request.ID))
	if err != nil {
		t.Fatal(err)
	}
	index, err := r.decodeIndex(stored)
	if err != nil || index.Revision != completed.Revision || index.Status != StatusCompleted {
		t.Fatalf("index regressed: %+v %v", index, err)
	}
}

func TestCreateFailureBoundariesRemainDiscoverableAndRetryable(t *testing.T) {
	for _, boundary := range []string{"index-before", "index-after", "blob-before", "blob-after", "event-before", "event-after", "advance-before", "advance-after"} {
		t.Run(boundary, func(t *testing.T) {
			r, table, blobs, request := fixture(t)
			ctx := context.Background()
			failure := errors.New("injected storage failure")
			unknown := errors.Join(contracts.ErrOutcomeUnknown, failure)
			fired := false
			table.hook = func(op string, item kv.KeyValueItem) (error, error) {
				if fired {
					return nil, nil
				}
				category := "event"
				if strings.Contains(item.PartitionKey, "/pending/") {
					category = "index"
					if op == "replace" {
						category = "advance"
					}
				}
				if boundary == category+"-before" {
					fired = true
					return failure, nil
				}
				if boundary == category+"-after" {
					fired = true
					return nil, unknown
				}
				return nil, nil
			}
			blobs.hook = func(blob.BlobKey) (error, error) {
				if fired {
					return nil, nil
				}
				if boundary == "blob-before" {
					fired = true
					return failure, nil
				}
				if boundary == "blob-after" {
					fired = true
					return nil, unknown
				}
				return nil, nil
			}
			_, err := r.Create(ctx, request)
			if err == nil || !errors.Is(err, failure) {
				t.Fatalf("injected failure hidden: %v", err)
			}
			if strings.HasSuffix(boundary, "-after") && !errors.Is(err, contracts.ErrOutcomeUnknown) {
				t.Fatalf("lost uncertain outcome: %v", err)
			}
			if boundary == "index-before" {
				if len(table.rows) != 0 || len(blobs.values) != 0 {
					t.Fatal("wrote elsewhere before discovery")
				}
			} else if _, err := table.Get(ctx, r.indexKey(request.ID)); err != nil {
				t.Fatal("lost recovery entry", err)
			}
			r = reopen(t, table, blobs)
			got, err := r.Create(ctx, request)
			if err != nil {
				t.Fatal("retry", err)
			}
			loaded, err := r.ReadForRecovery(ctx, request.ID)
			if err != nil || !reflect.DeepEqual(got, loaded) {
				t.Fatalf("restart recovery: %v", err)
			}
			if len(table.rows) != 2 || len(blobs.values) != 1 {
				t.Fatalf("retry duplicated committed data: %d rows, %d blobs", len(table.rows), len(blobs.values))
			}
		})
	}
}

func TestUpdateLostAcknowledgementsAndIndexRepair(t *testing.T) {
	for _, stage := range []string{"event", "index"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, request := fixture(t)
			ctx := context.Background()
			initial, err := r.Create(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			fired := false
			table.hook = func(op string, item kv.KeyValueItem) (error, error) {
				match := (stage == "event" && strings.Contains(item.PartitionKey, "/request/")) || (stage == "index" && op == "replace")
				if !fired && match {
					fired = true
					return nil, contracts.ErrOutcomeUnknown
				}
				return nil, nil
			}
			update := change(initial, StatusRunning, "start")
			_, err = r.TryUpdate(ctx, request.Scope, request.ID, update)
			if !errors.Is(err, contracts.ErrOutcomeUnknown) {
				t.Fatalf("lost uncertainty: %v", err)
			}
			if stage == "index" && !errors.Is(err, ErrIndexPending) {
				t.Fatalf("lost committed-event distinction: %v", err)
			}
			r = reopen(t, table, blobs)
			recovered, err := r.TryUpdate(ctx, request.Scope, request.ID, update)
			if err != nil || recovered.Revision != 2 {
				t.Fatalf("retry: %+v %v", recovered, err)
			}
			row, err := table.Get(ctx, r.indexKey(request.ID))
			if err != nil {
				t.Fatal(err)
			}
			index, err := r.decodeIndex(row)
			if err != nil || index.Revision != 2 {
				t.Fatalf("index repair: %+v %v", index, err)
			}
		})
	}
}

func TestOutcomeUnknownRemainsDiscoverable(t *testing.T) {
	r, _, _, request := fixture(t)
	ctx := context.Background()
	record, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []Status{StatusRunning, StatusOutcomeUnknown} {
		record, err = r.TryUpdate(ctx, request.Scope, request.ID, change(record, status, string(status)))
		if err != nil {
			t.Fatal(err)
		}
	}
	shard, _ := PendingShard(request.ID)
	page, err := r.ListPending(ctx, shard, 1, "")
	if err != nil || len(page.Requests) != 1 || page.Requests[0].Status != StatusOutcomeUnknown {
		t.Fatalf("unknown work lost: %+v %v", page, err)
	}
}

func TestTamperingMissingPayloadAndWrongSecretFailClosed(t *testing.T) {
	for _, mode := range []string{"ciphertext", "missing", "secret", "event", "scope"} {
		t.Run(mode, func(t *testing.T) {
			r, table, blobs, request := fixture(t)
			ctx := context.Background()
			if _, err := r.Create(ctx, request); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "ciphertext":
				for key, data := range blobs.values {
					data[len(data)-1] ^= 1
					blobs.values[key] = data
				}
			case "missing":
				clear(blobs.values)
			case "secret":
				var err error
				r, err = NewRepository(Options{Table: table, Blobs: blobs, Namespace: r.namespace, Secret: bytes.Repeat([]byte{9}, 32)})
				if err != nil {
					t.Fatal(err)
				}
			case "event":
				for key, row := range table.rows {
					if strings.Contains(key.PartitionKey, "/request/") {
						row.Item.Fields["event"] = kv.Bytes([]byte(`{}`))
						table.rows[key] = row
					}
				}
			case "scope":
				request.Scope.Tenant = "foreign"
			}
			if _, err := r.Read(ctx, request.Scope, request.ID); err == nil {
				t.Fatal("accepted missing, corrupt or foreign data")
			}
		})
	}
}

func TestInvalidRequestsFailBeforeStorageAndCancellationPreserved(t *testing.T) {
	for _, mutate := range []func(*CreateRequest){
		func(r *CreateRequest) { r.ID = "external-id" }, func(r *CreateRequest) { r.Scope.Project = "" }, func(r *CreateRequest) { r.Scope.Tenant = "bad\x00scope" },
		func(r *CreateRequest) { r.Kind = "cancel" }, func(r *CreateRequest) { r.RequestIndex = -1 }, func(r *CreateRequest) { r.Manifest = json.RawMessage(`[]`) },
		func(r *CreateRequest) { r.Manifest = json.RawMessage(`{"duplicate":1,"duplicate":2}`) }, func(r *CreateRequest) { r.CreatedAt = time.Time{} },
	} {
		r, table, blobs, request := fixture(t)
		mutate(&request)
		if _, err := r.Create(context.Background(), request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid accepted: %v", err)
		}
		if len(table.rows) != 0 || len(blobs.values) != 0 {
			t.Fatal("invalid input caused I/O")
		}
	}
	r, table, blobs, request := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Create(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if _, err := r.Read(nil, request.Scope, request.ID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context: %v", err)
	}
	if len(table.rows) != 0 || len(blobs.values) != 0 {
		t.Fatal("canceled input caused I/O")
	}
}

func TestHistoricalRetryAcrossPagesAndFailedTerminalState(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	record, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var update Update
	// The released event library defaults to 100 rows per page. Exercise both
	// full replay and historical CAS retry after crossing that boundary.
	for i := 0; i < 105; i++ {
		update = change(record, StatusRunning, fmt.Sprint(i))
		record, err = r.TryUpdate(ctx, request.Scope, request.ID, update)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, status := range []Status{StatusOutcomeUnknown, StatusRunning, StatusFailed} {
		record, err = r.TryUpdate(ctx, request.Scope, request.ID, change(record, status, string(status)))
		if err != nil {
			t.Fatal(err)
		}
	}
	r = reopen(t, table, blobs)
	retried, err := r.TryUpdate(ctx, request.Scope, request.ID, update)
	if err != nil || retried.Revision != 106 || retried.Status != StatusRunning {
		t.Fatalf("historical retry: %+v %v", retried, err)
	}
	loaded, err := r.Read(ctx, request.Scope, request.ID)
	if err != nil || !reflect.DeepEqual(loaded, record) {
		t.Fatalf("paged replay: %v", err)
	}
	if _, err := r.TryUpdate(ctx, request.Scope, request.ID, change(record, StatusRunning, "restart")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("failed request was mutable: %v", err)
	}
}

func TestRejectedUpdatesDoNotWrite(t *testing.T) {
	r, table, blobs, request := fixture(t)
	ctx := context.Background()
	record, err := r.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Update){
		func(u *Update) { u.ExpectedRevision = 0 }, func(u *Update) { u.ExpectedRevision = maxRevisions },
		func(u *Update) { u.Token = "" }, func(u *Update) { u.Status = "canceled" },
		func(u *Update) { u.Status = StatusProviderPending }, func(u *Update) { u.UpdatedAt = record.UpdatedAt.Add(-time.Second) },
		func(u *Update) { u.Progress = json.RawMessage(`null`) },
	} {
		u := change(record, StatusRunning, "start")
		mutate(&u)
		if _, err := r.TryUpdate(ctx, request.Scope, request.ID, u); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad update: %v", err)
		}
	}
	u := change(record, StatusRunning, "start")
	u.ExpectedRevision++
	if _, err := r.TryUpdate(ctx, request.Scope, request.ID, u); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("future revision: %v", err)
	}
	u.ExpectedRevision--
	if _, err := r.TryUpdate(ctx, Scope{Tenant: "other", Project: request.Scope.Project}, request.ID, u); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("foreign update: %v", err)
	}
	if len(table.rows) != 2 || len(blobs.values) != 1 {
		t.Fatal("rejected update wrote state")
	}
}
