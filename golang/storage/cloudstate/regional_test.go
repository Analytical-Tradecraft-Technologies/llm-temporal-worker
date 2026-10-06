package cloudstate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/regional"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestRegionalTableRejectsUnrelatedOrEventualReplicas(t *testing.T) {
	regions := []string{"region-primary", "region-fallback-a", "region-fallback-b"}
	table := &ddbtypes.TableDescription{MultiRegionConsistency: ddbtypes.MultiRegionConsistencyStrong, Replicas: []ddbtypes.ReplicaDescription{{RegionName: aws.String("region-fallback-a")}, {RegionName: aws.String("region-fallback-b")}}}
	if err := validateMRSCMembership(table, regions, "region-primary"); err != nil {
		t.Fatal(err)
	}
	table.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyEventual
	if err := validateMRSCMembership(table, regions, "region-primary"); !errors.Is(err, contracts.ErrUnsupported) {
		t.Fatalf("MREC accepted: %v", err)
	}
	table.MultiRegionConsistency = ddbtypes.MultiRegionConsistencyStrong
	table.Replicas = table.Replicas[:1]
	if err := validateMRSCMembership(table, regions, "region-primary"); !errors.Is(err, contracts.ErrUnsupported) {
		t.Fatalf("missing full replica accepted: %v", err)
	}
}
func TestRegionalRepositoryReconcilesUnknownWriteAfterFailover(t *testing.T) {
	_, table, blobs, request := fixture(t)
	opens := []int{0, 0}
	failPrimary := false
	routed := &regionalTable{router: regional.Router{Count: 2, Timeout: time.Second}, stores: map[int]kv.KeyValueStore{}}
	routed.open = func(_ context.Context, i int) (kv.KeyValueStore, error) {
		opens[i]++
		if i == 0 && failPrimary {
			return nil, contracts.ErrUnavailable
		}
		return table, nil
	}
	repo, err := NewRepository(Options{Table: routed, Blobs: blobs, Namespace: "requests-v1", Secret: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	table.hook = func(op string, _ kv.KeyValueItem) (error, error) {
		if op == "create" {
			table.hook = nil
			failPrimary = true
			return nil, &contracts.StorageError{Kind: contracts.ErrUnavailable, OutcomeUnknown: true}
		}
		return nil, nil
	}
	if _, err := repo.Create(context.Background(), request); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("unknown write lost: %v", err)
	}
	if opens[1] != 0 {
		t.Fatal("uncertain write blindly replayed")
	}
	// Remove the failed endpoint's cached handle, simulating a regional outage.
	routed.mu.Lock()
	delete(routed.stores, 0)
	routed.mu.Unlock()
	record, err := repo.Create(context.Background(), request)
	if err != nil || record.Revision != 1 || opens[1] == 0 {
		t.Fatalf("reconcile through alternate region: revision=%d opens=%v err=%v", record.Revision, opens, err)
	}
	again, err := repo.Create(context.Background(), request)
	if err != nil || again.Revision != 1 {
		t.Fatalf("duplicate after failover: %+v %v", again, err)
	}
}
func TestRegionalPaginationPinsCursor(t *testing.T) {
	store := &memoryTable{rows: map[kv.KeyValueKey]kv.KeyValueRecord{}}
	for _, key := range []string{"a", "b"} {
		if _, err := store.Create(context.Background(), kv.KeyValueItem{PartitionKey: "p", SortKey: key}); err != nil {
			t.Fatal(err)
		}
	}
	down := false
	opened := []int{0, 0}
	routed := &regionalTable{router: regional.Router{Count: 2, Timeout: time.Second}, stores: map[int]kv.KeyValueStore{}}
	routed.open = func(_ context.Context, i int) (kv.KeyValueStore, error) {
		opened[i]++
		if i == 0 && down {
			return nil, contracts.ErrUnavailable
		}
		return store, nil
	}
	first, err := routed.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "p", PageSize: 1})
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page: %v", err)
	}
	down = true
	delete(routed.stores, 0)
	routed.router.Active.Store(1)
	if _, err := routed.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "p", PageSize: 1, PageToken: first.NextPageToken}); !errors.Is(err, contracts.ErrUnavailable) || opened[1] != 0 {
		t.Fatalf("cursor moved regions: %v", err)
	}
	down = false
	second, err := routed.QueryPartition(context.Background(), kv.KeyValueQuery{PartitionKey: "p", PageSize: 1, PageToken: first.NextPageToken})
	if err != nil || len(second.Records) != 1 || second.Records[0].Item.SortKey != "b" {
		t.Fatalf("resume: %+v %v", second, err)
	}
}
func TestRegionalPayloadFallback(t *testing.T) {
	primary := &memoryBlobs{values: map[blob.BlobKey][]byte{}}
	alternate := &memoryBlobs{values: map[blob.BlobKey][]byte{"key": []byte("payload")}}
	routed := &regionalBlobs{router: regional.Router{Count: 2, Timeout: time.Second}, open: func(_ context.Context, i int) (blob.BlobStore, error) {
		if i == 0 {
			return primary, nil
		}
		return alternate, nil
	}}
	opened, err := routed.Open(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(opened.Body)
	opened.Body.Close()
	if err != nil || string(data) != "payload" {
		t.Fatalf("payload: %q %v", data, err)
	}
	routed.router.Active.Store(0)
	primary.open = func(blob.BlobKey) error { return contracts.ErrUnavailable }
	alternate.values = map[blob.BlobKey][]byte{}
	if _, err := routed.Open(context.Background(), "key"); !errors.Is(err, contracts.ErrUnavailable) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("replication lag became a definitive missing payload: %v", err)
	}
}

func TestRegionalOpenSurvivesPrimaryFailureAndRevalidatesMRSC(t *testing.T) {
	var eventual atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if strings.Contains(r.Header.Get("Authorization"), "/region-primary/") {
			w.WriteHeader(503)
			io.WriteString(w, `{"__type":"ServiceUnavailable"}`)
			return
		}
		mode := "STRONG"
		if eventual.Load() {
			mode = "EVENTUAL"
		}
		io.WriteString(w, `{"Table":{"MultiRegionConsistency":"`+mode+`","Replicas":[{"RegionName":"region-primary"},{"RegionName":"region-fallback-a"},{"RegionName":"region-fallback-b"}]}}`)
	}))
	defer server.Close()
	t.Setenv("AWS_ENDPOINT_URL_DYNAMODB", server.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	_, table, blobs, request := fixture(t)
	settings := Config{Provider: map[string]any{
		"type": "aws", "aws": map[string]any{"region": "region-primary", "allow_mrsc": true, "failover": map[string]any{
			"dynamodb_regions": []string{"region-fallback-a", "region-fallback-b"}, "payload_replicas": []map[string]any{{"region": "region-fallback-a", "bucket": "payloads-replica"}}, "attempt_timeout": "1s"}},
		"key_value_stores": map[string]string{"requests": "physical-table"}, "blob_stores": map[string]string{"payloads": "payloads-primary"}}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}
	initialize := func(_ context.Context, value map[string]any) (provider.StorageProvider, error) {
		aws := value["aws"].(map[string]any)
		if aws["failover"] != nil {
			t.Fatal("worker option leaked to cloud-storage factory")
		}
		p := &namedProvider{table: table, blobs: blobs}
		if aws["region"] == "region-primary" {
			p.tableErr = contracts.ErrUnavailable
			p.blobErr = contracts.ErrUnavailable
		}
		return p, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	repo, err := open(ctx, settings, make([]byte, 32), initialize)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Probe(ctx); err != nil {
		t.Fatalf("readiness with primary down: %v", err)
	}
	if _, err := repo.Create(ctx, request); err != nil {
		t.Fatalf("write with primary down: %v", err)
	}
	if _, err := repo.Read(ctx, request.Scope, request.ID); err != nil {
		t.Fatalf("read with primary down: %v", err)
	}
	if settings.Provider["aws"].(map[string]any)["failover"] == nil {
		t.Fatal("mutated caller configuration")
	}
	eventual.Store(true)
	if err := repo.Probe(ctx); !errors.Is(err, contracts.ErrUnsupported) {
		t.Fatalf("MRSC validation skipped during readiness: %v", err)
	}
}

func TestRegionalPayloadHandlesCachedButProbesRevalidate(t *testing.T) {
	ctx := context.Background()
	backend := &memoryBlobs{values: map[blob.BlobKey][]byte{}}
	opens := 0
	fail := false
	routed := &regionalBlobs{router: regional.Router{Count: 1, Timeout: time.Second}, open: func(context.Context, int) (blob.BlobStore, error) {
		opens++
		if fail {
			return nil, contracts.ErrUnavailable
		}
		return backend, nil
	}}
	fail = true
	if err := routed.Create(ctx, "key", strings.NewReader("payload"), 7); !errors.Is(err, contracts.ErrUnavailable) {
		t.Fatalf("failed initialization: %v", err)
	}
	fail = false
	if err := routed.Create(ctx, "key", strings.NewReader("payload"), 7); err != nil {
		t.Fatal(err)
	}
	opened, err := routed.Open(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	opened.Body.Close()
	if err := routed.ProbeRead(ctx, "absent-readiness-key"); err != nil {
		t.Fatal(err)
	}
	if opens != 2 {
		t.Fatalf("operations reopened cached handle: %d opens", opens)
	}
	fail = true
	if err := routed.probe(ctx); !errors.Is(err, contracts.ErrUnavailable) || opens != 3 {
		t.Fatalf("probe skipped fresh validation: opens=%d err=%v", opens, err)
	}
}

func TestRegionalOpenRejectsMalformedProviderMaps(t *testing.T) {
	for _, field := range []string{"key_value_stores", "blob_stores"} {
		for _, value := range []any{nil, "invalid", []string{"invalid"}, map[string]any{}, map[string]any{"requests": 42, "payloads": 42}, map[string]any{"requests": "", "payloads": ""}} {
			t.Run(field+"/"+fmt.Sprintf("%v", value), func(t *testing.T) {
				settings := Config{Provider: map[string]any{
					"type": "aws", "aws": map[string]any{"region": "region-primary", "allow_mrsc": true, "failover": map[string]any{
						"dynamodb_regions": []string{"region-fallback"}, "payload_replicas": []map[string]any{{"region": "region-fallback", "bucket": "replica"}}, "attempt_timeout": "1s"}},
					"key_value_stores": map[string]any{"requests": "table"}, "blob_stores": map[string]any{"payloads": "bucket"}}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}
				settings.Provider[field] = value
				called := false
				_, err := open(context.Background(), settings, make([]byte, 32), func(context.Context, map[string]any) (provider.StorageProvider, error) {
					called = true
					return nil, errors.New("unexpected initialization")
				})
				if !errors.Is(err, ErrInvalid) || called {
					t.Fatalf("malformed map reached initializer: called=%v err=%v", called, err)
				}
			})
		}
	}
}
