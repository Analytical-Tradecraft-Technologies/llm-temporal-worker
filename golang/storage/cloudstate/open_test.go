package cloudstate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers"
)

type namedProvider struct {
	provider.StorageProvider // Listing must not be required to open configured stores.
	table                    kv.KeyValueStore
	blobs                    blob.BlobStore
	names                    []string
	tableErr, blobErr        error
}

func (p *namedProvider) OpenKeyValueStore(_ context.Context, name string) (kv.KeyValueStore, error) {
	p.names = append(p.names, name)
	return p.table, p.tableErr
}
func (p *namedProvider) OpenBlobStore(_ context.Context, name string) (blob.BlobStore, error) {
	p.names = append(p.names, name)
	return p.blobs, p.blobErr
}

func TestOpenUsesParsedProviderConfigAndStoreAliases(t *testing.T) {
	_, table, blobs, request := fixture(t)
	p := &namedProvider{table: table, blobs: blobs}
	config := Config{Provider: map[string]any{"type": "aws", "key_value_stores": map[string]any{"requests": "physical-table"}}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}
	secret := bytes.Repeat([]byte{7}, 32)
	r, err := open(context.Background(), config, secret, func(_ context.Context, parsed map[string]any) (provider.StorageProvider, error) {
		if !reflect.DeepEqual(parsed, config.Provider) {
			t.Fatal("changed provider config")
		}
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.names, []string{"requests", "payloads"}) {
		t.Fatalf("store selection: %v", p.names)
	}
	// The caller may clear the input secret; the repository owns its own copy.
	clear(secret)
	if _, err := r.Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reopen(t, table, blobs).Read(context.Background(), request.Scope, request.ID); err != nil {
		t.Fatal("secret aliased caller memory", err)
	}
}

func TestOpenParentSnapshotStorage(t *testing.T) {
	_, table, blobs, _ := fixture(t)
	for storage, want := range map[string]bool{"": false, "inline": false, "blob": true} {
		config := Config{Provider: map[string]any{"type": "aws"}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1", ParentSnapshotStorage: storage}
		r, err := open(context.Background(), config, bytes.Repeat([]byte{7}, 32), func(context.Context, map[string]any) (provider.StorageProvider, error) {
			return &namedProvider{table: table, blobs: blobs}, nil
		})
		if err != nil || r.parentSnapshotBlob != want {
			t.Fatalf("%q: blob=%v err=%v", storage, r != nil && r.parentSnapshotBlob, err)
		}
	}
	config := Config{Provider: map[string]any{"type": "aws"}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1", ParentSnapshotStorage: "sideways"}
	if _, err := open(context.Background(), config, bytes.Repeat([]byte{7}, 32), func(context.Context, map[string]any) (provider.StorageProvider, error) {
		t.Fatal("opened a provider for an invalid setting")
		return nil, nil
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid setting: %v", err)
	}
}

func TestOpenAndConstructorRejectInvalidInputs(t *testing.T) {
	_, table, blobs, _ := fixture(t)
	base := Options{Table: table, Blobs: blobs, Namespace: "requests-v1", Secret: bytes.Repeat([]byte{7}, 32)}
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Table = nil }, func(o *Options) { o.Table = (*memoryTable)(nil) },
		func(o *Options) { o.Blobs = nil }, func(o *Options) { o.Blobs = (*memoryBlobs)(nil) },
		func(o *Options) { o.Namespace = "../escape" }, func(o *Options) { o.Secret = o.Secret[:31] },
	} {
		o := base
		mutate(&o)
		if _, err := NewRepository(o); !errors.Is(err, ErrInvalid) {
			t.Fatalf("constructor: %v", err)
		}
	}
	config := Config{RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}
	for _, mutate := range []func(*Config){func(c *Config) { c.Namespace = "" }, func(c *Config) { c.RequestTable = "" }, func(c *Config) { c.PayloadStore = "\n" }} {
		c := config
		mutate(&c)
		if _, err := open(context.Background(), c, base.Secret, func(context.Context, map[string]any) (provider.StorageProvider, error) {
			t.Fatal("initialized provider before validation")
			return nil, nil
		}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, stage := range []string{"factory", "nil-provider", "table", "blob"} {
		t.Run(stage, func(t *testing.T) {
			p := &namedProvider{table: table, blobs: blobs}
			failure := &contracts.StorageError{Kind: contracts.ErrPermissionDenied, Cause: errors.New("underlying SDK error")}
			if stage == "table" {
				p.tableErr = failure
			}
			if stage == "blob" {
				p.blobErr = failure
			}
			_, err := open(context.Background(), config, base.Secret, func(context.Context, map[string]any) (provider.StorageProvider, error) {
				if stage == "factory" {
					return nil, failure
				}
				if stage == "nil-provider" {
					return (*namedProvider)(nil), nil
				}
				return p, nil
			})
			if stage == "nil-provider" {
				if !errors.Is(err, ErrInvalid) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, failure) || !errors.Is(err, failure.Cause) {
				t.Fatalf("lost provider error: %v", err)
			}
			if stage == "table" && len(p.names) != 1 {
				t.Fatal("opened blob after table failure")
			}
		})
	}
	// The exported factory path must reject unsupported types without AWS access.
	config.Provider = map[string]any{"type": "unsupported"}
	if _, err := Open(context.Background(), config, base.Secret); err == nil {
		t.Fatal("accepted unsupported provider")
	}
}

func TestOpenProbeRejectsDeletedResources(t *testing.T) {
	_, table, blobs, _ := fixture(t)
	p := &namedProvider{table: table, blobs: blobs}
	r, err := open(context.Background(), Config{RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}, bytes.Repeat([]byte{7}, 32), func(context.Context, map[string]any) (provider.StorageProvider, error) { return p, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.blobErr = contracts.ErrNotFound
	if err := r.Probe(context.Background()); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("missing bucket reported ready: %v", err)
	}
	p.blobErr = nil
	p.tableErr = contracts.ErrPermissionDenied
	if err := r.Probe(context.Background()); !errors.Is(err, contracts.ErrPermissionDenied) {
		t.Fatalf("inaccessible table reported ready: %v", err)
	}
}

// awsTableProvider keeps the real AWS DynamoDB provider but serves payloads from
// memory, so readiness exercises the provider's table status handling.
type awsTableProvider struct {
	provider.StorageProvider
	blobs blob.BlobStore
}

func (p awsTableProvider) OpenBlobStore(context.Context, string) (blob.BlobStore, error) {
	return p.blobs, nil
}

func TestOpenProbeToleratesUpdatingTable(t *testing.T) {
	var status, queryError atomic.Value
	status.Store("ACTIVE")
	queryError.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch target := r.Header.Get("X-Amz-Target"); target {
		case "DynamoDB_20120810.DescribeTable":
			io.WriteString(w, `{"Table":{"TableName":"physical-table","TableStatus":"`+status.Load().(string)+`","TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/physical-table","TableId":"00000000-0000-0000-0000-000000000000",`+
				`"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}`)
		case "DynamoDB_20120810.Query":
			if kind := queryError.Load().(string); kind != "" {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#`+kind+`","message":"table is not accessible"}`)
				return
			}
			io.WriteString(w, `{"Items":[],"Count":0,"ScannedCount":0}`)
		default:
			t.Errorf("unexpected DynamoDB call %q", target)
			w.WriteHeader(http.StatusBadRequest)
		}
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
	_, _, blobs, _ := fixture(t)
	config := Config{Provider: map[string]any{"type": "aws", "aws": map[string]any{"region": "us-east-1"}, "key_value_stores": map[string]any{"requests": "physical-table"}}, RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1"}
	initialize := func(ctx context.Context, value map[string]any) (provider.StorageProvider, error) {
		backend, err := providers.FromJSON(ctx, value)
		if err != nil {
			return nil, err
		}
		return awsTableProvider{StorageProvider: backend, blobs: blobs}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := open(ctx, config, bytes.Repeat([]byte{7}, 32), initialize)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Probe(ctx); err != nil {
		t.Fatalf("ACTIVE: %v", err)
	}
	// UpdateTable (autoscaling, billing mode, SSE, streams) keeps the table usable.
	status.Store("UPDATING")
	if err := r.Probe(ctx); err != nil {
		t.Fatalf("UPDATING table reported unready: %v", err)
	}
	// Unusable statuses still fail readiness through the data-plane read.
	for state, kind := range map[string]string{"ARCHIVED": "ResourceNotFoundException", "INACCESSIBLE_ENCRYPTION_CREDENTIALS": "KMSAccessDeniedException", "DELETING": "ResourceNotFoundException"} {
		status.Store(state)
		queryError.Store(kind)
		if err := r.Probe(ctx); err == nil {
			t.Fatalf("%s table reported ready", state)
		}
	}
}
