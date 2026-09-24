package cloudstate

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
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
