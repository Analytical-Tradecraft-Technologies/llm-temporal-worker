package cloudstate

import (
	"context"
	"encoding/json"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers"
)

// Config embeds parsed provider JSON, including application aliases for existing
// tables/buckets. Secret is supplied separately by the worker's secret resolver.
// Worker settings use the same provider shape under state.requests.
type Config struct {
	Provider     map[string]any `json:"provider"`
	RequestTable string         `json:"request_table"`
	PayloadStore string         `json:"payload_store"`
	Namespace    string         `json:"namespace"`
	// ParentSnapshotStorage is "" or "inline" (the default) or "blob"; see
	// Options.ParentSnapshotBlob.
	ParentSnapshotStorage string `json:"parent_snapshot_storage,omitempty"`
}

func (c Config) parentSnapshotBlob() (bool, error) {
	switch c.ParentSnapshotStorage {
	case "", "inline":
		return false, nil
	case "blob":
		return true, nil
	}
	return false, ErrInvalid
}

// Open uses cloud-storage's provider factory (currently AWS IAM authentication).
// It opens existing resources; it never creates tables, buckets, or permissions.
func Open(ctx context.Context, config Config, secret []byte) (*Repository, error) {
	return open(ctx, config, secret, providers.FromJSON)
}

func open(ctx context.Context, config Config, secret []byte, initialize func(context.Context, map[string]any) (provider.StorageProvider, error)) (*Repository, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !namespacePattern.MatchString(config.Namespace) || !safeText(config.RequestTable, 256) || !safeText(config.PayloadStore, 256) || len(secret) != 32 {
		return nil, ErrInvalid
	}
	parentSnapshotBlob, err := config.parentSnapshotBlob()
	if err != nil {
		return nil, err
	}
	if aws, ok := config.Provider["aws"].(map[string]any); ok && aws["failover"] != nil {
		data, err := json.Marshal(aws["failover"])
		if err != nil {
			return nil, ErrInvalid
		}
		var f regionalOptions
		if err = json.Unmarshal(data, &f); err != nil {
			return nil, ErrInvalid
		}
		return openRegional(ctx, config, secret, initialize, f)
	}
	backend, err := initialize(ctx, config.Provider)
	if err != nil {
		return nil, err
	}
	if nilInterface(backend) {
		return nil, ErrInvalid
	}
	table, err := backend.OpenKeyValueStore(ctx, config.RequestTable)
	if err != nil {
		return nil, err
	}
	blobs, err := backend.OpenBlobStore(ctx, config.PayloadStore)
	if err != nil {
		return nil, err
	}
	repository, err := NewRepository(Options{Table: table, Blobs: blobs, Namespace: config.Namespace, Secret: secret, ParentSnapshotBlob: parentSnapshotBlob})
	if err != nil {
		return nil, err
	}
	repository.probeStores = func(ctx context.Context) error {
		if _, err := backend.OpenKeyValueStore(ctx, config.RequestTable); err != nil {
			return err
		}
		_, err := backend.OpenBlobStore(ctx, config.PayloadStore)
		return err
	}
	return repository, nil
}
