package cloudstate

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers"
)

// Config embeds parsed provider JSON, including application aliases for existing
// tables/buckets. Secret is supplied separately by the worker's secret resolver.
// This is an adapter composition API; it does not change the CLI settings yet.
type Config struct {
	Provider     map[string]any `json:"provider"`
	RequestTable string         `json:"request_table"`
	PayloadStore string         `json:"payload_store"`
	Namespace    string         `json:"namespace"`
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
	return NewRepository(Options{Table: table, Blobs: blobs, Namespace: config.Namespace, Secret: secret})
}
