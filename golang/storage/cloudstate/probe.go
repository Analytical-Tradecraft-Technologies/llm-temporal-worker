package cloudstate

import (
	"context"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// Probe verifies read access without creating resources or enumerating payloads.
// Write permissions are exercised only by actual requests, not readiness.
func (r *Repository) Probe(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if _, err := r.table.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: r.partition(0), PageSize: 1}); err != nil {
		return err
	}
	opened, err := r.blobs.Open(ctx, blob.BlobKey(r.namespace+"/payload/"+r.digest("readiness", nil)))
	if err == nil {
		if opened.Body == nil {
			return ErrCorrupt
		}
		return opened.Body.Close()
	}
	if errors.Is(err, contracts.ErrNotFound) {
		return nil
	}
	return err
}
