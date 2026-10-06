package cloudstate

import (
	"context"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// Probe verifies read access without creating resources or enumerating payloads.
// Open-created repositories also revalidate the named resources: the generic
// blob not-found error alone cannot distinguish a missing object from a missing
// bucket. Injected stores retain responsibility for resource-level health.
// Write permissions are exercised only by actual requests, not readiness.
func (r *Repository) Probe(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if r.probeStores != nil {
		if err := r.probeStores(ctx); err != nil {
			return err
		}
	}
	if _, err := r.table.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: r.partition(0), PageSize: 1}); err != nil {
		return err
	}
	probeKey := blob.BlobKey(r.namespace + "/payload/" + r.digest("readiness", nil))
	if regional, ok := r.blobs.(interface {
		ProbeRead(context.Context, blob.BlobKey) error
	}); ok {
		return regional.ProbeRead(ctx, probeKey)
	}
	opened, err := r.blobs.Open(ctx, probeKey)
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
