package cloudstate

import (
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// uncertainWriteStages are the writes of a saved request record that
// injectUncertainWrite can fail with an unknown outcome.
var uncertainWriteStages = []string{"blob", "event", "index-before", "index-after"}

// failTableWrites is a table hook that returns before and after for every
// write whose partition key contains fragment, limited to action unless it is
// empty. A before error rejects the write; an after error applies it and
// loses the acknowledgement.
func failTableWrites(action, fragment string, before, after error) func(string, kv.KeyValueItem) (error, error) {
	return func(gotAction string, item kv.KeyValueItem) (error, error) {
		if (action == "" || gotAction == action) && strings.Contains(item.PartitionKey, fragment) {
			return before, after
		}
		return nil, nil
	}
}

// forbidWrites reports "<actor> wrote a row" or "<actor> wrote a blob" as a
// test error for every write to a non-nil table or blob store.
func forbidWrites(t testing.TB, table *memoryTable, blobs *memoryBlobs, actor string) {
	if table != nil {
		table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error(actor + " wrote a row"); return nil, nil }
	}
	if blobs != nil {
		blobs.hook = func(blob.BlobKey) (error, error) { t.Error(actor + " wrote a blob"); return nil, nil }
	}
}

// injectUncertainWrite makes the first write at stage report an unknown
// outcome: the payload blob, the request event row, or the pending index
// row, failed before ("index-before") or after ("index-after") it is
// applied. The returned flag reports whether the fault fired.
func injectUncertainWrite(stage string, table *memoryTable, blobs *memoryBlobs) *bool {
	fault, fired := &contracts.StorageError{Kind: contracts.ErrOutcomeUnknown}, false
	blobs.hook = func(blob.BlobKey) (error, error) {
		if stage == "blob" && !fired {
			fired = true
			return nil, fault
		}
		return nil, nil
	}
	table.hook = func(action string, item kv.KeyValueItem) (error, error) {
		if !fired && (stage == "event" && action == "create" && strings.Contains(item.PartitionKey, "/request/") ||
			strings.HasPrefix(stage, "index") && action == "replace" && strings.Contains(item.PartitionKey, "/pending/")) {
			fired = true
			if stage == "index-before" {
				return fault, nil
			}
			return nil, fault
		}
		return nil, nil
	}
	return &fired
}
