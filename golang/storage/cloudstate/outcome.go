package cloudstate

import (
	"encoding/json"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// definitely reports whether err is target with a known outcome. An uncertain
// read or write is never reinterpreted as a definite miss or conflict: a later
// identical retry may still reconcile it.
func definitely(err, target error) bool {
	return errors.Is(err, target) && !errors.Is(err, contracts.ErrOutcomeUnknown)
}

// jsonItem is the KV item at key holding value's JSON encoding in field.
func jsonItem(key kv.KeyValueKey, field string, value any) kv.KeyValueItem {
	data, _ := json.Marshal(value)
	return kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{field: kv.Bytes(data)}}
}
