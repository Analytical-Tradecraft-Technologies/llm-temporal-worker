package cloudstate

import (
	"errors"
	"fmt"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func TestDefinitelyRejectsUncertainOutcomes(t *testing.T) {
	uncertain := errors.Join(contracts.ErrAlreadyExists, contracts.ErrOutcomeUnknown)
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"target", contracts.ErrAlreadyExists, true},
		{"wrapped target", fmt.Errorf("create: %w", contracts.ErrAlreadyExists), true},
		{"other error", contracts.ErrConflict, false},
		{"uncertain target", uncertain, false},
	} {
		if got := definitely(tc.err, contracts.ErrAlreadyExists); got != tc.want {
			t.Errorf("%s: definitely = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestJSONItemStoresEncodedValueInField(t *testing.T) {
	key := kv.KeyValueKey{PartitionKey: "partition", SortKey: "sort"}
	item := jsonItem(key, "cache", cachePointer{Version: 1, Blob: "blob", Stream: "stream"})
	if item.PartitionKey != "partition" || item.SortKey != "sort" || len(item.Fields) != 1 {
		t.Fatalf("item = %#v", item)
	}
	want := kv.Bytes([]byte(`{"version":1,"blob":"blob","stream":"stream"}`))
	if fmt.Sprint(item.Fields["cache"]) != fmt.Sprint(want) {
		t.Fatalf("cache field = %#v, want %#v", item.Fields["cache"], want)
	}
}
