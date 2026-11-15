package llm

import (
	"errors"
	"testing"
)

func TestStoreValidStoresOnlyAValidValue(t *testing.T) {
	target := "old"
	failure := errors.New("invalid")
	if err := storeValid(&target, "new", failure); err != failure || target != "old" {
		t.Fatalf("failed validation: err %v, target %q", err, target)
	}
	if err := storeValid(&target, "new", nil); err != nil || target != "new" {
		t.Fatalf("passed validation: err %v, target %q", err, target)
	}
}
