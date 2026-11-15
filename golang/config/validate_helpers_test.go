package config

import (
	"errors"
	"testing"
)

func TestFirstErrorStopsAtTheFirstFailingCheck(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	var ran []int
	check := func(index int, err error) func() error {
		return func() error { ran = append(ran, index); return err }
	}
	if err := firstError(check(1, nil), check(2, first), check(3, second)); err != first {
		t.Fatalf("firstError = %v, want %v", err, first)
	}
	if len(ran) != 2 || ran[0] != 1 || ran[1] != 2 {
		t.Fatalf("ran checks %v, want [1 2]", ran)
	}
	if err := firstError(check(4, nil)); err != nil {
		t.Fatalf("passing checks = %v", err)
	}
}

func TestClaimUniqueIDRejectsARepeatedID(t *testing.T) {
	seen := map[string]struct{}{}
	if err := claimUniqueID(seen, "a", "routes[0]", "route"); err != nil {
		t.Fatalf("first claim = %v", err)
	}
	err := claimUniqueID(seen, "a", "routes[1]", "route")
	if err == nil || err.Error() != `routes[1] duplicate route ID "a"` {
		t.Fatalf("repeated claim = %v", err)
	}
}
