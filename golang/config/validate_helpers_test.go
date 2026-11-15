package config

import (
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	testassert.NoError(t, firstError(check(4, nil)), "passing checks")
}

func TestClaimUniqueIDRejectsARepeatedID(t *testing.T) {
	seen := map[string]struct{}{}
	testassert.NoError(t, claimUniqueID(seen, "a", "routes[0]", "route"), "first claim")
	err := claimUniqueID(seen, "a", "routes[1]", "route")
	if err == nil || err.Error() != `routes[1] duplicate route ID "a"` {
		t.Fatalf("repeated claim = %v", err)
	}
}
