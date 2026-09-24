package durable_test

import (
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/storage/conformance"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func TestReferenceRollingBudget(t *testing.T) {
	conformance.RunDurableRollingBudget(t, func(t *testing.T) (durable.BudgetMaterializer, func() time.Time, func(time.Duration)) {
		now := time.Date(2026, 7, 26, 2, 1, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		m, err := durable.NewReferenceBudgetMaterializer("rolling-gen", "rolling-inc", clock)
		if err != nil {
			t.Fatal(err)
		}
		return m, clock, func(d time.Duration) { now = now.Add(d) }
	})
}
