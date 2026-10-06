package budget

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestSlidingWindowBoundaryInvariants(t *testing.T) {
	window := Window{ID: "window", Duration: 2 * time.Second, Bucket: time.Second, Limit: 10}
	at := time.Unix(0, int64(2500*time.Millisecond))
	tests := []struct {
		name    string
		buckets map[int64]pricing.MicroUSD
		amount  pricing.MicroUSD
		active  pricing.MicroUSD
		allowed bool
	}{
		{name: "empty exactly within", buckets: nil, amount: 10, active: 0, allowed: true},
		{name: "equal limit remains admissible", buckets: map[int64]pricing.MicroUSD{0: 4, 1: 3}, amount: 3, active: 7, allowed: true},
		{name: "one micro above limit rejects", buckets: map[int64]pricing.MicroUSD{0: 4, 1: 3}, amount: 4, active: 7, allowed: false},
		{name: "expired bucket does not consume capacity", buckets: map[int64]pricing.MicroUSD{-1: 100, 0: 4, 1: 3}, amount: 3, active: 7, allowed: true},
		{name: "overfull active total rejects", buckets: map[int64]pricing.MicroUSD{0: 6, 1: 5}, amount: 0, active: 11, allowed: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allowed, active, err := window.CanReserve(test.buckets, test.amount, at)
			if err != nil {
				t.Fatal(err)
			}
			if active != test.active || allowed != test.allowed {
				t.Fatalf("CanReserve(%v, %d) = allowed=%v active=%d, want allowed=%v active=%d", test.buckets, test.amount, allowed, active, test.allowed, test.active)
			}
		})
	}
}
