package budget

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func FuzzSlidingWindowBoundaries(f *testing.F) {
	f.Add(uint64(0), uint64(10), uint64(10))
	f.Add(uint64(7), uint64(3), uint64(100))
	f.Add(uint64(11), uint64(0), uint64(1))
	f.Fuzz(func(t *testing.T, activeRaw, amountRaw, expiredRaw uint64) {
		active := pricing.MicroUSD(activeRaw % 12)
		amount := pricing.MicroUSD(amountRaw % 12)
		expired := pricing.MicroUSD(expiredRaw % 12)
		window := Window{ID: "fuzz", Duration: 2 * time.Second, Bucket: time.Second, Limit: 10}
		at := time.Unix(0, int64(2500*time.Millisecond))
		allowed, gotActive, err := window.CanReserve(map[int64]pricing.MicroUSD{-1: expired, 0: active}, amount, at)
		if err != nil {
			t.Fatal(err)
		}
		wantAllowed := active <= window.Limit && amount <= window.Limit-active
		if gotActive != active || allowed != wantAllowed {
			t.Fatalf("CanReserve(active=%d, amount=%d, expired=%d) = allowed=%v active=%d; want allowed=%v active=%d", active, amount, expired, allowed, gotActive, wantAllowed, active)
		}
	})
}
