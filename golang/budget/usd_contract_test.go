package budget

import (
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestWindowExactUSDReservationPreservesSubMicroAmount(t *testing.T) {
	window := Window{Duration: time.Minute, Bucket: time.Second, LimitUSD: pricing.MustUSD("1.000000000000000001")}
	if err := window.ValidateUSD(2048); err != nil {
		t.Fatal(err)
	}
	allowed, active, err := window.CanReserveUSD(map[int64]pricing.USD{}, pricing.MustUSD("0.000000000000000001"), time.Unix(0, 0))
	if err != nil || !allowed || !active.IsZero() {
		t.Fatalf("CanReserveUSD = allowed %t active %s err %v", allowed, active.String(), err)
	}
}

func TestWindowExactUSDRejectsOverflow(t *testing.T) {
	window := Window{Duration: time.Minute, Bucket: time.Second, LimitUSD: pricing.MustUSD("1")}
	if _, _, err := window.CanReserveUSD(map[int64]pricing.USD{}, pricing.MustUSD("2"), time.Unix(0, 0)); err != nil {
		t.Fatalf("oversized reservation should be a denial, not a malformed request: %v", err)
	}
	if allowed, _, _ := window.CanReserveUSD(map[int64]pricing.USD{}, pricing.MustUSD("2"), time.Unix(0, 0)); allowed {
		t.Fatal("exact USD reservation exceeded its limit")
	}
}
