package budget

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

type Window struct {
	ID       string
	Duration time.Duration
	Bucket   time.Duration
	// LimitUSD is the authoritative exact budget limit. Limit is retained for
	// the versioned Redis compatibility path only.
	LimitUSD pricing.USD
	Limit    pricing.MicroUSD
}

func (window Window) Validate(maxBuckets int) error {
	if window.Duration <= 0 || window.Bucket <= 0 || (window.LimitUSD.IsZero() && window.Limit <= 0) {
		return fmt.Errorf("duration, bucket, and limit must be positive")
	}
	if window.Bucket > window.Duration || maxBuckets <= 0 {
		return fmt.Errorf("bucket exceeds bounded window")
	}
	buckets := int64(window.Duration/window.Bucket) + 2
	if buckets > int64(maxBuckets) {
		return fmt.Errorf("window has %d buckets, limit is %d", buckets, maxBuckets)
	}
	if !window.LimitUSD.IsZero() {
		if err := window.LimitUSD.Validate(); err != nil {
			return err
		}
	} else if window.Limit > pricing.RedisSafeLimit {
		return fmt.Errorf("window limit exceeds Redis-safe range")
	}
	return nil
}

// ValidateUSD applies the fixed-scale NUMERIC(38,18) contract and never
// converts through microUSD.
func (window Window) ValidateUSD(maxBuckets int) error {
	if window.Duration <= 0 || window.Bucket <= 0 || window.LimitUSD.IsZero() {
		return fmt.Errorf("duration, bucket, and exact USD limit must be positive")
	}
	if err := window.LimitUSD.Validate(); err != nil {
		return err
	}
	if window.Bucket > window.Duration || maxBuckets <= 0 {
		return fmt.Errorf("bucket exceeds bounded window")
	}
	if int64(window.Duration/window.Bucket)+2 > int64(maxBuckets) {
		return fmt.Errorf("window has %d buckets, limit is %d", int64(window.Duration/window.Bucket)+2, maxBuckets)
	}
	return nil
}

func FloorDiv(value, divisor int64) int64 {
	if divisor <= 0 {
		panic("floor division divisor must be positive")
	}
	quotient := value / divisor
	remainder := value % divisor
	if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
		quotient--
	}
	return quotient
}

func (window Window) Range(at time.Time) (first, last int64) {
	unit := window.Bucket.Nanoseconds()
	current := FloorDiv(at.UnixNano(), unit)
	first = FloorDiv(at.Add(-window.Duration).UnixNano(), unit)
	return first, current
}

func (window Window) ActiveSum(buckets map[int64]pricing.MicroUSD, at time.Time) (pricing.MicroUSD, error) {
	first, last := window.Range(at)
	indices := make([]int64, 0, last-first+1)
	for index := first; index <= last; index++ {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	total := pricing.MicroUSD(0)
	for _, index := range indices {
		value := buckets[index]
		var err error
		total, err = total.Add(value)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func (window Window) CanReserve(buckets map[int64]pricing.MicroUSD, amount pricing.MicroUSD, at time.Time) (bool, pricing.MicroUSD, error) {
	active, err := window.ActiveSum(buckets, at)
	if err != nil {
		return false, 0, err
	}
	if amount < 0 || amount > pricing.RedisSafeLimit {
		return false, 0, fmt.Errorf("reservation is outside safe range")
	}
	if active > window.Limit || amount > window.Limit-active {
		return false, active, nil
	}
	return true, active, nil
}

func (window Window) RetryAfter(buckets map[int64]pricing.MicroUSD, amount pricing.MicroUSD, at time.Time) (time.Duration, error) {
	ok, active, err := window.CanReserve(buckets, amount, at)
	if err != nil || ok {
		return 0, err
	}
	first, last := window.Range(at)
	bucketNanos := window.Bucket.Nanoseconds()
	durationNanos := window.Duration.Nanoseconds()
	for index := first; index <= last; index++ {
		value := buckets[index]
		if value == 0 {
			continue
		}
		active, err = active.Sub(value)
		if err != nil {
			return 0, err
		}
		if active > window.Limit || amount > window.Limit-active {
			continue
		}
		expires := (index+1)*bucketNanos + durationNanos
		candidate := time.Unix(0, expires).Sub(at)
		if candidate < 0 {
			candidate = 0
		}
		return candidate, nil
	}
	return math.MaxInt64, nil
}

func (window Window) ActiveSumUSD(buckets map[int64]pricing.USD, at time.Time) (pricing.USD, error) {
	first, last := window.Range(at)
	total := pricing.MustUSD("0")
	for index := first; index <= last; index++ {
		value := buckets[index]
		var err error
		total, err = total.Add(value)
		if err != nil {
			return pricing.USD{}, err
		}
	}
	return total, nil
}

func (window Window) CanReserveUSD(buckets map[int64]pricing.USD, amount pricing.USD, at time.Time) (bool, pricing.USD, error) {
	if err := window.ValidateUSD(1 << 20); err != nil {
		return false, pricing.USD{}, err
	}
	if err := amount.Validate(); err != nil {
		return false, pricing.USD{}, err
	}
	active, err := window.ActiveSumUSD(buckets, at)
	if err != nil {
		return false, pricing.USD{}, err
	}
	remaining, err := window.LimitUSD.Sub(active)
	if err != nil || amount.Cmp(remaining) > 0 {
		return false, active, nil
	}
	return true, active, nil
}
