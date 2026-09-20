// Package usage normalizes provider token accounting for catalog pricing.
package usage

import "fmt"

// OrdinaryInput removes the cache subsets from an inclusive input total.
// Validate before subtracting so malformed counts cannot wrap or underbill.
func OrdinaryInput(total, cacheRead, cacheWrite int64) (int64, error) {
	if total < 0 || cacheRead < 0 || cacheWrite < 0 {
		return 0, fmt.Errorf("provider input token counts must not be negative")
	}
	if cacheRead > total || cacheWrite > total-cacheRead {
		return 0, fmt.Errorf("provider cache token counts exceed input total")
	}
	return total - cacheRead - cacheWrite, nil
}
