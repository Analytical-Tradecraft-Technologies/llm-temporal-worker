package provider

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ParseRetryAfter parses a Retry-After response header according to RFC 9110.
// It supports delta-seconds and HTTP-date values; malformed and negative values
// are rejected so callers can use their usual retry policy instead.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	allDigits := true
	for _, character := range value {
		if character < '0' || character > '9' {
			allDigits = false
			break
		}
	}
	if allDigits {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64(math.MaxInt64/int64(time.Second)) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	if retryAt.Before(now) {
		return 0, false
	}
	return retryAt.Sub(now), true
}
