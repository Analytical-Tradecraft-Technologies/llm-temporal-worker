package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestValidateGrantedWindowReservationAllowsBucketRollover(t *testing.T) {
	granted := admission.WindowReservation{
		PolicyID: "policy-1", WindowID: "window-1", Bucket: 100,
		Amount: 25_000, Limit: 5_000_000, AmountUSD: pricing.MustUSD("0.025"),
		LimitUSD: pricing.MustUSD("5"), BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour),
	}
	actual := granted
	actual.Bucket++
	if err := validateGrantedWindowReservation(actual, granted); err != nil {
		t.Fatalf("valid unexpired grant failed after budget bucket rollover: %v", err)
	}

	actual = granted
	actual.LimitUSD = pricing.USD{}
	if err := validateGrantedWindowReservation(actual, granted); err != nil {
		t.Fatalf("equivalent micro-unit route limit did not match canonical grant: %v", err)
	}

	actual = granted
	actual.AmountUSD = pricing.MustUSD("0.026")
	if err := validateGrantedWindowReservation(actual, granted); err == nil || !strings.Contains(err.Error(), "amount exceeds") {
		t.Fatalf("larger route amount error = %v", err)
	}

	actual = granted
	actual.DurationNanos++
	if err := validateGrantedWindowReservation(actual, granted); err == nil || !strings.Contains(err.Error(), "window geometry") {
		t.Fatalf("changed budget window error = %v", err)
	}
}
