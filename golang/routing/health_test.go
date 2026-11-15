package routing

import (
	"testing"
	"time"
)

// newTestPassiveHealth opens a route after two failures, with a one-minute
// cooldown, on a clock frozen at Unix time 100.
func newTestPassiveHealth() *PassiveHealth {
	return NewPassiveHealth(HealthPolicy{Threshold: 2, Cooldown: time.Minute}, func() time.Time { return time.Unix(100, 0) })
}

func TestPassiveHealthDoesNotCountAmbiguous(t *testing.T) {
	health := newTestPassiveHealth()
	health.Record("r", "v1", FailureAmbiguous)
	if got := health.View("v1").Routes["r"]; got.Open {
		t.Fatal("ambiguous outcome opened route")
	}
	health.Record("r", "v1", FailureDefiniteTransient)
	health.Record("r", "v1", FailureDefiniteTransient)
	if got := health.View("v1").Routes["r"]; !got.Open {
		t.Fatal("threshold did not open route")
	}
	health.Record("r", "v2", FailureSuccess)
	if got := health.View("v2").Routes["r"]; got.Open {
		t.Fatal("snapshot change did not reset health")
	}
}

func TestPassiveHealthClosesStaleSnapshotState(t *testing.T) {
	health := newTestPassiveHealth()

	health.Record("r", "v1", FailureAuthentication)
	if got := health.View("v1").Routes["r"]; !got.Open || !got.AuthOpen {
		t.Fatalf("authentication failure did not open route: %#v", got)
	}

	// A configuration snapshot replacement is the operator's signal that an
	// authentication/configuration failure may be retried. Merely observing
	// the new snapshot must therefore not carry the old open state forward.
	if got := health.View("v2").Routes["r"]; got.Open || got.AuthOpen {
		t.Fatalf("stale snapshot state remained open: %#v", got)
	}
}
