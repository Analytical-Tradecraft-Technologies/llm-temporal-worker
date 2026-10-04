package control

import (
	"crypto/sha256"
	"testing"
	"time"
)

func TestRouteCircuitDefiniteFailuresReplayAndRecovery(t *testing.T) {
	var status RouteStatus
	at := time.Now().UTC()
	apply := func(offset int, availability Availability, code string) StatusEvent {
		o := StatusObservation{ConfigDigest: sha256.Sum256([]byte("config")), EndpointAccountHMAC: sha256.Sum256([]byte("account")), EvidenceDigest: sha256.Sum256([]byte(code)), RouteID: "route", EndpointID: "endpoint", Provider: "provider", EndpointFamily: "family", ConfigEpoch: "epoch", ObservedAt: at.Add(time.Duration(offset) * time.Second), ExpiresAt: at.Add(time.Minute), Source: SourceInference, Availability: availability, Credit: CreditOK, Billing: BillingOK, SafeErrorCode: code}
		e, err := NewStatusEvent(o)
		if err != nil {
			t.Fatal(err)
		}
		if !status.Apply(e) {
			t.Fatal("observation not applied")
		}
		return e
	}
	first := apply(0, AvailabilityUnavailable, "provider_unavailable")
	if status.Apply(first) || status.ConsecutiveDefiniteFailures != 1 {
		t.Fatal("replay counted twice")
	}
	apply(1, AvailabilityUnknown, "ambiguous_dispatch")
	if status.ConsecutiveDefiniteFailures != 1 || status.Circuit != CircuitClosed {
		t.Fatal("uncertain outcome opened circuit")
	}
	apply(2, AvailabilityDegraded, "provider_rate_limited")
	apply(3, AvailabilityUnavailable, "provider_unavailable")
	if status.Circuit != CircuitOpen || status.ConsecutiveDefiniteFailures != 3 {
		t.Fatal("definite failures failed to open circuit")
	}
	apply(4, AvailabilityAvailable, "")
	if status.Circuit != CircuitClosed || status.ConsecutiveDefiniteFailures != 0 {
		t.Fatal("success failed to reset circuit")
	}
	apply(5, AvailabilityUnavailable, "authentication")
	if status.Circuit != CircuitOpen {
		t.Fatal("authentication failure did not open immediately")
	}
}
