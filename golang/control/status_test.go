package control

import (
	"testing"
	"time"
)

func digest(value byte) [32]byte {
	var digest [32]byte
	digest[0] = value
	return digest
}

func observation(at time.Time) StatusObservation {
	return StatusObservation{
		ConfigDigest: digest(1), RouteID: "route", EndpointID: "endpoint", EndpointAccountHMAC: digest(2),
		Provider: "provider", EndpointFamily: "chat", ObservedAt: at, Source: SourceInference,
		Availability: AvailabilityAvailable, Credit: CreditOK, Billing: BillingOK, EvidenceDigest: digest(3),
		ConfigEpoch: "epoch-1", ExpiresAt: at.Add(time.Minute),
	}
}

func TestStatusEventRejectsUnsafeProviderText(t *testing.T) {
	value := observation(time.Unix(100, 0))
	value.ProviderCode = "429 quota\n"
	if _, err := NewStatusEvent(value); err == nil {
		t.Fatal("unsafe provider code was accepted")
	}
}

func TestStatusEventRequiresEvidenceForCreditIncident(t *testing.T) {
	value := observation(time.Unix(100, 0))
	value.Credit = CreditExhausted
	if _, err := NewStatusEvent(value); err == nil {
		t.Fatal("exhausted credit without provider evidence was accepted")
	}
	value.ProviderCode = "provider_specific_free_text"
	if _, err := NewStatusEvent(value); err == nil {
		t.Fatal("exhausted credit with undocumented provider evidence was accepted")
	}
	value.ProviderCode = "insufficient_quota"
	if _, err := NewStatusEvent(value); err != nil {
		t.Fatalf("documented provider evidence rejected: %v", err)
	}
	value.Source, value.SafeErrorCode = SourceOperator, "credit_exhausted"
	value.ProviderCode = ""
	if _, err := NewStatusEvent(value); err != nil {
		t.Fatalf("operator evidence rejected: %v", err)
	}
}

func TestRouteStatusCreditIncidentIsStickyUntilAuthoritativeClear(t *testing.T) {
	base := time.Unix(100, 0)
	status := RouteStatus{}
	value := observation(base)
	value.Credit, value.Billing = CreditExhausted, BillingIssue
	value.Source, value.ProviderCode = SourceManagementAPI, "insufficient_quota"
	event, err := NewStatusEvent(value)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(event) || status.Credit != CreditExhausted || status.Billing != BillingIssue {
		t.Fatalf("incident projection = %#v", status)
	}
	if !status.CreditConfirmedAt.Equal(base) {
		t.Fatalf("initial credit confirmation = %v, want %v", status.CreditConfirmedAt, base)
	}
	value = observation(base.Add(time.Second))
	value.Source, value.Credit, value.Billing = SourceInference, CreditOK, BillingOK
	event, err = NewStatusEvent(value)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(event) || status.Credit != CreditExhausted || status.Billing != BillingIssue {
		t.Fatalf("inference observation cleared incident: %#v", status)
	}
	if !status.CreditConfirmedAt.Equal(base) {
		t.Fatalf("inference changed credit confirmation = %v, want %v", status.CreditConfirmedAt, base)
	}
	value = observation(base.Add(1250 * time.Millisecond))
	value.Source, value.Credit, value.Billing, value.ProviderCode = SourceInference, CreditOK, BillingOK, "quota_restored"
	event, err = NewStatusEvent(value)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(event) || !status.CreditConfirmedAt.Equal(base) {
		t.Fatalf("inference restore changed credit confirmation = %v, want %v", status.CreditConfirmedAt, base)
	}
	value = observation(base.Add(1500 * time.Millisecond))
	value.Source, value.Credit, value.Billing = SourceStartupProbe, CreditUnknown, BillingUnknown
	event, err = NewStatusEvent(value)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(event) || status.Credit != CreditExhausted || status.Billing != BillingIssue {
		t.Fatalf("unknown health observation cleared incident: %#v", status)
	}
	if !status.CreditConfirmedAt.Equal(base) {
		t.Fatalf("startup probe changed credit confirmation = %v, want %v", status.CreditConfirmedAt, base)
	}
	value = observation(base.Add(2 * time.Second))
	value.Source, value.Credit, value.Billing, value.ProviderCode = SourceManagementAPI, CreditOK, BillingOK, "quota_restored"
	event, err = NewStatusEvent(value)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(event) || status.Credit != CreditOK || status.Billing != BillingOK {
		t.Fatalf("authoritative clear not applied: %#v", status)
	}
	if !status.CreditConfirmedAt.IsZero() {
		t.Fatalf("authoritative clear retained credit confirmation = %v", status.CreditConfirmedAt)
	}
}

func TestRouteStatusInferenceProviderCodeRefreshesConfirmation(t *testing.T) {
	base := time.Unix(200, 0)
	status := RouteStatus{}
	incident := observation(base)
	incident.Credit, incident.Billing = CreditExhausted, BillingIssue
	incident.Source, incident.ProviderCode = SourceManagementAPI, "insufficient_quota"
	first, err := NewStatusEvent(incident)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(first) {
		t.Fatal("initial incident was not applied")
	}
	withEvidence := observation(base.Add(time.Second))
	withEvidence.Source = SourceInference
	withEvidence.Credit, withEvidence.Billing = CreditExhausted, BillingIssue
	withEvidence.ProviderCode = "billing_hard_limit"
	second, err := NewStatusEvent(withEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(second) {
		t.Fatal("provider-evidence inference was not applied")
	}
	if !status.CreditConfirmedAt.Equal(withEvidence.ObservedAt) {
		t.Fatalf("provider-evidence inference confirmation = %v, want %v", status.CreditConfirmedAt, withEvidence.ObservedAt)
	}
}

func TestRouteStatusRejectsStaleEvent(t *testing.T) {
	base := time.Unix(100, 0)
	status := RouteStatus{}
	first, _ := NewStatusEvent(observation(base))
	if !status.Apply(first) {
		t.Fatal("first event rejected")
	}
	second, _ := NewStatusEvent(observation(base))
	if status.Apply(second) {
		t.Fatal("stale event applied")
	}
}

func TestRouteStatusRejectsForeignConfigAfterEpochChange(t *testing.T) {
	base := time.Unix(300, 0)
	status := RouteStatus{}
	first, err := NewStatusEvent(observation(base))
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(first) {
		t.Fatal("first event rejected")
	}

	foreign := observation(base.Add(time.Second))
	foreign.ConfigDigest = digest(9)
	foreign.ConfigEpoch = "epoch-2"
	foreign.RouteID = first.RouteID
	foreign.EndpointID = first.EndpointID
	foreignEvent, err := NewStatusEvent(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if status.Apply(foreignEvent) {
		t.Fatal("event from a different configuration digest was applied")
	}
	if status.ConfigDigest != first.ConfigDigest || status.ConfigEpoch != first.ConfigEpoch || status.LastEventDigest != first.EventDigest {
		t.Fatalf("foreign event changed projection: %#v", status)
	}
}

func TestRouteStatusPermissionDeniedCountsTowardThreshold(t *testing.T) {
	base := time.Unix(400, 0)
	status := RouteStatus{}
	for i := 0; i < 3; i++ {
		denied := observation(base.Add(time.Duration(i) * time.Second))
		denied.Availability, denied.SafeErrorCode = AvailabilityUnavailable, "permission_denied"
		event, err := NewStatusEvent(denied)
		if err != nil {
			t.Fatal(err)
		}
		if !status.Apply(event) {
			t.Fatal("permission denial rejected")
		}
		if want := i == 2; (status.Circuit == CircuitOpen) != want {
			t.Fatalf("after %d permission denials circuit = %s", i+1, status.Circuit)
		}
	}
}

func TestRouteStatusLaterFailureDoesNotShortenOpenHold(t *testing.T) {
	base := time.Unix(500, 0)
	status := RouteStatus{}
	auth := observation(base)
	auth.Availability, auth.SafeErrorCode, auth.ExpiresAt = AvailabilityUnavailable, "authentication", base.Add(24*time.Hour)
	authEvent, err := NewStatusEvent(auth)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(authEvent) || status.Circuit != CircuitOpen {
		t.Fatalf("authentication failure did not open the circuit: %#v", status)
	}
	transient := observation(base.Add(time.Second))
	transient.Availability, transient.SafeErrorCode = AvailabilityUnavailable, "provider_unavailable"
	transientEvent, err := NewStatusEvent(transient)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(transientEvent) || status.Circuit != CircuitOpen || !status.StaleAfter.Equal(auth.ExpiresAt) {
		t.Fatalf("later failure shortened the open hold: %#v", status)
	}
	success := observation(base.Add(2 * time.Second))
	successEvent, err := NewStatusEvent(success)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Apply(successEvent) || status.Circuit != CircuitClosed || !status.StaleAfter.Equal(success.ExpiresAt) {
		t.Fatalf("success did not close the circuit: %#v", status)
	}
}
