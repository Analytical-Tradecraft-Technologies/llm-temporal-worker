package redis

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
)

func providerTestEvent(t *testing.T, at time.Time, route string, mutate func(*control.StatusObservation)) control.StatusEvent {
	t.Helper()
	observation := control.StatusObservation{ConfigDigest: sha256.Sum256([]byte("provider-config")), RouteID: route, EndpointID: "endpoint", EndpointAccountHMAC: sha256.Sum256([]byte("account")), Provider: "openai", EndpointFamily: "responses", ObservedAt: at, ExpiresAt: at.Add(time.Minute), Source: control.SourceInference, Availability: control.AvailabilityAvailable, Credit: control.CreditOK, Billing: control.BillingOK, ConfigEpoch: "epoch-1", EvidenceDigest: sha256.Sum256([]byte("evidence"))}
	if mutate != nil {
		mutate(&observation)
	}
	event, err := control.NewStatusEvent(observation)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func providerTestInventory(at time.Time) control.InventorySnapshot {
	models := []control.Model{{ProviderModelID: "model-a", Lifecycle: control.LifecycleAvailable, SafeMetadata: map[string]string{"kind": "text"}}, {ProviderModelID: "model-b", Lifecycle: control.LifecycleDeprecated}}
	return control.InventorySnapshot{ConfigDigest: sha256.Sum256([]byte("provider-config")), Provider: "openai", EndpointID: "endpoint", EndpointAccountHMAC: sha256.Sum256([]byte("account")), EndpointFamily: "responses", Region: "global", Source: control.InventoryProviderAPI, ObservedAt: at, ExpiresAt: at.Add(time.Minute), Complete: true, Models: models, InventoryDigest: control.InventoryDigest(models)}
}

func TestProviderProjectionPreservesSeparateIncidentEvidence(t *testing.T) {
	at := time.Now().UTC()
	incident := providerTestEvent(t, at, "route", func(o *control.StatusObservation) {
		o.Credit = control.CreditExhausted
		o.Billing = control.BillingIssue
		o.ProviderCode = "insufficient_quota"
	})
	record, applied := applyProviderEvent(providerStatusRecord{}, incident)
	if !applied {
		t.Fatal("initial incident not applied")
	}
	// Inference cannot downgrade an exhausted-credit incident to low/unknown,
	// nor replace its authoritative evidence with an ordinary observation.
	for i, credit := range []control.CreditState{control.CreditOK, control.CreditLow, control.CreditUnknown} {
		event := providerTestEvent(t, at.Add(time.Duration(i+1)*time.Second), "route", func(o *control.StatusObservation) { o.Credit = credit })
		record, applied = applyProviderEvent(record, event)
		if !applied || record.Status.Credit != control.CreditExhausted || record.Status.Billing != control.BillingIssue || record.CreditEvidence.EventDigest != incident.EventDigest || record.BillingEvidence.EventDigest != incident.EventDigest {
			t.Fatalf("lost sticky evidence: %#v", record)
		}
		raw, _ := json.Marshal(record)
		var decoded providerStatusRecord
		if err := decodeProviderStatus(string(raw), incident.ConfigDigest, &decoded); err != nil {
			t.Fatalf("decode sticky projection: %v", err)
		}
	}
	// Authoritative credit clearance must not lose a separate billing incident.
	clear := providerTestEvent(t, at.Add(4*time.Second), "route", func(o *control.StatusObservation) {
		o.Source = control.SourceManagementAPI
		o.Billing = control.BillingUnknown
	})
	record, applied = applyProviderEvent(record, clear)
	if !applied || record.CreditEvidence != nil || record.BillingEvidence == nil || record.Status.Billing != control.BillingIssue {
		t.Fatalf("partial clearance: %#v", record)
	}
	clear = providerTestEvent(t, at.Add(5*time.Second), "route", func(o *control.StatusObservation) { o.Source = control.SourceOperator })
	record, applied = applyProviderEvent(record, clear)
	if !applied || record.CreditEvidence != nil || record.BillingEvidence != nil || !record.Status.CreditConfirmedAt.IsZero() {
		t.Fatalf("incident not cleared: %#v", record)
	}
}

func TestProviderProjectionRejectsStaleDuplicateAndForeignEvents(t *testing.T) {
	at := time.Now().UTC()
	event := providerTestEvent(t, at, "route", nil)
	record, _ := applyProviderEvent(providerStatusRecord{}, event)
	for _, candidate := range []control.StatusEvent{event, providerTestEvent(t, at.Add(-time.Second), "route", nil), providerTestEvent(t, at.Add(time.Second), "foreign-route", nil), providerTestEvent(t, at.Add(time.Second), "route", func(o *control.StatusObservation) { o.EndpointAccountHMAC = sha256.Sum256([]byte("other")) })} {
		updated, applied := applyProviderEvent(record, candidate)
		if applied || updated.Status.LastEventDigest != event.EventDigest {
			t.Fatal("stale/foreign event changed state")
		}
	}
}

func TestProviderProjectionRejectsCorruptRecords(t *testing.T) {
	event := providerTestEvent(t, time.Now().UTC(), "route", nil)
	record, _ := applyProviderEvent(providerStatusRecord{}, event)
	for _, change := range []func(*providerStatusRecord){func(r *providerStatusRecord) { r.Schema = "future" }, func(r *providerStatusRecord) { r.Status.ConfigDigest = [32]byte{} }, func(r *providerStatusRecord) { r.Status.Circuit = "invalid" }, func(r *providerStatusRecord) { r.Status.Credit = control.CreditExhausted }, func(r *providerStatusRecord) { r.Status.StaleAfter = r.Status.ObservedAt }} {
		bad := record
		change(&bad)
		raw, _ := json.Marshal(bad)
		var decoded providerStatusRecord
		if err := decodeProviderStatus(string(raw), event.ConfigDigest, &decoded); err == nil {
			t.Fatal("corrupt record accepted")
		}
	}
}

func TestProviderInventoryValidation(t *testing.T) {
	snapshot := providerTestInventory(time.Now().UTC())
	if err := validateInventorySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.InventoryDigest = [32]byte{1}
	if err := validateInventorySnapshot(snapshot); err == nil {
		t.Fatal("incorrect digest accepted")
	}
	snapshot = providerTestInventory(time.Now().UTC())
	snapshot.Models[0].DisplayName = "unsafe\ntext"
	snapshot.InventoryDigest = control.InventoryDigest(snapshot.Models)
	if err := validateInventorySnapshot(snapshot); err == nil {
		t.Fatal("unsafe model accepted")
	}
}

func FuzzProviderStatusDecode(f *testing.F) {
	f.Add("{}")
	f.Add("null")
	f.Add("not-json")
	f.Fuzz(func(t *testing.T, raw string) {
		var record providerStatusRecord
		_ = decodeProviderStatus(raw, sha256.Sum256([]byte("config")), &record)
	})
}
