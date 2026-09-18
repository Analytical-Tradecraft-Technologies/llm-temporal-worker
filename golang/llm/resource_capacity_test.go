package llm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestResourceCapacityLeaseRecordsAreClosedAndIdentityBound(t *testing.T) {
	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	request := ResourceCapacityAcquireRequestV1{APIVersion: ResourceCapacityAcquireAPIVersion, Context: RequestContext{Tenant: "tenant", Project: "competition", Actor: "forecast-workflow"}, ResourceClass: ResourceClassPythonStage2, LeaseKey: "run/attempt/stage2/ensemble/2", GenerationID: "capacity-2026-08-11", ManifestSHA256: strings.Repeat("a", 64), QueueDeadline: now.Add(370 * time.Second)}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ResourceCapacityAcquireRequestV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	unknown := []byte(strings.Replace(string(encoded), `"lease_key":`, `"caller_limit":99,"lease_key":`, 1))
	if err := json.Unmarshal(unknown, &decoded); err == nil {
		t.Fatal("unknown caller capacity field was accepted")
	}
	lease := ResourceCapacityLeaseV1{APIVersion: ResourceCapacityAcquireAPIVersion, Context: request.Context, ResourceClass: request.ResourceClass, LeaseKey: request.LeaseKey, LeaseID: strings.Repeat("b", 64), GenerationID: request.GenerationID, ManifestSHA256: request.ManifestSHA256, QueueDeadline: request.QueueDeadline, AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
	if _, err := json.Marshal(lease); err != nil {
		t.Fatal(err)
	}
	renew := ResourceCapacityRenewRequestV1{APIVersion: ResourceCapacityRenewAPIVersion, Lease: lease}
	if _, err := json.Marshal(renew); err != nil {
		t.Fatal(err)
	}
	release := ResourceCapacityReleaseRequestV1{APIVersion: ResourceCapacityReleaseAPIVersion, Lease: lease}
	receipt := ResourceCapacityReleaseResponseV1{APIVersion: ResourceCapacityReleaseAPIVersion, LeaseID: lease.LeaseID, Released: true}
	if err := receipt.ValidateFor(release); err != nil {
		t.Fatal(err)
	}
	receipt.LeaseID = strings.Repeat("c", 64)
	if err := receipt.ValidateFor(release); err == nil {
		t.Fatal("release receipt substitution was accepted")
	}
}

func TestForecastEventCapacityClassIsSignedServerPolicy(t *testing.T) {
	request := ResourceCapacityAcquireRequestV1{
		APIVersion:     ResourceCapacityAcquireAPIVersion,
		Context:        RequestContext{Tenant: "customer", Project: "metaculus", Actor: "forecast-workflow"},
		ResourceClass:  ResourceClassForecastEvent,
		LeaseKey:       "forecast-event:" + strings.Repeat("b", 64),
		GenerationID:   "capacity-2026-08-11",
		ManifestSHA256: strings.Repeat("a", 64),
		QueueDeadline:  time.Date(2026, 8, 11, 6, 0, 5, 0, time.UTC),
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	request.ResourceClass = "caller-defined"
	if err := request.Validate(); err == nil {
		t.Fatal("caller-defined resource capacity class was accepted")
	}
}
