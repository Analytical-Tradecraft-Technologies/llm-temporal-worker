package budget

import (
	"testing"

	"github.com/google/uuid"
)

func TestBudgetIdentitiesAreDeterministicSnapshotBoundUUIDs(t *testing.T) {
	firstDigest, secondDigest := [32]byte{1}, [32]byte{2}
	policyID, err := PolicyIdentity(firstDigest, "policy-a")
	if err != nil {
		t.Fatal(err)
	}
	policyReplay, _ := PolicyIdentity(firstDigest, "policy-a")
	policyOtherSnapshot, _ := PolicyIdentity(secondDigest, "policy-a")
	if _, err := uuid.Parse(policyID); err != nil || policyID != policyReplay || policyID == policyOtherSnapshot {
		t.Fatalf("policy identities = %q replay=%q other=%q parse=%v", policyID, policyReplay, policyOtherSnapshot, err)
	}
	windowID, err := WindowIdentity(policyID, 0)
	if err != nil {
		t.Fatal(err)
	}
	windowReplay, _ := WindowIdentity(policyID, 0)
	windowNext, _ := WindowIdentity(policyID, 1)
	if _, err := uuid.Parse(windowID); err != nil || windowID != windowReplay || windowID == windowNext {
		t.Fatalf("window identities = %q replay=%q next=%q parse=%v", windowID, windowReplay, windowNext, err)
	}
}

func TestBudgetIdentitiesRejectUnboundInputs(t *testing.T) {
	if _, err := PolicyIdentity([32]byte{}, "policy-a"); err == nil {
		t.Fatal("zero configuration digest was accepted")
	}
	if _, err := PolicyIdentity([32]byte{1}, " policy-a"); err == nil {
		t.Fatal("non-canonical policy key was accepted")
	}
	if _, err := WindowIdentity("not-a-uuid", 0); err == nil {
		t.Fatal("invalid policy UUID was accepted")
	}
	if policyID, _ := PolicyIdentity([32]byte{1}, "policy-a"); policyID != "" {
		if _, err := WindowIdentity(policyID, -1); err == nil {
			t.Fatal("negative window index was accepted")
		}
	}
}
