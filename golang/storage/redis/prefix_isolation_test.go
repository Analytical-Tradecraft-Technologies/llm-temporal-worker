package redis

import (
	"strings"
	"testing"
)

func TestConfiguredPrefixesIsolateEveryWorkerKey(t *testing.T) {
	secret := []byte("01234567890123456789012345678901")
	left, err := newKeySpace(KeyOptions{Prefix: "worker-a", HashTag: "admission", KeySecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	right, err := newKeySpace(KeyOptions{Prefix: "worker-b", HashTag: "admission", KeySecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	leftKeys := []string{left.operationKey("tenant", "operation"), left.operationIndexKey("operation"), left.budgetKey("policy", "window"), left.continuationIndexKey("handle"), left.continuationKey("tenant", "handle"), left.continuationOperationKey("tenant", "parent", "operation")}
	rightKeys := []string{right.operationKey("tenant", "operation"), right.operationIndexKey("operation"), right.budgetKey("policy", "window"), right.continuationIndexKey("handle"), right.continuationKey("tenant", "handle"), right.continuationOperationKey("tenant", "parent", "operation")}
	for index := range leftKeys {
		if leftKeys[index] == rightKeys[index] {
			t.Fatalf("key %d aliases across prefixes: %q", index, leftKeys[index])
		}
		if !strings.HasPrefix(leftKeys[index], "worker-a:{admission}:") || !strings.HasPrefix(rightKeys[index], "worker-b:{admission}:") {
			t.Fatalf("key %d lost configured namespace: %q / %q", index, leftKeys[index], rightKeys[index])
		}
	}
}

func TestConfiguredHashTagCoversEveryWorkerKeyFamily(t *testing.T) {
	const expectedPrefix = "worker:{slot.v1}:"
	secret := []byte("01234567890123456789012345678901")
	space, err := newKeySpace(KeyOptions{Prefix: "worker", HashTag: "slot.v1", KeySecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewBudgetKeySpace(KeyOptions{Prefix: "worker", HashTag: "slot.v1", KeySecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		space.scopeKey("tenant"),
		space.operationIndexKey("operation"),
		space.operationKey("tenant", "operation"),
		space.budgetKey("policy", "window"),
		space.continuationIndexKey("handle"),
		space.continuationKey("tenant", "handle"),
		space.continuationOperationKey("tenant", "parent", "operation"),
		space.throttleKey(string(ThrottleRequests), "tenant"),
		space.throttleReservationKey("reservation"),
		budget.ActiveGenerationKey(),
		budget.EventsKey(),
		budget.WorkersKey(),
		budget.ManifestKey("generation-1"),
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, expectedPrefix) {
			t.Fatalf("worker key escaped configured hash tag: %q", key)
		}
		if got := strings.Count(key, "{slot.v1}"); got != 1 {
			t.Fatalf("worker key contains %d configured hash tags: %q", got, key)
		}
	}
}
