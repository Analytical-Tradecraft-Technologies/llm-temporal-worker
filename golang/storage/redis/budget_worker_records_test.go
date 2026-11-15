package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestListWorkerRecordsFiltersPrefixAndKeep(t *testing.T) {
	fake := &budgetWorkerHashFake{values: map[string]string{
		budgetWorkerLeaseFieldPrefix + "a":  "keep-a",
		budgetWorkerLeaseFieldPrefix + "b":  "drop-b",
		budgetWorkerRosterFieldPrefix + "a": "keep-roster",
	}}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetKeySpaceForAdapter(t), BudgetWorkerLeaseOptions{SessionID: "session-a"})
	if err != nil {
		t.Fatal(err)
	}
	identity := func(raw string) (string, error) { return raw, nil }
	kept, err := listWorkerRecords(context.Background(), store, "generation-a", budgetWorkerLeaseFieldPrefix, "test records", identity, func(raw string) bool {
		return strings.HasPrefix(raw, "keep")
	})
	if err != nil || len(kept) != 1 || kept[0] != "keep-a" {
		t.Fatalf("kept = %q, %v", kept, err)
	}
	failed := errors.New("decode failed")
	if _, err := listWorkerRecords(context.Background(), store, "generation-a", budgetWorkerRosterFieldPrefix, "test records", func(string) (string, error) { return "", failed }, func(string) bool { return true }); !errors.Is(err, failed) {
		t.Fatalf("decode error = %v", err)
	}
	if _, err := listWorkerRecords(context.Background(), nil, "generation-a", budgetWorkerLeaseFieldPrefix, "test records", identity, func(string) bool { return true }); err == nil || err.Error() != "Redis budget worker lease store is nil" {
		t.Fatalf("nil store error = %v", err)
	}
}

func TestRegisteredAtKeepsExistingRosterOrFallback(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetKeySpaceForAdapter(t), BudgetWorkerLeaseOptions{SessionID: "session-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	fallback := now.Add(time.Hour)
	if got, err := store.registeredAt(context.Background(), fallback, "read roster"); err != nil || !got.Equal(fallback) {
		t.Fatalf("missing roster = %v, %v; want fallback", got, err)
	}
	if _, err := store.Register(context.Background(), "generation-a", "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, err := store.registeredAt(context.Background(), fallback, "read roster"); err != nil || !got.Equal(now) {
		t.Fatalf("existing roster = %v, %v; want %v", got, err, now)
	}
	fake.getErr = errors.New("redis down")
	if _, err := store.registeredAt(context.Background(), fallback, "read roster"); !errors.Is(err, fake.getErr) || !strings.HasPrefix(err.Error(), "read roster: ") {
		t.Fatalf("read error = %v", err)
	}
}
