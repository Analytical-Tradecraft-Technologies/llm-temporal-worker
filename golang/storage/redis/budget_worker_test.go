package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

type budgetWorkerHashFake struct {
	values   map[string]string
	getErr   error
	hsetErr  error
	hdelErr  error
	lastHSet []interface{}
	lastHDel []string
}

func (fake *budgetWorkerHashFake) HGet(_ context.Context, _ string, field string) *redisclient.StringCmd {
	if fake.getErr != nil {
		return redisclient.NewStringResult("", fake.getErr)
	}
	value, ok := fake.values[field]
	if !ok {
		return redisclient.NewStringResult("", redisclient.Nil)
	}
	return redisclient.NewStringResult(value, nil)
}

func (fake *budgetWorkerHashFake) HSet(_ context.Context, _ string, values ...interface{}) *redisclient.IntCmd {
	fake.lastHSet = append([]interface{}(nil), values...)
	if fake.hsetErr != nil {
		return redisclient.NewIntResult(0, fake.hsetErr)
	}
	for index := 0; index+1 < len(values); index += 2 {
		field, _ := values[index].(string)
		value, _ := values[index+1].(string)
		fake.values[field] = value
	}
	return redisclient.NewIntResult(1, nil)
}

func (fake *budgetWorkerHashFake) HGetAll(_ context.Context, _ string) *redisclient.MapStringStringCmd {
	copyValues := make(map[string]string, len(fake.values))
	for key, value := range fake.values {
		copyValues[key] = value
	}
	return redisclient.NewMapStringStringResult(copyValues, nil)
}

func (fake *budgetWorkerHashFake) HDel(_ context.Context, _ string, fields ...string) *redisclient.IntCmd {
	fake.lastHDel = append([]string(nil), fields...)
	if fake.hdelErr != nil {
		return redisclient.NewIntResult(0, fake.hdelErr)
	}
	var removed int64
	for _, field := range fields {
		if _, ok := fake.values[field]; ok {
			delete(fake.values, field)
			removed++
		}
	}
	return redisclient.NewIntResult(removed, nil)
}

func testBudgetWorkerKeys(t *testing.T) BudgetKeySpace {
	t.Helper()
	keys, err := NewBudgetKeySpace(KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: []byte(strings.Repeat("k", 32))})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestBudgetWorkerLeaseKeepsSessionAcrossRenewAndPersistsRoster(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-a", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Register(context.Background(), "generation-a", "", time.Minute)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if first.SessionID != "session-a" || first.Cursor != "" || len(fake.values) != 2 {
		t.Fatalf("registered lease/roster = %#v", fake.values)
	}
	now = now.Add(20 * time.Second)
	second, err := store.Renew(context.Background(), "generation-a", "12-3", time.Minute)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if second.SessionID != first.SessionID || second.Cursor != "12-3" || !second.LeaseExpiresAt.After(first.LeaseExpiresAt) {
		t.Fatalf("renewed lease = %#v, first = %#v", second, first)
	}
	live, err := store.Live(context.Background(), "generation-a", now)
	if err != nil || len(live) != 1 || live[0].Cursor != "12-3" {
		t.Fatalf("Live = %#v, %v", live, err)
	}
	if err := store.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(fake.values) != 1 || !strings.HasPrefix(fake.lastHDel[0], budgetWorkerLeaseFieldPrefix) {
		t.Fatalf("release removed roster or wrong field: %#v", fake.values)
	}
	roster, err := store.Roster(context.Background(), "generation-a")
	if err != nil || len(roster) != 1 || roster[0].SessionID != "session-a" {
		t.Fatalf("persistent roster = %#v, %v", roster, err)
	}
}

func TestBudgetWorkerLeaseRejectsExpiredAndRegressingRenewals(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-b", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Register(context.Background(), "generation-a", "20-0", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Renew(context.Background(), "generation-a", "19-9", time.Minute); !errors.Is(err, ErrBudgetWorkerCursorRegression) {
		t.Fatalf("cursor regression error = %v", err)
	}
	now = now.Add(time.Minute)
	if renewed, err := store.Renew(context.Background(), "generation-a", "20-1", time.Minute); err != nil || renewed.Cursor != "20-1" {
		t.Fatalf("same-session expired lease reconnect = %#v, %v", renewed, err)
	}
	if err := store.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Renew(context.Background(), "generation-a", "20-2", time.Minute); !errors.Is(err, ErrBudgetWorkerLeaseMissing) {
		t.Fatalf("released lease error = %v", err)
	}
}

func TestBudgetWorkerLeasePrunesOnlyExpiredGenerationLeases(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	first, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-c", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Register(context.Background(), "generation-a", "1-0", time.Second); err != nil {
		t.Fatal(err)
	}
	second, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-d", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Register(context.Background(), "generation-b", "1-0", time.Hour); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	removed, err := first.PruneExpired(context.Background(), "generation-a", now, 10)
	if err != nil || removed != 1 {
		t.Fatalf("PruneExpired = %d, %v", removed, err)
	}
	if len(fake.values) != 3 { // first roster, second lease, second roster
		t.Fatalf("prune removed unexpected records: %#v", fake.values)
	}
}

func TestBudgetWorkerLeaseGeneratesDistinctOpaqueSessions(t *testing.T) {
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	keys := testBudgetWorkerKeys(t)
	left, err := NewBudgetWorkerLeaseStore(fake, keys, BudgetWorkerLeaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewBudgetWorkerLeaseStore(fake, keys, BudgetWorkerLeaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if left.SessionID() == "" || left.SessionID() == right.SessionID() || len(left.SessionID()) != 32 {
		t.Fatalf("session IDs = %q and %q", left.SessionID(), right.SessionID())
	}
}
