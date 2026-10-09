package redis

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

type budgetWorkerHashFake struct {
	values     map[string]string
	getErr     error
	hsetErr    error
	hdelErr    error
	lastHSet   []interface{}
	lastHDel   []string
	beforeEval func(string)
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

func (fake *budgetWorkerHashFake) HScan(ctx context.Context, _ string, _ uint64, match string, _ int64) *redisclient.ScanCmd {
	page := make([]string, 0, len(fake.values)*2)
	for field, value := range fake.values {
		if strings.HasSuffix(match, "*") && !strings.HasPrefix(field, strings.TrimSuffix(match, "*")) {
			continue
		}
		page = append(page, field, value)
	}
	cmd := redisclient.NewScanCmd(ctx, nil)
	cmd.SetVal(page, 0)
	return cmd
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

func (fake *budgetWorkerHashFake) Eval(_ context.Context, _ string, _ []string, args ...interface{}) *redisclient.Cmd {
	field, _ := args[0].(string)
	if fake.beforeEval != nil {
		beforeEval := fake.beforeEval
		fake.beforeEval = nil
		beforeEval(field)
	}
	expected, _ := args[1].(string)
	current, exists := fake.values[field]
	if !exists && (expected != "" || len(args) != 5) {
		return redisclient.NewCmdResult(int64(-1), nil)
	}
	if (expected == "" && exists) || current != expected {
		return redisclient.NewCmdResult(int64(-2), nil)
	}
	if len(args) == 5 {
		leaseJSON, _ := args[2].(string)
		rosterField, _ := args[3].(string)
		rosterJSON, _ := args[4].(string)
		fake.values[field] = leaseJSON
		fake.values[rosterField] = rosterJSON
		return redisclient.NewCmdResult(int64(1), nil)
	}
	delete(fake.values, field)
	return redisclient.NewCmdResult(int64(1), nil)
}

func testBudgetWorkerKeys(t *testing.T) BudgetKeySpace {
	t.Helper()
	keys, err := NewBudgetKeySpace(KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: []byte(strings.Repeat("k", 32))})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// Capture the read result before another caller changes Redis, then deliver
// that stale result to Register. The same schedule runs against the command
// fake and real Redis; no timing or simulated script outcome is needed.
type budgetWorkerReadInterleaving struct {
	BudgetWorkerLeaseRedisClient
	field     string
	afterRead func()
}

func (client *budgetWorkerReadInterleaving) HGet(ctx context.Context, key, field string) *redisclient.StringCmd {
	result := client.BudgetWorkerLeaseRedisClient.HGet(ctx, key, field)
	if field == client.field && client.afterRead != nil {
		afterRead := client.afterRead
		client.afterRead = nil
		afterRead()
	}
	return result
}

func runBudgetWorkerRegistrationInterleavings(t *testing.T, makeClient func(*testing.T) (BudgetWorkerLeaseRedisClient, BudgetKeySpace)) {
	t.Helper()
	for _, scenario := range []struct {
		name    string
		initial bool
		action  string
		want    error
	}{
		{name: "renewed lease", initial: true, action: "renew", want: ErrBudgetWorkerLeaseConflict},
		{name: "concurrent first registration", action: "register", want: ErrBudgetWorkerLeaseConflict},
		{name: "released lease", initial: true, action: "release", want: ErrBudgetWorkerLeaseMissing},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base, keys := makeClient(t)
			client := &budgetWorkerReadInterleaving{BudgetWorkerLeaseRedisClient: base}
			now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
			store, err := NewBudgetWorkerLeaseStore(client, keys, BudgetWorkerLeaseOptions{SessionID: "register-interleaving", Clock: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if scenario.initial {
				if _, err := store.Register(ctx, "generation-a", "1-0", time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			client.field = store.leaseField()
			var winner map[string]string
			client.afterRead = func() {
				now = now.Add(time.Second)
				var err error
				switch scenario.action {
				case "renew":
					_, err = store.Renew(ctx, "generation-a", "1-1", 2*time.Minute)
				case "register":
					_, err = store.Register(ctx, "generation-a", "1-1", 2*time.Minute)
				case "release":
					err = store.Release(ctx)
				}
				if err != nil {
					t.Fatalf("intervening %s: %v", scenario.action, err)
				}
				winner, err = base.HGetAll(ctx, keys.WorkersKey()).Result()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.Register(ctx, "generation-a", "", time.Minute); !errors.Is(err, scenario.want) {
				t.Errorf("stale Register = %v, want %v", err, scenario.want)
			}
			actual, err := base.HGetAll(ctx, keys.WorkersKey()).Result()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, winner) {
				t.Fatal("stale registration changed the winning lease or roster")
			}
			if scenario.action != "release" {
				priorRoster, err := decodeBudgetWorkerRoster(winner[store.rosterField()])
				if err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Second)
				lease, err := store.Register(ctx, "generation-a", "", 3*time.Minute)
				if err != nil || lease.Cursor != "1-1" || !lease.LeaseExpiresAt.Equal(now.Add(3*time.Minute)) {
					t.Fatalf("fresh registration retry = %#v, %v", lease, err)
				}
				rosterRaw, err := base.HGet(ctx, keys.WorkersKey(), store.rosterField()).Result()
				if err != nil {
					t.Fatal(err)
				}
				roster, err := decodeBudgetWorkerRoster(rosterRaw)
				if err != nil || !roster.RegisteredAt.Equal(priorRoster.RegisteredAt) {
					t.Fatalf("registration retry changed original roster timestamp: %#v, %v", roster, err)
				}
			}
		})
	}
}

func TestBudgetWorkerRegistrationRejectsInterveningWrites(t *testing.T) {
	runBudgetWorkerRegistrationInterleavings(t, func(t *testing.T) (BudgetWorkerLeaseRedisClient, BudgetKeySpace) {
		return &budgetWorkerHashFake{values: make(map[string]string)}, testBudgetWorkerKeys(t)
	})
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

func TestBudgetWorkerLeaseRenewRejectsStaleCompareAndSwap(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-cas", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Register(context.Background(), "generation-a", "1-0", time.Minute); err != nil {
		t.Fatal(err)
	}
	staleRaw := fake.values[store.leaseField()]
	now = now.Add(time.Second)
	if _, err := store.Renew(context.Background(), "generation-a", "1-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	current, err := decodeBudgetWorkerLease(fake.values[store.leaseField()])
	if err != nil {
		t.Fatal(err)
	}
	current.Cursor = "1-2"
	roster, err := decodeBudgetWorkerRoster(fake.values[store.rosterField()])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.writeRecords(context.Background(), current, roster, staleRaw); !errors.Is(err, ErrBudgetWorkerLeaseConflict) {
		t.Fatalf("stale renewal error = %v", err)
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

func TestBudgetWorkerLeasePruneDoesNotDeleteRenewedLease(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	fake := &budgetWorkerHashFake{values: make(map[string]string)}
	store, err := NewBudgetWorkerLeaseStore(fake, testBudgetWorkerKeys(t), BudgetWorkerLeaseOptions{SessionID: "session-prune-cas", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Register(context.Background(), "generation-a", "1-0", time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	fake.beforeEval = func(field string) {
		lease := BudgetWorkerLease{Schema: BudgetWorkerLeaseSchema, SessionID: "session-prune-cas", GenerationID: "generation-a", Cursor: "1-1", LeaseExpiresAt: now.Add(time.Minute), UpdatedAt: now}
		raw, marshalErr := marshalBudgetWorkerRecord(lease)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		fake.values[field] = raw
	}
	removed, err := store.PruneExpired(context.Background(), "generation-a", now, 1)
	if err != nil || removed != 0 {
		t.Fatalf("PruneExpired = %d, %v", removed, err)
	}
	if _, err := decodeBudgetWorkerLease(fake.values[store.leaseField()]); err != nil {
		t.Fatalf("renewed lease was removed or invalid: %v", err)
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
