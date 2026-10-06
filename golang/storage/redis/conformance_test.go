package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/redis/go-redis/v9"
)

func TestContinuationPersistenceContractExpiresEveryDerivedKey(t *testing.T) {
	for _, fragment := range []string{
		"redis.call('SET', KEYS[2], ARGV[1], 'EX', tostring(ttl), 'NX')",
		"redis.call('EXPIRE', KEYS[1], tostring(ttl))",
		"redis.call('EXPIRE', KEYS[3], tostring(ttl))",
	} {
		if !strings.Contains(continuationFunctionSource, fragment) {
			t.Fatalf("continuation persistence contract is missing %q", fragment)
		}
	}
}

func TestRedisKeyNamespacesDoNotAliasDerivedState(t *testing.T) {
	base := testKeyOptions()
	differentSecret := append([]byte(nil), base.KeySecret...)
	differentSecret[len(differentSecret)-1] ^= 1
	variants := []struct {
		name string
		keys KeyOptions
	}{
		{name: "prefix", keys: KeyOptions{Prefix: "other", HashTag: base.HashTag, KeySecret: append([]byte(nil), base.KeySecret...)}},
		{name: "hash tag", keys: KeyOptions{Prefix: base.Prefix, HashTag: "other", KeySecret: append([]byte(nil), base.KeySecret...)}},
		{name: "key secret", keys: KeyOptions{Prefix: base.Prefix, HashTag: base.HashTag, KeySecret: differentSecret}},
	}
	primary, err := newKeySpace(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range variants {
		t.Run(test.name, func(t *testing.T) {
			variant, err := newKeySpace(test.keys)
			if err != nil {
				t.Fatal(err)
			}
			for _, pair := range []struct {
				name  string
				base  string
				other string
			}{
				{name: "operation", base: primary.operationKey("tenant-private-42", "operation-private-99"), other: variant.operationKey("tenant-private-42", "operation-private-99")},
				{name: "operation index", base: primary.operationIndexKey("operation-private-99"), other: variant.operationIndexKey("operation-private-99")},
				{name: "continuation", base: primary.continuationKey("tenant-private-42", "handle-private-99"), other: variant.continuationKey("tenant-private-42", "handle-private-99")},
				{name: "continuation index", base: primary.continuationIndexKey("handle-private-99"), other: variant.continuationIndexKey("handle-private-99")},
				{name: "continuation operation index", base: primary.continuationOperationKey("tenant-private-42", "parent-private-99", "operation-private-99"), other: variant.continuationOperationKey("tenant-private-42", "parent-private-99", "operation-private-99")},
			} {
				if pair.base == pair.other {
					t.Fatalf("%s key aliases across namespaces: %q", pair.name, pair.base)
				}
				if strings.Contains(pair.base, "private") || strings.Contains(pair.other, "private") {
					t.Fatalf("%s key leaked an identifier: %q / %q", pair.name, pair.base, pair.other)
				}
			}
		})
	}
}

func TestContinuationRuntimeFixtureExpiresAllDerivedKeysTogether(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	fixture := newContinuationRuntimeFixture(func() time.Time { return now })
	store, err := NewContinuationStore(ContinuationOptions{
		Invoker: fixture,
		Reader:  fixture,
		Keys:    testKeyOptions(),
		Keyring: testKeyring(t),
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	child := testContinuation(t, now)
	child.ParentID = parent.String()
	child.Depth = 1
	child.ExpiresAt = now.Add(time.Second)
	first, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: child, OperationKey: "operation-private-99"})
	if err != nil {
		t.Fatal(err)
	}
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		space.continuationIndexKey(first.String()),
		space.continuationKey(child.Tenant, first.String()),
		space.continuationOperationKey(child.Tenant, parent.String(), "operation-private-99"),
	}
	for _, key := range keys {
		expiresAt, ok := fixture.expiry(key)
		if !ok || !expiresAt.Equal(child.ExpiresAt) {
			t.Fatalf("derived key %q expiry = %v, present=%t; want %v", key, expiresAt, ok, child.ExpiresAt)
		}
	}
	now = child.ExpiresAt
	if _, err := store.Get(context.Background(), first); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("expired child read = %v, want not found", err)
	}
	replacement := testContinuation(t, now)
	replacement.ParentID = parent.String()
	replacement.Depth = 1
	replacement.ExpiresAt = now.Add(time.Hour)
	second, err := store.PutChild(context.Background(), state.PutChildRequest{Parent: parent, Child: replacement, OperationKey: "operation-private-99"})
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("expired operation idempotency key returned stale child %q", first)
	}
}

func TestContinuationStoreFailsClosedForUnavailableRedisState(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	fixture := newContinuationRuntimeFixture(func() time.Time { return now })
	store, err := NewContinuationStore(ContinuationOptions{
		Invoker: fixture,
		Reader:  fixture,
		Keys:    testKeyOptions(),
		Keyring: testKeyring(t),
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	backendErr := errors.New("dial tcp redis.example.internal: connection reset")
	fixture.putErr = backendErr
	if _, err := store.CreateRoot(context.Background(), testContinuation(t, now)); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "redis.example.internal") {
		t.Fatalf("write transport error = %v, want redacted unavailable state", err)
	}
	fixture.putErr = nil
	handle, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	fixture.getErr = backendErr
	if _, err := store.Get(context.Background(), handle); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "redis.example.internal") {
		t.Fatalf("read transport error = %v, want redacted unavailable state", err)
	}
	fixture.getErr = nil
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	fixture.set(space.continuationKey("tenant-a", handle.String()), "not-json", time.Time{})
	if _, err := store.Get(context.Background(), handle); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("malformed continuation record = %v, want unavailable state", err)
	}
	fixture.set(space.continuationIndexKey(handle.String()), "not-a-redis-key", time.Time{})
	if _, err := store.Get(context.Background(), handle); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("malformed continuation index = %v, want unavailable state", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, handle); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled continuation read = %v", err)
	}
}

func TestContinuationStoreTreatsExpiryRaceAsNotFound(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	fixture := newContinuationRuntimeFixture(func() time.Time { return now })
	store, err := NewContinuationStore(ContinuationOptions{
		Invoker: fixture,
		Reader:  fixture,
		Keys:    testKeyOptions(),
		Keyring: testKeyring(t),
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.CreateRoot(context.Background(), testContinuation(t, now))
	if err != nil {
		t.Fatal(err)
	}
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	indexKey := space.continuationIndexKey(handle.String())
	recordKey := space.continuationKey("tenant-a", handle.String())
	fixture.mu.Lock()
	delete(fixture.values, recordKey)
	fixture.afterMissing = func(key string) {
		if key == recordKey {
			delete(fixture.values, indexKey)
		}
	}
	fixture.mu.Unlock()
	if _, err := store.Get(context.Background(), handle); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("expiration race read = %v, want not found", err)
	}
}

type continuationRuntimeFixture struct {
	mu           sync.Mutex
	now          func() time.Time
	values       map[string]fixtureValue
	getErr       error
	putErr       error
	afterMissing func(string)
}

type fixtureValue struct {
	value     string
	expiresAt time.Time
}

func newContinuationRuntimeFixture(now func() time.Time) *continuationRuntimeFixture {
	return &continuationRuntimeFixture{now: now, values: make(map[string]fixtureValue)}
}

func (fixture *continuationRuntimeFixture) Get(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.getErr != nil {
		return "", fixture.getErr
	}
	value, ok := fixture.get(key)
	if !ok {
		if fixture.afterMissing != nil {
			fixture.afterMissing(key)
		}
		return "", redis.Nil
	}
	return value, nil
}

func (fixture *continuationRuntimeFixture) Put(ctx context.Context, keys []string, value, handle, ttl string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.putErr != nil {
		return "", fixture.putErr
	}
	if len(keys) != 2 && len(keys) != 3 {
		return "", errors.New("invalid continuation fixture keys")
	}
	seconds, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil || seconds <= 0 {
		return "", errors.New("invalid continuation fixture TTL")
	}
	if len(keys) == 3 {
		if existing, ok := fixture.get(keys[2]); ok {
			return "existing\x00" + existing, nil
		}
	}
	if recordKey, ok := fixture.get(keys[0]); ok {
		if _, exists := fixture.get(recordKey); exists {
			return "conflict", nil
		}
		delete(fixture.values, keys[0])
	}
	if _, exists := fixture.get(keys[1]); exists {
		return "conflict", nil
	}
	expiresAt := fixture.now().Add(time.Duration(seconds) * time.Second)
	fixture.set(keys[1], value, expiresAt)
	fixture.set(keys[0], keys[1], expiresAt)
	if len(keys) == 3 {
		fixture.set(keys[2], handle, expiresAt)
	}
	return "created", nil
}

func (fixture *continuationRuntimeFixture) expiry(key string) (time.Time, bool) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	value, ok := fixture.values[key]
	return value.expiresAt, ok
}

func (fixture *continuationRuntimeFixture) set(key, value string, expiresAt time.Time) {
	fixture.values[key] = fixtureValue{value: value, expiresAt: expiresAt}
}

func (fixture *continuationRuntimeFixture) get(key string) (string, bool) {
	value, ok := fixture.values[key]
	if !ok {
		return "", false
	}
	if !value.expiresAt.IsZero() && !fixture.now().Before(value.expiresAt) {
		delete(fixture.values, key)
		return "", false
	}
	return value.value, true
}
