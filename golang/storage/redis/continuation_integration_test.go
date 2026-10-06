//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	redisclient "github.com/redis/go-redis/v9"
)

func TestLiveRedisContinuationConformance(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	now := time.Now().UTC()
	keys := KeyOptions{
		Prefix:    fmt.Sprintf("live-continuation-%d", now.UnixNano()),
		HashTag:   "admission",
		KeySecret: []byte("01234567890123456789012345678901"),
	}
	keyring := testKeyring(t)
	store, err := NewContinuationStore(ContinuationOptions{Client: client, Keys: keys, Keyring: keyring, Clock: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	space, err := newKeySpace(keys)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := make([]string, 0, 8)
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		if len(cleanup) != 0 {
			_ = client.Del(cleanupContext, cleanup...).Err()
		}
	})

	root := testContinuation(t, now)
	root.ExpiresAt = now.Add(time.Minute)
	parent, err := store.CreateRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	cleanup = append(cleanup,
		space.continuationIndexKey(parent.String()),
		space.continuationKey(root.Tenant, parent.String()),
	)
	for _, test := range []struct {
		name string
		keys KeyOptions
	}{
		{name: "prefix", keys: KeyOptions{Prefix: keys.Prefix + "-other", HashTag: keys.HashTag, KeySecret: append([]byte(nil), keys.KeySecret...)}},
		{name: "hash tag", keys: KeyOptions{Prefix: keys.Prefix, HashTag: keys.HashTag + "-other", KeySecret: append([]byte(nil), keys.KeySecret...)}},
		{name: "key secret", keys: KeyOptions{Prefix: keys.Prefix, HashTag: keys.HashTag, KeySecret: append([]byte(nil), keys.KeySecret...)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "key secret" {
				test.keys.KeySecret[len(test.keys.KeySecret)-1] ^= 1
			}
			other, err := NewContinuationStore(ContinuationOptions{Client: client, Keys: test.keys, Keyring: keyring, Clock: time.Now})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := other.Get(ctx, parent); !errors.Is(err, state.ErrNotFound) {
				t.Fatalf("different key namespace read = %v, want not found", err)
			}
		})
	}

	child := testContinuation(t, now)
	child.ParentID = parent.String()
	child.Depth = 1
	child.ExpiresAt = now.Add(time.Minute)
	first, err := store.PutChild(ctx, state.PutChildRequest{Parent: parent, Child: child, OperationKey: "live-operation"})
	if err != nil {
		t.Fatal(err)
	}
	childKeys := []string{
		space.continuationIndexKey(first.String()),
		space.continuationKey(child.Tenant, first.String()),
		space.continuationOperationKey(child.Tenant, parent.String(), "live-operation"),
	}
	cleanup = append(cleanup, childKeys...)
	for _, key := range childKeys {
		ttl, err := client.PTTL(ctx, key).Result()
		if err != nil || ttl <= 0 {
			t.Fatalf("continuation key %q TTL = %v, %v; want a positive expiry", key, ttl, err)
		}
		if _, err := client.PExpire(ctx, key, time.Millisecond).Result(); err != nil {
			t.Fatalf("force expiry for %q: %v", key, err)
		}
	}
	if err := waitForLiveRedisKeysAbsent(ctx, client, childKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, first); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("fully expired child read = %v, want not found", err)
	}

	replacement := testContinuation(t, now)
	replacement.ParentID = parent.String()
	replacement.Depth = 1
	replacement.ExpiresAt = now.Add(time.Minute)
	second, err := store.PutChild(ctx, state.PutChildRequest{Parent: parent, Child: replacement, OperationKey: "live-operation"})
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("expired operation index returned stale continuation %q", first)
	}
	cleanup = append(cleanup,
		space.continuationIndexKey(second.String()),
		space.continuationKey(replacement.Tenant, second.String()),
	)
}

func waitForLiveRedisKeysAbsent(ctx context.Context, client redisclient.UniversalClient, keys []string) error {
	deadline := time.Now().Add(2 * time.Second)
	for {
		count, err := client.Exists(ctx, keys...).Result()
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Redis keys did not expire: %v", keys)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
