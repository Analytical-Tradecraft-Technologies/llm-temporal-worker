//go:build integration

package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestLiveRedisModelCatalogPublishesOnlyNewerAndLeasesOneRefresher(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("model-catalog")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewModelCatalogStore(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if digest, err := store.LatestDigest(ctx); err != nil || digest != "" {
		t.Fatalf("empty store digest = %q, %v", digest, err)
	}
	if _, _, _, found, err := store.Latest(ctx); err != nil || found {
		t.Fatalf("empty store found = %v, %v", found, err)
	}
	fetched := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"schema":"openrouter-catalog/v1"}`)
	if published, err := store.Publish(ctx, "digest-1", fetched, body); err != nil || !published {
		t.Fatalf("first publish = %v, %v", published, err)
	}
	for name, at := range map[string]time.Time{"older": fetched.Add(-time.Minute), "equal": fetched} {
		if published, err := store.Publish(ctx, "digest-"+name, at, []byte(name)); err != nil || published {
			t.Fatalf("%s publish replaced a newer catalog: %v, %v", name, published, err)
		}
	}
	digest, at, stored, found, err := store.Latest(ctx)
	if err != nil || !found || digest != "digest-1" || !at.Equal(fetched) || string(stored) != string(body) {
		t.Fatalf("Latest() = %q %v %q %v %v", digest, at, stored, found, err)
	}
	if published, err := store.Publish(ctx, "digest-2", fetched.Add(time.Hour), []byte("newer")); err != nil || !published {
		t.Fatalf("newer publish = %v, %v", published, err)
	}
	if digest, err := store.LatestDigest(ctx); err != nil || digest != "digest-2" {
		t.Fatalf("digest after newer publish = %q, %v", digest, err)
	}

	if acquired, err := store.AcquireRefresh(ctx, "worker-a", time.Minute); err != nil || !acquired {
		t.Fatalf("first lease = %v, %v", acquired, err)
	}
	if acquired, err := store.AcquireRefresh(ctx, "worker-b", time.Minute); err != nil || acquired {
		t.Fatalf("second lease = %v, %v", acquired, err)
	}
	if err := store.ReleaseRefresh(ctx, "worker-b"); err != nil {
		t.Fatal(err)
	}
	if acquired, _ := store.AcquireRefresh(ctx, "worker-c", time.Minute); acquired {
		t.Fatal("a non-owner released the refresh lease")
	}
	if err := store.ReleaseRefresh(ctx, "worker-a"); err != nil {
		t.Fatal(err)
	}
	if acquired, err := store.AcquireRefresh(ctx, "worker-c", time.Minute); err != nil || !acquired {
		t.Fatalf("lease after release = %v, %v", acquired, err)
	}

	keysFound, err := client.Keys(ctx, keys.Prefix+":*").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keysFound {
		if !strings.HasPrefix(key, keys.Prefix+":{"+keys.HashTag+"}:") || strings.Contains(key, "openrouter") {
			t.Fatalf("model catalog key %q is outside the hashed namespace", key)
		}
	}
}
