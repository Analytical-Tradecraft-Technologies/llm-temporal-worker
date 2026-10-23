//go:build integration

package redis

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestLiveRedisResponseResultRoundTripExpiresAndIsolatesScopes(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("response-result")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewResponseResultStore(ResponseResultOptions{Client: client, Keys: keys, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	created := time.Unix(1700000000, 0).UTC()

	if _, found, err := store.GetResponseResult(ctx, "acme", "search", "resp_a"); err != nil || found {
		t.Fatalf("missing entry = found %v, %v", found, err)
	}
	if err := store.PutResponseResult(ctx, "acme", "search", "resp_a", ResponseResult{CreatedAt: created, Response: responseResultTestResponse("resp_a")}); err != nil {
		t.Fatal(err)
	}
	result, found, err := store.GetResponseResult(ctx, "acme", "search", "resp_a")
	if err != nil || !found || result.Response == nil || result.Response.Checkpoint.Handle != "checkpoint" || !result.CreatedAt.Equal(created) {
		t.Fatalf("completed entry = %+v, %v, %v", result, found, err)
	}
	ttl, err := client.TTL(ctx, store.key("acme", "search", "resp_a")).Result()
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Fatalf("entry TTL = %v, %v", ttl, err)
	}
	raw, err := client.Get(ctx, store.key("acme", "search", "resp_a")).Bytes()
	if err != nil || bytes.Contains(raw, []byte("private answer")) {
		t.Fatalf("Redis holds readable output: %v", err)
	}

	// The same operation key in another project is a different operation.
	if _, found, err := store.GetResponseResult(ctx, "acme", "other", "resp_a"); err != nil || found {
		t.Fatalf("entry leaked across projects: found %v, %v", found, err)
	}

	if err := store.PutResponseResult(ctx, "acme", "search", "resp_f", ResponseResult{CreatedAt: created, Failure: responseResultTestFailure()}); err != nil {
		t.Fatal(err)
	}
	result, found, err = store.GetResponseResult(ctx, "acme", "search", "resp_f")
	if err != nil || !found || result.Failure == nil || result.Failure.ErrorCode != "budget_denied" {
		t.Fatalf("failed entry = %+v, %v, %v", result, found, err)
	}

	// A value copied under another key is rejected rather than served.
	if err := client.Set(ctx, store.key("acme", "search", "resp_b"), raw, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetResponseResult(ctx, "acme", "search", "resp_b"); err == nil || found {
		t.Fatalf("a relocated value was served: found %v, %v", found, err)
	}
}
