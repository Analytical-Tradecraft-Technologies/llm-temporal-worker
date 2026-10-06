//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// TestLiveRedisBudgetStatusFailsClosedOnExpiryBacklog runs the budget status
// Lua against real Redis with a drain bound smaller than the expired backlog.
// The read must not publish a snapshot that still counts expired reservations;
// the partial drain it did make is kept, so a retry converges.
func TestLiveRedisBudgetStatusFailsClosedOnExpiryBacklog(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "llmtw-test:budget-status:" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":"
	pointer, stream, window, expiry := prefix+"active", prefix+"stream", prefix+"window", prefix+"expiry"
	t.Cleanup(func() { _ = client.Del(context.Background(), pointer, stream, window, expiry).Err() })

	generation, incarnation, digest := "generation-1", "incarnation-1", strings.Repeat("a", 64)
	active, err := json.Marshal(map[string]string{"generation_id": generation, "incarnation_id": incarnation, "manifest_digest": digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, pointer, active, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(ctx, &redisclient.XAddArgs{Stream: stream, ID: "1-0", Values: []string{"event", "seed"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, window, map[string]any{
		"schema": BudgetStatusWindowSchema, "generation_id": generation, "incarnation_id": incarnation, "manifest_digest": digest,
		"member_key": "policy/window", "limit_nano_usd": "1000000000", "reserved_nano_usd": "300", "accounted_nano_usd": "0",
		"coverage_start": "2026-01-01T00:00:00Z", "coverage_end": "2027-01-01T00:00:00Z",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	past := float64(time.Now().Add(-time.Minute).UnixMilli())
	if err := client.ZAdd(ctx, expiry, redisclient.Z{Score: past, Member: generation + "|100"}, redisclient.Z{Score: past, Member: generation + "|200"}).Err(); err != nil {
		t.Fatal(err)
	}

	read := func(limit int) []any {
		t.Helper()
		raw, err := budgetStatusScript.Run(ctx, client, []string{pointer, stream, window, expiry}, "read", generation, incarnation, digest, strconv.FormatInt(time.Now().UnixMilli(), 10), "1", strconv.Itoa(limit)).Result()
		if err != nil {
			t.Fatal(err)
		}
		result, ok := raw.([]any)
		if !ok || len(result) != 2 {
			t.Fatalf("budget status result = %#v", raw)
		}
		return result
	}

	if result := read(1); result[0] != "state_unavailable" {
		t.Fatalf("bounded drain with a remaining backlog = %#v, want state_unavailable", result)
	}
	if remaining, err := client.ZCard(ctx, expiry).Result(); err != nil || remaining != 1 {
		t.Fatalf("expired entries after one bounded drain = %d, %v; want 1", remaining, err)
	}
	result := read(1)
	if result[0] != "ok" {
		t.Fatalf("drain that empties the backlog = %#v, want ok", result)
	}
	var snapshot struct {
		Members []struct {
			Reserved string `json:"reserved_nano_usd"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(result[1].(string)), &snapshot); err != nil || len(snapshot.Members) != 1 || snapshot.Members[0].Reserved != "0" {
		t.Fatalf("snapshot = %s, %v; want reserved 0", result[1], err)
	}
}
