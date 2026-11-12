//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	durable "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
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

// TestLiveRedisBudgetStatusRefusesDurableLedgerSpend reproduces #1500 against
// real Redis: admission spends through the durable-budget ledger, an operator
// publishes a generation naming the same policy/window and loads the budget
// status Function, and nothing writes the budget:v2 window hashes. The reader
// must refuse that read rather than report zero usage for a window with live
// spend. The production CLI composes no budget reader for the same reason.
func TestLiveRedisBudgetStatusRefusesDurableLedgerSpend(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.FunctionLoad(ctx, BudgetStatusFunctionLibrarySource()).Err(); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("load budget status Function: %v", err)
	}
	options := liveKeyOptions("budget-status-durable")
	cleanupLivePrefix(t, client, options.Prefix)

	now := time.Now().UTC().Truncate(time.Second)
	materializer, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{
		Client: client, Mode: AdmissionModeFunction, Keys: options,
		GenerationID: "generation-live", IncarnationID: "incarnation-live",
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := materializer.Accept(ctx, durable.ReserveRequest{
		OperationID: "spend", GenerationID: "generation-live", ExpiresAt: now.Add(time.Hour),
		Reservations: []admission.WindowReservation{{
			PolicyID: "policy", WindowID: "hour", Bucket: now.Unix() / 60,
			AmountUSD: pricing.MustUSD("0.25"), LimitUSD: pricing.MustUSD("1"),
			BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour),
		}},
	})
	if err != nil || !accepted.Accepted {
		t.Fatalf("durable reservation = %#v, %v", accepted, err)
	}
	space, err := newKeySpace(options)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := client.HGetAll(ctx, space.durableBudgetKey("policy", "hour")).Result()
	if err != nil || ledger["sum:"+strconv.FormatInt(now.Unix()/60, 10)] != "250000000" {
		t.Fatalf("durable ledger = %v, %v; want the 0.25 USD reservation", ledger, err)
	}

	keys, err := NewBudgetKeySpace(options)
	if err != nil {
		t.Fatal(err)
	}
	start := now.Truncate(time.Hour)
	members := []BudgetManifestMember{{PolicyID: "policy", WindowID: "hour", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), ConfigVersion: "config-1", PriceVersion: "price-1", CoverageStart: start, CoverageEnd: start.Add(time.Hour), BucketCount: 60, BucketWidth: time.Minute, BucketCatalogDigest: strings.Repeat("c", 64), LimitNanoUSD: "1000000000"}}
	catalog, err := MemberCatalogDigest(members)
	if err != nil {
		t.Fatal(err)
	}
	generations, err := NewRedisBudgetGenerationPort(client, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := generations.PublishGeneration(ctx, BudgetManifest{Schema: BudgetManifestSchema, GenerationID: "status-generation", IncarnationID: "status-incarnation", ConfigVersion: "config-1", PriceVersion: "price-1", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), RebuildComplete: true, CoverageStart: start, CoverageEnd: start.Add(time.Hour), PolicyCount: 1, WindowCount: 1, BucketCount: 60, StreamHighWaterMark: "1-0", RoundingVersion: BudgetRoundingVersion, MemberCatalogDigest: catalog, Members: members}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewRedisBudgetStatusReader(BudgetStatusReaderOptions{Client: client, Mode: AdmissionModeFunction, Generation: generations, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadBudgetStatus(ctx, control.BudgetStatusQuery{}, time.Now())
	if !errors.Is(err, ErrBudgetStatusUnavailable) {
		t.Fatalf("budget status over durable spend = %+v, %v; want a refused read, never zero usage", result, err)
	}
}
