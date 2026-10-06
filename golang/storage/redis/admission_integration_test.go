//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
)

// Run with a pinned Redis image and persistence profile whose admission
// Function was deliberately provisioned before this test, for example:
//
//	LLMTW_REDIS_ADDR=127.0.0.1:6379 go test -tags=integration ./storage/redis -run Live
//
// The ordinary offline suite intentionally does not require Docker or a live
// daemon. CI/deployment promotion must run this gate with Redis Functions/Lua
// enabled, AOF/RDB settings matching the target profile, and the concurrent
// exact-boundary scenarios from docs/testing/strategy.md.
func TestLiveRedisAdmission(t *testing.T) {
	client := openLiveRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewAdmissionStore(AdmissionOptions{
		Client:          client,
		Mode:            AdmissionModeFunction,
		FunctionVersion: AdmissionFunctionVersion,
		Keys:            testKeyOptions(),
		MaxRecordBytes:  256 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	reservation := testReservation(1, 100)
	reservation.Bucket = now.Unix() / int64(time.Minute/time.Second)
	result, err := store.Begin(ctx, admission.BeginRequest{ID: "live-integration", ScopeKey: "live-tenant/live", RequestDigest: admission.Digest([]byte("live")), Reservation: 1, Reservations: []admission.WindowReservation{reservation}, ExpiresAt: now.Add(time.Minute)})
	if err != nil || result.Denied != nil {
		t.Fatalf("live begin = %#v, %v", result, err)
	}
	replay, err := store.Begin(ctx, admission.BeginRequest{ID: "live-integration", ScopeKey: "live-tenant/live", RequestDigest: admission.Digest([]byte("live")), Reservation: 1, Reservations: []admission.WindowReservation{reservation}, ExpiresAt: now.Add(time.Minute)})
	if err != nil || !replay.Existing {
		t.Fatalf("live replay = %#v, %v", replay, err)
	}
}
