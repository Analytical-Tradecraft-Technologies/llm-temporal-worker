//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestLiveRedisThrottleAcquireReplayDenialAndRelease(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{
		Client: client,
		Mode:   AdmissionModeFunction,
		Keys:   keys,
	})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{
		{Kind: ThrottleRequests, Scope: "tenant-a", Amount: 2, Limit: 2, Window: time.Minute},
		{Kind: ThrottleTokens, Scope: "tenant-a", Amount: 4, Limit: 8, Window: time.Minute},
	}
	first, err := store.Acquire(context.Background(), "live-throttle-1", limits)
	if err != nil || first.Existing {
		t.Fatalf("live throttle acquire = %#v, %v", first, err)
	}
	replay, err := store.Acquire(context.Background(), "live-throttle-1", limits)
	if err != nil || !replay.Existing {
		t.Fatalf("live throttle replay = %#v, %v", replay, err)
	}
	if _, err := store.Acquire(context.Background(), "live-throttle-2", limits); !errors.Is(err, ErrThrottleDenied) {
		t.Fatalf("live throttle denial = %v", err)
	}
	if err := store.Release(context.Background(), first.Reservation); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(context.Background(), "live-throttle-3", limits); err != nil {
		t.Fatalf("live throttle after release = %v", err)
	}
}

// Counters are fixed windows from the first acquire: a steady rate within the
// limit keeps being admitted instead of growing until a full idle window
// passes (#976).
func TestLiveRedisThrottleCountersRollEachWindow(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-roll")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{{Kind: ThrottleRequests, Scope: "tenant-roll", Amount: 1, Limit: 3, Window: 2 * time.Second}}
	for index := 0; index < 4; index++ {
		if index > 0 {
			time.Sleep(700 * time.Millisecond)
		}
		if _, err := store.Acquire(context.Background(), fmt.Sprintf("roll-%d", index), limits); err != nil {
			t.Fatalf("acquire %d at a steady in-limit rate = %v", index, err)
		}
	}
}

// A release returns capacity only to the window it was charged in. After that
// window ended the counter already reset, so releasing must not free capacity
// in the next window (#976).
func TestLiveRedisThrottleReleaseOnlyRefundsItsOwnWindow(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-window")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	limits := []ThrottleLimit{{Kind: ThrottleRequests, Scope: "tenant-window", Amount: 1, Limit: 2, Window: 2 * time.Second}}
	if _, err := store.Acquire(ctx, "window-0", limits); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	late, err := store.Acquire(ctx, "window-1", limits)
	if err != nil {
		t.Fatal(err)
	}
	// The first window ends at 2s; window-1's reservation lives until 3s.
	time.Sleep(1200 * time.Millisecond)
	if _, err := store.Acquire(ctx, "window-2", limits); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, late.Reservation); err != nil {
		t.Fatalf("release of a reservation from the previous window = %v", err)
	}
	if _, err := store.Acquire(ctx, "window-3", limits); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "window-4", limits); !errors.Is(err, ErrThrottleDenied) {
		t.Fatalf("third acquire in a window of two = %v, want denial", err)
	}
}
