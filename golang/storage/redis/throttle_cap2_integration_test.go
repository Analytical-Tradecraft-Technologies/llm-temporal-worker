//go:build integration

package redis

import (
	"context"
	"testing"
	"time"
)

func TestLiveRedisThrottleCapTwoThirdReplayWaitsThenAcquires(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-cap-two")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{{Kind: ThrottleConcurrency, Scope: "generation:python-stage2", Amount: 1, Limit: 2, Window: 2 * time.Second}}
	first, err := store.Acquire(context.Background(), "cap2-first", limits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Acquire(context.Background(), "cap2-second", limits)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan ThrottleAcquireResult, 2)
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			value, err := store.AcquireFair(context.Background(), "cap2-third", "python-stage2", 2*time.Second, 5*time.Millisecond, limits)
			results <- value
			errors <- err
		}()
	}
	waitForQueueDepth(t, client, store.space.throttleQueueKey("python-stage2"), 1)
	if err := store.Release(context.Background(), first.Reservation); err != nil {
		t.Fatal(err)
	}
	one, two := <-results, <-results
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if one.Reservation.ID != "cap2-third" || two.Reservation.ID != "cap2-third" || one.Reservation.Digest != two.Reservation.Digest {
		t.Fatalf("replayed third lease differed: %#v %#v", one, two)
	}
	if !one.Existing && !two.Existing {
		t.Fatal("replayed third acquire incremented capacity twice")
	}
	if err := store.Release(context.Background(), one.Reservation); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(context.Background(), two.Reservation); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(context.Background(), second.Reservation); err != nil {
		t.Fatal(err)
	}
}
