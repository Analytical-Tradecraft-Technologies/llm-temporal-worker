//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

type fairAcquireResult struct {
	id          string
	reservation ThrottleReservation
	err         error
}

func TestLiveRedisThrottleQueueIsStrictFIFO(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-strict-fifo")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{{Kind: ThrottleConcurrency, Scope: "generation:global", Amount: 1, Limit: 1, Window: 2 * time.Second}}
	blocker, err := store.Acquire(context.Background(), "fifo-blocker", limits)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan fairAcquireResult, 2)
	acquire := func(id string) {
		value, err := store.AcquireFair(context.Background(), id, "global", 2*time.Second, 5*time.Millisecond, limits)
		results <- fairAcquireResult{id: id, reservation: value.Reservation, err: err}
	}
	go acquire("fifo-first")
	waitForQueueDepth(t, client, store.space.throttleQueueKey("global"), 1)
	go acquire("fifo-second")
	waitForQueueDepth(t, client, store.space.throttleQueueKey("global"), 2)
	if err := store.Release(context.Background(), blocker.Reservation); err != nil {
		t.Fatal(err)
	}
	first := <-results
	if first.err != nil || first.id != "fifo-first" {
		t.Fatalf("first grant = %#v", first)
	}
	select {
	case early := <-results:
		t.Fatalf("second waiter bypassed live first lease: %#v", early)
	case <-time.After(40 * time.Millisecond):
	}
	if err := store.Release(context.Background(), first.reservation); err != nil {
		t.Fatal(err)
	}
	second := <-results
	if second.err != nil || second.id != "fifo-second" {
		t.Fatalf("second grant = %#v", second)
	}
	if err := store.Release(context.Background(), second.reservation); err != nil {
		t.Fatal(err)
	}
}

func waitForQueueDepth(t *testing.T, client interface {
	ZCard(context.Context, string) *redisclient.IntCmd
}, key string, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if value, err := client.ZCard(context.Background(), key).Result(); err == nil && value == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("queue %q did not reach depth %d", key, want)
}
