//go:build integration

package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestLiveRedisThrottleRequestWindowPersistsAcrossRelease(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-rate-window")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{{Kind: ThrottleRequests, Scope: "generation:route", Amount: 1, Limit: 60, Window: time.Minute}}
	reservations := make([]ThrottleReservation, 0, 60)
	for index := range 60 {
		acquired, err := store.Acquire(context.Background(), "request-window-"+strconv.Itoa(index), limits)
		if err != nil {
			t.Fatalf("request %d: %v", index, err)
		}
		reservations = append(reservations, acquired.Reservation)
	}
	for _, reservation := range reservations {
		if err := store.Release(context.Background(), reservation); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Acquire(context.Background(), "request-window-overflow", limits); !errors.Is(err, ErrThrottleDenied) {
		t.Fatalf("61st request = %v, want denial", err)
	}
}

func TestLiveRedisThrottleFairQueueCancellationAndRenewal(t *testing.T) {
	client := openLiveRedis(t)
	keys := liveKeyOptions("throttle-fair-lease")
	cleanupLivePrefix(t, client, keys.Prefix)
	store, err := NewThrottleStore(ThrottleOptions{Client: client, Mode: AdmissionModeFunction, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{{Kind: ThrottleConcurrency, Scope: "generation:python-stage2", Amount: 1, Limit: 1, Window: time.Second}}
	blocker, err := store.Acquire(context.Background(), "blocker", limits)
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := store.AcquireFair(cancelCtx, "cancelled-waiter", "python-stage2", time.Second, 5*time.Millisecond, limits); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled waiter = %v", err)
	}
	if err := store.Release(context.Background(), blocker.Reservation); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireFair(context.Background(), "next-waiter", "python-stage2", time.Second, 5*time.Millisecond, limits)
	if err != nil {
		t.Fatalf("stale cancelled waiter blocked queue: %v", err)
	}
	if err := store.Renew(context.Background(), lease.Reservation, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := store.Lookup(context.Background(), lease.Reservation.ID); err != nil {
		t.Fatalf("renewed lease expired at original TTL: %v", err)
	}
	if err := store.Release(context.Background(), lease.Reservation); err != nil {
		t.Fatal(err)
	}
}
