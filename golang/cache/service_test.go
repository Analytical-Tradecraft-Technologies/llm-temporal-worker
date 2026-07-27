package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testFingerprint() Fingerprint {
	return ComputeTestFingerprint("task9")
}

// ComputeTestFingerprint keeps tests from depending on a prompt or exposing
// raw request material merely to exercise the coordinator's map key.
func ComputeTestFingerprint(seed string) Fingerprint {
	var fingerprint Fingerprint
	for index := range fingerprint {
		fingerprint[index] = byte(seed[index%len(seed)] + byte(index))
	}
	return fingerprint
}

func TestFillCoordinatorCollapsesConcurrentFills(t *testing.T) {
	coordinator, err := NewFillCoordinator(64)
	if err != nil {
		t.Fatal(err)
	}
	key := testFingerprint()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fill := func(context.Context) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return []byte("normalized-template"), nil
	}

	const workers = 100
	results := make(chan FillResult, workers)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for index := 0; index < workers; index++ {
		go func() {
			defer group.Done()
			result, err := coordinator.Fill(context.Background(), key, fill)
			results <- result
			errorsCh <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fill owner did not start")
	}
	// Let all goroutines reach the coordinator before releasing the owner.
	time.Sleep(20 * time.Millisecond)
	close(release)
	group.Wait()
	close(results)
	close(errorsCh)
	var shared int
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	for result := range results {
		if string(result.Response) != "normalized-template" {
			t.Fatalf("response=%q", result.Response)
		}
		if result.Shared {
			shared++
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider fills=%d, want one", got)
	}
	if shared != workers-1 {
		t.Fatalf("shared results=%d, want %d", shared, workers-1)
	}
}

func TestFillCoordinatorWaiterCanCancel(t *testing.T) {
	coordinator, err := NewFillCoordinator(64)
	if err != nil {
		t.Fatal(err)
	}
	key := testFingerprint()
	started := make(chan struct{})
	release := make(chan struct{})
	ownerResult := make(chan error, 1)
	go func() {
		_, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("ok"), nil
		})
		ownerResult <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := coordinator.Fill(ctx, key, func(context.Context) ([]byte, error) { return nil, errors.New("must not run") }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error=%v, want deadline", err)
	}
	close(release)
	if err := <-ownerResult; err != nil {
		t.Fatal(err)
	}
}

func TestFillCoordinatorDoesNotRetainFailuresAndCopiesResponse(t *testing.T) {
	coordinator, err := NewFillCoordinator(4)
	if err != nil {
		t.Fatal(err)
	}
	key := testFingerprint()
	if _, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) {
		return nil, errors.New("transient")
	}); err == nil {
		t.Fatal("failure unexpectedly suppressed")
	}
	value := []byte("ok")
	result, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) { return value, nil })
	if err != nil || result.Shared {
		t.Fatalf("retry result=%#v err=%v", result, err)
	}
	value[0] = 'x'
	if string(result.Response) != "ok" {
		t.Fatalf("result aliases fill response: %q", result.Response)
	}
	if _, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) { return []byte("12345"), nil }); err == nil {
		t.Fatal("oversized response unexpectedly accepted")
	}
}

func TestFillCoordinatorRecoversPanicsAndAllowsRetry(t *testing.T) {
	coordinator, err := NewFillCoordinator(0)
	if err != nil {
		t.Fatal(err)
	}
	key := testFingerprint()
	if _, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) {
		panic("fixture panic")
	}); err == nil {
		t.Fatal("panic unexpectedly reported as success")
	}
	if result, err := coordinator.Fill(context.Background(), key, func(context.Context) ([]byte, error) { return []byte("retry"), nil }); err != nil || string(result.Response) != "retry" {
		t.Fatalf("retry result=%#v err=%v", result, err)
	}
}

func TestFillCoordinatorRejectsInvalidInputs(t *testing.T) {
	coordinator, err := NewFillCoordinator(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		ctx  context.Context
		key  Fingerprint
		fn   FillFunc
	}{
		{name: "nil context", ctx: nil, key: testFingerprint(), fn: func(context.Context) ([]byte, error) { return nil, nil }},
		{name: "zero key", ctx: context.Background(), fn: func(context.Context) ([]byte, error) { return nil, nil }},
		{name: "nil function", ctx: context.Background(), key: testFingerprint()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := coordinator.Fill(test.ctx, test.key, test.fn); err == nil {
				t.Fatal("invalid input unexpectedly accepted")
			}
		})
	}
	if _, err := NewFillCoordinator(-1); err == nil {
		t.Fatal("negative response limit unexpectedly accepted")
	}
}
