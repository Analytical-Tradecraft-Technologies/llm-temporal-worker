package control

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func snapshot(at time.Time) InventorySnapshot {
	return InventorySnapshot{ConfigDigest: digest(1), Provider: "provider", EndpointID: "endpoint", EndpointAccountHMAC: digest(2), EndpointFamily: "chat", Region: "us", Source: InventoryProviderAPI, ObservedAt: at, Complete: true, InventoryDigest: digest(4), ExpiresAt: at.Add(time.Minute), Models: []Model{{ProviderModelID: "model-a", Lifecycle: LifecycleAvailable, CapabilityDigest: digest(5)}}}
}

func TestInventorySnapshotRequiresSortedBoundedModels(t *testing.T) {
	value := snapshot(time.Unix(100, 0))
	value.Models = []Model{{ProviderModelID: "model-b"}, {ProviderModelID: "model-a"}}
	if err := value.Validate(); err == nil {
		t.Fatal("unsorted models were accepted")
	}
	value = snapshot(time.Unix(100, 0))
	value.Source, value.Models = InventoryUnsupported, nil
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := value.ProvenanceAt(time.Unix(101, 0)); got != ProvenanceUnsupported {
		t.Fatalf("unsupported provenance = %q", got)
	}
}

func TestRefreshCoordinatorCollapsesConcurrentCalls(t *testing.T) {
	coordinator := NewRefreshCoordinator()
	var mu sync.Mutex
	calls := 0
	fetch := func(context.Context) (InventorySnapshot, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return snapshot(time.Unix(100, 0)), nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := coordinator.Refresh(context.Background(), "endpoint", fetch); err != nil {
				t.Errorf("refresh: %v", err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("fetch calls = %d, want 1", calls)
	}
}

func TestRefreshCoordinatorReturnsStaleSnapshotOnFailure(t *testing.T) {
	coordinator := NewRefreshCoordinator()
	if _, err := coordinator.Refresh(context.Background(), "endpoint", func(context.Context) (InventorySnapshot, error) {
		return snapshot(time.Unix(100, 0)), nil
	}); err != nil {
		t.Fatal(err)
	}
	want := snapshot(time.Unix(100, 0))
	got, err := coordinator.Refresh(context.Background(), "endpoint", func(context.Context) (InventorySnapshot, error) {
		return InventorySnapshot{}, context.DeadlineExceeded
	})
	if err == nil || got.InventoryDigest != want.InventoryDigest {
		t.Fatalf("failed refresh = digest %x, err %v; want stale digest %x and error", got.InventoryDigest, err, want.InventoryDigest)
	}
}

// signalContext lets the test deterministically stop the waiter after it has
// captured the in-flight entry but before that entry is completed. This makes
// sure a subsequent refresh cannot cause the waiter to observe the wrong
// attempt's result.
type signalContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func (ctx *signalContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.done) })
	return ctx.Context.Done()
}

func TestRefreshCoordinatorWaiterKeepsCompletedAttempt(t *testing.T) {
	coordinator := NewRefreshCoordinator()
	wantErr := errors.New("first refresh failed")
	first := &refreshEntry{
		snapshot: snapshot(time.Unix(100, 0)),
		wait:     make(chan struct{}),
		err:      wantErr,
	}
	coordinator.mu.Lock()
	coordinator.entries["endpoint"] = first
	coordinator.mu.Unlock()

	ctx := &signalContext{Context: context.Background(), done: make(chan struct{})}
	result := make(chan struct {
		snapshot InventorySnapshot
		err      error
	}, 1)
	go func() {
		got, err := coordinator.Refresh(ctx, "endpoint", func(context.Context) (InventorySnapshot, error) {
			return InventorySnapshot{}, errors.New("waiter unexpectedly became refresh owner")
		})
		result <- struct {
			snapshot InventorySnapshot
			err      error
		}{got, err}
	}()
	<-ctx.done

	// Complete the first attempt while replacing the map entry with a new
	// refresh. The waiter must still return the first attempt's error and
	// snapshot, rather than reading this replacement entry.
	coordinator.mu.Lock()
	close(first.wait)
	coordinator.entries["endpoint"] = &refreshEntry{snapshot: snapshot(time.Unix(200, 0))}
	coordinator.mu.Unlock()

	got := <-result
	if !errors.Is(got.err, wantErr) {
		t.Fatalf("waiter error = %v, want %v", got.err, wantErr)
	}
	if got.snapshot.InventoryDigest != first.snapshot.InventoryDigest {
		t.Fatalf("waiter snapshot = %x, want first attempt %x", got.snapshot.InventoryDigest, first.snapshot.InventoryDigest)
	}
}

func TestConfiguredModelDoesNotMutateConfiguration(t *testing.T) {
	configured := []string{"configured-model"}
	if !ConfiguredModel(configured, "configured-model") || ConfiguredModel(configured, "discovered-model") {
		t.Fatal("configured model predicate incorrect")
	}
	if len(configured) != 1 {
		t.Fatal("discovery changed configured route list")
	}
}
