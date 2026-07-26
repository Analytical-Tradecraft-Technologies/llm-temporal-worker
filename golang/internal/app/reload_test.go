package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

type fakeClients struct {
	mu       sync.Mutex
	closed   int
	closeErr error
}

func (clients *fakeClients) Close(context.Context) error {
	clients.mu.Lock()
	defer clients.mu.Unlock()
	clients.closed++
	return clients.closeErr
}

func (clients *fakeClients) Count() int {
	clients.mu.Lock()
	defer clients.mu.Unlock()
	return clients.closed
}

func TestReloadRejectsInvalidConfigAndDrainsOldClients(t *testing.T) {
	initial := exampleConfig(t)
	created := make(chan *fakeClients, 2)
	application, err := New(context.Background(), Options{
		InitialConfig: initial,
		Builder:       SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) {
			clients := &fakeClients{}
			created <- clients
			return clients, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	oldClients := <-created
	oldVersion := application.Current().Config.ConfigVersion()
	if err := application.Reload(context.Background(), []byte("version: invalid\n")); err == nil {
		t.Fatal("invalid reload unexpectedly succeeded")
	}
	if application.Current().Config.ConfigVersion() != oldVersion {
		t.Fatal("invalid reload replaced the active snapshot")
	}
	old := application.Current()
	lease, err := application.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	reloaded := make(chan error, 1)
	go func() { reloaded <- application.Reload(context.Background(), initial) }()
	newClients := <-created
	if oldClients.Count() != 0 {
		t.Fatal("old clients closed while an Activity lease was active")
	}
	lease.Release()
	if err := <-reloaded; err != nil {
		t.Fatal(err)
	}
	if err := old.WaitClosed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := application.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if oldClients.Count() != 1 || newClients.Count() != 1 {
		t.Fatalf("closed counts = %d, %d; want one each", oldClients.Count(), newClients.Count())
	}
}

func TestReloadClientConstructionFailureKeepsOldSnapshot(t *testing.T) {
	failure := errors.New("client construction failed")
	count := 0
	application, err := New(context.Background(), Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) {
			count++
			if count > 1 {
				return nil, failure
			}
			return ClientSetFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := application.Current().Config.ConfigVersion()
	if err := application.Reload(context.Background(), exampleConfig(t)); !errors.Is(err, failure) {
		t.Fatalf("reload error = %v", err)
	}
	if application.Current().Config.ConfigVersion() != before {
		t.Fatal("client construction failure replaced active snapshot")
	}
}

func TestReloadRunsReplacementValidatorBeforeConstructingClients(t *testing.T) {
	initial := exampleConfig(t)
	created := 0
	rejected := errors.New("replacement requires process restart")
	application, err := New(context.Background(), Options{
		InitialConfig: initial,
		Builder:       SnapshotBuilder{},
		ReplacementValidator: func(current, replacement *config.Snapshot) error {
			if current == nil || replacement == nil {
				t.Fatal("replacement validator received a nil snapshot")
			}
			if current.Config().State.Redis.KeyPrefix == replacement.Config().State.Redis.KeyPrefix {
				t.Fatal("replacement validator did not receive the changed configuration")
			}
			return rejected
		},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) {
			created++
			return ClientSetFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(initial), "key_prefix: llmtw", "key_prefix: worker-b", 1)
	if err := application.Reload(context.Background(), []byte(changed)); !errors.Is(err, rejected) {
		t.Fatalf("reload error = %v, want replacement rejection", err)
	}
	if created != 1 {
		t.Fatalf("reload constructed %d client sets, want 1", created)
	}
	if got := application.Current().Config.Config().State.Redis.KeyPrefix; got != "llmtw" {
		t.Fatalf("active key prefix = %q, want llmtw", got)
	}
	if err := application.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDependencyVerificationFailureNeverPublishesOrLeaksClients(t *testing.T) {
	failure := errors.New("required dependency is unavailable")
	created := make(chan *fakeClients, 2)
	verificationCalls := 0
	application, err := New(context.Background(), Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) {
			clients := &fakeClients{}
			created <- clients
			return clients, nil
		},
		Verify: func(context.Context, *config.Snapshot, ClientSet) error {
			verificationCalls++
			if verificationCalls > 1 {
				return failure
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	oldClients := <-created
	before := application.Current().Config.ConfigVersion()
	if err := application.Reload(context.Background(), exampleConfig(t)); !errors.Is(err, failure) {
		t.Fatalf("reload error = %v, want dependency verification failure", err)
	}
	newClients := <-created
	if application.Current().Config.ConfigVersion() != before {
		t.Fatal("dependency verification failure replaced active snapshot")
	}
	if oldClients.Count() != 0 {
		t.Fatal("dependency verification closed the active snapshot")
	}
	if newClients.Count() != 1 {
		t.Fatalf("unpublished client close count = %d, want 1", newClients.Count())
	}
}

func TestInitialDependencyVerificationFailureClosesClients(t *testing.T) {
	failure := errors.New("required dependency is unavailable")
	clients := &fakeClients{}
	application, err := New(context.Background(), Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) { return clients, nil },
		Verify:  func(context.Context, *config.Snapshot, ClientSet) error { return failure },
	})
	if !errors.Is(err, failure) {
		t.Fatalf("initialization error = %v, want dependency verification failure", err)
	}
	if application != nil {
		t.Fatal("application published a snapshot after failed dependency verification")
	}
	if clients.Count() != 1 {
		t.Fatalf("unpublished initial client close count = %d, want 1", clients.Count())
	}
}

func TestReloadFileRejectsOversizedReplacementWithoutPublishingIt(t *testing.T) {
	application, err := New(context.Background(), Options{InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{}})
	if err != nil {
		t.Fatal(err)
	}
	before := application.Current().Config.ConfigVersion()
	path := filepath.Join(t.TempDir(), "oversized.yaml")
	if err := os.WriteFile(path, make([]byte, maxReloadFileBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	err = application.ReloadFile(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "exceeds safe size") {
		t.Fatalf("reload error = %v", err)
	}
	if current := application.Current(); current == nil || current.Config.ConfigVersion() != before {
		t.Fatal("oversized replacement changed the published snapshot")
	}
}
