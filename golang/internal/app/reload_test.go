package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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

// sendFakeClients builds a fresh fakeClients for every snapshot and sends it
// on created.
func sendFakeClients(created chan<- *fakeClients) func(context.Context, *config.Snapshot) (ClientSet, error) {
	return func(context.Context, *config.Snapshot) (ClientSet, error) {
		clients := &fakeClients{}
		created <- clients
		return clients, nil
	}
}

func (clients *fakeClients) Count() int {
	clients.mu.Lock()
	defer clients.mu.Unlock()
	return clients.closed
}

// mustNewApp builds an App from options, failing the test on an error.
func mustNewApp(t *testing.T, options Options) *App {
	t.Helper()
	application, err := New(context.Background(), options)
	testassert.NoError(t, err)
	return application
}

// mustCloseApp closes application, failing the test on an error.
func mustCloseApp(t *testing.T, application *App) {
	t.Helper()
	testassert.NoError(t, application.Close(context.Background()))
}

func TestReloadRejectsInvalidConfigAndDrainsOldClients(t *testing.T) {
	initial := exampleConfig(t)
	created := make(chan *fakeClients, 2)
	application := mustNewApp(t, Options{
		InitialConfig: initial,
		Builder:       SnapshotBuilder{},
		Clients:       sendFakeClients(created),
	})
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
	testassert.NoError(t, err)
	reloaded := make(chan error, 1)
	go func() { reloaded <- application.Reload(context.Background(), initial) }()
	newClients := <-created
	if oldClients.Count() != 0 {
		t.Fatal("old clients closed while an Activity lease was active")
	}
	lease.Release()
	testassert.NoError(t, <-reloaded)
	testassert.NoError(t, old.WaitClosed(context.Background()))
	mustCloseApp(t, application)
	if oldClients.Count() != 1 || newClients.Count() != 1 {
		t.Fatalf("closed counts = %d, %d; want one each", oldClients.Count(), newClients.Count())
	}
}

func TestReloadClientConstructionFailureKeepsOldSnapshot(t *testing.T) {
	failure := errors.New("client construction failed")
	count := 0
	application := mustNewApp(t, Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) {
			count++
			if count > 1 {
				return nil, failure
			}
			return ClientSetFunc(func(context.Context) error { return nil }), nil
		},
	})
	before := application.Current().Config.ConfigVersion()
	testassert.ErrorIs(t, application.Reload(context.Background(), exampleConfig(t)), failure, "reload")
	if application.Current().Config.ConfigVersion() != before {
		t.Fatal("client construction failure replaced active snapshot")
	}
}

func TestReloadRunsReplacementValidatorBeforeConstructingClients(t *testing.T) {
	initial := exampleConfig(t)
	created := 0
	rejected := errors.New("replacement requires process restart")
	application := mustNewApp(t, Options{
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
	mustCloseApp(t, application)
}

func TestDependencyVerificationFailureNeverPublishesOrLeaksClients(t *testing.T) {
	failure := errors.New("required dependency is unavailable")
	created := make(chan *fakeClients, 2)
	verificationCalls := 0
	application := mustNewApp(t, Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: sendFakeClients(created),
		Verify: func(context.Context, *config.Snapshot, ClientSet) error {
			verificationCalls++
			if verificationCalls > 1 {
				return failure
			}
			return nil
		},
	})
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

func TestReloadCannotPublishAfterConcurrentClose(t *testing.T) {
	created := make(chan *fakeClients, 2)
	verificationStarted := make(chan struct{})
	releaseVerification := make(chan struct{})
	verificationCalls := 0
	application := mustNewApp(t, Options{
		InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{},
		Clients: sendFakeClients(created),
		Verify: func(context.Context, *config.Snapshot, ClientSet) error {
			verificationCalls++
			if verificationCalls > 1 {
				close(verificationStarted)
				<-releaseVerification
			}
			return nil
		},
	})
	initialClients := <-created
	reloaded := make(chan error, 1)
	go func() { reloaded <- application.Reload(context.Background(), exampleConfig(t)) }()
	replacementClients := <-created
	<-verificationStarted

	mustCloseApp(t, application)
	if current := application.Current(); current != nil {
		t.Fatal("Close left an active snapshot")
	}
	close(releaseVerification)
	if err := <-reloaded; err == nil {
		t.Fatal("reload published a replacement after Close")
	}
	if current := application.Current(); current != nil {
		t.Fatal("reload resurrected the application after Close")
	}
	mustCloseApp(t, application)
	if initialClients.Count() != 1 || replacementClients.Count() != 1 {
		t.Fatalf("closed counts = %d, %d; want one each", initialClients.Count(), replacementClients.Count())
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
	application := mustNewApp(t, Options{InitialConfig: exampleConfig(t), Builder: SnapshotBuilder{}})
	before := application.Current().Config.ConfigVersion()
	path := filepath.Join(t.TempDir(), "oversized.yaml")
	testassert.NoError(t, os.WriteFile(path, make([]byte, maxReloadFileBytes+1), 0600))
	err := application.ReloadFile(context.Background(), path)
	testassert.ErrorContains(t, err, "exceeds safe size")
	if current := application.Current(); current == nil || current.Config.ConfigVersion() != before {
		t.Fatal("oversized replacement changed the published snapshot")
	}
}

func TestAcquireDuringRepeatedReloadKeepsClientsAlive(t *testing.T) {
	initial := exampleConfig(t)
	application := mustNewApp(t, Options{
		InitialConfig: initial, Builder: SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (ClientSet, error) { return &fakeClients{}, nil },
	})
	defer func() {
		if err := application.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	stop := make(chan struct{})
	failures := make(chan error, 1)
	var readers sync.WaitGroup
	report := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	for i := 0; i < 32; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lease, err := application.Acquire()
				if err != nil {
					report(err)
					return
				}
				clients := lease.Snapshot().Clients.(*fakeClients)
				if clients.Count() != 0 {
					report(errors.New("acquired clients already closed"))
				}
				lease.Release()
			}
		}()
	}
	for i := 0; i < 100; i++ {
		if err := application.Reload(context.Background(), initial); err != nil {
			report(err)
			break
		}
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	mustCloseApp(t, application)
	if lease, err := application.Acquire(); err == nil {
		lease.Release()
		t.Fatal("acquired after close")
	}
}
