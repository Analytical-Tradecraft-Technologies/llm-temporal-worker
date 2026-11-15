package runtime

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

type snapshotExecutionProbe struct {
	recordingV1Runtime
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

// call counts one runtime call and, when entered is set, signals entry and
// holds the call until release or cancellation.
func (p *snapshotExecutionProbe) call(ctx context.Context) (llm.ExecutionResultV1, error) {
	p.calls.Add(1)
	if p.entered != nil {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return llm.ExecutionResultV1{}, ctx.Err()
		}
	}
	return llm.ExecutionResultV1{State: llm.ExecutionAcquired}, nil
}

func (p *snapshotExecutionProbe) PrepareExecutionV1(ctx context.Context, _ llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}
func (p *snapshotExecutionProbe) AcquireBudgetV1(ctx context.Context, _ llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}
func (p *snapshotExecutionProbe) GenerateStepV1(ctx context.Context, _ llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}
func (p *snapshotExecutionProbe) CompactStepV1(ctx context.Context, _ llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}
func (p *snapshotExecutionProbe) PollExecutionV1(ctx context.Context, _ llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}
func (p *snapshotExecutionProbe) CompleteExecutionV1(ctx context.Context, _ llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return p.call(ctx)
}

func TestSnapshotExecutionLeaseSurvivesReload(t *testing.T) {
	first := &snapshotExecutionProbe{entered: make(chan struct{}), release: make(chan struct{})}
	second := &snapshotExecutionProbe{}
	var builds, closed atomic.Int32
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t), Builder: app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			if builds.Add(1) == 1 {
				return &snapshotClients{v1Runtime: first, clients: app.ClientSetFunc(func(context.Context) error { closed.Add(1); return nil })}, nil
			}
			return &snapshotClients{v1Runtime: second}, nil
		},
	})
	testassert.NoError(t, err)
	defer application.Close(context.Background())
	proxy := &snapshotV1Runtime{application: application}
	pollContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := proxy.PollExecutionV1(pollContext, llm.ExecutionReferenceV1{})
		done <- err
	}()
	select {
	case <-first.entered:
	case <-time.After(time.Second):
		t.Fatal("poll not entered")
	}
	old := application.Current()
	reloaded := make(chan error, 1)
	data := runtimeConfig(t)
	go func() { reloaded <- application.Reload(context.Background(), data) }()
	deadline := time.Now().Add(time.Second)
	for application.Current() == old {
		if time.Now().After(deadline) {
			close(first.release)
			t.Fatal("replacement snapshot was not published")
		}
		runtime.Gosched()
	}
	if closed.Load() != 0 {
		t.Fatal("reload closed clients while poll was using them")
	}
	testassert.NoError(t, testassert.Err(proxy.AcquireBudgetV1(context.Background(), llm.ExecutionReferenceV1{})))
	if second.calls.Load() != 1 {
		t.Fatal("new activity did not use reloaded snapshot")
	}
	close(first.release)
	select {
	case err := <-done:
		testassert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("poll did not complete")
	}
	select {
	case err := <-reloaded:
		testassert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reload did not drain")
	}
	if closed.Load() != 1 {
		t.Fatal("retired clients not closed after lease release")
	}
}

func TestSnapshotExecutionStepsFailClosedWithoutBoundedRuntime(t *testing.T) {
	application := newStaticClientsApp(t, &snapshotClients{v1Runtime: &recordingV1Runtime{}})
	proxy := &snapshotV1Runtime{application: application}
	calls := []func() error{
		func() error { _, err := proxy.PrepareExecutionV1(nil, llm.PrepareExecutionV1{}); return err },
		func() error { _, err := proxy.AcquireBudgetV1(nil, llm.ExecutionReferenceV1{}); return err },
		func() error { _, err := proxy.GenerateStepV1(nil, llm.GenerateRequestV1{}); return err },
		func() error { _, err := proxy.CompactStepV1(nil, llm.CompactRequestV1{}); return err },
		func() error { _, err := proxy.PollExecutionV1(nil, llm.ExecutionReferenceV1{}); return err },
		func() error { _, err := proxy.CompleteExecutionV1(nil, llm.ExecutionReferenceV1{}); return err },
	}
	for _, call := range calls {
		if err := call(); err == nil {
			t.Fatal("bounded runtime unexpectedly configured")
		}
	}
}
