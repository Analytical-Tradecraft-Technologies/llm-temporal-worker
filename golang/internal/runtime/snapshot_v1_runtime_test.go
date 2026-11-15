package runtime

import (
	"context"
	"sync/atomic"
	"testing"

	appactivity "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

type recordingV1Runtime struct {
	generateCalls atomic.Int32
	queryCalls    atomic.Int32
}

func (runtime *recordingV1Runtime) GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	runtime.generateCalls.Add(1)
	return llm.GenerateResponseV1{}, nil
}

func (runtime *recordingV1Runtime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, nil
}

func (runtime *recordingV1Runtime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	runtime.queryCalls.Add(1)
	return llm.QueryResponseV1{}, nil
}

var _ appactivity.V1Runtime = (*recordingV1Runtime)(nil)

func TestSnapshotV1RuntimeUsesTheCurrentSnapshotAndHoldsItsLease(t *testing.T) {
	first := &recordingV1Runtime{}
	second := &recordingV1Runtime{}
	var builds atomic.Int32
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder: app.SnapshotBuilder{References: config.ReferenceResolverFunc(func(context.Context, *config.Config) error {
			return nil
		})},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			if builds.Add(1) == 1 {
				return &snapshotClients{v1Runtime: first}, nil
			}
			return &snapshotClients{v1Runtime: second}, nil
		},
	})
	testassert.NoError(t, err, "app.New() error")
	runtime := &snapshotV1Runtime{application: application}
	testassert.NoError(t, testassert.Err(runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{})), "first GenerateV1() error")
	if first.generateCalls.Load() != 1 || second.generateCalls.Load() != 0 {
		t.Fatalf("first dispatch counts = %d/%d", first.generateCalls.Load(), second.generateCalls.Load())
	}
	testassert.NoError(t, application.Reload(context.Background(), runtimeConfig(t)), "app.Reload() error")
	testassert.NoError(t, testassert.Err(runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{})), "reloaded GenerateV1() error")
	if first.generateCalls.Load() != 1 || second.generateCalls.Load() != 1 {
		t.Fatalf("reloaded dispatch counts = %d/%d", first.generateCalls.Load(), second.generateCalls.Load())
	}
	testassert.NoError(t, application.Close(context.Background()), "application close")
}

func TestSnapshotV1RuntimeDoesNotUseFallbackWhenSourceIsUnconfigured(t *testing.T) {
	fallback := &recordingV1Runtime{}
	application := newStaticClientsApp(t, &snapshotClients{v1Runtime: nil})
	runtime := &snapshotV1Runtime{application: application, fallback: fallback}
	if _, err := runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{}); err == nil {
		t.Fatal("unconfigured snapshot unexpectedly used fallback runtime")
	}
	if fallback.generateCalls.Load() != 0 {
		t.Fatal("fallback runtime was called across an authoritative nil source")
	}
}

func TestSnapshotV1RuntimeUsesFallbackForLegacyClientSet(t *testing.T) {
	fallback := &recordingV1Runtime{}
	application := newStaticClientsApp(t, app.ClientSetFunc(func(context.Context) error { return nil }))
	runtime := &snapshotV1Runtime{application: application, fallback: fallback}
	testassert.NoError(t, testassert.Err(runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{})), "fallback GenerateV1() error")
	if fallback.generateCalls.Load() != 1 {
		t.Fatalf("fallback calls = %d, want 1", fallback.generateCalls.Load())
	}
}

func TestSnapshotQueryServiceFallsBackToSnapshotV1Runtime(t *testing.T) {
	v1Runtime := &recordingV1Runtime{}
	application := newStaticClientsApp(t, &snapshotClients{v1Runtime: v1Runtime})
	service := &snapshotQueryService{application: application}
	testassert.NoError(t, testassert.Err(service.Execute(context.Background(), llm.QueryRequestV1{})), "snapshot query fallback error")
	if v1Runtime.queryCalls.Load() != 1 {
		t.Fatalf("v1 QueryV1 calls = %d, want 1", v1Runtime.queryCalls.Load())
	}
}

// newStaticClientsApp builds an application from runtimeConfig whose every
// snapshot uses clients. The application closes when the test ends.
func newStaticClientsApp(t *testing.T, clients app.ClientSet) *app.App {
	t.Helper()
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder:       app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			return clients, nil
		},
	})
	testassert.NoError(t, err, "app.New() error")
	t.Cleanup(func() { _ = application.Close(context.Background()) })
	return application
}
