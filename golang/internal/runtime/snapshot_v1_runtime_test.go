package runtime

import (
	"context"
	"sync/atomic"
	"testing"

	appactivity "github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

type recordingV1Runtime struct {
	generateCalls atomic.Int32
}

func (runtime *recordingV1Runtime) GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	runtime.generateCalls.Add(1)
	return llm.GenerateResponseV1{}, nil
}

func (runtime *recordingV1Runtime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, nil
}

func (runtime *recordingV1Runtime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
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
	if err != nil {
		t.Fatalf("app.New() error = %v", err)
	}
	runtime := &snapshotV1Runtime{application: application}
	if _, err := runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{}); err != nil {
		t.Fatalf("first GenerateV1() error = %v", err)
	}
	if first.generateCalls.Load() != 1 || second.generateCalls.Load() != 0 {
		t.Fatalf("first dispatch counts = %d/%d", first.generateCalls.Load(), second.generateCalls.Load())
	}
	if err := application.Reload(context.Background(), runtimeConfig(t)); err != nil {
		t.Fatalf("app.Reload() error = %v", err)
	}
	if _, err := runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{}); err != nil {
		t.Fatalf("reloaded GenerateV1() error = %v", err)
	}
	if first.generateCalls.Load() != 1 || second.generateCalls.Load() != 1 {
		t.Fatalf("reloaded dispatch counts = %d/%d", first.generateCalls.Load(), second.generateCalls.Load())
	}
	if err := application.Close(context.Background()); err != nil {
		t.Fatalf("application close = %v", err)
	}
}

func TestSnapshotV1RuntimeDoesNotUseFallbackWhenSourceIsUnconfigured(t *testing.T) {
	fallback := &recordingV1Runtime{}
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder:       app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			return &snapshotClients{v1Runtime: nil}, nil
		},
	})
	if err != nil {
		t.Fatalf("app.New() error = %v", err)
	}
	defer application.Close(context.Background())
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
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: runtimeConfig(t),
		Builder:       app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			return app.ClientSetFunc(func(context.Context) error { return nil }), nil
		},
	})
	if err != nil {
		t.Fatalf("app.New() error = %v", err)
	}
	defer application.Close(context.Background())
	runtime := &snapshotV1Runtime{application: application, fallback: fallback}
	if _, err := runtime.GenerateV1(context.Background(), llm.GenerateRequestV1{}); err != nil {
		t.Fatalf("fallback GenerateV1() error = %v", err)
	}
	if fallback.generateCalls.Load() != 1 {
		t.Fatalf("fallback calls = %d, want 1", fallback.generateCalls.Load())
	}
}
