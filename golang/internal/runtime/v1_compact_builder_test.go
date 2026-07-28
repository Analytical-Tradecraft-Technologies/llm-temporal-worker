package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	durable "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func validCompactPorts() durable.CompactPorts {
	return durable.CompactPorts{
		Replay: func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) {
			return durable.CompactReplay{}, nil
		},
		CacheLookup: func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.CompactCacheDecision, error) {
			return durable.CompactCacheDecision{}, nil
		},
		Route: func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
			return durable.RoutePlan{}, nil
		},
		Reserve: func(context.Context, llm.CompactRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			return durable.ReserveResult{}, nil
		},
		Journal: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.JournalReceipt, error) {
			return durable.JournalReceipt{}, nil
		},
		Dispatch: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.JournalReceipt) (durable.CompactDispatchResult, error) {
			return durable.CompactDispatchResult{}, nil
		},
		Finalize: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ReserveResult, durable.CompactDispatchResult) (durable.CompactFinalization, error) {
			return durable.CompactFinalization{}, nil
		},
		FinalizeCache: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.CompactCacheDecision) (durable.CompactFinalization, error) {
			return durable.CompactFinalization{}, nil
		},
		Reconcile: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult, durable.CompactFinalization) error {
			return nil
		},
	}
}

func completeCompactCapabilities(factory CompactPortsFactory) V1RuntimeCapabilities {
	return V1RuntimeCapabilities{
		Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Version: "snapshot-1"}},
		Planner:  routing.DeterministicPlanner{}, Adapters: engine.AdapterMap{},
		Checkpoints: CheckpointCapabilities{Repository: builderCheckpointRepository{}, Blobs: builderCheckpointBlobReader{}, Materializer: builderCheckpointMaterializer{}},
		Journal:     builderJournal{}, Clock: time.Now, CompactPortsFactory: factory,
	}
}

func TestCompactV1RuntimeBuilderRequiresCompactFactory(t *testing.T) {
	capabilities := completeCompactCapabilities(nil)
	if err := capabilities.ValidateCompact(); err == nil || !strings.Contains(err.Error(), "ports factory") {
		t.Fatalf("ValidateCompact() error = %v, want missing factory", err)
	}
}

func TestCompactV1RuntimeBuilderConstructsCompactOnlyRuntime(t *testing.T) {
	clients := &generateBuilderClientSet{capabilities: completeCompactCapabilities(func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
		return validCompactPorts(), nil
	})}
	runtimeValue, err := NewCompactV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err != nil {
		t.Fatalf("builder error = %v", err)
	}
	if _, ok := runtimeValue.(*activity.CompactOnlyV1Runtime); !ok {
		t.Fatalf("runtime type = %T, want CompactOnlyV1Runtime", runtimeValue)
	}
	if _, err := runtimeValue.GenerateV1(context.Background(), llm.GenerateRequestV1{}); err == nil {
		t.Fatal("Compact-only runtime unexpectedly served Generate")
	}
}

func TestCompactV1RuntimeBuilderRejectsIncompletePorts(t *testing.T) {
	clients := &generateBuilderClientSet{capabilities: completeCompactCapabilities(func(context.Context, V1RuntimeCapabilities) (durable.CompactPorts, error) {
		return durable.CompactPorts{}, nil
	})}
	_, err := NewCompactV1RuntimeBuilder()(context.Background(), &config.Snapshot{}, nil, clients)
	if err == nil || !errors.Is(err, ErrCompactV1Composition) || !strings.Contains(err.Error(), "validate Compact ports") {
		t.Fatalf("builder error = %v, want invalid-port failure", err)
	}
}
