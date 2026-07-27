package activity

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func TestNewDurableV1RuntimeRejectsIncompleteGeneratePorts(t *testing.T) {
	_, err := NewDurableV1Runtime(durable.GeneratePorts{}, durable.CompactPorts{}, nil)
	if err == nil {
		t.Fatal("NewDurableV1Runtime accepted incomplete phase ports")
	}
}

func TestDurableV1RuntimeValidateRejectsNilRuntime(t *testing.T) {
	var runtime *DurableV1Runtime
	if err := runtime.Validate(); err == nil {
		t.Fatal("nil DurableV1Runtime validated successfully")
	}
}

func TestNewDurableV1RuntimeAllowsIndependentQueryComposition(t *testing.T) {
	generate := durable.GeneratePorts{
		Replay: func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
			return durable.GenerateReplay{}, nil
		},
		CacheLookup: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay) (durable.CacheDecision, error) {
			return durable.CacheDecision{}, nil
		},
		CompactionDecision: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.CompactionDecision, error) {
			return durable.CompactionDecision{}, nil
		},
		Compact: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay) (durable.GenerateReplay, error) {
			return durable.GenerateReplay{}, nil
		},
		Route: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
			return durable.RoutePlan{}, nil
		},
		Reserve: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			return durable.ReserveResult{}, nil
		},
		Journal: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.JournalReceipt, error) {
			return durable.JournalReceipt{}, nil
		},
		Dispatch: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.JournalReceipt) (durable.DispatchResult, error) {
			return durable.DispatchResult{}, nil
		},
		Finalize: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ReserveResult, durable.DispatchResult) (durable.GenerateFinalization, error) {
			return durable.GenerateFinalization{}, nil
		},
		FinalizeCache: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.GenerateFinalization, error) {
			return durable.GenerateFinalization{}, nil
		},
		Reconcile: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult, durable.GenerateFinalization) error {
			return nil
		},
	}
	compact := durable.CompactPorts{
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
	runtime, err := NewDurableV1Runtime(generate, compact, nil)
	if err != nil {
		t.Fatalf("NewDurableV1Runtime() error = %v", err)
	}
	if runtime.Query != nil {
		t.Fatal("runtime unexpectedly installed a Query callback")
	}
}
