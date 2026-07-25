package runtime

import (
	"context"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// snapshotV1Runtime keeps the worker's Activity registration stable while
// resolving the implementation from the snapshot captured by each call. The
// lease remains held for the complete method, so a reload cannot close the
// Redis/PostgreSQL/provider clients while a v1 phase is still using them.
//
// fallback is used only when a custom ClientSet does not implement
// V1RuntimeSource. ProductionEngineFactory always implements the source and a
// nil source value is authoritative (fail closed), preventing a stale global
// runtime from crossing a reload boundary.
type snapshotV1Runtime struct {
	application *app.App
	fallback    activity.V1Runtime
}

var _ activity.V1Runtime = (*snapshotV1Runtime)(nil)

func (runtime *snapshotV1Runtime) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var response llm.GenerateResponseV1
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		var err error
		response, err = current.GenerateV1(ctx, request)
		return err
	})
	return response, err
}

func (runtime *snapshotV1Runtime) CompactV1(ctx context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var response llm.CompactResponseV1
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		var err error
		response, err = current.CompactV1(ctx, request)
		return err
	})
	return response, err
}

func (runtime *snapshotV1Runtime) QueryV1(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var response llm.QueryResponseV1
	err := runtime.with(ctx, func(current activity.V1Runtime) error {
		var err error
		response, err = current.QueryV1(ctx, request)
		return err
	})
	return response, err
}

func (runtime *snapshotV1Runtime) with(ctx context.Context, function func(activity.V1Runtime) error) error {
	if runtime == nil || runtime.application == nil {
		return snapshotV1RuntimeUnavailable()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if function == nil {
		return snapshotV1RuntimeUnavailable()
	}
	lease, err := runtime.application.Acquire()
	if err != nil {
		return provider.NewError(provider.CodeStateUnavailable, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation, "durable v1 snapshot is unavailable")
	}
	defer lease.Release()
	current := v1RuntimeForSnapshot(lease.Snapshot(), runtime.fallback)
	if !isV1RuntimeConfigured(current) {
		return snapshotV1RuntimeUnavailable()
	}
	return function(current)
}

func snapshotV1RuntimeUnavailable() error {
	return provider.NewError(provider.CodeConfiguration, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "durable v1 runtime is not configured")
}
