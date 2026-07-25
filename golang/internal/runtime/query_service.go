package runtime

import (
	"context"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// snapshotQueryService resolves the control-plane service from the same
// immutable app snapshot that supplied the current engine. This prevents a
// reload from leaving Query activities pointed at a closed PostgreSQL pool or
// stale authorization policy while an in-flight Activity still uses the old
// engine lease.
type snapshotQueryService struct {
	application *app.App
	// fallback is the snapshot-dispatching V1Runtime used when a snapshot has
	// no dedicated control-plane QueryService. It is intentionally a runtime
	// seam, not a legacy engine adapter.
	fallback activity.V1Runtime
}

func (service *snapshotQueryService) Execute(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if service == nil || service.application == nil {
		return llm.QueryResponseV1{}, queryServiceUnavailable("runtime query service is unavailable")
	}
	lease, err := service.application.Acquire()
	if err != nil {
		return llm.QueryResponseV1{}, queryServiceUnavailable("runtime query snapshot is unavailable")
	}
	defer lease.Release()
	snapshot := lease.Snapshot()
	clients, ok := snapshot.Clients.(*snapshotClients)
	if !ok || clients == nil {
		return llm.QueryResponseV1{}, queryServiceUnavailable("runtime query service is not configured")
	}
	if service := clients.QueryService(); service != nil {
		return service.Execute(ctx, request)
	}
	if runtime := clients.V1Runtime(); runtime != nil {
		return runtime.QueryV1(ctx, request)
	}
	if service.fallback != nil {
		return service.fallback.QueryV1(ctx, request)
	}
	return llm.QueryResponseV1{}, queryServiceUnavailable("runtime query service is not configured")
}

func queryServiceUnavailable(message string) error {
	return provider.NewError(provider.CodeConfiguration, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, message)
}

var _ activity.QueryService = (*snapshotQueryService)(nil)
