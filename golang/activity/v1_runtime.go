package activity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// V1Runtime is the application-owned implementation of the durable v1
// boundary. The Activity package owns validation, payload limits, Temporal
// error conversion, and registration; checkpoint, cache, provider, and query
// state remain behind this interface so they can be supplied by the durable
// engine without coupling the Temporal adapter to a storage implementation.
//
// Implementations must perform one-shot work. In particular, none of these
// methods is a token/event stream and the Activity layer never dispatches
// llm.StreamingEngine.
type V1Runtime interface {
	GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error)
	CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error)
	QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)
}

// ReserveBatchV1Runtime is the optional durable batch-admission capability.
// Keeping it separate from V1Runtime preserves custom Generate/Compact/Query
// implementations while the registered Activity still fails closed unless
// the active snapshot exposes atomic batch admission.
type ReserveBatchV1Runtime interface {
	ReserveBatchV1(context.Context, llm.ReserveBatchRequestV1) (llm.ReserveBatchResponseV1, error)
}

type AllocateBatchGrantsV1Runtime interface {
	AllocateBatchGrantsV1(context.Context, llm.AllocateBatchGrantsRequestV1) (llm.AllocateBatchGrantsResponseV1, error)
}
type CloseBatchV1Runtime interface {
	CloseBatchV1(context.Context, llm.CloseBatchRequestV1) (llm.CloseBatchResponseV1, error)
}
type ResourceCapacityV1Runtime interface {
	AcquireResourceCapacityV1(context.Context, llm.ResourceCapacityAcquireRequestV1) (llm.ResourceCapacityLeaseV1, error)
	RenewResourceCapacityV1(context.Context, llm.ResourceCapacityRenewRequestV1) (llm.ResourceCapacityLeaseV1, error)
	ReleaseResourceCapacityV1(context.Context, llm.ResourceCapacityReleaseRequestV1) (llm.ResourceCapacityReleaseResponseV1, error)
}

// QueryService is the control-plane implementation used by llm.query.v1.
// Keeping this interface separate from V1Runtime allows query reads to be
// deployed before the Generate/Compact durable engine is composed.
type QueryService interface {
	Execute(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)
}

// UnconfiguredV1Runtime makes an incomplete production composition fail
// closed before any provider or storage work. It is intentionally useful as a
// concrete value: callers can distinguish a missing durable implementation
// from a nil Activities object without exposing a provider error body.
type UnconfiguredV1Runtime struct{}

func (UnconfiguredV1Runtime) unavailable(phase provider.Phase) error {
	return provider.NewError(provider.CodeConfiguration, phase, provider.DispatchNotDispatched, provider.RetryNever, "durable v1 runtime is not configured")
}

func (runtime UnconfiguredV1Runtime) GenerateV1(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	return llm.GenerateResponseV1{}, runtime.unavailable(provider.PhaseStateLoad)
}

func (runtime UnconfiguredV1Runtime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, runtime.unavailable(provider.PhaseStateLoad)
}

func (runtime UnconfiguredV1Runtime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, runtime.unavailable(provider.PhaseStateLoad)
}

// GenerateV1 dispatches a bounded, closed Generate v1 record. The pointer
// result is deliberate: Temporal must not serialize an invalid zero response
// alongside an Activity error.
func (activities *Activities) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (*llm.GenerateResponseV1, error) {
	if err := validateV1Request(ctx, MarshalGenerateV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil || activities.V1Runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.GenerateResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = activities.V1Runtime.GenerateV1(dispatchContext, request)
		if err != nil {
			return err
		}
		_, err = MarshalGenerateResponseV1(response, activities.payloadLimits())
		if err != nil {
			return v1OutputError("Generate", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// CompactV1 dispatches a bounded, closed Compact v1 record. Compact has its
// own response contract and never returns a normal Generate answer.
func (activities *Activities) CompactV1(ctx context.Context, request llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
	if err := validateV1Request(ctx, MarshalCompactV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil || activities.V1Runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.CompactResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = activities.V1Runtime.CompactV1(dispatchContext, request)
		if err != nil {
			return err
		}
		_, err = MarshalCompactResponseV1(response, activities.payloadLimits())
		if err != nil {
			return v1OutputError("Compact", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// QueryV1 dispatches a bounded, closed Query v1 record. Query is included in
// the registration set so the task queue has one exact v1 namespace; its
// implementation is still supplied by the control-plane runtime.
func (activities *Activities) QueryV1(ctx context.Context, request llm.QueryRequestV1) (*llm.QueryResponseV1, error) {
	if err := validateV1Request(ctx, MarshalQueryV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil || (activities.V1Runtime == nil && activities.QueryService == nil) {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.QueryResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		if activities.QueryService != nil {
			response, err = activities.QueryService.Execute(dispatchContext, request)
		} else {
			response, err = activities.V1Runtime.QueryV1(dispatchContext, request)
		}
		if err != nil {
			return err
		}
		_, err = MarshalQueryResponseV1(response, activities.payloadLimits())
		if err != nil {
			return v1OutputError("Query", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// ReserveBatchV1 prices and atomically reserves a complete ordered panel
// before any child Generate Activity can dispatch provider bytes.
func (activities *Activities) ReserveBatchV1(ctx context.Context, request llm.ReserveBatchRequestV1) (*llm.ReserveBatchResponseV1, error) {
	if err := validateV1Request(ctx, MarshalReserveBatchV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(ReserveBatchV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.ReserveBatchResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.ReserveBatchV1(dispatchContext, request)
		if err != nil {
			return err
		}
		_, err = MarshalReserveBatchResponseV1(response, activities.payloadLimits())
		if err != nil {
			return v1OutputError("ReserveBatch", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (activities *Activities) AllocateBatchGrantsV1(ctx context.Context, request llm.AllocateBatchGrantsRequestV1) (*llm.AllocateBatchGrantsResponseV1, error) {
	if err := validateV1Request(ctx, MarshalAllocateBatchGrantsV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(AllocateBatchGrantsV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.AllocateBatchGrantsResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.AllocateBatchGrantsV1(dispatchContext, request)
		if err != nil {
			return err
		}
		if _, err = MarshalAllocateBatchGrantsResponseV1(response, activities.payloadLimits()); err != nil {
			return v1OutputError("AllocateBatchGrants", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (activities *Activities) CloseBatchV1(ctx context.Context, request llm.CloseBatchRequestV1) (*llm.CloseBatchResponseV1, error) {
	if err := validateV1Request(ctx, MarshalCloseBatchV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(CloseBatchV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.CloseBatchResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.CloseBatchV1(dispatchContext, request)
		if err != nil {
			return err
		}
		if _, err = MarshalCloseBatchResponseV1(response, activities.payloadLimits()); err != nil {
			return v1OutputError("CloseBatch", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (activities *Activities) AcquireResourceCapacityV1(ctx context.Context, request llm.ResourceCapacityAcquireRequestV1) (*llm.ResourceCapacityLeaseV1, error) {
	if err := validateV1Request(ctx, MarshalResourceCapacityAcquireV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(ResourceCapacityV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.ResourceCapacityLeaseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.AcquireResourceCapacityV1(dispatchContext, request)
		if err != nil {
			return err
		}
		if _, err = MarshalResourceCapacityLeaseV1(response, activities.payloadLimits()); err != nil {
			return v1OutputError("AcquireResourceCapacity", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (activities *Activities) RenewResourceCapacityV1(ctx context.Context, request llm.ResourceCapacityRenewRequestV1) (*llm.ResourceCapacityLeaseV1, error) {
	if err := validateV1Request(ctx, MarshalResourceCapacityRenewV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(ResourceCapacityV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.ResourceCapacityLeaseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.RenewResourceCapacityV1(dispatchContext, request)
		if err != nil {
			return err
		}
		if _, err = MarshalResourceCapacityLeaseV1(response, activities.payloadLimits()); err != nil {
			return v1OutputError("RenewResourceCapacity", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (activities *Activities) ReleaseResourceCapacityV1(ctx context.Context, request llm.ResourceCapacityReleaseRequestV1) (*llm.ResourceCapacityReleaseResponseV1, error) {
	if err := validateV1Request(ctx, MarshalResourceCapacityReleaseV1, request, activities); err != nil {
		return nil, err
	}
	if activities == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := activities.V1Runtime.(ResourceCapacityV1Runtime)
	if !ok || runtime == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.ResourceCapacityReleaseResponseV1
	err := activities.runV1(ctx, func(dispatchContext context.Context) error {
		var err error
		response, err = runtime.ReleaseResourceCapacityV1(dispatchContext, request)
		if err != nil {
			return err
		}
		if _, err = MarshalResourceCapacityReleaseResponseV1(response, activities.payloadLimits()); err != nil {
			return v1OutputError("ReleaseResourceCapacity", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// runV1 preserves the existing Activity lifecycle around the durable runtime
// seam. Runtime implementations may block on provider or storage work, so the
// adapter binds telemetry and sends bounded provider_wait heartbeats exactly as
// the legacy Generate path does. It returns only the sanitized Temporal error.
func (activities *Activities) runV1(ctx context.Context, dispatch func(context.Context) error) (resultErr error) {
	ctx = observability.WithTracer(ctx, activities.Tracer)
	ctx = observability.WithMetrics(ctx, activities.Metrics)
	started := time.Now()
	var rawErr error
	defer func() {
		status, errorClass := activityMetricOutcome(resultErr)
		activities.Metrics.RecordActivity(status, errorClass, time.Since(started), "total")
		if activityCanceled(resultErr) {
			return
		}
		failureErr := resultErr
		if rawErr != nil {
			failureErr = rawErr
		}
		if origin, failed := activityFailureOrigin(failureErr); failed {
			activities.Metrics.RecordActivityFailure(origin)
		}
	}()

	keepaliveInterval, err := activities.keepaliveInterval()
	if err != nil {
		return ToTemporalError(err)
	}
	var heartbeater Heartbeater
	dispatchContext := ctx
	var keepalive *heartbeatKeepalive
	if rawHeartbeater := activities.newHeartbeater(); rawHeartbeater != nil {
		serializedHeartbeater := &serializedHeartbeater{target: rawHeartbeater}
		heartbeater = &deduplicatingHeartbeater{target: serializedHeartbeater}
		ctx = engine.WithHeartbeat(ctx, heartbeater)
		if err := heartbeater.Beat(ctx, engine.Progress{Phase: "planning"}); err != nil {
			return ToTemporalError(err)
		}
		dispatchContext, keepalive = startHeartbeatKeepalive(ctx, serializedHeartbeater, keepaliveInterval, activities.heartbeatTickerFactory)
		defer func() {
			if keepalive != nil {
				_ = keepalive.stop()
			}
		}()
	}
	rawErr = dispatch(dispatchContext)
	if rawErr != nil && sdkactivity.IsActivity(ctx) {
		sdkactivity.GetLogger(ctx).Error("V1 runtime diagnostic", "error", rawErr)
	}
	keepaliveErr := keepalive.stop()
	keepalive = nil
	if ctxErr := ctx.Err(); ctxErr != nil && !preferV1DispatchError(rawErr, ctxErr) {
		return ToTemporalError(ctxErr)
	}
	if keepaliveErr != nil {
		return ToTemporalError(heartbeatKeepaliveFailure(keepaliveErr))
	}
	if rawErr != nil {
		return ToTemporalError(rawErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ToTemporalError(ctxErr)
	}
	return nil
}

// preferV1DispatchError retains the sanitized state-load deadline produced by
// checkpoint materialization. The Activity context normally wins over a
// runtime error, matching the legacy one-shot lifecycle; this narrow exception
// prevents a mapped state-load deadline from being replaced by an untyped
// context error before ToTemporalError can preserve its retry disposition.
func preferV1DispatchError(rawErr, ctxErr error) bool {
	if rawErr == nil || !errors.Is(ctxErr, context.DeadlineExceeded) {
		return false
	}
	var providerErr *provider.Error
	return errors.As(rawErr, &providerErr) && providerErr.Code == provider.CodeDeadlineExceeded && providerErr.Phase == provider.PhaseStateLoad
}

func (activities *Activities) generateV1Temporal(ctx context.Context, request llm.GenerateRequestV1) (*llm.GenerateResponseV1, error) {
	return activities.GenerateV1(ctx, request)
}

func (activities *Activities) compactV1Temporal(ctx context.Context, request llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
	return activities.CompactV1(ctx, request)
}

func (activities *Activities) queryV1Temporal(ctx context.Context, request llm.QueryRequestV1) (*llm.QueryResponseV1, error) {
	return activities.QueryV1(ctx, request)
}

func (activities *Activities) reserveBatchV1Temporal(ctx context.Context, request llm.ReserveBatchRequestV1) (*llm.ReserveBatchResponseV1, error) {
	return activities.ReserveBatchV1(ctx, request)
}

func (activities *Activities) allocateBatchGrantsV1Temporal(ctx context.Context, request llm.AllocateBatchGrantsRequestV1) (*llm.AllocateBatchGrantsResponseV1, error) {
	return activities.AllocateBatchGrantsV1(ctx, request)
}
func (activities *Activities) closeBatchV1Temporal(ctx context.Context, request llm.CloseBatchRequestV1) (*llm.CloseBatchResponseV1, error) {
	return activities.CloseBatchV1(ctx, request)
}
func (activities *Activities) acquireResourceCapacityV1Temporal(ctx context.Context, request llm.ResourceCapacityAcquireRequestV1) (*llm.ResourceCapacityLeaseV1, error) {
	return activities.AcquireResourceCapacityV1(ctx, request)
}
func (activities *Activities) renewResourceCapacityV1Temporal(ctx context.Context, request llm.ResourceCapacityRenewRequestV1) (*llm.ResourceCapacityLeaseV1, error) {
	return activities.RenewResourceCapacityV1(ctx, request)
}
func (activities *Activities) releaseResourceCapacityV1Temporal(ctx context.Context, request llm.ResourceCapacityReleaseRequestV1) (*llm.ResourceCapacityReleaseResponseV1, error) {
	return activities.ReleaseResourceCapacityV1(ctx, request)
}

// RegisterV1 installs the exact nine versioned names. It is separate from
// Register so callers that still exercise the pre-release direct helper in a
// unit test cannot accidentally put that envelope on a production task
// queue. New production composition calls RegisterV1 through Register when a
// V1Runtime is present (including UnconfiguredV1Runtime's fail-closed seam).
func (activities *Activities) RegisterV1(registry worker.ActivityRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterActivityWithOptions(activities.generateV1Temporal, sdkactivity.RegisterOptions{Name: GenerateActivityName})
	registry.RegisterActivityWithOptions(activities.compactV1Temporal, sdkactivity.RegisterOptions{Name: CompactActivityName})
	registry.RegisterActivityWithOptions(activities.queryV1Temporal, sdkactivity.RegisterOptions{Name: QueryActivityName})
	registry.RegisterActivityWithOptions(activities.reserveBatchV1Temporal, sdkactivity.RegisterOptions{Name: ReserveBatchActivityName})
	registry.RegisterActivityWithOptions(activities.allocateBatchGrantsV1Temporal, sdkactivity.RegisterOptions{Name: llm.AllocateBatchGrantsActivityName})
	registry.RegisterActivityWithOptions(activities.closeBatchV1Temporal, sdkactivity.RegisterOptions{Name: llm.CloseBatchActivityName})
	registry.RegisterActivityWithOptions(activities.acquireResourceCapacityV1Temporal, sdkactivity.RegisterOptions{Name: llm.ResourceCapacityAcquireActivityName})
	registry.RegisterActivityWithOptions(activities.renewResourceCapacityV1Temporal, sdkactivity.RegisterOptions{Name: llm.ResourceCapacityRenewActivityName})
	registry.RegisterActivityWithOptions(activities.releaseResourceCapacityV1Temporal, sdkactivity.RegisterOptions{Name: llm.ResourceCapacityReleaseActivityName})
}

func (activities *Activities) payloadLimits() PayloadLimits {
	if activities == nil {
		return PayloadLimits{}
	}
	return activities.PayloadLimits
}

func validateV1Request[T any](ctx context.Context, marshal func(T, PayloadLimits) ([]byte, error), request T, activities *Activities) error {
	if ctx == nil {
		return ToTemporalError(provider.NewError(provider.CodeInvalidArgument, provider.PhaseDecode, provider.DispatchNotDispatched, provider.RetryNever, "Activity context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := marshal(request, activities.payloadLimits()); err != nil {
		return ToTemporalError(provider.NewError(provider.CodeInvalidArgument, provider.PhaseDecode, provider.DispatchNotDispatched, provider.RetryNever, "v1 Activity payload is invalid or exceeds its limit"))
	}
	return nil
}

func v1OutputError(kind string, err error) error {
	// The underlying codec error can include bounded field names, but not the
	// record itself. Keep the emitted Temporal message stable and content-free.
	return fmt.Errorf("%s v1 response failed contract validation: %w", kind, err)
}
