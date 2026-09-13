package activity

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

type Activities struct {
	Engine                     llm.Engine
	Heartbeater                Heartbeater
	HeartbeaterFactory         func() Heartbeater
	HeartbeatKeepaliveInterval time.Duration
	Metrics                    *observability.Metrics
	Tracer                     *observability.Tracer
	PayloadLimits              PayloadLimits
	// V1Runtime owns durable checkpoint/cache/provider/control state for the
	// closed v1 Activity records. Runtime composition supplies this explicitly;
	// a nil value is rejected before dispatch.
	V1Runtime V1Runtime
	// RequireDurableProviderFence disables the legacy Generate surface for a
	// durable worker. Only GenerateV1 owns the immutable Redis reservation,
	// PostgreSQL journal, and signed provider-capacity lease required before a
	// network write. Development helpers leave this false.
	RequireDurableProviderFence bool
	// QueryService is an optional control-plane seam for llm.query.v1. It is
	// independent from Generate/Compact composition so query callers cannot
	// accidentally dispatch inference work while the durable query handlers
	// are being assembled.
	QueryService QueryService

	// heartbeatTickerFactory is a test seam. Production leaves it nil and uses
	// the bounded real-time ticker in startHeartbeatKeepalive.
	heartbeatTickerFactory func(time.Duration) heartbeatTicker
}

func (activities *Activities) Generate(ctx context.Context, payload GenerateRequest) (result GenerateResponse, resultErr error) {
	if activities == nil {
		return GenerateResponse{}, ToTemporalError(provider.NewError(provider.CodeInternal, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetryNever, "Activity engine is unavailable"))
	}
	ctx = observability.WithTracer(ctx, activities.Tracer)
	ctx = observability.WithMetrics(ctx, activities.Metrics)
	started := time.Now()
	var rawEngineErr error
	var outputValidationFailure bool
	defer func() {
		status, errorClass := activityMetricOutcome(resultErr)
		activities.Metrics.RecordActivity(status, errorClass, time.Since(started), "total")
		if activityCanceled(resultErr) {
			return
		}
		if outputValidationFailure {
			activities.Metrics.RecordActivityFailure("worker")
			return
		}
		failureErr := resultErr
		if rawEngineErr != nil {
			// ToTemporalError intentionally turns untyped engine failures into a
			// caller-safe invalid-argument response. Preserve the original engine
			// error for worker-SLO attribution so unknown provenance fails closed.
			failureErr = rawEngineErr
		}
		if origin, failed := activityFailureOrigin(failureErr); failed {
			activities.Metrics.RecordActivityFailure(origin)
		}
	}()
	if activities.RequireDurableProviderFence {
		return GenerateResponse{}, ToTemporalError(provider.NewError(provider.CodeConfiguration, provider.PhaseAdmission, provider.DispatchNotDispatched, provider.RetryNever, "legacy Generate cannot dispatch without the durable v1 reservation fence"))
	}
	if activities.Engine == nil {
		return GenerateResponse{}, ToTemporalError(provider.NewError(provider.CodeInternal, provider.PhaseFinalize, provider.DispatchNotDispatched, provider.RetryNever, "Activity engine is unavailable"))
	}
	request, err := payload.Validate(activities.PayloadLimits.inlineBytes())
	if err != nil {
		return GenerateResponse{}, ToTemporalError(err)
	}
	keepaliveInterval, err := activities.keepaliveInterval()
	if err != nil {
		return GenerateResponse{}, ToTemporalError(err)
	}
	var heartbeater Heartbeater
	generateContext := ctx
	var keepalive *heartbeatKeepalive
	if rawHeartbeater := activities.newHeartbeater(); rawHeartbeater != nil {
		serializedHeartbeater := &serializedHeartbeater{target: rawHeartbeater}
		heartbeater = &deduplicatingHeartbeater{target: serializedHeartbeater}
		ctx = engine.WithHeartbeat(ctx, heartbeater)
		if err := heartbeater.Beat(ctx, engine.Progress{Phase: "planning"}); err != nil {
			return GenerateResponse{}, ToTemporalError(err)
		}
		generateContext, keepalive = startHeartbeatKeepalive(ctx, serializedHeartbeater, keepaliveInterval, activities.heartbeatTickerFactory)
		defer func() {
			if keepalive != nil {
				_ = keepalive.stop()
			}
		}()
	}
	response, err := activities.Engine.Generate(generateContext, request)
	keepaliveErr := keepalive.stop()
	keepalive = nil
	if ctxErr := ctx.Err(); ctxErr != nil {
		return GenerateResponse{}, ToTemporalError(ctxErr)
	}
	if keepaliveErr != nil {
		return GenerateResponse{}, ToTemporalError(heartbeatKeepaliveFailure(keepaliveErr))
	}
	if err != nil {
		rawEngineErr = err
		return GenerateResponse{}, ToTemporalError(err)
	}
	result, resultErr, outputValidationFailure = activities.completeGenerate(ctx, response, heartbeater)
	return result, resultErr
}

func activityMetricOutcome(err error) (status, errorClass string) {
	if err == nil {
		return "completed", "none"
	}
	if activityCanceled(err) {
		return "canceled", "none"
	}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		switch applicationErr.Type() {
		case ErrorTypeBudgetWait:
			return "failed", "budget_denied"
		case ErrorTypeProviderTransient:
			return "failed", "provider_unavailable"
		}
	}
	return "failed", "internal"
}

func activityCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || temporal.IsCanceledError(err)
}

// activityFailureOrigin keeps the worker SLO classification separate from the
// stable activity_total label schema. Unknown errors deliberately count as
// worker-origin failures so an unrecognized failure cannot improve the SLO.
func activityFailureOrigin(err error) (origin string, failed bool) {
	if err == nil || activityCanceled(err) {
		return "", false
	}
	var providerErr *provider.Error
	if errors.As(err, &providerErr) {
		if providerErr.Code == provider.CodeCanceled && !errors.Is(providerErr, provider.ErrProviderPreDispatch) {
			return "", false
		}
		return activityFailureOriginDetails(SafeErrorDetails{Code: string(providerErr.Code), Phase: string(providerErr.Phase)})
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		return "worker", true
	}
	var details SafeErrorDetails
	if applicationErr.Details(&details) != nil {
		return "worker", true
	}
	return activityFailureOriginDetails(details)
}

func activityFailureOriginDetails(details SafeErrorDetails) (origin string, failed bool) {
	switch provider.Code(details.Code) {
	case provider.CodeConfiguration, provider.CodeInternal, provider.CodeStateUnavailable, provider.CodeStateCorrupt:
		return "worker", true
	case provider.CodeNoRoute:
		if provider.Phase(details.Phase) == provider.PhasePrice {
			return "worker", true
		}
		return "caller", true
	case provider.CodeAuthentication, provider.CodePermissionDenied,
		provider.CodeProviderRateLimited, provider.CodeProviderUnavailable,
		provider.CodeProviderInvalidResponse, provider.CodeDeadlineExceeded,
		provider.CodeAmbiguousDispatch:
		return "provider", true
	case provider.CodeCanceled:
		return "", false
	case provider.CodeInvalidArgument, provider.CodeUnsupportedCapability,
		provider.CodeOperationConflict:
		return "caller", true
	case provider.CodeBudgetDenied:
		return "budget", true
	default:
		return "worker", true
	}
}

func (activities *Activities) completeGenerate(ctx context.Context, response llm.Response, heartbeater Heartbeater) (GenerateResponse, error, bool) {
	result := GenerateResponse{APIVersion: APIVersion, Response: response, Metadata: ResultMetadata{OperationID: response.OperationID}}
	if err := result.Validate(activities.PayloadLimits.inlineBytes()); err != nil {
		return GenerateResponse{}, ToTemporalError(err), true
	}
	if heartbeater != nil {
		if err := heartbeater.Beat(ctx, engine.Progress{OperationID: response.OperationID, Phase: "finalization"}); err != nil {
			return GenerateResponse{}, ToTemporalError(err), false
		}
	}
	return result, nil, false
}

// generateTemporal keeps the Temporal Activity result absent when Generate
// returns an error. The SDK serializes any non-nil result before propagating
// the error, and a zero GenerateResponse is not a valid durable response.
func (activities *Activities) generateTemporal(ctx context.Context, payload GenerateRequest) (*GenerateResponse, error) {
	result, err := activities.Generate(ctx, payload)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (activities *Activities) newHeartbeater() Heartbeater {
	if activities == nil {
		return nil
	}
	if activities.HeartbeaterFactory != nil {
		return activities.HeartbeaterFactory()
	}
	return activities.Heartbeater
}

func (activities *Activities) keepaliveInterval() (time.Duration, error) {
	if activities == nil || activities.HeartbeatKeepaliveInterval == 0 {
		return DefaultHeartbeatKeepaliveInterval, nil
	}
	if activities.HeartbeatKeepaliveInterval < 0 {
		return 0, provider.NewError(provider.CodeConfiguration, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "Activity heartbeat keepalive interval must be positive")
	}
	return activities.HeartbeatKeepaliveInterval, nil
}

// serializedHeartbeater prevents the periodic provider-wait heartbeat from
// racing lifecycle heartbeats emitted by the engine on the same Activity.
type serializedHeartbeater struct {
	target Heartbeater
	mu     sync.Mutex
}

func (heartbeater *serializedHeartbeater) Beat(ctx context.Context, progress engine.Progress) error {
	if heartbeater == nil || heartbeater.target == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	heartbeater.mu.Lock()
	defer heartbeater.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return heartbeater.target.Beat(ctx, progress)
}

// deduplicatingHeartbeater suppresses duplicate lifecycle facts emitted by an
// Activity and the engine it invokes. It is allocated for one Activity call,
// so it cannot couple concurrent invocation timestamps or mutable state.
type deduplicatingHeartbeater struct {
	target Heartbeater

	mu   sync.Mutex
	last engine.Progress
	set  bool
}

func (heartbeater *deduplicatingHeartbeater) Beat(ctx context.Context, progress engine.Progress) error {
	if heartbeater == nil || heartbeater.target == nil {
		return nil
	}
	heartbeater.mu.Lock()
	duplicate := heartbeater.set && sameProgress(heartbeater.last, progress)
	if !duplicate {
		heartbeater.last = progress
		heartbeater.set = true
	}
	heartbeater.mu.Unlock()
	if duplicate {
		return ctx.Err()
	}
	return heartbeater.target.Beat(ctx, progress)
}

func sameProgress(left, right engine.Progress) bool {
	return left.OperationID == right.OperationID && left.Phase == right.Phase && left.RouteIndex == right.RouteIndex && left.ClassIndex == right.ClassIndex && left.OutputItems == right.OutputItems
}

// Register installs the exact versioned Activity name rather than relying on
// a Go method name that could change during a refactor.
func (activities *Activities) Register(registry worker.ActivityRegistry) {
	if activities != nil && (activities.V1Runtime != nil || activities.QueryService != nil) {
		activities.RegisterV1(registry)
		return
	}
	registry.RegisterActivityWithOptions(activities.generateTemporal, sdkactivity.RegisterOptions{Name: GenerateActivityName})
}
