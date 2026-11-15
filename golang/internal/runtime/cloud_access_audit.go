package runtime

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	temporalactivity "go.temporal.io/sdk/activity"
)

// auditPrepared reports an allowed access: caller passed the scope policy and
// p was prepared, or loaded, from durable state (#1541). A denial never
// reaches here; the scope resolver logs it (#1535).
func (r *CloudExecutionRuntime) auditPrepared(ctx context.Context, caller llm.RequestContext, p PreparedCloudRequest) {
	r.auditAccess(ctx, requestAccessRecord(ctx, observability.RequestAccessPrepared, caller, p))
}

// auditCompleted reports a terminal result returned to caller: a completed
// response, or a failure that will not be retried. Intermediate states and
// step errors are not access completions and report nothing.
func (r *CloudExecutionRuntime) auditCompleted(ctx context.Context, caller llm.RequestContext, p PreparedCloudRequest, result llm.ExecutionResultV1, err error) {
	if err != nil || (result.State != llm.ExecutionCompleted && (result.State != llm.ExecutionFailed || result.Retryable)) {
		return
	}
	record := requestAccessRecord(ctx, observability.RequestAccessCompleted, caller, p)
	record.Status, record.ErrorCode = string(result.State), result.ErrorCode
	if route, ok := ctx.Value(accessRouteKey{}).(*string); ok {
		record.RouteID = *route
	}
	switch {
	case result.Generate != nil:
		record.Checkpoint = string(result.Generate.Checkpoint.Handle)
	case result.Compact != nil:
		record.Checkpoint = string(result.Compact.Checkpoint.Handle)
	}
	r.auditAccess(ctx, record)
}

// accessRouteKey carries, within one Activity step, the route of the provider
// execution whose result the step reported (finishProviderStep), whether just
// submitted or resumed. The cloud path's public response does
// not name its route, so this is the only place the access event learns it.
type accessRouteKey struct{}

// withAccessRoute returns ctx with an empty route holder for one step.
func withAccessRoute(ctx context.Context) context.Context {
	return context.WithValue(ctx, accessRouteKey{}, new(string))
}

// noteAccessRoute records routeID as the route of this step's provider
// execution. A replayed result or a cache hit records none, and its access
// event omits route_id.
func noteAccessRoute(ctx context.Context, routeID string) {
	if route, ok := ctx.Value(accessRouteKey{}).(*string); ok {
		*route = routeID
	}
}

// auditAccess hands record to the configured sink, or by default to the
// Activity's logger. Like the query audit hook its error is ignored: an audit
// sink failure never fails or retries a request.
func (r *CloudExecutionRuntime) auditAccess(ctx context.Context, record observability.RequestAccessRecord) {
	audit := r.options.AccessAudit
	if audit == nil {
		audit = observability.LoggerFromContext(ctx).RequestAccess
	}
	_ = audit(ctx, record)
}

// requestAccessRecord builds the identifiers common to both phases. The
// tenant and project are the request's verified scope; the actor is the
// caller of this step.
func requestAccessRecord(ctx context.Context, phase observability.RequestAccessPhase, caller llm.RequestContext, p PreparedCloudRequest) observability.RequestAccessRecord {
	request := p.Record.Request
	record := observability.RequestAccessRecord{
		Phase: phase, Kind: request.Kind, ActivityType: activityTypeName(ctx),
		Tenant: request.Scope.Tenant, Project: request.Scope.Project, Actor: caller.Actor,
		OperationID: string(request.ID),
	}
	switch {
	case p.Generate != nil:
		record.OperationKey = p.Generate.OperationKey
		if p.Generate.Parent != nil {
			record.ParentCheckpoint = string(*p.Generate.Parent)
		}
	case p.Compact != nil:
		record.OperationKey, record.ParentCheckpoint = p.Compact.OperationKey, string(p.Compact.Parent)
	}
	return record
}

// prepareCaller is the caller context of input; Prepare has verified that
// exactly one of Generate and Compact is set.
func prepareCaller(input llm.PrepareExecutionV1) llm.RequestContext {
	if input.Generate != nil {
		return input.Generate.Context
	}
	if input.Compact != nil {
		return input.Compact.Context
	}
	return llm.RequestContext{}
}

// activityTypeName is the registered Temporal Activity type of ctx, or empty
// outside an Activity.
func activityTypeName(ctx context.Context) string {
	if ctx == nil || !temporalactivity.IsActivity(ctx) {
		return ""
	}
	return temporalactivity.GetInfo(ctx).ActivityType.Name
}
