package runtime

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// The cloud runtime records the request series the legacy engine records,
// through the recorder the v1 Activity places in the context. Every recorder
// method is nil-safe, so callers without metrics record nothing.

// recordCloudBudgetAdmission counts one acquire decision per budget policy.
func recordCloudBudgetAdmission(ctx context.Context, request durable.ReserveRequest, result durable.ReserveResult) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil {
		return
	}
	outcome := "accepted"
	if !result.Accepted {
		outcome = "denied"
	}
	seen := make(map[string]struct{}, len(request.Reservations))
	for _, reservation := range request.Reservations {
		if _, ok := seen[reservation.PolicyID]; ok || reservation.PolicyID == "" {
			continue
		}
		seen[reservation.PolicyID] = struct{}{}
		metrics.RecordBudgetAdmission(reservation.PolicyID, outcome)
	}
	if result.Accepted {
		metrics.RecordOperationState("reserved")
	}
}

// recordCloudProviderAttempt counts one provider submission and its latency.
func recordCloudProviderAttempt(ctx context.Context, call provider.Call, accepted bool, duration time.Duration) {
	outcome := "failure"
	if accepted {
		outcome = "success"
	}
	observability.MetricsFromContext(ctx).RecordProviderAttempt(call.EndpointID, call.Model, string(call.ServiceClass), outcome, duration)
}

// recordCloudExecutionOutcome counts the transition a saved provider
// execution made: an unresolved (ambiguous) attempt, or a completed
// response's service class and cost status.
func recordCloudExecutionOutcome(ctx context.Context, plan cloudstate.BudgetPlan, before cloudstate.ExecutionStage, next cloudstate.ProviderExecution) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil || next.Stage == before {
		return
	}
	endpoint, model := plan.Route.EndpointID, plan.Route.Model
	switch next.Stage {
	case cloudstate.ExecutionSubmitting:
		metrics.RecordOperationState("dispatching")
	case cloudstate.ExecutionFailed:
		metrics.RecordOperationState("failed")
	case cloudstate.ExecutionUnknown:
		metrics.RecordOperationState("ambiguous")
		metrics.RecordAmbiguous(endpoint)
	case cloudstate.ExecutionSucceeded:
		metrics.RecordOperationState("completed")
		if next.Response == nil {
			return
		}
		response := next.Response
		actual := response.Service.Attempted
		actualLabel := "unknown"
		if response.Service.Actual != nil {
			actual = *response.Service.Actual
			actualLabel = string(actual)
		}
		if actual == "" {
			actual = plan.AttemptedClass
		}
		metrics.RecordServiceClass(string(response.Service.Requested), actualLabel, endpoint)
		switch {
		case response.Cost.Status == llm.CostStatusKnown && response.Cost.ActualCostUSD != nil:
			metrics.RecordCostStatus(endpoint, model, string(actual), "exact", response.Cost.Method)
			metrics.RecordExactCost(endpoint, model, string(actual), response.Cost.Method)
		case response.Cost.Status == llm.CostStatusUnknown:
			metrics.RecordCostStatus(endpoint, model, string(actual), "unknown", "")
		}
	}
}

// recordCloudCache counts a published request's cache outcome: a hit and its
// use receipt, or a miss and whether it filled the cache.
func recordCloudCache(ctx context.Context, disposition string) {
	metrics := observability.MetricsFromContext(ctx)
	switch disposition {
	case "hit":
		metrics.RecordCache("hit")
		metrics.RecordCache("use")
	case "miss_populated":
		metrics.RecordCache("miss")
		metrics.RecordCache("fill")
	case "miss_not_populated":
		metrics.RecordCache("miss")
	}
}

// recordCloudPoll counts one poll of a provider-owned job: started, then
// completed, failed or retry (still pending, or a transient read failure).
func recordCloudPoll(ctx context.Context, outcome string) {
	observability.MetricsFromContext(ctx).RecordPendingPoll(outcome)
}

// tracedStep runs one durable execution step inside a span named for it, with
// the resulting execution state and any error recorded. The span carries no
// request content.
func tracedStep(ctx context.Context, step string, run func(context.Context) (llm.ExecutionResultV1, error)) (llm.ExecutionResultV1, error) {
	tracer := observability.FromContext(ctx)
	spanCtx, span := tracer.Start(ctx, "llmtw.cloud."+step)
	defer span.End()
	result, err := run(spanCtx)
	if err != nil {
		tracer.RecordError(span, err)
		return result, err
	}
	span.SetAttributes(attribute.String("state", string(result.State)))
	return result, nil
}

// recordCloudContinuation counts checkpoint lineage use: a request that
// extends a parent checkpoint reuses it, and every published child is created.
func recordCloudContinuation(ctx context.Context, decision string) {
	observability.MetricsFromContext(ctx).RecordContinuation(decision)
}
