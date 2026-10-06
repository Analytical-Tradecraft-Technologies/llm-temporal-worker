package runtime

import (
	"context"
	"time"

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
}

// recordCloudProviderAttempt counts one provider submission and its latency.
func recordCloudProviderAttempt(ctx context.Context, call provider.Call, stage cloudstate.ExecutionStage, duration time.Duration) {
	outcome := "failure"
	if stage == cloudstate.ExecutionSucceeded || stage == cloudstate.ExecutionPending {
		outcome = "success"
	}
	observability.MetricsFromContext(ctx).RecordProviderAttempt(call.EndpointID, call.Model, string(call.ServiceClass), outcome, duration)
}

// recordCloudExecutionOutcome counts the transition a provider outcome made:
// an unresolved (ambiguous) attempt, or a completed response's service class
// and cost status.
func recordCloudExecutionOutcome(ctx context.Context, call provider.Call, before cloudstate.ExecutionStage, next cloudstate.ProviderExecution) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil || next.Stage == before {
		return
	}
	switch next.Stage {
	case cloudstate.ExecutionUnknown:
		metrics.RecordAmbiguous(call.EndpointID)
	case cloudstate.ExecutionSucceeded:
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
			actual = call.ServiceClass
		}
		metrics.RecordServiceClass(string(response.Service.Requested), actualLabel, call.EndpointID)
		switch {
		case response.Cost.Status == llm.CostStatusKnown && response.Cost.ActualCostUSD != nil:
			metrics.RecordCostStatus(call.EndpointID, call.Model, string(actual), "exact", response.Cost.Method)
			metrics.RecordExactCost(call.EndpointID, call.Model, string(actual), response.Cost.Method)
		case response.Cost.Status == llm.CostStatusUnknown:
			metrics.RecordCostStatus(call.EndpointID, call.Model, string(actual), "unknown", "")
		}
	}
}
