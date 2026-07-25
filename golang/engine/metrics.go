package engine

import (
	"context"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func recordAdmission(ctx context.Context, reservations []admission.WindowReservation, outcome string) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil {
		return
	}
	seen := make(map[string]struct{}, len(reservations))
	for _, reservation := range reservations {
		if reservation.PolicyID == "" {
			continue
		}
		if _, ok := seen[reservation.PolicyID]; ok {
			continue
		}
		seen[reservation.PolicyID] = struct{}{}
		metrics.RecordBudgetAdmission(reservation.PolicyID, outcome)
	}
}

func recordOperationState(ctx context.Context, state admission.OperationState) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil {
		return
	}
	switch state {
	case admission.StateReserved, admission.StateDispatching, admission.StateCompleted, admission.StateAmbiguous:
		metrics.RecordOperationState(string(state))
	default:
		metrics.RecordOperationState("failed")
	}
}

func recordCompletion(ctx context.Context, response llm.Response) {
	metrics := observability.MetricsFromContext(ctx)
	if metrics == nil {
		return
	}
	recordOperationState(ctx, admission.StateCompleted)
	actual := response.Service.Attempted
	if response.Service.Actual != nil {
		actual = *response.Service.Actual
	}
	metrics.RecordServiceClass(string(response.Service.Requested), string(actual), response.Route.EndpointID)
	if response.Cost.Status == llm.CostStatusKnown {
		if response.Cost.ActualCostUSD != nil {
			metrics.RecordCostStatus(response.Route.EndpointID, response.Route.ResolvedModel, string(actual), "exact", response.Cost.Method)
			metrics.RecordExactCost(response.Route.EndpointID, response.Route.ResolvedModel, string(actual), response.Cost.Method)
		}
		if response.Cost.ActualCostUSD != nil {
			if materialized, err := compatibilityActualMicroUSD(*response.Cost.ActualCostUSD); err == nil {
				metrics.RecordCost(response.Route.EndpointID, response.Route.ResolvedModel, string(actual), response.Cost.Method, float64(materialized))
			}
		}
	} else if response.Cost.Status == llm.CostStatusUnknown {
		metrics.RecordCostStatus(response.Route.EndpointID, response.Route.ResolvedModel, string(actual), "unknown", "")
	}
}
