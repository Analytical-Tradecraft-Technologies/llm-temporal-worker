package engine

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func (engine *Engine) startTrace(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	return observability.FromContext(ctx).Start(ctx, name, attrs...)
}

func (engine *Engine) recordTraceError(ctx context.Context, span oteltrace.Span, err error) {
	observability.FromContext(ctx).RecordError(span, err)
}

func requestTraceAttrs(request llm.Request) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("service_class", string(request.ServiceClass))}
	if request.Context.Tenant != "" {
		attrs = append(attrs, attribute.String("tenant", request.Context.Tenant))
	}
	return attrs
}

func operationTraceAttrs(operationID string, candidate routing.Candidate) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("operation_id", operationID)}
	if candidate.RouteID != "" {
		attrs = append(attrs, attribute.String("route_id", candidate.RouteID))
	}
	if candidate.EndpointID != "" {
		attrs = append(attrs, attribute.String("endpoint", candidate.EndpointID))
	}
	if candidate.Model != "" {
		attrs = append(attrs, attribute.String("model", candidate.Model))
	}
	if candidate.AttemptedClass != "" {
		attrs = append(attrs, attribute.String("service_class", string(candidate.AttemptedClass)))
	}
	return attrs
}
