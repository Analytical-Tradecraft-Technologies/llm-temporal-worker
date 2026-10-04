package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type TraceOptions struct {
	Enabled     bool
	Exporter    trace.SpanExporter
	SampleRatio *float64
	Batch       bool
}

type Tracer struct {
	provider *trace.TracerProvider
	tracer   oteltrace.Tracer
}

func NewTracer(options TraceOptions) *Tracer {
	if !options.Enabled {
		// A zero-value Tracer uses the no-op path, including parent context
		// propagation, without allocating an SDK provider or recording spans.
		return &Tracer{}
	}
	providerOptions := make([]trace.TracerProviderOption, 0, 2)
	if options.Exporter != nil {
		if options.SampleRatio != nil {
			ratio := *options.SampleRatio
			if ratio < 0 {
				ratio = 0
			} else if ratio > 1 {
				ratio = 1
			}
			providerOptions = append(providerOptions, trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(ratio))))
		}
		if options.Batch {
			providerOptions = append(providerOptions, trace.WithBatcher(options.Exporter))
		} else {
			providerOptions = append(providerOptions, trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(options.Exporter)))
		}
	}
	provider := trace.NewTracerProvider(providerOptions...)
	return &Tracer{provider: provider, tracer: provider.Tracer("github.com/mfow/llm-temporal-worker")}
}

var noopTracer = NewTracer(TraceOptions{})

func (tracer *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	if tracer == nil || tracer.tracer == nil {
		noopContext, span := oteltrace.NewNoopTracerProvider().Tracer("llmtw").Start(ctx, "event")
		return noopContext, span
	}
	filtered := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if safe, ok := safeTraceAttr(attr); ok {
			filtered = append(filtered, safe)
		}
	}
	return tracer.tracer.Start(ctx, safeSpanName(name), oteltrace.WithAttributes(filtered...))
}

func (tracer *Tracer) RecordError(span oteltrace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	attrs := errorAttrs(err)
	for _, attr := range attrs {
		if safe, ok := safeTraceAttr(attribute.String(attr.Key, attr.Value.String())); ok {
			span.SetAttributes(safe)
		}
	}
	span.SetStatus(codes.Error, "operation failed")
}

func (tracer *Tracer) Shutdown(ctx context.Context) error {
	if tracer == nil || tracer.provider == nil {
		return nil
	}
	return tracer.provider.Shutdown(ctx)
}

// Flush delivers all completed spans within the caller's shutdown deadline.
// Runtime uses Shutdown afterwards to release the exporter transport.
func (tracer *Tracer) Flush(ctx context.Context) error {
	if tracer == nil || tracer.provider == nil {
		return nil
	}
	return tracer.provider.ForceFlush(ctx)
}

type tracerContextKey struct{}

// WithTracer binds one configured tracer to a request context without making
// the provider-neutral engine depend on process-global OpenTelemetry state.
func WithTracer(ctx context.Context, tracer *Tracer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if tracer == nil {
		return ctx
	}
	return context.WithValue(ctx, tracerContextKey{}, tracer)
}

// FromContext returns a no-op-safe tracer when the runtime has not bound one.
func FromContext(ctx context.Context) *Tracer {
	if ctx != nil {
		if tracer, ok := ctx.Value(tracerContextKey{}).(*Tracer); ok && tracer != nil {
			return tracer
		}
	}
	return noopTracer
}

func safeSpanName(name string) string {
	if name == "" || len(name) > 96 || strings.ContainsAny(name, "\r\n") {
		return "worker.event"
	}
	lower := strings.ToLower(name)
	for _, word := range unsafeMessageWords {
		if strings.Contains(lower, word) {
			return "worker.event"
		}
	}
	return name
}

func safeTraceAttr(attr attribute.KeyValue) (attribute.KeyValue, bool) {
	key := string(attr.Key)
	if key == "tenant" {
		return attribute.String("tenant_hash", hashTenant(attr.Value.AsString())), true
	}
	switch key {
	case "trace_id", "span_id", "workflow_id", "run_id", "activity_id", "operation_id", "route_id", "tenant_hash", "endpoint", "model", "service_class", "phase", "error_code", "status", "outcome":
	default:
		return attribute.KeyValue{}, false
	}
	if attr.Value.Type() != attribute.STRING {
		return attribute.KeyValue{}, false
	}
	value := attr.Value.AsString()
	if value == "" || len(value) > 96 || strings.ContainsAny(value, "\r\n") {
		return attribute.KeyValue{}, false
	}
	switch key {
	case "service_class":
		if !llm.ServiceClass(value).Valid() {
			return attribute.KeyValue{}, false
		}
	case "error_code":
		if !provider.Code(value).Valid() {
			return attribute.KeyValue{}, false
		}
	case "phase":
		if !provider.Phase(value).Valid() {
			return attribute.KeyValue{}, false
		}
	}
	return attr, true
}

func hashTenant(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

// MemoryExporter is a bounded test/export hook. Production deployments can
// supply an OTLP exporter without changing worker lifecycle code.
type MemoryExporter struct {
	mu    sync.Mutex
	spans []trace.ReadOnlySpan
}

func (exporter *MemoryExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	if exporter == nil {
		return nil
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	exporter.spans = append(exporter.spans, spans...)
	return nil
}

func (*MemoryExporter) Shutdown(context.Context) error { return nil }

func (exporter *MemoryExporter) Spans() []trace.ReadOnlySpan {
	if exporter == nil {
		return nil
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	return append([]trace.ReadOnlySpan(nil), exporter.spans...)
}

var _ trace.SpanExporter = (*MemoryExporter)(nil)
