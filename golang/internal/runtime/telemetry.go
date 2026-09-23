package runtime

import (
	"context"
	"errors"
	"os"
	"strconv"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TraceExporterFactory creates the one process-owned trace exporter. It is an
// injectable seam so runtime tests do not contact an OTLP collector.
type TraceExporterFactory func(context.Context, config.TracingConfig) (sdktrace.SpanExporter, error)

func newRuntimeTelemetry(ctx context.Context, configuration config.Config, options Options) (*observability.Metrics, *observability.Tracer, *observability.Logger, error) {
	metrics := options.Metrics
	if metrics == nil {
		var err error
		metrics, err = newMetrics(configuration)
		if err != nil {
			return nil, nil, nil, errors.New("construct metrics failed")
		}
	}

	logger, err := newRuntimeLogger(configuration, options)
	if err != nil {
		return nil, nil, nil, err
	}

	tracer := options.Tracer
	if tracer != nil {
		return metrics, tracer, logger, nil
	}
	if !configuration.Telemetry.Tracing.Enabled {
		return metrics, observability.NewTracer(observability.TraceOptions{}), logger, nil
	}

	ratio, err := strconv.ParseFloat(configuration.Telemetry.Tracing.SampleRatio, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return nil, nil, nil, errors.New("construct tracer failed")
	}
	factory := options.TraceExporterFactory
	if factory == nil {
		factory = defaultTraceExporter
	}
	exporter, err := factory(ctx, configuration.Telemetry.Tracing)
	if err != nil || exporter == nil {
		return nil, nil, nil, errors.New("construct trace exporter failed")
	}
	return metrics, observability.NewTracer(observability.TraceOptions{
		Enabled:     true,
		Exporter:    exporter,
		SampleRatio: &ratio,
		Batch:       true,
	}), logger, nil
}

// newRuntimeLogger is shared by runtime construction and standalone Temporal
// client construction so SDK events use the same configured slog behavior.
func newRuntimeLogger(configuration config.Config, options Options) (*observability.Logger, error) {
	if options.Logger != nil {
		return options.Logger, nil
	}
	output := options.LogOutput
	if output == nil {
		output = os.Stderr
	}
	logger, err := observability.NewLogger(observability.LogOptions{
		Format:         configuration.Telemetry.Logs.Format,
		Level:          configuration.Telemetry.Logs.Level,
		ContentLogging: configuration.Telemetry.ContentLogging,
		Output:         output,
	})
	if err != nil {
		return nil, errors.New("construct logger failed")
	}
	return logger, nil
}

// defaultTraceExporter uses OTLP/gRPC with its secure default transport. The
// configured endpoint is validated before runtime construction, while secrets
// stay outside tracing configuration and never enter exporter options.
func defaultTraceExporter(ctx context.Context, tracing config.TracingConfig) (sdktrace.SpanExporter, error) {
	return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(tracing.OTLPEndpoint))
}
