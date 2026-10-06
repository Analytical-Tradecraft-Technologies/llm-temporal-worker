package engine

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	memory "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/memory"
)

func TestGenerateEmitsSafeLifecycleSpans(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "telemetry", response: successfulResponse()})
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	if _, err := harness.engine.Generate(ctx, baseRequest("telemetry-span")); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	spans := exporter.Spans()
	names := make(map[string]struct{}, len(spans))
	for _, span := range spans {
		names[span.Name()] = struct{}{}
		for _, attr := range span.Attributes() {
			if string(attr.Key) == "tenant" || strings.Contains(attr.Value.AsString(), "tenant-1") || strings.Contains(attr.Value.AsString(), "hello") {
				t.Fatalf("trace leaked request content: span=%s attr=%#v", span.Name(), attr)
			}
		}
	}
	for _, want := range []string{"llmtw.generate", "llmtw.normalize", "llmtw.state.load", "llmtw.planning", "llmtw.admission", "llmtw.provider_attempt", "llmtw.finalization"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("lifecycle spans = %v, missing %q", names, want)
		}
	}
}

func TestGenerateRejectsInvalidServiceClassWithoutExportingIt(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "telemetry-invalid-class", response: successfulResponse()})
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	request := baseRequest("telemetry-invalid-generate")
	request.ServiceClass = llm.ServiceClass("secret-token-value")

	if _, err := harness.engine.Generate(ctx, request); err == nil {
		t.Fatal("Generate accepted an invalid service class")
	}
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoServiceClassTraceAttribute(t, exporter)
}

func TestStreamRejectsInvalidServiceClassWithoutExportingIt(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "telemetry-invalid-stream", response: successfulResponse()})
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	request := baseRequest("telemetry-invalid-stream")
	request.ServiceClass = llm.ServiceClass("secret-token-value")

	if stream, err := harness.engine.Stream(ctx, request); err == nil || stream != nil {
		t.Fatalf("Stream invalid service class = stream:%v err:%v, want nil stream and error", stream, err)
	}
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoServiceClassTraceAttribute(t, exporter)
}

func assertNoServiceClassTraceAttribute(t *testing.T, exporter *observability.MemoryExporter) {
	t.Helper()
	for _, span := range exporter.Spans() {
		for _, attr := range span.Attributes() {
			if string(attr.Key) == "service_class" {
				t.Fatalf("trace exported rejected service class: span=%s attr=%#v", span.Name(), attr)
			}
		}
	}
}

func TestGenerateEmitsContinuationWriteSpan(t *testing.T) {
	response := successfulResponse()
	response.Continuation = &llm.Continuation{Handle: "provider-opaque-handle"}
	harness := newHarness(t, &fakeAdapter{name: "telemetry-continuation", response: response})
	keyring, err := state.NewKeyring([]state.Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	continuations, err := memory.NewContinuationStore(memory.ContinuationOptions{Keyring: keyring, Clock: func() time.Time { return harness.clock }})
	if err != nil {
		t.Fatal(err)
	}
	harness.engine.dependencies.Continuations = continuations
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	if _, err := harness.engine.Generate(ctx, baseRequest("continuation-span")); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, span := range exporter.Spans() {
		if span.Name() == "llmtw.continuation_write" {
			return
		}
	}
	t.Fatal("continuation write did not emit a trace span")
}

func TestStreamUsesAdmissionTraceContextForProviderAttempt(t *testing.T) {
	adapter := &streamingAdapter{
		fakeAdapter: &fakeAdapter{name: "telemetry-stream", response: successfulResponse()},
		events:      []provider.Event{provider.OutputStarted{Index: 0}, provider.TextDelta{Index: 0, Text: "ok"}, provider.OutputFinished{Index: 0}, provider.StreamCompleted{}},
	}
	harness := newHarness(t, adapter)
	exporter := &observability.MemoryExporter{}
	tracer := observability.NewTracer(observability.TraceOptions{Enabled: true, Exporter: exporter})
	ctx := observability.WithTracer(context.Background(), tracer)
	stream, err := harness.engine.Stream(ctx, baseRequest("stream-trace-parent"))
	if err != nil {
		t.Fatal(err)
	}
	readTerminalStream(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracer.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	var admissionID, providerParentID string
	for _, span := range exporter.Spans() {
		switch span.Name() {
		case "llmtw.admission":
			admissionID = span.SpanContext().SpanID().String()
		case "llmtw.provider_attempt":
			providerParentID = span.Parent().SpanID().String()
		}
	}
	if admissionID == "" || providerParentID == "" {
		t.Fatalf("missing admission or provider attempt trace span")
	}
	if providerParentID != admissionID {
		t.Fatalf("provider attempt parent = %s, want admission span %s", providerParentID, admissionID)
	}
}
