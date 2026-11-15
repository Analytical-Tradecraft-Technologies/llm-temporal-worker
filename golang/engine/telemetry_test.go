package engine

import (
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestGenerateEmitsSafeLifecycleSpans(t *testing.T) {
	harness := newHarness(t, &fakeAdapter{name: "telemetry", response: successfulResponse()})
	trace := newTestTrace()
	if _, err := harness.engine.Generate(trace.ctx, baseRequest("telemetry-span")); err != nil {
		t.Fatal(err)
	}
	trace.shutdown(t)
	spans := trace.exporter.Spans()
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
	harness, trace, request := invalidServiceClassTrace(t, "telemetry-invalid-class", "telemetry-invalid-generate")
	if _, err := harness.engine.Generate(trace.ctx, request); err == nil {
		t.Fatal("Generate accepted an invalid service class")
	}
	assertNoServiceClassTraceAttribute(t, trace)
}

func TestStreamRejectsInvalidServiceClassWithoutExportingIt(t *testing.T) {
	harness, trace, request := invalidServiceClassTrace(t, "telemetry-invalid-stream", "telemetry-invalid-stream")
	if stream, err := harness.engine.Stream(trace.ctx, request); err == nil || stream != nil {
		t.Fatalf("Stream invalid service class = stream:%v err:%v, want nil stream and error", stream, err)
	}
	assertNoServiceClassTraceAttribute(t, trace)
}

// invalidServiceClassTrace builds a traced harness and a request whose
// service class is a secret-looking invalid value.
func invalidServiceClassTrace(t *testing.T, adapterName, operationKey string) (testHarness, testTrace, llm.Request) {
	t.Helper()
	harness := newHarness(t, &fakeAdapter{name: adapterName, response: successfulResponse()})
	request := baseRequest(operationKey)
	request.ServiceClass = llm.ServiceClass("secret-token-value")
	return harness, newTestTrace(), request
}

// assertNoServiceClassTraceAttribute flushes trace and requires that no span
// exported a service_class attribute.
func assertNoServiceClassTraceAttribute(t *testing.T, trace testTrace) {
	t.Helper()
	trace.shutdown(t)
	for _, span := range trace.exporter.Spans() {
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
	harness.useMemoryContinuations(t, 1)
	trace := newTestTrace()
	if _, err := harness.engine.Generate(trace.ctx, baseRequest("continuation-span")); err != nil {
		t.Fatal(err)
	}
	trace.shutdown(t)
	for _, span := range trace.exporter.Spans() {
		if span.Name() == "llmtw.continuation_write" {
			return
		}
	}
	t.Fatal("continuation write did not emit a trace span")
}

func TestStreamUsesAdmissionTraceContextForProviderAttempt(t *testing.T) {
	harness := newHarness(t, newStreamingAdapter("telemetry-stream", okStreamEvents()...))
	trace := newTestTrace()
	stream, err := harness.engine.Stream(trace.ctx, baseRequest("stream-trace-parent"))
	if err != nil {
		t.Fatal(err)
	}
	readTerminalStream(t, stream)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	trace.shutdown(t)
	var admissionID, providerParentID string
	for _, span := range trace.exporter.Spans() {
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
