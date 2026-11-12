package activity

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// rootWorkflowContext stands in for a workflow context with no values.
type rootWorkflowContext struct{ workflow.Context }

func (rootWorkflowContext) Value(any) any { return nil }

// A run that pinned its start-time limit encodes and decodes its payloads
// under that limit, whatever the worker's configured limit is now; without a
// pin, and outside workflow code, the configured limit applies (#1511).
func TestBoundedDataConverterAppliesPinnedWorkflowLimit(t *testing.T) {
	current := PayloadLimits{MaxInlineBytes: 1 << 10}
	started := PayloadLimits{MaxInlineBytes: 8 << 10}
	value := strings.Repeat("x", 4<<10)
	wrapped := 0
	dataConverter := WrappedBoundedDataConverter(current, func(bounded converter.DataConverter) converter.DataConverter {
		wrapped++
		return converter.NewCodecDataConverter(bounded, converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}))
	})
	aware, ok := dataConverter.(converter.ContextAware)
	if !ok {
		t.Fatal("bounded converter is not context aware")
	}
	unpinned := workflow.Context(rootWorkflowContext{})
	pinned := WithPinnedPayloadLimits(unpinned, started)

	if got := WorkflowPayloadLimits(pinned, current); got.InlineBytes() != started.InlineBytes() {
		t.Fatalf("pinned limit = %d, want %d", got.InlineBytes(), started.InlineBytes())
	}
	if got := WorkflowPayloadLimits(unpinned, current); got != current {
		t.Fatalf("unpinned limit = %+v, want %+v", got, current)
	}
	for name, refuses := range map[string]converter.DataConverter{
		"configured":       dataConverter,
		"unpinned":         aware.WithWorkflowContext(unpinned),
		"activity context": aware.WithContext(context.Background()),
	} {
		if _, err := refuses.ToPayload(value); err == nil || !strings.Contains(err.Error(), "limit is 1024") {
			t.Errorf("%s: ToPayload = %v, want the configured limit", name, err)
		}
	}

	before := wrapped
	accepts := aware.WithWorkflowContext(pinned)
	if wrapped != before+1 {
		t.Fatal("pinned converter did not rebuild the wrapped stack")
	}
	payload, err := accepts.ToPayload(value)
	if err != nil {
		t.Fatalf("pinned ToPayload: %v", err)
	}
	if encoding := string(payload.GetMetadata()[converter.MetadataEncoding]); encoding != "binary/zlib" {
		t.Fatalf("pinned payload encoding = %q, want the wrapped codec", encoding)
	}
	var decoded string
	if err := accepts.FromPayload(payload, &decoded); err != nil || decoded != value {
		t.Fatalf("pinned FromPayload = %v", err)
	}
	if err := dataConverter.FromPayload(payload, &decoded); err == nil {
		t.Fatal("configured converter decoded a payload above its limit")
	}
	if tooLarge, err := accepts.ToPayload(strings.Repeat("x", 9<<10)); err == nil {
		t.Fatalf("pinned ToPayload accepted %d bytes above the pinned limit", len(tooLarge.GetData()))
	}
}
