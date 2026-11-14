package openairesponses

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/streamtest"
)

func TestDecodeStreamIsFragmentationInvariant(t *testing.T) {
	wire := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_item.added\ndata: {\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\nevent: response.output_text.delta\ndata: {\"output_index\":0,\"delta\":\"hello\"}\n\nevent: response.output_item.done\ndata: {\"output_index\":0}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	want, err := DecodeStream(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunks := range [][][]byte{streamtest.Fragment(wire, 1), streamtest.RandomFragment(wire, 4, 9)} {
		got, err := DecodeStream(streamtest.NewChunkReader(chunks))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fragmented events differ\n got: %#v\nwant: %#v", got, want)
		}
	}
	assembler := provider.NewAssembler("responses-stream")
	for _, event := range want {
		if err := assembler.Add(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := assembler.Result(); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeStreamEnumeratesSplitPointsEmptyReadsAndCRLF(t *testing.T) {
	wire := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_item.added\ndata: {\"output_index\":0,\"item\":{\"type\":\"message\"}}\n\nevent: response.output_text.delta\ndata: {\"output_index\":0,\"delta\":\"hello\"}\n\nevent: response.output_item.done\ndata: {\"output_index\":0}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	streamtest.SplitInvariant(t, wire, DecodeStream)
}

func TestDecodeStreamRejectsUnknownEvent(t *testing.T) {
	if _, err := DecodeStream(strings.NewReader("event: response.unknown\ndata: {}\n\n")); err == nil {
		t.Fatal("unknown event unexpectedly succeeded")
	}
}

func TestDecodeStreamRejectsTerminalWithUnfinishedOutput(t *testing.T) {
	wire := "event: response.output_item.added\ndata: {}\n\nevent: response.completed\ndata: {}\n\n"
	if _, err := DecodeStream(strings.NewReader(wire)); err == nil {
		t.Fatal("terminal stream with unfinished output unexpectedly succeeded")
	}
}
