package runtime

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func pinningState(endpoint string) llm.ProviderState {
	return llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json", Opaque: []byte(endpoint)}
}

func pinningProvenance(ordinal int, endpoint string) state.ProviderStateProvenance {
	return state.ProviderStateProvenance{Ordinal: ordinal, Provider: "openai", EndpointID: endpoint, EndpointFamily: "openai_responses", ModelLineage: "model"}
}

func TestProviderStatePinsStripOnlyOtherLineages(t *testing.T) {
	thinking := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ProviderStatePart{Provider: "anthropic", EndpointFamily: "messages", MediaType: "thinking", Opaque: []byte("a")}, llm.TextPart{Text: "kept"}}}
	items := []llm.Item{preparationMessage("q"), pinningState("a"), thinking, pinningState("unrecorded"), pinningState("b")}
	pins, ok := newProviderStatePins(items, []state.ProviderStateProvenance{pinningProvenance(1, "a"), pinningProvenance(2, "a"), pinningProvenance(4, "b")})
	if !ok || pins.latest.EndpointID != "b" {
		t.Fatalf("pins = %+v ok=%v", pins, ok)
	}
	stripped, dropped := pins.strip(items, pinningProvenance(0, "b").Pinning())
	// The unrecorded state is never stripped; the message keeps its text.
	if dropped != 2 || len(stripped) != 4 || itemHasProviderState(stripped[1]) || !reflect.DeepEqual(stripped[3], items[4]) || !reflect.DeepEqual(stripped[2], items[3]) {
		t.Fatalf("stripped %d: %+v", dropped, stripped)
	}
	if same, dropped := pins.strip(items, pinningProvenance(0, "a").Pinning()); dropped != 1 || len(same) != 4 {
		t.Fatalf("pinned lineage lost its own state: %d %+v", dropped, same)
	}
	if !itemHasProviderState(items[2]) {
		t.Fatal("strip mutated its input")
	}
	for _, bad := range [][]state.ProviderStateProvenance{
		{pinningProvenance(0, "a")},                            // not provider state
		{pinningProvenance(9, "a")},                            // outside transcript
		{pinningProvenance(4, "b"), pinningProvenance(1, "a")}, // out of order
	} {
		if _, ok := newProviderStatePins(items, bad); ok {
			t.Fatalf("accepted %+v", bad)
		}
	}
}

func TestCompactedProvenanceFollowsRetainedItems(t *testing.T) {
	reference := llm.Reference{URI: "https://example.com"}
	items := []llm.Item{preparationMessage("q1"), pinningState("a"), reference, preparationMessage("a1"), preparationMessage("q2"), pinningState("a"), preparationMessage("a2")}
	parent := state.MaterializedState{Items: items, ProviderStateProvenance: []state.ProviderStateProvenance{pinningProvenance(1, "a"), pinningProvenance(5, "a")}}
	// Retain the second exchange: the first one's state is summarized away,
	// and the reference before the boundary moves to the head of the suffix.
	selection := compaction.PrefixSelection{Prefix: []llm.Item{items[0], items[1], items[3]}, Retained: []llm.Item{reference, items[4], items[5], items[6]}}
	got, err := compactedProvenance(parent, selection, true)
	if err != nil || fmt.Sprint(got) != fmt.Sprint([]state.ProviderStateProvenance{pinningProvenance(3, "a")}) {
		t.Fatalf("got %+v err=%v", got, err)
	}
	child := append([]llm.Item{preparationMessage("summary")}, selection.Retained...)
	if !itemHasProviderState(child[got[0].Ordinal]) {
		t.Fatal("remapped ordinal does not name the retained state")
	}
	// Whatever boundary SelectPrefix picks, every remapped ordinal names
	// retained provider state in the child.
	for turns := 0; turns <= 4; turns++ {
		selection, err := compaction.SelectPrefix(items, turns)
		if err != nil {
			t.Fatal(err)
		}
		got, err := compactedProvenance(parent, selection, true)
		if err != nil {
			t.Fatal(err)
		}
		child := append([]llm.Item{preparationMessage("summary")}, selection.Retained...)
		retained := 0
		for _, item := range child {
			if itemHasProviderState(item) {
				retained++
			}
		}
		if len(got) != retained {
			t.Fatalf("turns=%d: %d provenance entries for %d retained states", turns, len(got), retained)
		}
		for _, value := range got {
			if !itemHasProviderState(child[value.Ordinal]) {
				t.Fatalf("turns=%d: ordinal %d does not name provider state", turns, value.Ordinal)
			}
		}
	}
	if same, err := compactedProvenance(parent, compaction.PrefixSelection{}, false); err != nil || fmt.Sprint(same) != fmt.Sprint(parent.ProviderStateProvenance) {
		t.Fatalf("no-work compaction changed provenance: %+v %v", same, err)
	}
}
