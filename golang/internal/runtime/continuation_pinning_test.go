package runtime

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

func pinningState(endpoint string) llm.ProviderState {
	return llm.ProviderState{Provider: "openai", EndpointFamily: "responses", MediaType: "application/vnd.openai.reasoning+json", Opaque: []byte(endpoint)}
}

func pinningProvenance(ordinal int, endpoint string) state.ProviderStateProvenance {
	return state.ProviderStateProvenance{Ordinal: ordinal, Provider: "openai", EndpointID: endpoint, EndpointFamily: "openai_responses", ModelLineage: "model", Account: "account-" + endpoint}
}

// servingPin is the pin of a route on endpoint with the given account.
func servingPin(endpoint, account string) state.Pinning {
	return state.Pinning{Provider: "openai", EndpointID: endpoint, AccountRegion: account, Family: "openai_responses", ModelLineage: "model"}
}

func TestProviderStatePinsStripOnlyOtherLineages(t *testing.T) {
	thinking := llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.ProviderStatePart{Provider: "anthropic", EndpointFamily: "messages", MediaType: "thinking", Opaque: []byte("a")}, llm.TextPart{Text: "kept"}}}
	items := []llm.Item{preparationMessage("q"), pinningState("a"), thinking, pinningState("unrecorded"), pinningState("b")}
	pins, ok := newProviderStatePins(items, []state.ProviderStateProvenance{pinningProvenance(1, "a"), pinningProvenance(2, "a"), pinningProvenance(4, "b")})
	if !ok || pins.latest.EndpointID != "b" {
		t.Fatalf("pins = %+v ok=%v", pins, ok)
	}
	stripped, dropped := pins.strip(items, servingPin("b", "account-b"))
	// The unrecorded state is never stripped; the message keeps its text.
	if dropped != 2 || len(stripped) != 4 || itemHasProviderState(stripped[1]) || !reflect.DeepEqual(stripped[3], items[4]) || !reflect.DeepEqual(stripped[2], items[3]) {
		t.Fatalf("stripped %d: %+v", dropped, stripped)
	}
	if same, dropped := pins.strip(items, servingPin("a", "account-a")); dropped != 1 || len(same) != 4 {
		t.Fatalf("pinned lineage lost its own state: %d %+v", dropped, same)
	}
	if !itemHasProviderState(items[2]) {
		t.Fatal("strip mutated its input")
	}
	// The same endpoint ID on another account is another lineage.
	if _, dropped := pins.strip(items, servingPin("b", "account-other")); dropped != 3 {
		t.Fatalf("a reused endpoint ID kept another account's state: dropped %d", dropped)
	}
	if pins.admits(llm.PortabilityStrict, routing.Candidate{Provider: "openai", EndpointID: "b", Family: "openai_responses", Model: "model", EndpointAccountDigest: [32]byte{1}}) {
		t.Fatal("strict mode admitted another account behind the pinned endpoint ID")
	}
	// Provenance recorded without an account fails closed: no route, not
	// even its endpoint's, and not a route of unknown account either.
	unrecorded := pinningProvenance(4, "b")
	unrecorded.Account = ""
	unproven, ok := newProviderStatePins(items, []state.ProviderStateProvenance{unrecorded})
	if !ok {
		t.Fatal("provenance without an account was rejected as corrupt")
	}
	for _, pin := range []state.Pinning{servingPin("b", "account-b"), servingPin("b", unknownRouteAccount), candidatePinning(routing.Candidate{Provider: "openai", EndpointID: "b", Family: "openai_responses", Model: "model"})} {
		if _, dropped := unproven.strip(items, pin); dropped != 1 {
			t.Fatalf("unproven account state replayed to %+v", pin)
		}
	}
	if unproven.admits(llm.PortabilityStrict, routing.Candidate{Provider: "openai", EndpointID: "b", Family: "openai_responses", Model: "model"}) ||
		!unproven.admits(llm.PortabilityBestEffort, routing.Candidate{Provider: "openai", EndpointID: "b", Family: "openai_responses", Model: "model"}) {
		t.Fatal("unproven account is not strict-pinned and best-effort portable")
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

func TestReplayedProvenanceKeepsOnlyTheOriginOutput(t *testing.T) {
	input := []llm.Item{preparationMessage("q"), pinningState("a"), preparationMessage("next")}
	output := []llm.Item{pinningState("b"), preparationMessage("answer")}
	// A snapshot origin repeats its inherited entry (ordinal 1); only the
	// entry inside the output belongs to the replayed response.
	origin := []state.ProviderStateProvenance{pinningProvenance(1, "a"), pinningProvenance(3, "b")}
	got, ok := replayedProvenance(origin, input, output)
	if !ok || !reflect.DeepEqual(got, origin[1:]) {
		t.Fatalf("replayed provenance = %+v ok=%v", got, ok)
	}
	// An origin without provenance replays family-only.
	if got, ok := replayedProvenance(nil, input, output); !ok || got != nil {
		t.Fatalf("legacy origin = %+v ok=%v", got, ok)
	}
	for name, corrupt := range map[string][]state.ProviderStateProvenance{
		"past the output":   {pinningProvenance(5, "b")},
		"not state":         {pinningProvenance(4, "b")},
		"invalid ordinals":  {pinningProvenance(3, "b"), pinningProvenance(3, "b")},
		"incomplete record": {{Ordinal: 3, Provider: "openai"}},
	} {
		if _, ok := replayedProvenance(corrupt, input, output); ok {
			t.Fatalf("%s: corrupt origin provenance accepted", name)
		}
	}
}
