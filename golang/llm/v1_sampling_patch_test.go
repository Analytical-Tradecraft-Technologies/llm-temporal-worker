package llm

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSettingsPatchV1SamplingAndReasoningLeavesRoundTrip(t *testing.T) {
	topP, stop, seed, mode, budget := DecimalV1("0.50"), []string{"END", "\n\n"}, int64(42), ReasoningModeEnabled, 2048
	set := SettingsPatchV1{TopP: Patch[DecimalV1]{Set: &topP}, StopSequences: Patch[[]string]{Set: &stop}, Seed: Patch[int64]{Set: &seed},
		ReasoningMode: Patch[ReasoningMode]{Set: &mode}, ReasoningTokenBudget: Patch[int]{Set: &budget}}
	encoded, err := set.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"reasoning_mode":{"set":"enabled"},"reasoning_token_budget":{"set":2048},"seed":{"set":42},"stop_sequences":{"set":["END","\n\n"]},"top_p":{"set":"0.5"}}`
	if string(encoded) != want {
		t.Fatalf("encoded = %s; want %s", encoded, want)
	}
	var decoded SettingsPatchV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TopP.Set == nil || *decoded.TopP.Set != "0.5" || !reflect.DeepEqual(*decoded.StopSequences.Set, stop) || *decoded.Seed.Set != seed ||
		*decoded.ReasoningMode.Set != mode || *decoded.ReasoningTokenBudget.Set != budget {
		t.Fatalf("decoded = %+v", decoded)
	}

	clear := SettingsPatchV1{TopP: Patch[DecimalV1]{Clear: true}, StopSequences: Patch[[]string]{Clear: true}, Seed: Patch[int64]{Clear: true},
		ReasoningMode: Patch[ReasoningMode]{Clear: true}, ReasoningTokenBudget: Patch[int]{Clear: true}}
	encoded, err = clear.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip SettingsPatchV1
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || !reflect.DeepEqual(roundTrip, clear) {
		t.Fatalf("clear round trip = %+v, %v; want %+v", roundTrip, err, clear)
	}

	// Omitted leaves are never written, so existing patches encode unchanged.
	temperature := DecimalV1("0.2")
	encoded, err = SettingsPatchV1{Temperature: Patch[DecimalV1]{Set: &temperature}}.MarshalJSON()
	if err != nil || string(encoded) != `{"temperature":{"set":"0.2"}}` {
		t.Fatalf("legacy patch encoded = %s, %v", encoded, err)
	}
}

func TestSettingsPatchV1MarshalRejectsOutOfBoundsLeaves(t *testing.T) {
	zero, above, bad := DecimalV1("0"), DecimalV1("1.5"), DecimalV1("0.x")
	empty, blank, duplicate := []string{}, []string{""}, []string{"a", "a"}
	tooMany := make([]string, MaxStopSequencesV1+1)
	for index := range tooMany {
		tooMany[index] = strings.Repeat("s", index+1)
	}
	tooLong := []string{strings.Repeat("s", MaxStopSequenceLengthV1+1)}
	negativeSeed, largeSeed := int64(-1), int64(MaxSeedV1+1)
	badMode, zeroBudget := ReasoningMode("on"), 0
	topP := DecimalV1("0.5")
	for name, patch := range map[string]SettingsPatchV1{
		"top_p zero":           {TopP: Patch[DecimalV1]{Set: &zero}},
		"top_p above one":      {TopP: Patch[DecimalV1]{Set: &above}},
		"top_p malformed":      {TopP: Patch[DecimalV1]{Set: &bad}},
		"top_p set and clear":  {TopP: Patch[DecimalV1]{Set: &topP, Clear: true}},
		"stop empty":           {StopSequences: Patch[[]string]{Set: &empty}},
		"stop blank":           {StopSequences: Patch[[]string]{Set: &blank}},
		"stop duplicate":       {StopSequences: Patch[[]string]{Set: &duplicate}},
		"stop too many":        {StopSequences: Patch[[]string]{Set: &tooMany}},
		"stop too long":        {StopSequences: Patch[[]string]{Set: &tooLong}},
		"seed negative":        {Seed: Patch[int64]{Set: &negativeSeed}},
		"seed above maximum":   {Seed: Patch[int64]{Set: &largeSeed}},
		"reasoning mode":       {ReasoningMode: Patch[ReasoningMode]{Set: &badMode}},
		"reasoning budget":     {ReasoningTokenBudget: Patch[int]{Set: &zeroBudget}},
		"invalid stop with ok": {TopP: Patch[DecimalV1]{Set: &topP}, StopSequences: Patch[[]string]{Set: &blank}},
	} {
		t.Run(name, func(t *testing.T) {
			if encoded, err := patch.MarshalJSON(); err == nil {
				t.Fatalf("MarshalJSON accepted %s", encoded)
			}
		})
	}
}
