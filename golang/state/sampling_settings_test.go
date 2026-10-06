package state

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestApplySettingsPatchV1InheritsReplacesAndClearsSamplingLeaves(t *testing.T) {
	topP, stop, seed, mode, budget := llm.DecimalV1("0.900"), []string{"END"}, int64(7), llm.ReasoningModeEnabled, 2048
	parent, err := ApplySettingsPatchV1(RootModelState("model"), llm.SettingsPatchV1{
		TopP: llm.Patch[llm.DecimalV1]{Set: &topP}, StopSequences: llm.Patch[[]string]{Set: &stop}, Seed: llm.Patch[int64]{Set: &seed},
		ReasoningMode: llm.Patch[llm.ReasoningMode]{Set: &mode}, ReasoningTokenBudget: llm.Patch[int]{Set: &budget}})
	if err != nil {
		t.Fatal(err)
	}
	if parent.TopP.String() != "0.9" || !reflect.DeepEqual(parent.StopSequences, stop) || *parent.Seed != 7 || parent.ReasoningMode != mode || *parent.ReasoningTokenBudget != 2048 {
		t.Fatalf("parent = %+v", parent)
	}
	stop[0] = "changed"
	if parent.StopSequences[0] != "END" || topP != "0.900" {
		t.Fatal("state aliases or rewrote the caller's patch")
	}

	// An empty child patch inherits every leaf unchanged.
	inherited, err := ApplySettingsPatchV1(parent, llm.SettingsPatchV1{})
	if err != nil || !reflect.DeepEqual(inherited, parent) {
		t.Fatalf("inherited = %+v, %v; want %+v", inherited, err, parent)
	}
	inherited.StopSequences[0] = "mutated"
	if parent.StopSequences[0] != "END" {
		t.Fatal("child state aliases the parent's stop sequences")
	}

	// Each leaf replaces or clears independently of the others.
	replacement := []string{"STOP", "HALT"}
	child, err := ApplySettingsPatchV1(parent, llm.SettingsPatchV1{StopSequences: llm.Patch[[]string]{Set: &replacement}, Seed: llm.Patch[int64]{Clear: true},
		ReasoningTokenBudget: llm.Patch[int]{Clear: true}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(child.StopSequences, replacement) || child.Seed != nil || child.ReasoningTokenBudget != nil || child.TopP.String() != "0.9" || child.ReasoningMode != mode {
		t.Fatalf("child = %+v", child)
	}
	cleared, err := ApplySettingsPatchV1(child, llm.SettingsPatchV1{TopP: llm.Patch[llm.DecimalV1]{Clear: true}, StopSequences: llm.Patch[[]string]{Clear: true},
		ReasoningMode: llm.Patch[llm.ReasoningMode]{Clear: true}})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.TopP != nil || cleared.StopSequences != nil || cleared.ReasoningMode != "" || cleared.Model != "model" {
		t.Fatalf("cleared = %+v", cleared)
	}
}

func TestApplySettingsPatchRejectsOutOfBoundsSamplingLeaves(t *testing.T) {
	for name, patch := range map[string]SettingsPatch{
		"top_p":            {TopP: SetPatch(llm.DecimalV1("1.01"))},
		"stop_sequences":   {StopSequences: SetPatch([]string{})},
		"seed":             {Seed: SetPatch(int64(-1))},
		"reasoning mode":   {ReasoningMode: SetPatch(llm.ReasoningMode(""))},
		"reasoning budget": {ReasoningTokenBudget: SetPatch(0)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ApplySettingsPatch(RootModelState("model"), patch); err == nil {
				t.Fatal("out-of-bounds leaf was applied")
			}
			if _, err := (CheckpointBlobCodec{}).EncodeSettingsPatch(patch); err == nil {
				t.Fatal("out-of-bounds leaf was encoded")
			}
		})
	}
	state := RootModelState("model")
	state.StopSequences = []string{"a", "a"}
	if err := state.Validate(); err == nil {
		t.Fatal("materialized state with duplicate stop sequences validated")
	}
}

func TestCheckpointBlobCodecRoundTripsSamplingLeaves(t *testing.T) {
	codec := CheckpointBlobCodec{}
	patch := SettingsPatch{TopP: SetPatch(llm.DecimalV1("0.123456789012345678")), StopSequences: SetPatch([]string{"END"}), Seed: SetPatch(int64(llm.MaxSeedV1)),
		ReasoningMode: SetPatch(llm.ReasoningModeAdaptive), ReasoningTokenBudget: ClearPatch[int]()}
	encoded, err := codec.EncodeSettingsPatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`"top_p":{"set":"0.123456789012345678"}`, `"stop_sequences":{"set":["END"]}`, `"seed":{"set":9007199254740991}`,
		`"reasoning_mode":{"set":"adaptive"}`, `"reasoning_token_budget":{"clear":true}`} {
		if !strings.Contains(string(encoded), fragment) {
			t.Fatalf("encoded patch is missing %s: %s", fragment, encoded)
		}
	}
	decoded, err := codec.DecodeSettingsPatch(encoded)
	if err != nil || !reflect.DeepEqual(decoded, patch) {
		t.Fatalf("decoded = %+v, %v; want %+v", decoded, err, patch)
	}

	settings := RootModelState("model")
	settings.TopP = &[]llm.DecimalV1{"0.5"}[0]
	settings.StopSequences = []string{"END", "STOP"}
	settings.Seed = &[]int64{0}[0]
	settings.ReasoningMode = llm.ReasoningModeDisabled
	settings.ReasoningTokenBudget = &[]int{1024}[0]
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	snapshot := CheckpointSnapshot{Items: items, Settings: settings, Depth: 1, Lineage: []Handle{"root"}}
	snapshot.Digest = snapshot.digest()
	encodedSnapshot, err := codec.EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	gotSnapshot, err := codec.DecodeSnapshot(encodedSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSnapshot.Settings, settings) || gotSnapshot.Digest != snapshot.Digest {
		t.Fatalf("snapshot settings = %+v; want %+v", gotSnapshot.Settings, settings)
	}
}

// legacyModelState is ModelState as it was before the sampling and reasoning
// leaves existed. Checkpoints written then must keep their digests and their
// encoded snapshot settings.
type legacyModelState struct {
	WebSearch             bool
	WebFetch              bool
	CodeExecution         bool
	Model                 string
	ServiceClass          llm.ServiceClass
	ServiceClassFallbacks []llm.ServiceClass
	Portability           llm.PortabilityMode
	Instructions          []llm.Instruction
	Tools                 []llm.Tool
	ToolPolicy            llm.ToolPolicy
	Output                *llm.OutputSpec
	Temperature           *float64
	TemperatureDecimal    *llm.DecimalV1
	ReasoningEffort       llm.ReasoningEffort
	ReasoningSummary      llm.ReasoningSummary
	CompactionPolicy      json.RawMessage
	Extensions            map[string]json.RawMessage
}

func TestSnapshotDigestAndEncodingUnchangedWithoutSamplingLeaves(t *testing.T) {
	temperature := 0.2
	decimal := llm.DecimalV1("0.2")
	settings := RootModelState("model")
	settings.Temperature, settings.TemperatureDecimal = &temperature, &decimal
	settings.ReasoningEffort = llm.ReasoningEffortHigh
	items := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	snapshot := CheckpointSnapshot{Items: items, Settings: settings, Depth: 1, Lineage: []Handle{"root"}}

	legacy := legacyModelState{Model: settings.Model, ServiceClass: settings.ServiceClass, Portability: settings.Portability,
		Temperature: settings.Temperature, TemperatureDecimal: settings.TemperatureDecimal, ReasoningEffort: settings.ReasoningEffort}
	data, err := json.Marshal(struct {
		Schema   string
		Items    []llm.Item
		Settings legacyModelState
		Depth    int32
		Lineage  []Handle
	}{checkpointSchemaVersion, items, legacy, snapshot.Depth, snapshot.Lineage})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.digest() != sha256.Sum256(data) {
		t.Fatal("snapshot digest changed for a state that does not use the new leaves")
	}

	snapshot.Digest = snapshot.digest()
	encoded, err := (CheckpointBlobCodec{}).EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"top_p", "stop_sequences", "seed", "reasoning_mode", "reasoning_token_budget"} {
		if strings.Contains(string(encoded), `"`+name+`"`) {
			t.Fatalf("snapshot without %s encodes it: %s", name, encoded)
		}
	}
}
