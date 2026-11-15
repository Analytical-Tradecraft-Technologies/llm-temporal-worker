package state

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// gptPinning is a candidate lineage on another provider than the anthropic
// constraints the pinning tests start from.
var gptPinning = Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"}

// assertPinningDecision checks CheckPinning's decision for candidate and
// returns the result for further checks.
func assertPinningDecision(t *testing.T, constraints Constraints, candidate Pinning, want Compatibility, label string) CompatibilityResult {
	t.Helper()
	got := CheckPinning(constraints, candidate)
	if got.Decision != want {
		t.Fatalf("%s = %#v, want %s", label, got, want)
	}
	return got
}

func TestCheckPinning(t *testing.T) {
	assertPinningDecision(t, Constraints{}, Pinning{}, CompatibilityCompatible, "absent continuation")
	base := Constraints{Present: true, Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude", TranscriptComplete: true, Portability: llm.PortabilityBestEffort}
	assertPinningDecision(t, base, Pinning{Provider: "anthropic", EndpointID: "prod", AccountRegion: "us-east-1", Family: "messages", ModelLineage: "claude"}, CompatibilityCompatible, "same lineage")
	assertPinningDecision(t, base, gptPinning, CompatibilityPortable, "portable transcript")
	base.RequiresOpaqueState = true
	assertPinningDecision(t, base, gptPinning, CompatibilityRejected, "opaque mismatch")
	base.RequiresOpaqueState = false
	base.TranscriptComplete = false
	base.Portability = llm.PortabilityStrict
	assertPinningDecision(t, base, gptPinning, CompatibilityRejected, "incomplete strict transcript")
}

func TestCheckPinningDropsOnlyOptionalStateForBestEffortPortableTranscript(t *testing.T) {
	constraints := Constraints{
		Present:            true,
		Provider:           "anthropic",
		EndpointID:         "prod",
		Family:             "messages",
		ModelLineage:       "claude",
		TranscriptComplete: true,
		Portability:        llm.PortabilityBestEffort,
	}
	result := assertPinningDecision(t, constraints, gptPinning, CompatibilityPortable, "optional provider state portability")
	if len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "provider_state_dropped" {
		t.Fatalf("optional provider state portability = %#v", result)
	}

	constraints.RequiresOpaqueState = true
	assertPinningDecision(t, constraints, gptPinning, CompatibilityRejected, "required provider state portability")
}

func TestCheckPinningRejectsRequiredStateWithIncompleteConstraint(t *testing.T) {
	constraints := Constraints{Present: true, RequiresOpaqueState: true, TranscriptComplete: true, Portability: llm.PortabilityBestEffort}
	assertPinningDecision(t, constraints, gptPinning, CompatibilityRejected, "incomplete required constraint")
}

func TestCheckPinningRequiresExactAccountScopeForRequiredState(t *testing.T) {
	constraints := Constraints{Present: true, Provider: "openai", EndpointID: "prod", AccountRegion: "us-east-1", Family: "responses", ModelLineage: "gpt", RequiresOpaqueState: true}
	assertPinningDecision(t, constraints, gptPinning, CompatibilityRejected, "missing candidate account scope")
	scoped := gptPinning
	scoped.AccountRegion = "us-east-1"
	assertPinningDecision(t, constraints, scoped, CompatibilityCompatible, "matching candidate account scope")
}
