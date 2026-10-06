package state

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCheckPinning(t *testing.T) {
	if got := CheckPinning(Constraints{}, Pinning{}); got.Decision != CompatibilityCompatible {
		t.Fatalf("absent continuation = %#v", got)
	}
	base := Constraints{Present: true, Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude", TranscriptComplete: true, Portability: llm.PortabilityBestEffort}
	if got := CheckPinning(base, Pinning{Provider: "anthropic", EndpointID: "prod", AccountRegion: "us-east-1", Family: "messages", ModelLineage: "claude"}); got.Decision != CompatibilityCompatible {
		t.Fatalf("same lineage = %#v", got)
	}
	if got := CheckPinning(base, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"}); got.Decision != CompatibilityPortable {
		t.Fatalf("portable transcript = %#v", got)
	}
	base.RequiresOpaqueState = true
	if got := CheckPinning(base, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"}); got.Decision != CompatibilityRejected {
		t.Fatalf("opaque mismatch = %#v", got)
	}
	base.RequiresOpaqueState = false
	base.TranscriptComplete = false
	base.Portability = llm.PortabilityStrict
	if got := CheckPinning(base, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"}); got.Decision != CompatibilityRejected {
		t.Fatalf("incomplete strict transcript = %#v", got)
	}
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
	result := CheckPinning(constraints, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"})
	if result.Decision != CompatibilityPortable || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != "provider_state_dropped" {
		t.Fatalf("optional provider state portability = %#v", result)
	}

	constraints.RequiresOpaqueState = true
	result = CheckPinning(constraints, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"})
	if result.Decision != CompatibilityRejected {
		t.Fatalf("required provider state portability = %#v, want rejected", result)
	}
}

func TestCheckPinningRejectsRequiredStateWithIncompleteConstraint(t *testing.T) {
	constraints := Constraints{Present: true, RequiresOpaqueState: true, TranscriptComplete: true, Portability: llm.PortabilityBestEffort}
	result := CheckPinning(constraints, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"})
	if result.Decision != CompatibilityRejected {
		t.Fatalf("incomplete required constraint = %#v, want rejected", result)
	}
}

func TestCheckPinningRequiresExactAccountScopeForRequiredState(t *testing.T) {
	constraints := Constraints{Present: true, Provider: "openai", EndpointID: "prod", AccountRegion: "us-east-1", Family: "responses", ModelLineage: "gpt", RequiresOpaqueState: true}
	result := CheckPinning(constraints, Pinning{Provider: "openai", EndpointID: "prod", Family: "responses", ModelLineage: "gpt"})
	if result.Decision != CompatibilityRejected {
		t.Fatalf("missing candidate account scope = %#v, want rejected", result)
	}
	result = CheckPinning(constraints, Pinning{Provider: "openai", EndpointID: "prod", AccountRegion: "us-east-1", Family: "responses", ModelLineage: "gpt"})
	if result.Decision != CompatibilityCompatible {
		t.Fatalf("matching candidate account scope = %#v, want compatible", result)
	}
}
