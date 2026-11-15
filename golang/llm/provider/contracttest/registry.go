package contracttest

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
)

// ArtifactKind identifies the representation required for a contract case.
type ArtifactKind string

const (
	ArtifactSemantic ArtifactKind = "semantic"
	ArtifactWire     ArtifactKind = "wire"
	ArtifactEvents   ArtifactKind = "events"
)

// CaseRequirement is one entry in the code-owned adapter contract matrix.
// Capability is empty for a universal requirement. A capability-scoped case is
// required only when its metadata fact says the feature is supported.
type CaseRequirement struct {
	ID         string
	Capability string
	Artifacts  []ArtifactKind
}

var caseRegistry = []CaseRequirement{
	{ID: "semantic-request", Artifacts: []ArtifactKind{ArtifactSemantic}},
	{ID: "captured-wire-request", Artifacts: []ArtifactKind{ArtifactWire}},
	{ID: "response", Artifacts: []ArtifactKind{ArtifactSemantic, ArtifactWire}},
	{ID: "classified-error", Artifacts: []ArtifactKind{ArtifactWire}},
	{ID: "security-redaction", Artifacts: []ArtifactKind{ArtifactWire}},
	{ID: "usage-cost", Capability: "usage", Artifacts: []ArtifactKind{ArtifactSemantic, ArtifactWire}},
	// Decoder fixtures prove captured event reconstruction. They do not claim a
	// public adapter streaming invocation, which remains governed separately by
	// the streaming capability fact.
	{ID: "full-stream", Capability: "stream_decoder", Artifacts: []ArtifactKind{ArtifactEvents}},
	{ID: "fragmented-stream", Capability: "stream_decoder", Artifacts: []ArtifactKind{ArtifactEvents}},
	{ID: "strict-loss", Capability: "strict_loss", Artifacts: []ArtifactKind{ArtifactSemantic, ArtifactWire}},
	{ID: "best-effort-diagnostic", Capability: "best_effort", Artifacts: []ArtifactKind{ArtifactSemantic}},
	{ID: "class-facts", Capability: "service_class", Artifacts: []ArtifactKind{ArtifactSemantic, ArtifactWire}},
	{ID: "continuation-compatibility", Capability: "continuation", Artifacts: []ArtifactKind{ArtifactSemantic, ArtifactWire}},
}

// CaseRequirements returns the code-owned fixture matrix. A copy is returned
// so tests and tooling cannot mutate the registry that governs validation.
func CaseRequirements() []CaseRequirement {
	result := make([]CaseRequirement, 0, len(caseRegistry))
	for _, requirement := range caseRegistry {
		copy := requirement
		copy.Artifacts = append([]ArtifactKind(nil), requirement.Artifacts...)
		result = append(result, copy)
	}
	return result
}

// GovernedCapabilityFacts returns every metadata capability fact that an
// enforced profile must declare, including facts that do not have a fixture
// artifact of their own.
func GovernedCapabilityFacts() []string {
	result := append([]string(nil), governedCapabilityFacts...)
	sort.Strings(result)
	return result
}

// governedCapabilityFacts remain mandatory metadata for every enforced
// profile, including facts such as public streaming that do not themselves
// require an offline fixture artifact.
var governedCapabilityFacts = []string{
	"usage",
	"streaming",
	"stream_decoder",
	"strict_loss",
	"best_effort",
	"service_class",
	"continuation",
}

// RequiredCases returns the complete matrix applicable to the profile's
// declared capabilities. The returned slice is independent of the registry.
func RequiredCases(capabilityFacts map[string]string) []CaseRequirement {
	required := make([]CaseRequirement, 0, len(caseRegistry))
	for _, requirement := range caseRegistry {
		if requirement.Capability != "" && !capabilityIsSupported(capabilityFacts[requirement.Capability]) {
			continue
		}
		copy := requirement
		copy.Artifacts = append([]ArtifactKind(nil), requirement.Artifacts...)
		required = append(required, copy)
	}
	return required
}

func registeredCase(id string) (CaseRequirement, bool) {
	for _, requirement := range caseRegistry {
		if requirement.ID == id {
			return requirement, true
		}
	}
	return CaseRequirement{}, false
}

func capabilityIsSupported(fact string) bool {
	switch strings.ToLower(strings.TrimSpace(fact)) {
	case "native", "emulated", "supported":
		return true
	default:
		return false
	}
}

func validateEnforcedCoverage(manifest Manifest, metadata Metadata) error {
	declared := make(map[string]Case, len(manifest.Cases))
	for _, fixtureCase := range manifest.Cases {
		declared[fixtureCase.ID] = fixtureCase
	}
	for _, requirement := range RequiredCases(metadata.CapabilityFacts) {
		fixtureCase, ok := declared[requirement.ID]
		if !ok {
			return fmt.Errorf("missing required case %q", requirement.ID)
		}
		for _, artifact := range requirement.Artifacts {
			if !fixtureCase.Artifacts.has(artifact) {
				return fmt.Errorf("required case %q is missing %s artifact", requirement.ID, artifact)
			}
		}
	}
	if err := validateEnforcedCapabilityFacts(metadata.CapabilityFacts); err != nil {
		return err
	}
	return nil
}

func validateEnforcedCapabilityFacts(capabilityFacts map[string]string) error {
	for _, capability := range registryCapabilities() {
		fact, ok := capabilityFacts[capability]
		if !ok || strings.TrimSpace(fact) == "" {
			return fmt.Errorf("metadata is missing enforced capability fact")
		}
		switch strings.ToLower(strings.TrimSpace(fact)) {
		case "native", "emulated", "supported", "unsupported", "not_applicable":
		default:
			return fmt.Errorf("metadata has an invalid enforced capability fact")
		}
	}
	return nil
}

func registryCapabilities() []string {
	capabilities := make(map[string]struct{})
	for _, capability := range governedCapabilityFacts {
		capabilities[capability] = struct{}{}
	}
	for _, requirement := range caseRegistry {
		if requirement.Capability != "" {
			capabilities[requirement.Capability] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(capabilities))
}

func (artifacts Artifacts) has(kind ArtifactKind) bool {
	switch kind {
	case ArtifactSemantic:
		return artifacts.Semantic != ""
	case ArtifactWire:
		return artifacts.Wire != ""
	case ArtifactEvents:
		return artifacts.Events != ""
	default:
		return false
	}
}
