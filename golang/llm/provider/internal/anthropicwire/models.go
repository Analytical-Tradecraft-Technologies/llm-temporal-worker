package anthropicwire

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// SamplingRule is how a Claude model treats temperature, top_p and top_k on
// API version 2023-06-01, which both Messages adapters send.
type SamplingRule int

const (
	// SamplingUnrestricted is a model with no documented sampling rule, or
	// one the table does not know. Every field is sent as given.
	SamplingUnrestricted SamplingRule = iota
	// SamplingExclusive accepts any one of temperature and top_p but returns
	// a 400 when a request sets both (Claude Opus 4.1 to 4.6, Sonnet 4.5 and
	// 4.6, Haiku 4.5).
	SamplingExclusive
	// SamplingDefaultsOnly accepts a sampling field only at its default
	// value; any other value, and temperature with top_p, returns a 400.
	SamplingDefaultsOnly
	// SamplingRejected returns a 400 for any sampling field at any value.
	SamplingRejected
)

// ModelRules records the per-model request rules that every route to a Claude
// model shares, so the Anthropic and Bedrock Messages adapters reject a
// request the model would refuse before it is admitted and dispatched.
// The rules come from each model's migration guide:
// https://platform.claude.com/docs/en/about-claude/models/migration-guide
type ModelRules struct {
	// Name is the model's display name for error messages.
	Name string
	// Sampling is the model's temperature, top_p and top_k rule.
	Sampling SamplingRule
	// DefaultTopP is the top_p a SamplingDefaultsOnly model accepts, or nil
	// when its default is not published and any top_p is rejected.
	DefaultTopP *float64
	// ExtraHighEffort reports whether output_config.effort accepts xhigh.
	ExtraHighEffort bool
}

var haiku55DefaultTopP = 0.99

// modelRules is keyed by the canonical first-party model ID; see
// canonicalModel for the date, version and Bedrock prefix forms it accepts.
var modelRules = map[string]ModelRules{
	"claude-opus-4-1":   {Name: "Claude Opus 4.1", Sampling: SamplingExclusive},
	"claude-opus-4-5":   {Name: "Claude Opus 4.5", Sampling: SamplingExclusive},
	"claude-opus-4-6":   {Name: "Claude Opus 4.6", Sampling: SamplingExclusive},
	"claude-sonnet-4-5": {Name: "Claude Sonnet 4.5", Sampling: SamplingExclusive},
	"claude-sonnet-4-6": {Name: "Claude Sonnet 4.6", Sampling: SamplingExclusive},
	"claude-haiku-4-5":  {Name: "Claude Haiku 4.5", Sampling: SamplingExclusive},
	"claude-opus-4-7":   {Name: "Claude Opus 4.7", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-opus-4-8":   {Name: "Claude Opus 4.8", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-opus-5":     {Name: "Claude Opus 5", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-opus-5-5":   {Name: "Claude Opus 5.5", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-sonnet-5":   {Name: "Claude Sonnet 5", Sampling: SamplingDefaultsOnly, ExtraHighEffort: true},
	"claude-sonnet-5-5": {Name: "Claude Sonnet 5.5", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-haiku-5-5":  {Name: "Claude Haiku 5.5", Sampling: SamplingDefaultsOnly, DefaultTopP: &haiku55DefaultTopP, ExtraHighEffort: true},
	"claude-fable-5":    {Name: "Claude Fable 5", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-fable-5-1":  {Name: "Claude Fable 5.1", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-fable-5-2":  {Name: "Claude Fable 5.2", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-mythos-5":   {Name: "Claude Mythos 5", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-mythos-5-1": {Name: "Claude Mythos 5.1", Sampling: SamplingRejected, ExtraHighEffort: true},
	"claude-mythos-5-2": {Name: "Claude Mythos 5.2", Sampling: SamplingRejected, ExtraHighEffort: true},
}

// LookupModel returns the rules for model and whether the table knows it.
// An unknown model gets the zero rules: sampling is sent as given and xhigh
// effort is rejected.
func LookupModel(model string) (ModelRules, bool) {
	rules, ok := modelRules[canonicalModel(model)]
	return rules, ok
}

// canonicalModel reduces a provider model ID to its first-party base ID:
// "us.anthropic.claude-sonnet-4-5-20250929-v1:0" (Bedrock), "anthropic.claude-opus-5-5",
// "claude-opus-4-1-20250805" and "claude-opus-4-5@20251101" (Vertex) all
// reduce to the ID without the region prefix, date and version.
func canonicalModel(model string) string {
	id := strings.ToLower(strings.TrimSpace(model))
	if index := strings.LastIndex(id, "anthropic."); index >= 0 {
		id = id[index+len("anthropic."):]
	}
	if index := strings.IndexAny(id, "@:"); index >= 0 {
		id = id[:index]
	}
	if index := strings.LastIndex(id, "-v"); index >= 0 && allDigits(id[index+2:]) {
		id = id[:index]
	}
	if index := strings.LastIndex(id, "-"); index >= 0 && len(id)-index-1 == 8 && allDigits(id[index+1:]) {
		id = id[:index]
	}
	return id
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ValidateModelSampling rejects the sampling fields in the final wire
// document that model refuses, as a non-retryable unsupported capability
// before dispatch, so the planner can choose another route instead of the
// provider returning a 400 after admission. It runs on the wire rather than
// the request so it sees exactly what would be sent.
func ValidateModelSampling(model string, wire map[string]any) error {
	rules, ok := LookupModel(model)
	if !ok || rules.Sampling == SamplingUnrestricted {
		return nil
	}
	unsupported := func(format string, args ...any) error {
		return provider.NewUnsupportedCapabilityError(rules.Name + " " + fmt.Sprintf(format, args...))
	}
	temperature, hasTemperature := wire["temperature"]
	topP, hasTopP := wire["top_p"]
	_, hasTopK := wire["top_k"]
	if rules.Sampling == SamplingRejected {
		for _, field := range []string{"temperature", "top_p", "top_k"} {
			if _, exists := wire[field]; exists {
				return unsupported("does not support %s; omit the sampling parameters", field)
			}
		}
		return nil
	}
	if hasTemperature && hasTopP {
		return unsupported("does not support combining temperature and top_p")
	}
	if rules.Sampling == SamplingExclusive {
		return nil
	}
	if hasTemperature {
		if value, ok := number(temperature); !ok || value != 1 {
			return unsupported("only supports temperature=1")
		}
	}
	if hasTopP {
		if rules.DefaultTopP == nil {
			return unsupported("does not support top_p")
		}
		if value, ok := number(topP); !ok || value != *rules.DefaultTopP {
			return unsupported("only supports top_p=%v", *rules.DefaultTopP)
		}
	}
	if hasTopK {
		return unsupported("does not support top_k")
	}
	return nil
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}
