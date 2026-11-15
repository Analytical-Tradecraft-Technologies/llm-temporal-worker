package provider

import (
	"encoding/json"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

type CompileInput struct {
	Request    llm.Request
	Query      CapabilityQuery
	Capability CapabilitySet
	Strict     bool
	Metadata   CallMetadata
}

type Call struct {
	EndpointID   string
	Family       Family
	Model        string
	OperationKey string
	ServiceClass llm.ServiceClass
	SDKParams    any
	// OutputSchema is the caller's json_schema output schema when the adapter
	// sent the provider a lowered form of it. The lift validates the final
	// JSON against this original, not against the wire schema.
	OutputSchema json.RawMessage
	Metadata     CallMetadata
}

type CallMetadata struct {
	WebSearch           bool
	WebFetch            bool
	CodeExecution       bool
	SchemaDigest        [32]byte
	EstimatedBytes      int
	CapabilityVersion   string
	ProviderTier        string
	OpaqueStateRequired bool
	// SequentialToolUseDropped records a best-effort compilation that sent
	// the provider no restriction for a ToolPolicy with Parallel false. The
	// lift reports it as a response diagnostic.
	SequentialToolUseDropped bool
	// ExtensionsDropped records a best-effort compilation that sent the
	// provider none of the request's extensions because the adapter has no
	// extension allowlist. The lift reports it as a response diagnostic.
	ExtensionsDropped bool
	// PriceCeiling, when set, holds the unit prices the call's budget
	// reservation was quoted at. An adapter that can bound the upstream
	// price sends it, so the provider never serves the call above them.
	PriceCeiling *PriceCeiling
}

// PriceCeiling is the most a call may cost per unit, as exact USD decimals:
// per million prompt tokens, per million completion tokens and per request.
type PriceCeiling struct {
	PromptPerMillion     string
	CompletionPerMillion string
	PerRequest           string
}

type Result struct {
	Response llm.Response
}

// RouteFacts reports the call's endpoint and requested model, served through
// family by resolvedModel.
func (call Call) RouteFacts(family Family, resolvedModel string) llm.RouteFacts {
	return llm.RouteFacts{EndpointID: call.EndpointID, APIFamily: string(family), RequestedModel: call.Model, ResolvedModel: resolvedModel}
}

// ServiceFacts reports the call's service class served without fallback as
// actual, from the provider's providerValue.
func (call Call) ServiceFacts(actual *llm.ServiceClass, providerValue string) llm.ServiceFacts {
	return llm.ServiceFacts{Requested: call.ServiceClass, Attempted: call.ServiceClass, Actual: actual, ProviderValue: providerValue}
}
