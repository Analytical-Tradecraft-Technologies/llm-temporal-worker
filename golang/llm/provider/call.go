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
}

type Result struct {
	Response llm.Response
}
