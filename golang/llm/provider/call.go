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
}

type Result struct {
	Response llm.Response
}
