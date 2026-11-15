package provider

import (
	"context"
	"fmt"
	"maps"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The helpers in this file are the steps every adapter's Compile shares, so a
// request is normalized, capability-checked and sized the same way whatever
// endpoint family carries it. Each returns a compile-phase *Error as error.

// NormalizeCompileRequest normalizes the request and its service class, and
// checks the model against the capability query and, when pinnedModel is set,
// the profile's pinned model.
func NormalizeCompileRequest(request llm.Request, query CapabilityQuery, pinnedModel string) (llm.Request, llm.ServiceClass, error) {
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	if err := llm.ValidateServiceClassFallbacks(serviceClass, normalized.ServiceClassFallbacks); err != nil {
		return llm.Request{}, "", NewCompileError(err.Error())
	}
	if query.Model != "" && query.Model != normalized.Model {
		return llm.Request{}, "", NewCompileError(fmt.Sprintf("model %q does not match capability query %q", normalized.Model, query.Model))
	}
	if pinnedModel != "" && pinnedModel != normalized.Model {
		return llm.Request{}, "", NewCompileError(fmt.Sprintf("model %q is not the pinned profile model %q", normalized.Model, pinnedModel))
	}
	return normalized, serviceClass, nil
}

// CompileCapabilities returns the capability set the caller supplied, or the
// adapter's own from load when the caller supplied none, with version as the
// fallback version.
func CompileCapabilities(set CapabilitySet, load func() (CapabilitySet, error), version string) (CapabilitySet, error) {
	if set.Version == "" && len(set.Features) == 0 {
		loaded, err := load()
		if err != nil {
			return CapabilitySet{}, NewCompileError(err.Error())
		}
		set = loaded
	}
	if set.Version == "" {
		set.Version = version
	}
	return set, nil
}

// RequireFeatures checks that set supports every feature natively or by
// emulation. extra, when set, adds a family-specific check that runs on each
// feature right after it resolves.
func RequireFeatures(set CapabilitySet, features []Feature, strict bool, extra func(Feature, Capability) error) error {
	for _, feature := range features {
		capability, err := set.Resolve(feature, strict)
		if err != nil {
			return NewUnsupportedFeatureError(feature, err.Error())
		}
		if capability.State != CapabilityNative && capability.State != CapabilityEmulated {
			return NewUnsupportedFeatureError(feature, fmt.Sprintf("capability %q is %s", feature, capability.State))
		}
		if extra != nil {
			if err := extra(feature, capability); err != nil {
				return err
			}
		}
	}
	return nil
}

// CompileMetadata completes the call metadata: the request digest unless the
// caller supplied one, the capability version and provider tier, whether
// opaque continuation state is required, and the canonical size estimate
// unless the caller supplied one.
func CompileMetadata(metadata CallMetadata, normalized llm.Request, capabilityVersion, providerTier string) (CallMetadata, error) {
	if metadata.SchemaDigest == ([32]byte{}) {
		digest, err := llm.RequestDigest(normalized)
		if err != nil {
			return CallMetadata{}, NewCompileError(err.Error())
		}
		metadata.SchemaDigest = digest
	}
	metadata.CapabilityVersion = capabilityVersion
	metadata.ProviderTier = providerTier
	metadata.OpaqueStateRequired = normalized.Continuation != nil
	if metadata.EstimatedBytes == 0 {
		canonical, err := CanonicalRequestBytes(normalized)
		if err != nil {
			return CallMetadata{}, NewCompileError(err.Error())
		}
		metadata.EstimatedBytes = len(canonical)
	}
	return metadata, nil
}

// NewCall completes metadata as CompileMetadata does and returns the call
// that carries params for the normalized request on endpointID, with the
// request's json_schema output schema.
func NewCall(endpointID string, family Family, normalized llm.Request, serviceClass llm.ServiceClass, params any, metadata CallMetadata, capabilityVersion, providerTier string) (Call, error) {
	metadata, err := CompileMetadata(metadata, normalized, capabilityVersion, providerTier)
	if err != nil {
		return Call{}, err
	}
	return Call{
		EndpointID:   endpointID,
		Family:       family,
		Model:        normalized.Model,
		OperationKey: normalized.OperationKey,
		ServiceClass: serviceClass,
		SDKParams:    params,
		OutputSchema: OutputSchema(normalized),
		Metadata:     metadata,
	}, nil
}

// Clone returns a copy of set whose feature map can be changed without
// touching the original.
func (set CapabilitySet) Clone() CapabilitySet {
	features := make(map[Feature]Capability, len(set.Features))
	maps.Copy(features, set.Features)
	set.Features = features
	return set
}

// StartCompile checks the caller context and that the capability query, when
// it names a family or endpoint, names this adapter's. label prefixes a query
// mismatch, as in "openai chat".
func StartCompile(ctx context.Context, query CapabilityQuery, label string, family Family, endpointID string) error {
	if err := ctx.Err(); err != nil {
		return NewCompileContextError(err)
	}
	if err := ValidateCapabilityQuery(query, label, family, endpointID); err != nil {
		return NewCompileError(err.Error())
	}
	return nil
}

// RecordHostedTools copies the hosted tools request enables into the call
// metadata.
func (metadata *CallMetadata) RecordHostedTools(request llm.Request) {
	metadata.WebSearch = request.WebSearch
	metadata.WebFetch = request.WebFetch
	metadata.CodeExecution = request.CodeExecution
}

// HostedToolFlag names the first provider-hosted tool the request asks for,
// or returns "" when it asks for none.
func HostedToolFlag(request llm.Request) string {
	switch {
	case request.WebSearch:
		return "web_search"
	case request.WebFetch:
		return "web_fetch"
	case request.CodeExecution:
		return "code_execution"
	}
	return ""
}

// RequirePinnedContinuation rejects a continuation pinned to an endpoint
// other than endpointID: a stored response exists only on the endpoint that
// created it (#1667).
func RequirePinnedContinuation(request llm.Request, endpointID string) error {
	if continuation := request.Continuation; continuation != nil && continuation.Pinned && continuation.EndpointID != "" && continuation.EndpointID != endpointID {
		return NewCompileError(fmt.Sprintf("continuation endpoint %q is not pinned endpoint %q", continuation.EndpointID, endpointID))
	}
	return nil
}

// MixedInstructionLevels reports whether instructions mix application and
// policy levels, which an endpoint with a single system prompt or role cannot
// keep apart.
func MixedInstructionLevels(instructions []llm.Instruction) bool {
	application, policy := false, false
	for _, instruction := range instructions {
		if instruction.Level == llm.InstructionLevelPolicy {
			policy = true
		} else {
			application = true
		}
	}
	return application && policy
}

// StructuredOutputKind reports whether an output format kind other than text
// asks for structured output.
func StructuredOutputKind(kind llm.OutputKind) bool {
	return kind != "" && kind != llm.OutputKindText
}

// RequestFeatures lists the capabilities a request needs, in first-use order:
// text and usage; partFeature of every instruction and message part; tool
// calls for tool items, tools or a tool policy; stateFeatures for a provider
// state input item; structured output when structured reports the output
// format kind needs it; reasoning; and continuation.
func RequestFeatures(request llm.Request, partFeature func(llm.Part) Feature, stateFeatures []Feature, structured func(llm.OutputKind) bool) []Feature {
	features := []Feature{FeatureText, FeatureUsage}
	for _, instruction := range request.Instructions {
		for _, part := range instruction.Content {
			features = append(features, partFeature(part))
		}
	}
	for _, item := range request.Input {
		switch value := item.(type) {
		case llm.Message:
			for _, part := range value.Content {
				features = append(features, partFeature(part))
			}
		case llm.ToolCall, llm.ToolResult:
			features = append(features, FeatureToolCall)
		case llm.ProviderState:
			features = append(features, stateFeatures...)
		}
	}
	if len(request.Tools) > 0 || request.ToolPolicy.Mode != "" {
		features = append(features, FeatureToolCall)
	}
	if request.Output != nil && structured(request.Output.Format.Kind) {
		features = append(features, FeatureStructuredOutput)
	}
	if request.Reasoning != nil {
		features = append(features, FeatureReasoning)
	}
	if request.Continuation != nil {
		features = append(features, FeatureContinuation)
	}
	return UniqueFeatures(features)
}

// EmptyModelMessage reports whether item is a model message with no content,
// such as a lifted content_filter or empty stop reply. The adapters skip it
// when replaying history: providers reject an assistant message without
// content, and it carries no history to preserve.
func EmptyModelMessage(item llm.Item) bool {
	message, ok := item.(llm.Message)
	return ok && message.Actor == llm.ActorModel && len(message.Content) == 0
}
