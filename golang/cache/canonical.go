package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

const (
	// CanonicalizerVersion is bumped whenever semantic normalization changes.
	CanonicalizerVersion = "cache-canonical/v1"
	SemanticProfile      = "semantic/v1"
	// MaxManifestBytes bounds the audit manifest and keeps it off the cache
	// hot path. Callers should use immutable content digests for larger input.
	MaxManifestBytes = 256 << 10
)

// OperationKind domain-separates normal model output from compaction output.
type OperationKind string

const (
	OperationGenerate OperationKind = "generate"
	OperationCompact  OperationKind = "compact"
)

// Namespace identifies the cache disclosure boundary. Actor and arbitrary
// observability tags intentionally do not appear here.
type Namespace struct {
	Tenant  string `json:"tenant,omitempty"`
	Project string `json:"project,omitempty"`
}

// Input is the semantic portion of one cache request. Request fields that are
// per-call controls (operation key, service class, fallback classes, actor,
// and tags) are removed by canonicalization. Conversation and opaque provider
// state are represented by immutable digests rather than raw content.
type Input struct {
	Operation          OperationKind
	Namespace          Namespace
	Config             ConfigDigest
	Route              RouteIdentity
	CapabilityLowering CapabilityVersion
	Epoch              CacheEpoch
	Conversation       ConversationDigest
	ProviderState      ProviderStateDigest
	Request            llm.Request
	Variant            int32
}

func (input Input) validate() error {
	if input.Operation != OperationGenerate && input.Operation != OperationCompact {
		return fmt.Errorf("unsupported cache operation %q", input.Operation)
	}
	if input.Config == "" {
		return fmt.Errorf("configuration digest is required")
	}
	if input.CapabilityLowering == "" {
		return fmt.Errorf("capability lowering version is required")
	}
	if input.Epoch == "" {
		return fmt.Errorf("cache epoch is required")
	}
	if err := input.Route.validate(); err != nil {
		return err
	}
	if input.Variant < 0 {
		return fmt.Errorf("cache variant is invalid")
	}
	if input.Request.OperationKey == "" {
		return fmt.Errorf("request operation key is required before canonicalization")
	}
	return nil
}

// Canonical returns the bounded, deterministic semantic manifest. It is safe
// to persist this alongside the digest for audit, but callers should replace
// large transcript content with ConversationDigest/BlobRef values before
// persistence.
func (input Input) Canonical() ([]byte, error) {
	if err := input.validate(); err != nil {
		return nil, err
	}
	normalized, err := llm.NormalizeRequest(input.Request)
	if err != nil {
		return nil, fmt.Errorf("normalize cache request: %w", err)
	}
	// Request.MarshalJSON validates the complete request. Use a generic object
	// only after that validation, then remove controls which are not semantic.
	raw, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("marshal cache request: %w", err)
	}
	var request map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		return nil, fmt.Errorf("decode cache request: %w", err)
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("cache request has trailing value %v", token)
		}
		return nil, fmt.Errorf("cache request trailing input: %w", err)
	}
	namespace := input.Namespace
	if context, ok := request["context"].(map[string]any); ok {
		if namespace.Tenant == "" {
			namespace.Tenant, _ = context["tenant"].(string)
		}
		if namespace.Project == "" {
			namespace.Project, _ = context["project"].(string)
		}
	}
	delete(request, "operation_key")
	delete(request, "service_class")
	delete(request, "service_class_fallbacks")
	delete(request, "continuation") // represented by conversation/provider-state digests
	delete(request, "context")
	manifest := map[string]any{
		"canonicalizer":         CanonicalizerVersion,
		"semantic_profile":      SemanticProfile,
		"operation":             string(input.Operation),
		"namespace":             namespace,
		"config_digest":         string(input.Config),
		"route":                 input.Route,
		"capability_lowering":   string(input.CapabilityLowering),
		"cache_epoch":           string(input.Epoch),
		"conversation_digest":   string(input.Conversation),
		"provider_state_digest": string(input.ProviderState),
		"request":               request,
		"variant":               input.Variant,
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal cache manifest: %w", err)
	}
	return llm.CanonicalJSONWithLimits(encoded, MaxManifestBytes, 128)
}
