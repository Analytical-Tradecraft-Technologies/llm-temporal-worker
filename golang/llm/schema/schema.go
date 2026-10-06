package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Schema is an immutable, canonical Draft 2020-12 schema compiled without
// network or filesystem reference resolution.
type Schema struct {
	canonical []byte
	digest    [32]byte
	compiled  *jsonschema.Schema
	limits    Limits
}

// Parse parses, canonicalizes, checks the v1 subset, and compiles one schema.
func Parse(data []byte) (*Schema, error) {
	return ParseWithLimits(data, DefaultLimits())
}

// ParseWithLimits is Parse with explicit resource limits.
func ParseWithLimits(data []byte, limits Limits) (*Schema, error) {
	if limits.MaxBytes <= 0 || limits.MaxDepth <= 0 {
		return nil, fmt.Errorf("schema limits must be positive")
	}
	canonical, err := llm.CanonicalJSONWithLimits(data, limits.MaxBytes, limits.MaxDepth)
	if err != nil {
		return nil, fmt.Errorf("schema JSON: %w", err)
	}
	document, err := decodeSchemaDocument(canonical)
	if err != nil {
		return nil, fmt.Errorf("schema document: %w", err)
	}
	if err := validateSubset(document); err != nil {
		return nil, err
	}

	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteLoader{})
	if err := compiler.AddResource(schemaResourceURL, document); err != nil {
		return nil, fmt.Errorf("schema resource: %w", err)
	}
	compiled, err := compiler.Compile(schemaResourceURL)
	if err != nil {
		return nil, fmt.Errorf("schema compile: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return &Schema{canonical: append([]byte(nil), canonical...), digest: digest, compiled: compiled, limits: limits}, nil
}

const schemaResourceURL = "urn:llmtw/schema/v1/root"

type noRemoteLoader struct{}

func (noRemoteLoader) Load(rawURL string) (any, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("reference URL is invalid")
	}
	return nil, fmt.Errorf("remote schema reference %q is disabled", u.String())
}

// Canonical returns a copy of the schema's canonical JSON representation.
func (schema *Schema) Canonical() []byte {
	if schema == nil {
		return nil
	}
	return append([]byte(nil), schema.canonical...)
}

// Digest returns the SHA-256 digest of Canonical().
func (schema *Schema) Digest() [32]byte {
	if schema == nil {
		return [32]byte{}
	}
	return schema.digest
}

// DigestHex returns the lowercase digest used in safe metadata.
func (schema *Schema) DigestHex() string {
	if schema == nil {
		return ""
	}
	return hex.EncodeToString(schema.digest[:])
}

// Validate validates one JSON instance with duplicate-key, depth, and size
// checks performed before the third-party validator sees it.
func (schema *Schema) Validate(instance []byte) error {
	if schema == nil || schema.compiled == nil {
		return fmt.Errorf("schema is nil")
	}
	canonical, err := llm.CanonicalJSONWithLimits(instance, schema.limits.MaxBytes, schema.limits.MaxDepth)
	if err != nil {
		return fmt.Errorf("instance JSON: %w", err)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(canonical))
	if err != nil {
		return fmt.Errorf("instance JSON: %w", err)
	}
	if err := schema.compiled.Validate(value); err != nil {
		return normalizeValidationError(err)
	}
	return nil
}

// Validate is a convenience wrapper for one schema and instance.
func Validate(schemaData, instance []byte) error {
	compiled, err := Parse(schemaData)
	if err != nil {
		return err
	}
	return compiled.Validate(instance)
}
