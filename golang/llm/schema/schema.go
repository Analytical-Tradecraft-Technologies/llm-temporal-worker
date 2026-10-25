package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	// applications is the worst-case number of subschemas applied to one
	// instance value; it bounds each validation's work.
	applications int
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
	// An unbounded numeric keyword would otherwise compile with the
	// constraint silently dropped, or panic in the meta-schema check.
	if err := checkNumbers(document, ""); err != nil {
		return nil, fmt.Errorf("schema document: %w", err)
	}

	compiled, err := compileDocument(document)
	if err != nil {
		return nil, err
	}
	// Validation runs after the provider has been paid, so a schema whose
	// worst case is exponential is refused here, before dispatch (#1521).
	applications, err := schemaFanout(document.(map[string]any), limits.MaxDepth)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(canonical)
	return &Schema{canonical: append([]byte(nil), canonical...), digest: digest, compiled: compiled, limits: limits, applications: applications}, nil
}

const schemaResourceURL = "urn:llmtw/schema/v1/root"

func compileDocument(document any) (compiled *jsonschema.Schema, err error) {
	defer func() {
		if recover() != nil {
			compiled, err = nil, fmt.Errorf("schema compile failed")
		}
	}()
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteLoader{})
	if err := compiler.AddResource(schemaResourceURL, document); err != nil {
		return nil, fmt.Errorf("schema resource: %w", err)
	}
	compiled, err = compiler.Compile(schemaResourceURL)
	if err != nil {
		return nil, fmt.Errorf("schema compile: %w", err)
	}
	return compiled, nil
}

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
	// Instance errors become provider safe messages, so none quotes the
	// instance: a duplicate key, a stray token or a number's path (#1661).
	canonical, err := llm.CanonicalJSONWithLimits(instance, schema.limits.MaxBytes, schema.limits.MaxDepth)
	if err != nil {
		if errors.Is(err, llm.ErrCanonicalJSONLimit) {
			return fmt.Errorf("instance JSON: %w", err)
		}
		return fmt.Errorf("instance JSON is not valid canonical JSON")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(canonical))
	if err != nil {
		return fmt.Errorf("instance JSON is not valid canonical JSON")
	}
	if err := checkNumbers(value, ""); err != nil {
		var bound *NumberBoundError
		if errors.As(err, &bound) {
			return fmt.Errorf("instance JSON: %w", &NumberBoundError{Path: safeInstancePath(bound.Path, ""), Reason: bound.Reason})
		}
		return fmt.Errorf("instance JSON has an invalid number")
	}
	if values := countValues(value); values > 0 && values > MaxValidationWork/max(schema.applications, 1) {
		return &ValidationError{Message: fmt.Sprintf("instance has too many values to validate against this schema within %d subschema applications", MaxValidationWork)}
	}
	return schema.validateValue(value)
}

// validateValue runs the third-party validator. Instances come from untrusted
// providers, so a validator panic becomes an ordinary validation failure
// rather than crashing the caller after payment (#1522).
func (schema *Schema) validateValue(value any) (err error) {
	defer func() {
		if recover() != nil {
			err = &ValidationError{Message: "schema validator failed on this instance"}
		}
	}()
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

// countValues counts the JSON values in a decoded instance.
func countValues(value any) int {
	switch value := value.(type) {
	case map[string]any:
		count := 1
		for _, child := range value {
			count += countValues(child)
		}
		return count
	case []any:
		count := 1
		for _, child := range value {
			count += countValues(child)
		}
		return count
	default:
		return 1
	}
}
