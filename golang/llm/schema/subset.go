package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Limits bounds schema and instance parsing before the third-party validator
// receives any values. Configuration can provide stricter limits for a route.
type Limits struct {
	MaxBytes int
	MaxDepth int
}

const (
	DefaultMaxBytes = 256 << 10
	DefaultMaxDepth = 64
)

func DefaultLimits() Limits {
	return Limits{MaxBytes: DefaultMaxBytes, MaxDepth: DefaultMaxDepth}
}

// SubsetError identifies a keyword that is outside the v1 locally supported
// JSON Schema subset. Path is a canonical JSON pointer into the schema.
type SubsetError struct {
	Path    string
	Keyword string
}

func (err *SubsetError) Error() string {
	if err.Path == "" {
		return fmt.Sprintf("unsupported JSON Schema keyword %q", err.Keyword)
	}
	return fmt.Sprintf("unsupported JSON Schema keyword %q at %s", err.Keyword, err.Path)
}

var supportedKeywords = map[string]struct{}{
	"$schema": {}, "$id": {}, "$anchor": {}, "$defs": {}, "$ref": {},
	"title": {}, "description": {}, "$comment": {}, "default": {}, "examples": {},
	"type": {}, "enum": {}, "const": {},
	"properties": {}, "required": {}, "additionalProperties": {},
	"propertyNames": {}, "minProperties": {}, "maxProperties": {},
	"items": {}, "prefixItems": {}, "minItems": {}, "maxItems": {}, "uniqueItems": {},
	"minLength": {}, "maxLength": {}, "pattern": {},
	"contentEncoding": {}, "contentMediaType": {},
	"minimum": {}, "maximum": {}, "exclusiveMinimum": {}, "exclusiveMaximum": {}, "multipleOf": {},
	"allOf": {}, "anyOf": {}, "oneOf": {}, "not": {}, "if": {}, "then": {}, "else": {},
	"format": {},
}

func validateSubset(root any) error {
	if _, ok := root.(map[string]any); !ok {
		return &SubsetError{Path: "", Keyword: "root"}
	}
	return walkSchema(root, "")
}

func walkSchema(value any, path string) error {
	if _, ok := value.(bool); ok {
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("schema at %s must be an object", displayPath(path))
	}
	for key, child := range object {
		if _, ok := supportedKeywords[key]; !ok {
			return &SubsetError{Path: pointerJoin(path, key), Keyword: key}
		}
		childPath := pointerJoin(path, key)
		switch key {
		case "$schema":
			if value, ok := child.(string); !ok || value != "https://json-schema.org/draft/2020-12/schema" {
				return fmt.Errorf("$schema at %s must name Draft 2020-12", displayPath(childPath))
			}
		case "$ref":
			ref, ok := child.(string)
			if !ok || !strings.HasPrefix(ref, "#") {
				return fmt.Errorf("remote or invalid $ref at %s", displayPath(childPath))
			}
		case "$defs", "properties":
			children, ok := child.(map[string]any)
			if !ok {
				return fmt.Errorf("%s at %s must be an object", key, displayPath(childPath))
			}
			for name, nested := range children {
				if err := walkSchema(nested, pointerJoin(childPath, name)); err != nil {
					return err
				}
			}
		case "additionalProperties":
			if flag, ok := child.(bool); ok {
				_ = flag
				continue
			}
			if err := walkSchema(child, childPath); err != nil {
				return err
			}
		case "propertyNames", "items", "not", "if", "then", "else":
			if err := walkSchema(child, childPath); err != nil {
				return err
			}
		case "prefixItems", "allOf", "anyOf", "oneOf":
			children, ok := child.([]any)
			if !ok {
				return fmt.Errorf("%s at %s must be an array", key, displayPath(childPath))
			}
			for index, nested := range children {
				if err := walkSchema(nested, fmt.Sprintf("%s/%d", childPath, index)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func decodeSchemaDocument(canonical []byte) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(string(canonical)))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	return document, nil
}

func pointerJoin(base, token string) string {
	token = strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	return base + "/" + token
}

func displayPath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}
