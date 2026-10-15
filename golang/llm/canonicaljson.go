package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

// The low-level canonicalizer has a finite resource envelope even when a
// caller does not yet have a compiled configuration snapshot. Applications
// should pass their stricter request/schema limits to CanonicalJSONWithLimits.
const (
	DefaultCanonicalMaxBytes = 8 << 20
	DefaultCanonicalMaxDepth = 128
	// MaxToolArgumentsDepth bounds the nesting of tool-call arguments below
	// DefaultCanonicalMaxDepth. Durable records wrap a tool call several
	// levels deep (progress, execution, response, output, item, and more for
	// a finalization plan or cache entry) and are themselves held to
	// DefaultCanonicalMaxDepth, so arguments any deeper than this could be
	// accepted from a provider yet never saved.
	MaxToolArgumentsDepth = 96
)

// ErrCanonicalJSONLimit marks input or output that exceeds the byte or
// nesting limit passed to the canonicalizer. It is deterministic for a given
// value and limit, unlike a malformed-JSON error.
var ErrCanonicalJSONLimit = errors.New("canonical JSON limit exceeded")

type canonicalLimitError struct{ message string }

func (err canonicalLimitError) Error() string { return err.message }
func (canonicalLimitError) Unwrap() error     { return ErrCanonicalJSONLimit }

type canonicalObject struct {
	values map[string]any
}

// CanonicalJSON parses one JSON value, rejects duplicate keys and trailing
// values, and emits deterministic JSON with recursively sorted object keys.
// Number tokens remain json.Number text rather than being converted through a
// binary floating-point value, preserving integers and decimal spellings
// exactly.
func CanonicalJSON(data []byte) ([]byte, error) {
	return CanonicalJSONWithLimits(data, DefaultCanonicalMaxBytes, DefaultCanonicalMaxDepth)
}

// CanonicalToolArguments is CanonicalJSON for tool-call arguments, whose
// nesting is held to MaxToolArgumentsDepth.
func CanonicalToolArguments(data []byte) ([]byte, error) {
	return CanonicalJSONWithLimits(data, DefaultCanonicalMaxBytes, MaxToolArgumentsDepth)
}

// CanonicalJSONWithLimits is CanonicalJSON with explicit byte and nesting
// limits. Both limits must be positive.
func CanonicalJSONWithLimits(data []byte, maxBytes, maxDepth int) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("canonical JSON byte limit must be positive")
	}
	if maxDepth <= 0 {
		return nil, fmt.Errorf("canonical JSON depth limit must be positive")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("canonical JSON is empty")
	}
	if len(data) > maxBytes {
		return nil, canonicalLimitError{fmt.Sprintf("canonical JSON exceeds %d bytes", maxBytes)}
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := parseCanonicalValue(decoder, 0, maxDepth)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("canonical JSON has trailing value %v", token)
		}
		return nil, fmt.Errorf("canonical JSON trailing input: %w", err)
	}

	// Canonical output is close to the input size, so one allocation usually
	// holds it instead of repeated buffer doubling on large values (#1112).
	var output bytes.Buffer
	output.Grow(len(data))
	if err := writeCanonicalValue(&output, value); err != nil {
		return nil, err
	}
	if output.Len() > maxBytes {
		return nil, canonicalLimitError{fmt.Sprintf("canonical JSON output exceeds %d bytes", maxBytes)}
	}
	return output.Bytes(), nil
}

func parseCanonicalValue(decoder *json.Decoder, depth, maxDepth int) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("canonical JSON ended before a value")
		}
		return nil, fmt.Errorf("canonical JSON token: %w", err)
	}
	if delimiter, ok := token.(json.Delim); ok {
		if depth >= maxDepth {
			return nil, canonicalLimitError{fmt.Sprintf("canonical JSON exceeds depth %d", maxDepth)}
		}
		switch delimiter {
		case '{':
			object := canonicalObject{values: make(map[string]any)}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("canonical JSON object key: %w", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fmt.Errorf("canonical JSON object key is not a string")
				}
				if _, exists := object.values[key]; exists {
					return nil, fmt.Errorf("canonical JSON duplicate object key %q", key)
				}
				value, err := parseCanonicalValue(decoder, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				object.values[key] = value
			}
			end, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("canonical JSON object end: %w", err)
			}
			if end != json.Delim('}') {
				return nil, fmt.Errorf("canonical JSON object ended with %v", end)
			}
			return object, nil
		case '[':
			values := make([]any, 0)
			for decoder.More() {
				value, err := parseCanonicalValue(decoder, depth+1, maxDepth)
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			end, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("canonical JSON array end: %w", err)
			}
			if end != json.Delim(']') {
				return nil, fmt.Errorf("canonical JSON array ended with %v", end)
			}
			return values, nil
		default:
			return nil, fmt.Errorf("canonical JSON unexpected delimiter %q", delimiter)
		}
	}
	switch value := token.(type) {
	case nil, bool, string, json.Number:
		return value, nil
	default:
		return nil, fmt.Errorf("canonical JSON token %T is unsupported", token)
	}
}

func writeCanonicalValue(output *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case canonicalObject:
		keys := make([]string, 0, len(value.values))
		for key := range value.values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := writeCanonicalString(output, key); err != nil {
				return fmt.Errorf("canonical JSON object key: %w", err)
			}
			output.WriteByte(':')
			if err := writeCanonicalValue(output, value.values[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
		return nil
	case []any:
		output.WriteByte('[')
		for index, item := range value {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := writeCanonicalValue(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
		return nil
	case json.Number:
		if !json.Valid([]byte(value)) {
			return fmt.Errorf("canonical JSON number is invalid")
		}
		output.WriteString(string(value))
		return nil
	case string:
		if err := writeCanonicalString(output, value); err != nil {
			return fmt.Errorf("canonical JSON string: %w", err)
		}
		return nil
	case bool:
		if value {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
		return nil
	case nil:
		output.WriteString("null")
		return nil
	default:
		return fmt.Errorf("canonical JSON value %T is unsupported", value)
	}
}

// writeCanonicalString writes value exactly as json.Marshal encodes a string.
// A string of printable ASCII that json.Marshal leaves unescaped is written
// directly, avoiding a temporary encoding per string; any other string
// (control, HTML-sensitive, quote, backslash or non-ASCII bytes) is encoded by
// json.Marshal itself.
func writeCanonicalString(output *bytes.Buffer, value string) error {
	for index := 0; index < len(value); index++ {
		if c := value[index]; c < 0x20 || c > 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			encoded, err := json.Marshal(value)
			if err != nil {
				return err
			}
			output.Write(encoded)
			return nil
		}
	}
	output.WriteByte('"')
	output.WriteString(value)
	output.WriteByte('"')
	return nil
}
