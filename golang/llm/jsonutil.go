package llm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"unicode/utf8"
)

var errJSONTrailingData = errors.New("json value has trailing data")

// referenceJSONDecoding forces every decoder in this package through the
// encoding/json Decoder paths that define the accepted language and the error
// text. It is false in production and only set by the differential tests that
// prove the fast paths below accept, reject and report exactly the same.
var referenceJSONDecoding bool

// decodeObject decodes one JSON object while rejecting duplicate keys. The
// nested values are retained as raw JSON so tagged unions can validate their
// own closed fields.
func decodeObject(data []byte) (map[string]json.RawMessage, error) {
	// The standard encoding/json decoder keeps the last value when an object
	// repeats a key. Public v1 records must not have that ambiguity at any
	// nesting level, including opaque extension/schema/provider JSON that is
	// retained as RawMessage.
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return nil, err
	}
	if !referenceJSONDecoding {
		if fields, ok := splitJSONObject(data, true); ok {
			return fields, nil
		}
	}
	return decodeObjectReference(data)
}

// decodeVerifiedObject splits one JSON object that is a sub-value of a
// document the caller's entry point already passed through
// rejectDuplicateJSONKeys. That scan walks the whole document, so repeating
// it for every nested value only multiplies the cost by the nesting depth.
// The returned values alias data; callers copy what they retain. Anything the
// splitter does not recognise is handed to decodeObject, which also produces
// every error.
func decodeVerifiedObject(data []byte) (map[string]json.RawMessage, error) {
	if !referenceJSONDecoding {
		if fields, ok := splitJSONObject(data, false); ok {
			return fields, nil
		}
	}
	return decodeObject(data)
}

// decodeVerifiedArray is the array counterpart of decodeVerifiedObject.
func decodeVerifiedArray(data []byte) ([]json.RawMessage, error) {
	if !referenceJSONDecoding {
		if values, ok := splitJSONArray(data); ok {
			return values, nil
		}
	}
	var values []json.RawMessage
	if err := decodeJSON(data, &values); err != nil {
		return nil, err
	}
	return values, nil
}

// decodeObjectReference is the encoding/json Decoder split of an object whose
// keys were already checked. It defines the result and the errors of
// decodeObject.
func decodeObjectReference(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, errors.New("expected JSON object")
	}

	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("expected JSON object key")
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate JSON object key %q", key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = append(json.RawMessage(nil), value...)
	}

	token, err = decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok = token.(json.Delim)
	if !ok || delim != '}' {
		return nil, errors.New("expected end of JSON object")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return fields, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return errJSONTrailingData
}

func decodeJSON(data []byte, dst any) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	// The v1 schemas never allow null for a present field. encoding/json
	// would decode it as the zero value, silently turning "unset" into an
	// empty string, zero, false, or an empty list that replaces inherited
	// settings.
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("null is not permitted")
	}
	if !referenceJSONDecoding {
		// The scan above established that data is exactly one JSON value, so
		// json.Unmarshal is the same decode without a buffered reader. The
		// listed destinations hold no interface values, which are the only
		// place UseNumber changes the result.
		switch dst.(type) {
		case *string, *bool, *int, *int64, *float64, *[]byte, *[]string, *[]json.RawMessage, *map[string]json.RawMessage:
			return json.Unmarshal(data, dst)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func checkUnknownFields(fields map[string]json.RawMessage, allowed ...string) error {
	for key := range fields {
		if !slices.Contains(allowed, key) {
			return fmt.Errorf("unknown JSON field %q", key)
		}
	}
	return nil
}

func requireField(fields map[string]json.RawMessage, key string) (json.RawMessage, error) {
	value, ok := fields[key]
	if !ok {
		return nil, fmt.Errorf("missing required JSON field %q", key)
	}
	return value, nil
}

// decodeStringField decodes one string-valued field. A value that is a JSON
// string literal cannot contain an object, so it needs no duplicate-key scan;
// every other value takes decodeJSON, which also produces every error.
func decodeStringField(key string, value json.RawMessage) (string, error) {
	if !referenceJSONDecoding {
		if result, ok := plainJSONString(value); ok {
			return string(result), nil
		}
		if len(value) > 0 && value[0] == '"' {
			var result string
			if err := json.Unmarshal(value, &result); err == nil {
				return result, nil
			}
		}
	}
	var result string
	if err := decodeJSON(value, &result); err != nil {
		return "", fmt.Errorf("%s: %w", key, err)
	}
	return result, nil
}

func optionalString(fields map[string]json.RawMessage, key string) (string, bool, error) {
	value, ok := fields[key]
	if !ok {
		return "", false, nil
	}
	result, err := decodeStringField(key, value)
	return result, true, err
}

func requiredString(fields map[string]json.RawMessage, key string) (string, error) {
	value, err := requireField(fields, key)
	if err != nil {
		return "", err
	}
	return decodeStringField(key, value)
}

func optionalBool(fields map[string]json.RawMessage, key string) (bool, bool, error) {
	value, ok := fields[key]
	if !ok {
		return false, false, nil
	}
	if !referenceJSONDecoding {
		switch string(value) {
		case "true":
			return true, true, nil
		case "false":
			return false, true, nil
		}
	}
	var result bool
	if err := decodeJSON(value, &result); err != nil {
		return false, true, fmt.Errorf("%s: %w", key, err)
	}
	return result, true, nil
}

func optionalInt(fields map[string]json.RawMessage, key string) (int, bool, error) {
	value, ok := fields[key]
	if !ok {
		return 0, false, nil
	}
	result, err := decodeIntJSON[int](value)
	if err != nil {
		return 0, true, fmt.Errorf("%s: %w", key, err)
	}
	return result, true, nil
}

// decodeIntJSON decodes a JSON integer. A plain decimal literal of at most 18
// digits is a complete JSON number that cannot overflow int64, so it is parsed
// directly; every other value takes decodeJSON, which also produces every
// error.
func decodeIntJSON[T int | int64](data []byte) (T, error) {
	if !referenceJSONDecoding {
		if value, ok := plainJSONInt(data); ok && int64(T(value)) == value {
			return T(value), nil
		}
	}
	var result T
	if err := decodeJSON(data, &result); err != nil {
		return 0, err
	}
	return result, nil
}

// decodeBytesJSON decodes a base64 JSON string exactly as encoding/json
// decodes into a []byte, without scanning and buffering a large payload
// again.
func decodeBytesJSON(data []byte) ([]byte, error) {
	if !referenceJSONDecoding {
		if encoded, ok := plainJSONString(data); ok {
			result := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
			if n, err := base64.StdEncoding.Decode(result, encoded); err == nil {
				return result[:n], nil
			}
		}
	}
	var result []byte
	if err := decodeJSON(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// plainJSONString returns the contents of a JSON string literal that
// encoding/json would decode to exactly the bytes between its quotes: no
// escapes, no control characters and valid UTF-8.
func plainJSONString(data []byte) ([]byte, bool) {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return nil, false
	}
	inner := data[1 : len(data)-1]
	for _, char := range inner {
		if char < 0x20 || char == '"' || char == '\\' {
			return nil, false
		}
	}
	if !utf8.Valid(inner) {
		return nil, false
	}
	return inner, true
}

func plainJSONInt(data []byte) (int64, bool) {
	digits := data
	if len(digits) > 0 && digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || len(digits) > 18 || (digits[0] == '0' && len(digits) > 1) {
		return 0, false
	}
	for _, char := range digits {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	value, err := strconv.ParseInt(string(data), 10, 64)
	return value, err == nil
}

func copyRaw(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func copyBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func validRawJSON(value json.RawMessage) bool {
	return len(value) > 0 && rejectDuplicateJSONKeys(value) == nil
}

// rejectDuplicateJSONKeys validates one JSON value while checking every
// object, rather than only the object decoded by a custom UnmarshalJSON
// method. encoding/json intentionally accepts duplicate keys and keeps the
// last value; accepting that behavior at a public boundary makes request
// digests and semantic validation depend on parser details.
//
// The common case is a well-formed value without duplicates, which json.Valid
// and an allocation-free key walk establish in two linear passes. Everything
// else, including every rejection, is decided by the reference scan.
func rejectDuplicateJSONKeys(data []byte) error {
	if !referenceJSONDecoding && json.Valid(data) {
		cursor := jsonCursor{data: data}
		cursor.skipSpace()
		if cursor.uniqueKeys() {
			return nil
		}
	}
	return rejectDuplicateJSONKeysReference(data)
}

// rejectDuplicateJSONKeysReference is the encoding/json token scan that
// defines which values rejectDuplicateJSONKeys accepts and the errors it
// returns.
func rejectDuplicateJSONKeysReference(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var visit func() error
	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON object key %q", key)
				}
				seen[key] = struct{}{}
				if err := visit(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil {
				return err
			}
			if end != json.Delim('}') {
				return fmt.Errorf("JSON object ended with %v", end)
			}
		case '[':
			for decoder.More() {
				if err := visit(); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil {
				return err
			}
			if end != json.Delim(']') {
				return fmt.Errorf("JSON array ended with %v", end)
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
		return nil
	}

	if err := visit(); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	return nil
}

// jsonCursor walks JSON text that json.Valid accepted or that is a sub-value
// of a document rejectDuplicateJSONKeys accepted. It reports anything it does
// not expect as a failure, which callers resolve through the reference
// decoders; it never decides on its own that a value is malformed.
type jsonCursor struct {
	data []byte
	pos  int
}

func (cursor *jsonCursor) skipSpace() {
	for cursor.pos < len(cursor.data) {
		switch cursor.data[cursor.pos] {
		case ' ', '\t', '\n', '\r':
			cursor.pos++
		default:
			return
		}
	}
}

// next consumes one byte, or returns zero at the end of the text.
func (cursor *jsonCursor) next() byte {
	if cursor.pos >= len(cursor.data) {
		return 0
	}
	char := cursor.data[cursor.pos]
	cursor.pos++
	return char
}

func (cursor *jsonCursor) peek() byte {
	if cursor.pos >= len(cursor.data) {
		return 0
	}
	return cursor.data[cursor.pos]
}

// skipString consumes a string literal and returns it including its quotes.
func (cursor *jsonCursor) skipString() ([]byte, bool) {
	start := cursor.pos
	if cursor.next() != '"' {
		return nil, false
	}
	for cursor.pos < len(cursor.data) {
		switch cursor.data[cursor.pos] {
		case '"':
			cursor.pos++
			return cursor.data[start:cursor.pos], true
		case '\\':
			cursor.pos += 2
		default:
			cursor.pos++
		}
	}
	return nil, false
}

// skipScalar consumes a number or a true, false or null literal.
func (cursor *jsonCursor) skipScalar() bool {
	start := cursor.pos
	for cursor.pos < len(cursor.data) {
		switch cursor.data[cursor.pos] {
		case ',', ']', '}', ':', '"', '{', '[', ' ', '\t', '\n', '\r':
			return cursor.pos > start
		}
		cursor.pos++
	}
	return cursor.pos > start
}

// skipValue consumes one value without inspecting nested object keys.
func (cursor *jsonCursor) skipValue() bool {
	switch cursor.peek() {
	case '"':
		_, ok := cursor.skipString()
		return ok
	case '{', '[':
		depth := 0
		for cursor.pos < len(cursor.data) {
			switch cursor.data[cursor.pos] {
			case '"':
				if _, ok := cursor.skipString(); !ok {
					return false
				}
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					cursor.pos++
					return true
				}
			}
			cursor.pos++
		}
		return false
	default:
		return cursor.skipScalar()
	}
}

// key consumes an object key and returns it as encoding/json would decode it.
func (cursor *jsonCursor) key() ([]byte, bool) {
	literal, ok := cursor.skipString()
	if !ok {
		return nil, false
	}
	if key, ok := plainJSONString(literal); ok {
		return key, true
	}
	var decoded string
	if err := json.Unmarshal(literal, &decoded); err != nil {
		return nil, false
	}
	return []byte(decoded), true
}

// uniqueKeys consumes one value and reports whether it was walked completely
// without finding an object that repeats a key.
func (cursor *jsonCursor) uniqueKeys() bool {
	switch cursor.peek() {
	case '{':
		cursor.pos++
		cursor.skipSpace()
		if cursor.peek() == '}' {
			cursor.pos++
			return true
		}
		// Most objects are small enough to compare keys pairwise without
		// allocating a set.
		var inline [16][]byte
		keys := inline[:0]
		var seen map[string]struct{}
		for {
			cursor.skipSpace()
			key, ok := cursor.key()
			if !ok {
				return false
			}
			if seen == nil {
				for _, previous := range keys {
					if bytes.Equal(previous, key) {
						return false
					}
				}
				if len(keys) < cap(keys) {
					keys = append(keys, key)
				} else {
					seen = make(map[string]struct{}, 2*len(keys))
					for _, previous := range keys {
						seen[string(previous)] = struct{}{}
					}
					seen[string(key)] = struct{}{}
				}
			} else {
				if _, exists := seen[string(key)]; exists {
					return false
				}
				seen[string(key)] = struct{}{}
			}
			cursor.skipSpace()
			if cursor.next() != ':' {
				return false
			}
			cursor.skipSpace()
			if !cursor.uniqueKeys() {
				return false
			}
			cursor.skipSpace()
			switch cursor.next() {
			case ',':
			case '}':
				return true
			default:
				return false
			}
		}
	case '[':
		cursor.pos++
		cursor.skipSpace()
		if cursor.peek() == ']' {
			cursor.pos++
			return true
		}
		for {
			cursor.skipSpace()
			if !cursor.uniqueKeys() {
				return false
			}
			cursor.skipSpace()
			switch cursor.next() {
			case ',':
			case ']':
				return true
			default:
				return false
			}
		}
	case '"':
		_, ok := cursor.skipString()
		return ok
	default:
		return cursor.skipScalar()
	}
}

// atEnd reports whether only whitespace remains.
func (cursor *jsonCursor) atEnd() bool {
	cursor.skipSpace()
	return cursor.pos == len(cursor.data)
}

// splitJSONObject splits an object into its fields without descending into
// the values. It reports false for anything other than an object with
// distinct keys, leaving the decision to the reference decoder. Values alias
// data unless copyValues is set.
func splitJSONObject(data []byte, copyValues bool) (map[string]json.RawMessage, bool) {
	cursor := jsonCursor{data: data}
	cursor.skipSpace()
	if cursor.next() != '{' {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	cursor.skipSpace()
	if cursor.peek() == '}' {
		cursor.pos++
		return fields, cursor.atEnd()
	}
	for {
		cursor.skipSpace()
		key, ok := cursor.key()
		if !ok {
			return nil, false
		}
		if _, exists := fields[string(key)]; exists {
			return nil, false
		}
		cursor.skipSpace()
		if cursor.next() != ':' {
			return nil, false
		}
		cursor.skipSpace()
		start := cursor.pos
		if !cursor.skipValue() {
			return nil, false
		}
		value := json.RawMessage(data[start:cursor.pos:cursor.pos])
		if copyValues {
			value = copyRaw(value)
		}
		fields[string(key)] = value
		cursor.skipSpace()
		switch cursor.next() {
		case ',':
		case '}':
			return fields, cursor.atEnd()
		default:
			return nil, false
		}
	}
}

// splitJSONArray is the array counterpart of splitJSONObject. Its elements
// always alias data.
func splitJSONArray(data []byte) ([]json.RawMessage, bool) {
	cursor := jsonCursor{data: data}
	cursor.skipSpace()
	if cursor.next() != '[' {
		return nil, false
	}
	values := []json.RawMessage{}
	cursor.skipSpace()
	if cursor.peek() == ']' {
		cursor.pos++
		return values, cursor.atEnd()
	}
	for {
		cursor.skipSpace()
		start := cursor.pos
		if !cursor.skipValue() {
			return nil, false
		}
		values = append(values, json.RawMessage(data[start:cursor.pos:cursor.pos]))
		cursor.skipSpace()
		switch cursor.next() {
		case ',':
		case ']':
			return values, cursor.atEnd()
		default:
			return nil, false
		}
	}
}

func marshalObject(fields map[string]any) ([]byte, error) {
	return json.Marshal(fields)
}
