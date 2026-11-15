package llm

import (
	"encoding/json"
	"maps"
	"strconv"
)

// SanitizeLoneSurrogates replaces every \u escape of an unpaired UTF-16
// surrogate in raw JSON with \ufffd. Go's JSON decoder tolerates a lone
// surrogate, but strict decoders (the OCaml client among them) reject the
// whole document, so a paid response carrying model-generated JSON with one
// would be undecodable for those callers. Escapes are only valid inside JSON
// strings, so the scan needs no string tracking. Paired surrogates and all
// other bytes are kept exactly; the input is returned unchanged when there is
// nothing to replace.
func SanitizeLoneSurrogates(raw json.RawMessage) json.RawMessage {
	var out []byte
	for index := 0; index < len(raw); {
		if raw[index] != '\\' || index+1 >= len(raw) {
			if out != nil {
				out = append(out, raw[index])
			}
			index++
			continue
		}
		if raw[index+1] != 'u' {
			if out != nil {
				out = append(out, raw[index:min(index+2, len(raw))]...)
			}
			index += 2
			continue
		}
		unit, ok := escapeUnit(raw, index)
		width := 6
		lone := false
		switch {
		case ok && unit >= 0xD800 && unit <= 0xDBFF:
			if low, paired := escapeUnit(raw, index+6); paired && low >= 0xDC00 && low <= 0xDFFF {
				width = 12
			} else {
				lone = true
			}
		case ok && unit >= 0xDC00 && unit <= 0xDFFF:
			lone = true
		case !ok:
			width = 2
		}
		if lone && out == nil {
			out = append(make([]byte, 0, len(raw)), raw[:index]...)
		}
		if out != nil {
			if lone {
				out = append(out, `\ufffd`...)
			} else {
				out = append(out, raw[index:min(index+width, len(raw))]...)
			}
		}
		index += width
	}
	if out == nil {
		return raw
	}
	return out
}

// escapeUnit reads the four hex digits of a \u escape starting at index.
func escapeUnit(raw []byte, index int) (uint64, bool) {
	if index+6 > len(raw) || raw[index] != '\\' || raw[index+1] != 'u' {
		return 0, false
	}
	value, err := strconv.ParseUint(string(raw[index+2:index+6]), 16, 16)
	return value, err == nil
}

// SanitizeRawMapSurrogates applies SanitizeLoneSurrogates to every value of a
// map of raw provider JSON, such as reference metadata or Usage.ProviderRaw.
// The input map is returned unchanged when no value needs replacing;
// otherwise a copy is returned so the caller's map is never mutated.
func SanitizeRawMapSurrogates(values map[string]json.RawMessage) map[string]json.RawMessage {
	var result map[string]json.RawMessage
	for key, value := range values {
		// SanitizeLoneSurrogates returns its input when nothing changes.
		sanitized := SanitizeLoneSurrogates(value)
		if len(value) == 0 || &sanitized[0] == &value[0] {
			continue
		}
		if result == nil {
			result = make(map[string]json.RawMessage, len(values))
			maps.Copy(result, values)
		}
		result[key] = sanitized
	}
	if result == nil {
		return values
	}
	return result
}

// SanitizeOutputSurrogates applies SanitizeLoneSurrogates to the open JSON a
// model or provider produced: tool call arguments, JSON parts of messages and
// tool results, and reference metadata. Other items are returned as they are.
func SanitizeOutputSurrogates(items []Item) []Item {
	if len(items) == 0 {
		return items
	}
	result := make([]Item, len(items))
	for index, item := range items {
		result[index] = sanitizeItemSurrogates(item)
	}
	return result
}

func sanitizeItemSurrogates(item Item) Item {
	if value, ok := sanitizeCopy(item, func(call *ToolCall) { call.Arguments = SanitizeLoneSurrogates(call.Arguments) }); ok {
		return value
	}
	if value, ok := sanitizeCopy(item, func(message *Message) { message.Content = sanitizePartSurrogates(message.Content) }); ok {
		return value
	}
	if value, ok := sanitizeCopy(item, func(result *ToolResult) { result.Content = sanitizePartSurrogates(result.Content) }); ok {
		return value
	}
	if value, ok := sanitizeCopy(item, func(reference *Reference) { reference.Metadata = SanitizeRawMapSurrogates(reference.Metadata) }); ok {
		return value
	}
	return item
}

func sanitizePartSurrogates(parts []Part) []Part {
	if len(parts) == 0 {
		return parts
	}
	result := make([]Part, len(parts))
	for index, part := range parts {
		result[index], _ = sanitizeCopy(part, func(part *JSONPart) { part.Value = SanitizeLoneSurrogates(part.Value) })
	}
	return result
}

// sanitizeCopy applies sanitize to value when it holds a T or a *T and
// reports whether it did. Pointer forms satisfy the item and part interfaces
// too; they are sanitized through a copy so the caller's value is never
// mutated, and a nil pointer is returned as it is.
func sanitizeCopy[I any, T any](value I, sanitize func(*T)) (I, bool) {
	switch concrete := any(value).(type) {
	case T:
		sanitize(&concrete)
		return any(concrete).(I), true
	case *T:
		if concrete == nil {
			return value, true
		}
		clone := *concrete
		sanitize(&clone)
		return any(&clone).(I), true
	default:
		return value, false
	}
}
