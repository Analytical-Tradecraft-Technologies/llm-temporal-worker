package llm

import (
	"encoding/json"
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

// SanitizeOutputSurrogates applies SanitizeLoneSurrogates to the open JSON a
// model produced: tool call arguments and JSON parts of messages and tool
// results. Items without open JSON are returned as they are.
func SanitizeOutputSurrogates(items []Item) []Item {
	if len(items) == 0 {
		return items
	}
	result := make([]Item, len(items))
	for index, item := range items {
		// Pointer forms satisfy Item too; sanitize a copy so the caller's
		// value is never mutated.
		switch value := item.(type) {
		case ToolCall:
			value.Arguments = SanitizeLoneSurrogates(value.Arguments)
			result[index] = value
		case *ToolCall:
			if value == nil {
				result[index] = item
				continue
			}
			copy := *value
			copy.Arguments = SanitizeLoneSurrogates(copy.Arguments)
			result[index] = &copy
		case Message:
			value.Content = sanitizePartSurrogates(value.Content)
			result[index] = value
		case *Message:
			if value == nil {
				result[index] = item
				continue
			}
			copy := *value
			copy.Content = sanitizePartSurrogates(copy.Content)
			result[index] = &copy
		case ToolResult:
			value.Content = sanitizePartSurrogates(value.Content)
			result[index] = value
		case *ToolResult:
			if value == nil {
				result[index] = item
				continue
			}
			copy := *value
			copy.Content = sanitizePartSurrogates(copy.Content)
			result[index] = &copy
		default:
			result[index] = item
		}
	}
	return result
}

func sanitizePartSurrogates(parts []Part) []Part {
	if len(parts) == 0 {
		return parts
	}
	result := make([]Part, len(parts))
	for index, part := range parts {
		switch value := part.(type) {
		case JSONPart:
			value.Value = SanitizeLoneSurrogates(value.Value)
			result[index] = value
		case *JSONPart:
			if value == nil {
				result[index] = part
				continue
			}
			copy := *value
			copy.Value = SanitizeLoneSurrogates(copy.Value)
			result[index] = &copy
		default:
			result[index] = part
		}
	}
	return result
}
