// Package toolcallid maps tool-call IDs onto the portable form that the
// Anthropic and Bedrock families accept for tool_use.id and tool_use_id.
//
// Those families require ^[A-Za-z0-9_-]+$ (Bedrock Converse also caps the
// length at 64), while other families return IDs such as
// "functions.lookup:0". A transcript that started on another family would
// otherwise be rejected after admission. The mapping is deterministic and
// applied to both the call and its result, so pairing is preserved.
package toolcallid

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// MaxLength is the longest ID that every target family accepts.
const MaxLength = 64

// hashLength is the number of hex characters of the original ID's SHA-256
// digest appended to a remapped ID so that distinct originals stay distinct.
const hashLength = 16

// Portable returns id unchanged when it already matches
// ^[A-Za-z0-9_-]{1,64}$. Otherwise it replaces each disallowed byte with
// '_', truncates the result, and appends '_' and a digest of the original.
func Portable(id string) string {
	if valid(id) {
		return id
	}
	var builder strings.Builder
	for index := 0; index < len(id); index++ {
		if allowed(id[index]) {
			builder.WriteByte(id[index])
		} else {
			builder.WriteByte('_')
		}
	}
	prefix := builder.String()
	if limit := MaxLength - hashLength - 1; len(prefix) > limit {
		prefix = prefix[:limit]
	}
	digest := sha256.Sum256([]byte(id))
	return prefix + "_" + hex.EncodeToString(digest[:])[:hashLength]
}

func valid(id string) bool {
	if len(id) == 0 || len(id) > MaxLength {
		return false
	}
	for index := 0; index < len(id); index++ {
		if !allowed(id[index]) {
			return false
		}
	}
	return true
}

func allowed(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
		(char >= '0' && char <= '9') || char == '_' || char == '-'
}
