package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

const requestDigestDomain = "llmtw/request/v1\x00"

// RequestDigest returns the SHA-256 digest of the canonical normalized request
// with operation_key intentionally excluded. Operation identity is scoped by
// the ledger; tenant remains in Context and therefore remains in this digest.
func RequestDigest(request Request) ([32]byte, error) {
	var zero [32]byte
	normalized, err := NormalizeRequest(request)
	if err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return zero, err
	}
	fields, err := decodeObject(encoded)
	if err != nil {
		return zero, err
	}
	delete(fields, "operation_key")
	withoutOperationKey, err := json.Marshal(fields)
	if err != nil {
		return zero, err
	}
	canonical, err := CanonicalJSON(withoutOperationKey)
	if err != nil {
		return zero, err
	}
	domainLen := len(requestDigestDomain)
	if len(canonical) > math.MaxInt-domainLen {
		return zero, fmt.Errorf("request digest input too large")
	}
	inputCap := domainLen + len(canonical)
	input := make([]byte, 0, inputCap)
	input = append(input, requestDigestDomain...)
	input = append(input, canonical...)
	return sha256.Sum256(input), nil
}

// RequestDigestHex is a stable lowercase representation for logs and
// content-addressed references. It does not include request content.
func RequestDigestHex(request Request) (string, error) {
	digest, err := RequestDigest(request)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// DigestCanonicalJSON returns a domain-independent SHA-256 over one
// canonicalized JSON value. RequestDigest should be used for request identity.
func DigestCanonicalJSON(data []byte) ([32]byte, error) {
	var zero [32]byte
	canonical, err := CanonicalJSON(data)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(canonical), nil
}
