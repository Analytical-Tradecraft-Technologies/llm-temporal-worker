package llm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	fields, err := normalized.jsonFields()
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
	hash := sha256.New()
	_, _ = hash.Write([]byte(requestDigestDomain))
	_, _ = hash.Write(canonical)
	var digest [32]byte
	hash.Sum(digest[:0])
	return digest, nil
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
