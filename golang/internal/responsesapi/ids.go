package responsesapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

const (
	responseIDPrefix = "resp_"
	// idempotencyKeyMaxBytes keeps "resp_" plus the key inside the v1
	// operation key bound and the workflow ID well inside Temporal's limit.
	idempotencyKeyMaxBytes = 200
	workflowIDPrefix       = "llm-responses-api/"
)

// The response ID is also the Generate operation key, so a poll reads the
// durable operation record directly. Generated IDs are random; an
// Idempotency-Key header makes the ID deterministic so a retried POST
// reattaches to the same paid request.
func newResponseID(random io.Reader) (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(random, value[:]); err != nil {
		return "", err
	}
	return responseIDPrefix + hex.EncodeToString(value[:]), nil
}

func idempotentResponseID(key string) (string, bool) {
	if len(key) == 0 || len(key) > idempotencyKeyMaxBytes {
		return "", false
	}
	for index := 0; index < len(key); index++ {
		if !idCharacter(key[index]) {
			return "", false
		}
	}
	return responseIDPrefix + key, true
}

func validResponseID(id string) bool {
	if len(id) <= len(responseIDPrefix) || id[:len(responseIDPrefix)] != responseIDPrefix {
		return false
	}
	_, ok := idempotentResponseID(id[len(responseIDPrefix):])
	return ok
}

func idCharacter(character byte) bool {
	return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' || character == ':'
}

// workflowID binds the response ID to the caller's tenant and project. Two
// tenants that choose the same Idempotency-Key get different workflows, and a
// key for one project cannot address another project's workflow.
func workflowID(identity llm.RequestContext, responseID string) string {
	digest := sha256.New()
	for _, component := range []string{identity.Tenant, identity.Project} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(component)))
		digest.Write(length[:])
		digest.Write([]byte(component))
	}
	return workflowIDPrefix + hex.EncodeToString(digest.Sum(nil)[:16]) + "/" + responseID
}

var defaultRandom io.Reader = rand.Reader
