package budget

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const identityDomain = "llmtw/budget-identity/v1\x00"

// PolicyIdentity binds a durable policy UUID to the immutable configuration
// snapshot and its operator-facing policy key. A changed snapshot cannot
// accidentally reuse PostgreSQL policy/window rows from an older policy.
func PolicyIdentity(configDigest [32]byte, policyKey string) (string, error) {
	if configDigest == ([32]byte{}) {
		return "", errors.New("budget policy identity requires a configuration digest")
	}
	if policyKey == "" || strings.TrimSpace(policyKey) != policyKey {
		return "", errors.New("budget policy identity requires a canonical policy key")
	}
	material := make([]byte, 0, len(identityDomain)+len("policy\x00")+len(configDigest)+len(policyKey))
	material = append(material, identityDomain...)
	material = append(material, "policy\x00"...)
	material = append(material, configDigest[:]...)
	material = append(material, policyKey...)
	return uuid.NewSHA1(uuid.NameSpaceOID, material).String(), nil
}

// WindowIdentity derives each durable window UUID from its compiled policy
// UUID and stable index. The policy UUID already binds the configuration
// digest, so this remains stable across processes without conflating snapshots.
func WindowIdentity(policyID string, index int) (string, error) {
	policyUUID, err := uuid.Parse(policyID)
	if err != nil {
		return "", fmt.Errorf("budget window identity policy UUID: %w", err)
	}
	if index < 0 {
		return "", errors.New("budget window identity index cannot be negative")
	}
	material := fmt.Sprintf("%swindow\x00%s\x00%d", identityDomain, policyUUID.String(), index)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(material)).String(), nil
}
