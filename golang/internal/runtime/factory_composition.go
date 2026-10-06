package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	durablestore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

// requireDurableV1RuntimeBuilder prevents a production durable snapshot from
// being returned with only the legacy engine composition. The legacy engine
// remains available to development fixtures and direct helper tests, but it
// does not implement the immutable checkpoint, operation, and budget contract
// exposed by the versioned Activities.
func requireDurableV1RuntimeBuilder(value config.Config, builder V1RuntimeBuilder) error {
	if value.State.Requests != nil && builder == nil {
		return fmt.Errorf("%w: cloud requests require V1RuntimeBuilder", ErrDurableV1Composition)
	}
	if value.State.Kind != config.StateKindDurable || !config.IsProductionEnvironment(value.Environment) {
		return nil
	}
	if builder == nil {
		return fmt.Errorf("%w: production durable snapshots require V1RuntimeBuilder", ErrDurableV1Composition)
	}
	return nil
}

// cloudRequestIdentity binds aliases and their physical provider mappings to
// composition without copying mutable maps or resolving any secret.
func cloudRequestIdentity(value *config.CloudRequestConfig) (durablestore.CloudIdentity, error) {
	if value == nil {
		return durablestore.CloudIdentity{}, nil
	}
	encoded, err := json.Marshal(value.Provider)
	if err != nil {
		return durablestore.CloudIdentity{}, fmt.Errorf("%w: encode cloud provider identity", ErrDurableV1Composition)
	}
	identity := durablestore.CloudIdentity{Provider: value.Provider.Type, Namespace: value.Namespace, RequestTable: value.RequestTable, PayloadStore: value.PayloadStore, ProviderDigest: sha256.Sum256(encoded)}
	if err := identity.Validate(); err != nil {
		return durablestore.CloudIdentity{}, err
	}
	return identity, nil
}
