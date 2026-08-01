package runtime

import (
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

// requireDurableV1RuntimeBuilder prevents a production durable snapshot from
// being returned with only the legacy engine composition. The legacy engine
// remains available to development fixtures and direct helper tests, but it
// does not implement the immutable checkpoint, operation, and budget contract
// exposed by the versioned Activities.
func requireDurableV1RuntimeBuilder(value config.Config, builder V1RuntimeBuilder) error {
	if value.State.Kind != config.StateKindDurable || value.Environment == "development" {
		return nil
	}
	if builder == nil {
		return fmt.Errorf("%w: production durable snapshots require V1RuntimeBuilder", ErrDurableV1Composition)
	}
	return nil
}
