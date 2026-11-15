package bedrockmessages

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestProfileAcceptsUndeclaredServiceClassesAsUnsupported(t *testing.T) {
	adaptertest.UndeclaredServiceClassesAreUnsupported(t, func(tiers map[llm.ServiceClass]string) (func(llm.ServiceClass) (string, error), error) {
		profile := DefaultProfile("standard-only")
		profile.ServiceTiers = tiers
		validated, err := NewProfile(profile)
		return validated.providerTier, err
	})
}
