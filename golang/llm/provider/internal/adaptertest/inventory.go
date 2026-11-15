package adaptertest

import (
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ModelIDs joins the provider model IDs of models with commas, in order.
func ModelIDs(models []provider.Model) string {
	ids := make([]string, len(models))
	for i, model := range models {
		ids[i] = model.ProviderModelID
	}
	return strings.Join(ids, ",")
}
