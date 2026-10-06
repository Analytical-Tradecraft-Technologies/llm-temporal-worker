package runtime

import "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"

// The bounded cloud runtime binds Redis directly to the same immutable
// snapshot. It uses the authoritative Redis budget port and
// never invokes a composition factory to obtain another budget authority.
func (capabilities V1RuntimeCapabilities) cloudBudgetBoundary() (durable.BudgetBoundary, error) {
	identity := durable.StateIdentity{Cloud: capabilities.CloudIdentity, Redis: capabilities.RedisIdentity, ConfigDigest: capabilities.ConfigDigest}
	if err := identity.Cloud.Validate(); err != nil {
		return durable.BudgetBoundary{}, err
	}
	boundary := durable.BudgetBoundary{Identity: identity, Materializer: capabilities.Budgets}
	if err := boundary.Validate(); err != nil {
		return durable.BudgetBoundary{}, err
	}
	return boundary, nil
}
