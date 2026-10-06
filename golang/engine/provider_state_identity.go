package engine

import "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"

// providerStateIdentity is the provider/family label an adapter puts on the
// opaque state it lifts (for example anthropic/messages), keyed by the routing
// family that produced it. Continuation storage pins state to the route's
// identity instead (configured provider, anthropic_messages), so finalization
// relabels state on write and toProviderContinuation restores the adapter
// label on read.
var providerStateIdentity = map[string]struct{ provider, family string }{
	string(provider.FamilyOpenAIResponses):   {provider: "openai", family: "responses"},
	string(provider.FamilyAnthropicMessages): {provider: "anthropic", family: "messages"},
	string(provider.FamilyBedrockMessages):   {provider: "bedrock", family: "messages"},
}

// adapterStateMatchesRoute reports whether lifted state carries the adapter
// label the route's family produces.
func adapterStateMatchesRoute(routeFamily, stateProvider, stateFamily string) bool {
	identity, ok := providerStateIdentity[routeFamily]
	return ok && identity.provider == stateProvider && identity.family == stateFamily
}

// adapterStateLabel returns the adapter label for state stored under a route
// family, or the stored values when the family has no known label.
func adapterStateLabel(routeFamily, storedProvider string) (string, string) {
	if identity, ok := providerStateIdentity[routeFamily]; ok {
		return identity.provider, identity.family
	}
	return storedProvider, routeFamily
}
