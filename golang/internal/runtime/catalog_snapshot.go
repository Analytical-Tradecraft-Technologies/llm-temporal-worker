package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// SnapshotLoader is the catalog-to-engine composition boundary. Catalog
// loading and verification stay in internal/catalog; this interface only
// publishes the complete immutable engine snapshot consumed by one request.
type SnapshotLoader interface {
	Load(context.Context, *config.Snapshot) (engine.Snapshot, error)
}

type SnapshotLoaderFunc func(context.Context, *config.Snapshot) (engine.Snapshot, error)

func (function SnapshotLoaderFunc) Load(ctx context.Context, snapshot *config.Snapshot) (engine.Snapshot, error) {
	return function(ctx, snapshot)
}

// CatalogSnapshotLoader is the default loader used by the production
// composition layer. It consumes verified capability and pricing catalogs and
// compiles config routes into routing.Catalog without duplicating catalog
// parsing or digest verification.
type CatalogSnapshotLoader struct {
	CatalogOptions catalog.Options
	Clock          func() time.Time
}

func (loader CatalogSnapshotLoader) Load(ctx context.Context, snapshot *config.Snapshot) (engine.Snapshot, error) {
	if snapshot == nil {
		return engine.Snapshot{}, fmt.Errorf("configuration snapshot is required")
	}
	if err := ctx.Err(); err != nil {
		return engine.Snapshot{}, err
	}
	clock := loader.Clock
	if clock == nil {
		clock = time.Now
	}
	value := snapshot.Config()
	bundle, err := catalog.LoadWithOptions(value, loader.CatalogOptions)
	if err != nil {
		return engine.Snapshot{}, fmt.Errorf("load verified catalogs: %w", err)
	}
	now := clock()
	price, err := mergePricingCatalogs(bundle, snapshot.ConfigVersion())
	if err != nil {
		return engine.Snapshot{}, err
	}
	routes, err := compileRoutes(value, bundle, now)
	if err != nil {
		return engine.Snapshot{}, err
	}
	policies, err := compileBudgetPolicies(value)
	if err != nil {
		return engine.Snapshot{}, err
	}
	return engine.Snapshot{
		Version:                  snapshot.ConfigVersion(),
		ConfigDigest:             snapshot.Digest(),
		ConfigEpoch:              snapshot.ConfigVersion(),
		Routes:                   routes,
		Health:                   routing.HealthView{},
		Prices:                   pricing.NewResolver(price),
		BudgetPolicies:           policies,
		RequireBudgetMatch:       value.Budgets.RequireMatch,
		RequirePriceWhenBudgeted: value.Pricing.RequirePriceWhenBudgeted,
		Environment:              value.Environment,
		ReservationLease:         time.Duration(value.State.ReservationLease),
		OperationRetention:       time.Duration(value.State.OperationTerminalRetention),
		ContinuationRetention:    time.Duration(value.State.ContinuationRetention),
	}, nil
}

func mergePricingCatalogs(bundle catalog.Bundle, configVersion string) (pricing.Catalog, error) {
	ids := make([]string, 0, len(bundle.Pricing))
	for id := range bundle.Pricing {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return pricing.Catalog{}, fmt.Errorf("verified pricing catalogs are required")
	}
	entries := make([]pricing.Entry, 0)
	for _, id := range ids {
		catalogValue := bundle.Pricing[id].Catalog
		if len(catalogValue.Entries) == 0 {
			return pricing.Catalog{}, fmt.Errorf("pricing catalog %q has no entries", id)
		}
		entries = append(entries, catalogValue.Entries...)
	}
	version := "runtime-prices"
	if strings.TrimSpace(configVersion) != "" {
		version += "/" + configVersion
	}
	merged, err := pricing.CompileUSD(version, entries)
	if err != nil {
		return pricing.Catalog{}, fmt.Errorf("merge pricing catalogs: %w", err)
	}
	return merged, nil
}

func compileRoutes(value config.Config, bundle catalog.Bundle, now time.Time) (routing.Catalog, error) {
	models := make(map[string]routing.Model, len(value.Models))
	modelNames := make([]string, 0, len(value.Models))
	for name := range value.Models {
		modelNames = append(modelNames, name)
	}
	sort.Strings(modelNames)
	for _, modelName := range modelNames {
		modelValue := value.Models[modelName]
		routes := make([]routing.Route, 0, len(modelValue.Routes))
		for index, routeValue := range modelValue.Routes {
			endpoint, ok := value.Endpoints[routeValue.Endpoint]
			if !ok {
				return routing.Catalog{}, fmt.Errorf("model %q route %d references missing endpoint %q", modelName, index, routeValue.Endpoint)
			}
			profile, ok := bundle.Capabilities[endpoint.CapabilityProfile]
			if !ok {
				return routing.Catalog{}, fmt.Errorf("endpoint %q capability profile %q is unavailable", routeValue.Endpoint, endpoint.CapabilityProfile)
			}
			family := endpointFamily(endpoint.Family)
			if profile.Family != family {
				return routing.Catalog{}, fmt.Errorf("endpoint %q capability family %q does not match %q", routeValue.Endpoint, profile.Family, family)
			}
			if profile.Model != routeValue.Model {
				return routing.Catalog{}, fmt.Errorf("route %q model %q does not match capability profile %q model %q", routeValue.ID, routeValue.Model, endpoint.CapabilityProfile, profile.Model)
			}
			if family == provider.FamilyBedrockConverse && profile.ServiceClassesDeclared {
				supportedClasses := make(map[llm.ServiceClass]struct{}, len(profile.ServiceClasses))
				for _, class := range profile.ServiceClasses {
					supportedClasses[class] = struct{}{}
				}
				for _, class := range routeValue.Classes {
					if _, supported := supportedClasses[class]; !supported {
						return routing.Catalog{}, fmt.Errorf("route %q service class %q is not declared by Bedrock capability profile %q for model %q", routeValue.ID, class, endpoint.CapabilityProfile, profile.Model)
					}
				}
			}
			providerName, routeRegion, priceVersion, priceAvailable, err := routePriceIdentity(bundle, routeValue.Endpoint, endpoint, routeValue.Model, routeValue.Classes, now)
			if err != nil {
				return routing.Catalog{}, fmt.Errorf("model %q route %q: %w", modelName, routeValue.ID, err)
			}
			providerTiers := make(map[llm.ServiceClass]string, len(routeValue.Classes))
			for _, class := range routeValue.Classes {
				tier := endpoint.ServiceClasses[class].ProviderValue
				if tier == "" {
					return routing.Catalog{}, fmt.Errorf("route class %q has no provider tier", class)
				}
				providerTiers[class] = tier
			}
			extensions := make([]string, 0, len(endpoint.Extensions))
			for name := range endpoint.Extensions {
				extensions = append(extensions, name)
			}
			sort.Strings(extensions)
			routes = append(routes, routing.Route{
				ID:                  routeValue.ID,
				EndpointID:          routeValue.Endpoint,
				Provider:            providerName,
				Family:              string(family),
				Region:              routeRegion,
				AccountRegion:       endpoint.AccountRegion,
				EndpointAccountHMAC: routing.DeriveEndpointAccountHMAC(providerName, routeValue.Endpoint, endpoint.AccountRegion, routeRegion, value.Version),
				Model:               routeValue.Model,
				ModelLineage:        routeValue.Model,
				Classes:             append([]llm.ServiceClass(nil), routeValue.Classes...),
				ProviderTiers:       providerTiers,
				AllowedTenants:      append([]string(nil), modelValue.AllowedTenants...),
				AllowedRegions:      append([]string(nil), modelValue.DataRegions...),
				Capabilities:        routingCapabilities(profile.Set),
				PriceVersion:        priceVersion,
				PriceAvailable:      priceAvailable,
				ExtensionNames:      extensions,
			})
		}
		models[modelName] = routing.Model{Name: modelName, Routes: routes}
	}
	return routing.CompileCatalog(value.Version, models)
}

func routePriceIdentity(bundle catalog.Bundle, endpointID string, endpoint config.EndpointConfig, model string, classes []llm.ServiceClass, now time.Time) (string, string, string, bool, error) {
	priceCatalog, ok := bundle.Pricing[endpoint.PriceCatalog]
	if !ok {
		return "", "", "", false, fmt.Errorf("price catalog %q is unavailable", endpoint.PriceCatalog)
	}
	// A current quote is not the source of the endpoint identity. The verified
	// catalog supplies that identity even when this route's model/tier is
	// deliberately unpriced, so the engine can make the candidate-level policy
	// decision without guessing a provider or region.
	providerName, routeRegion, err := routeCatalogIdentity(priceCatalog.Catalog.Entries, endpointID, endpoint, now)
	if err != nil {
		return "", "", "", false, err
	}
	var priceVersion string
	priceAvailable := true
	family := endpointFamily(endpoint.Family)
	for _, class := range classes {
		tier := endpoint.ServiceClasses[class].ProviderValue
		var found *pricing.Entry
		for index := range priceCatalog.Catalog.Entries {
			entry := &priceCatalog.Catalog.Entries[index]
			// Keep the identity checks explicit: provider names are catalog data,
			// while endpoint/family/model/tier are operator configuration.
			if entry.EndpointID != endpointID || entry.Family != string(family) || entry.Model != model || entry.ProviderTier != tier || !entry.Active(now) {
				continue
			}
			if endpoint.Region != "" && entry.Region != endpoint.Region {
				continue
			}
			if found != nil {
				return "", "", "", false, fmt.Errorf("multiple active price entries for model %q tier %q", model, tier)
			}
			copy := *entry
			found = &copy
		}
		if found == nil {
			priceAvailable = false
			continue
		}
		if found.Provider != providerName || found.Region != routeRegion {
			return "", "", "", false, fmt.Errorf("active price entry does not match verified endpoint identity")
		}
		entryVersion := found.Version
		if entryVersion == "" {
			entryVersion = priceCatalog.Version
		}
		if priceVersion == "" {
			priceVersion = entryVersion
		} else if priceVersion != entryVersion {
			return "", "", "", false, fmt.Errorf("price entries use multiple versions")
		}
	}
	return providerName, routeRegion, priceVersion, priceAvailable, nil
}

func routeCatalogIdentity(entries []pricing.Entry, endpointID string, endpoint config.EndpointConfig, now time.Time) (string, string, error) {
	family := endpointFamily(endpoint.Family)
	active := make([]pricing.Entry, 0)
	references := make([]pricing.Entry, 0)
	for _, entry := range entries {
		if entry.EndpointID != endpointID || entry.Family != string(family) {
			continue
		}
		if endpoint.Region != "" && entry.Region != endpoint.Region {
			continue
		}
		references = append(references, entry)
		if entry.Active(now) {
			active = append(active, entry)
		}
	}
	if len(references) == 0 {
		return "", "", fmt.Errorf("no price entry for endpoint %q family %q", endpointID, family)
	}
	// Prefer a current catalog identity. If the whole endpoint is between price
	// intervals, accept an identity only when every verified historical/future
	// reference agrees; otherwise refusing the unpriced route is safer than
	// choosing a provider or region from an arbitrary interval.
	identity := active
	if len(identity) == 0 {
		identity = references
	}
	providerName, routeRegion := identity[0].Provider, identity[0].Region
	for _, entry := range identity[1:] {
		if entry.Provider != providerName {
			return "", "", fmt.Errorf("price entries use multiple providers")
		}
		if entry.Region != routeRegion {
			return "", "", fmt.Errorf("price entries use multiple regions")
		}
	}
	return providerName, routeRegion, nil
}

func compileBudgetPolicies(value config.Config) ([]budget.Policy, error) {
	policies := make([]budget.Policy, 0, len(value.Budgets.Policies))
	for _, policyValue := range value.Budgets.Policies {
		policy := budget.Policy{ID: policyValue.ID, Match: budget.Matcher{
			Tenant:       policyValue.Match.Tenant,
			Project:      policyValue.Match.Project,
			ActorPrefix:  policyValue.Match.ActorPrefix,
			Environment:  policyValue.Match.Environment,
			LogicalModel: policyValue.Match.LogicalModel,
			EndpointID:   policyValue.Match.EndpointID,
			ServiceClass: policyValue.Match.ServiceClass,
		}}
		policy.Windows = make([]budget.Window, 0, len(policyValue.Windows))
		for index, windowValue := range policyValue.Windows {
			legacyLimit, err := compatibilityBudgetLimit(windowValue.LimitUSD, pricing.MicroUSD(windowValue.LimitMicroUSD))
			if err != nil {
				return nil, fmt.Errorf("budget policy %q window %d: %w", policyValue.ID, index, err)
			}
			policy.Windows = append(policy.Windows, budget.Window{ID: fmt.Sprintf("%s/%d", policyValue.ID, index), Duration: time.Duration(windowValue.Duration), Bucket: time.Duration(windowValue.Bucket), LimitUSD: windowValue.LimitUSD, Limit: legacyLimit})
		}
		if err := policy.Validate(value.Limits.MaxBudgetBucketsPerWindow); err != nil {
			return nil, fmt.Errorf("budget policy %q: %w", policy.ID, err)
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

// compatibilityBudgetLimit materializes an exact config limit for the legacy
// Redis admission boundary. A positive sub-micro USD limit is rounded up to
// one micro-dollar so an exact-only budget cannot become an accidental zero
// limit while the admission wire contract remains integer microUSD.
func compatibilityBudgetLimit(exact pricing.USD, legacy pricing.MicroUSD) (pricing.MicroUSD, error) {
	if exact.IsZero() {
		return legacy, nil
	}
	converted, err := pricing.MicroFromUSD(exact)
	if err != nil {
		return 0, fmt.Errorf("exact budget limit cannot be represented at Redis boundary: %w", err)
	}
	if converted == 0 {
		return 1, nil
	}
	return converted, nil
}

func endpointFamily(value string) provider.Family {
	if value == "azure_openai_responses" {
		return provider.FamilyOpenAIResponses
	}
	if value == "azure_openai_chat" {
		return provider.FamilyOpenAIChat
	}
	if value == "anthropic_aws_messages" {
		return provider.FamilyAnthropicMessages
	}
	if value == "bedrock_anthropic_messages" {
		return provider.FamilyBedrockMessages
	}
	if value == "bedrock_converse" {
		return provider.FamilyBedrockConverse
	}
	return provider.Family(value)
}

// routingCapabilities narrows the provider contract to the capability fields
// consumed by route planning. Provider-specific adapter features remain on the
// provider profile and are not silently widened into routing behavior.
func routingCapabilities(value provider.CapabilitySet) routing.CapabilitySet {
	result := routing.CapabilitySet{Version: value.Version, Features: make(map[routing.Feature]routing.Capability)}
	for source, target := range map[provider.Feature]routing.Feature{
		provider.FeatureText:             routing.FeatureText,
		provider.FeatureToolCall:         routing.FeatureToolCall,
		provider.FeatureStructuredOutput: routing.FeatureStructuredOutput,
		provider.FeatureReasoning:        routing.FeatureReasoning,
		provider.FeatureContinuation:     routing.FeatureContinuation,
	} {
		if capability, ok := value.Features[source]; ok {
			result.Features[target] = routing.Capability{State: routing.CapabilityState(capability.State), Transform: capability.Transform, Reason: capability.Reason}
		}
	}
	return result
}
