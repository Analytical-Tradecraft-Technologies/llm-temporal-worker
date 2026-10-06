package modelsync

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

// ProviderOpenRouter is the price-catalog provider identity of OpenRouter
// routes.
const ProviderOpenRouter = "openrouter"

// Source is one configured endpoint that serves synced models.
type Source struct {
	EndpointID string
	// RulesProvider names the rules provider a direct endpoint serves; it is
	// also the price-catalog provider identity. Empty for OpenRouter.
	RulesProvider       string
	Family              string
	Region              string
	AccountRegion       string
	EndpointAccountHMAC [32]byte
	// Tiers maps each service class the endpoint offers to its provider_value.
	Tiers            map[llm.ServiceClass]string
	Capabilities     routing.CapabilitySet
	ProviderFeatures map[string]routing.Capability
	ExtensionNames   []string
}

func (source Source) provider() string {
	if source.RulesProvider == "" {
		return ProviderOpenRouter
	}
	return source.RulesProvider
}

// Input is everything one worker needs to derive synced routes and prices.
type Input struct {
	// Document is the latest published OpenRouter catalog; nil before the
	// first fetch completes, when only rules-declared extra models compile.
	Document *Document
	Rules    Rules
	// OpenRouter is nil when the OpenRouter endpoint is disabled.
	OpenRouter *Source
	// Direct endpoints in the configured preference order.
	Direct []Source
	// Reserved names configured logical models. A configured model always
	// wins, so a synced model with the same name is skipped.
	Reserved map[string]struct{}
}

// Compiled is the synced part of one engine snapshot.
type Compiled struct {
	Models map[string]routing.Model
	Prices []pricing.Entry
}

// Compile derives routes and price entries. Every route is unpinned
// (PriceVersion ""), so a quote binds whichever synced price is current when
// the request is planned, and a later refresh never invalidates a persisted
// plan.
func Compile(input Input) (Compiled, error) {
	compiled := Compiled{Models: map[string]routing.Model{}}
	prices := map[string]pricing.Entry{}
	addPrice := func(entry pricing.Entry) {
		key := strings.Join([]string{entry.Provider, entry.Family, entry.EndpointID, entry.Region, entry.Model, entry.ProviderTier}, "\x00")
		prices[key] = entry
	}
	if input.Document != nil {
		encoded, err := input.Document.Encode()
		if err != nil {
			return Compiled{}, err
		}
		version := "openrouter/" + Digest(encoded)[:16]
		provenance := "openrouter@" + input.Document.FetchedAt.UTC().Format("2006-01-02T15:04:05Z")
		for _, model := range input.Document.Models {
			if _, reserved := input.Reserved[model.ID]; reserved || input.Rules.Excluded(model.ID) {
				continue
			}
			routes := make([]routing.Route, 0, len(input.Direct)+1)
			for _, source := range input.Direct {
				route, entries, ok, err := directRoute(input.Rules, source, model, version, provenance)
				if err != nil {
					return Compiled{}, fmt.Errorf("model %q endpoint %q: %w", model.ID, source.EndpointID, err)
				}
				if !ok {
					continue
				}
				routes = append(routes, route)
				for _, entry := range entries {
					addPrice(entry)
				}
			}
			if input.OpenRouter != nil {
				route, entries, err := openRouterRoute(*input.OpenRouter, model, version, provenance)
				if err != nil {
					return Compiled{}, fmt.Errorf("model %q: %w", model.ID, err)
				}
				routes = append(routes, route)
				for _, entry := range entries {
					addPrice(entry)
				}
			}
			if len(routes) > 0 {
				compiled.Models[model.ID] = routing.Model{Name: model.ID, Routes: routes}
			}
		}
	}
	if err := compileExtraModels(input, compiled.Models, addPrice); err != nil {
		return Compiled{}, err
	}
	keys := make([]string, 0, len(prices))
	for key := range prices {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	compiled.Prices = make([]pricing.Entry, 0, len(keys))
	for _, key := range keys {
		compiled.Prices = append(compiled.Prices, prices[key])
	}
	return compiled, nil
}

// compileExtraModels adds the rules-declared direct-only models.
func compileExtraModels(input Input, models map[string]routing.Model, addPrice func(pricing.Entry)) error {
	version := "model-sync-rules/" + input.Rules.Digest[:min(16, len(input.Rules.Digest))]
	for _, source := range input.Direct {
		provider := input.Rules.Providers[source.RulesProvider]
		ids := make([]string, 0, len(provider.ExtraModels))
		for id := range provider.ExtraModels {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if _, reserved := input.Reserved[id]; reserved || input.Rules.Excluded(id) {
				continue
			}
			if existing, listed := models[id]; listed && !extraOnly(existing) {
				// OpenRouter lists the model after all; its published price wins.
				continue
			}
			extra := provider.ExtraModels[id]
			unit, err := extra.Prices.unitPrices()
			if err != nil {
				return fmt.Errorf("extra model %q: %w", id, err)
			}
			classes, tiers := source.classes(func(string) bool { return true })
			route := source.route(extra.Model, classes, extra.ContextTokens, extra.OutputTokens)
			route.ID = "sync-" + source.EndpointID
			for _, tier := range tiers {
				addPrice(pricing.Entry{Provider: source.provider(), Family: source.Family, EndpointID: source.EndpointID, Region: source.Region,
					Model: extra.Model, ProviderTier: tier, Prices: unit, Provenance: "model-sync-rules", Version: version})
			}
			model := models[id]
			model.Name = id
			model.Routes = append(model.Routes, route)
			models[id] = model
		}
	}
	return nil
}

func extraOnly(model routing.Model) bool {
	for _, route := range model.Routes {
		if route.Provider == ProviderOpenRouter {
			return false
		}
	}
	return true
}

func directRoute(rules Rules, source Source, model Model, version, provenance string) (routing.Route, []pricing.Entry, bool, error) {
	directModel, ok := rules.DirectModel(source.RulesProvider, model.ID)
	if !ok {
		return routing.Route{}, nil, false, nil
	}
	tags := rules.Providers[source.RulesProvider].Tiers
	byTag := make(map[string]Endpoint, len(model.Endpoints))
	for _, endpoint := range model.Endpoints {
		byTag[endpoint.Tag] = endpoint
	}
	// A class is offered only when OpenRouter publishes the price of the
	// upstream tier it maps to.
	classes, tiers := source.classes(func(tier string) bool {
		_, priced := byTag[tags[tier]]
		return tags[tier] != "" && priced
	})
	if len(classes) == 0 {
		return routing.Route{}, nil, false, nil
	}
	entries := make([]pricing.Entry, 0, len(tiers))
	contextTokens, outputTokens := model.ContextLength, model.MaxCompletionTokens
	for _, class := range classes {
		tier := source.Tiers[class]
		endpoint := byTag[tags[tier]]
		unit, err := basePrices(endpoint.Pricing)
		if err != nil {
			return routing.Route{}, nil, false, err
		}
		entries = append(entries, pricing.Entry{Provider: source.provider(), Family: source.Family, EndpointID: source.EndpointID, Region: source.Region,
			Model: directModel, ProviderTier: tier, Prices: unit, Provenance: provenance + " tag " + endpoint.Tag, Version: version})
		contextTokens = minPositive(contextTokens, endpoint.ContextLength)
		outputTokens = minPositive(outputTokens, endpoint.MaxCompletionTokens)
		// The base price applies only below the first long-prompt override,
		// so the route never admits a prompt that would be billed above it.
		if len(endpoint.Pricing.Overrides) > 0 {
			contextTokens = minPositive(contextTokens, endpoint.Pricing.Overrides[0].MinPromptTokens)
		}
	}
	route := source.route(directModel, classes, contextTokens, outputTokens)
	route.ID = "sync-" + source.EndpointID
	narrowCapabilities(&route, model)
	return route, entries, true, nil
}

func openRouterRoute(source Source, model Model, version, provenance string) (routing.Route, []pricing.Entry, error) {
	unit, err := maxPrices(model.Endpoints)
	if err != nil {
		return routing.Route{}, nil, err
	}
	classes, tiers := source.classes(func(string) bool { return true })
	entries := make([]pricing.Entry, 0, len(tiers))
	for _, tier := range tiers {
		entries = append(entries, pricing.Entry{Provider: ProviderOpenRouter, Family: source.Family, EndpointID: source.EndpointID, Region: source.Region,
			Model: model.ID, ProviderTier: tier, Prices: unit, Provenance: provenance + " max over upstream endpoints", Version: version})
	}
	route := source.route(model.ID, classes, model.ContextLength, model.MaxCompletionTokens)
	route.ID = "sync-" + source.EndpointID
	narrowCapabilities(&route, model)
	return route, entries, nil
}

// classes returns the endpoint's service classes in canonical order, keeping
// those whose provider tier passes keep, and the distinct tiers they use.
func (source Source) classes(keep func(string) bool) ([]llm.ServiceClass, []string) {
	classes := make([]llm.ServiceClass, 0, len(source.Tiers))
	seen := map[string]struct{}{}
	tiers := make([]string, 0, len(source.Tiers))
	for _, class := range []llm.ServiceClass{llm.ServiceClassEconomy, llm.ServiceClassStandard, llm.ServiceClassPriority} {
		tier, offered := source.Tiers[class]
		if !offered || !keep(tier) {
			continue
		}
		classes = append(classes, class)
		if _, duplicate := seen[tier]; !duplicate {
			seen[tier] = struct{}{}
			tiers = append(tiers, tier)
		}
	}
	return classes, tiers
}

func (source Source) route(model string, classes []llm.ServiceClass, contextTokens, outputTokens int64) routing.Route {
	providerTiers := make(map[llm.ServiceClass]string, len(classes))
	for _, class := range classes {
		providerTiers[class] = source.Tiers[class]
	}
	capabilities := routing.CapabilitySet{Version: source.Capabilities.Version, Features: make(map[routing.Feature]routing.Capability, len(source.Capabilities.Features))}
	for feature, capability := range source.Capabilities.Features {
		capabilities.Features[feature] = capability
	}
	return routing.Route{
		EndpointID:          source.EndpointID,
		Provider:            source.provider(),
		Family:              source.Family,
		Region:              source.Region,
		AccountRegion:       source.AccountRegion,
		EndpointAccountHMAC: source.EndpointAccountHMAC,
		Model:               model,
		ModelLineage:        model,
		Classes:             classes,
		ProviderTiers:       providerTiers,
		Capabilities:        capabilities,
		ProviderFeatures:    source.ProviderFeatures,
		PriceAvailable:      true,
		ExtensionNames:      append([]string(nil), source.ExtensionNames...),
		OutputTokens:        max(outputTokens, 0),
		ContextTokens:       max(contextTokens, 0),
	}
}

// narrowCapabilities marks a routing feature unsupported when OpenRouter's
// supported_parameters show the model cannot accept it, so planning rejects
// the route instead of the provider rejecting the request. It only narrows:
// the capability version still identifies the endpoint's declaration.
func narrowCapabilities(route *routing.Route, model Model) {
	if len(model.SupportedParameters) == 0 {
		return
	}
	supported := make(map[string]struct{}, len(model.SupportedParameters))
	for _, parameter := range model.SupportedParameters {
		supported[parameter] = struct{}{}
	}
	requires := map[routing.Feature][]string{
		routing.FeatureToolCall:         {"tools"},
		routing.FeatureStructuredOutput: {"response_format", "structured_outputs"},
		routing.FeatureReasoning:        {"reasoning", "include_reasoning", "reasoning_effort"},
	}
	for feature, parameters := range requires {
		capability, declared := route.Capabilities.Features[feature]
		if !declared || capability.State == routing.CapabilityUnsupported {
			continue
		}
		found := false
		for _, parameter := range parameters {
			if _, ok := supported[parameter]; ok {
				found = true
				break
			}
		}
		if !found {
			route.Capabilities.Features[feature] = routing.Capability{State: routing.CapabilityUnsupported, Reason: "OpenRouter does not list a supporting parameter for this model"}
		}
	}
}

func minPositive(current, candidate int64) int64 {
	switch {
	case candidate <= 0:
		return current
	case current <= 0:
		return candidate
	default:
		return min(current, candidate)
	}
}

// basePrices converts one endpoint's published base price to catalog unit
// prices. Reasoning is billed inside output on every synced family, so its
// separate price is zero. An unlisted cache-read price is charged as input
// and an unlisted cache-write price as free, matching OpenRouter's listing.
func basePrices(prices Pricing) (pricing.UnitPrices, error) {
	cacheRead := prices.InputCacheRead
	if cacheRead == "" {
		cacheRead = prices.Prompt
	}
	return unitPrices(prices.Prompt, prices.Completion, cacheRead, maxPrice(prices.InputCacheWrite, prices.InputCacheWrite1h), prices.Request)
}

// maxPrices is the componentwise maximum over every upstream endpoint and
// long-prompt override, the most OpenRouter can bill for one request.
func maxPrices(endpoints []Endpoint) (pricing.UnitPrices, error) {
	var input, output, cacheRead, cacheWrite, request string
	for _, endpoint := range endpoints {
		prices := endpoint.Pricing
		read := prices.InputCacheRead
		if read == "" {
			read = prices.Prompt
		}
		input, output = maxPrice(input, prices.Prompt), maxPrice(output, prices.Completion)
		cacheRead = maxPrice(cacheRead, read)
		cacheWrite = maxPrice(cacheWrite, maxPrice(prices.InputCacheWrite, prices.InputCacheWrite1h))
		request = maxPrice(request, prices.Request)
		for _, override := range prices.Overrides {
			input, output = maxPrice(input, override.Prompt), maxPrice(output, override.Completion)
			cacheRead = maxPrice(cacheRead, override.InputCacheRead)
			cacheWrite = maxPrice(cacheWrite, maxPrice(override.InputCacheWrite, override.InputCacheWrite1h))
		}
	}
	return unitPrices(input, output, cacheRead, cacheWrite, request)
}

func unitPrices(input, output, cacheRead, cacheWrite, request string) (pricing.UnitPrices, error) {
	var result pricing.UnitPrices
	var err error
	if result.InputPerMillion, err = perMillion(input); err != nil {
		return pricing.UnitPrices{}, fmt.Errorf("input price: %w", err)
	}
	if result.OutputPerMillion, err = perMillion(output); err != nil {
		return pricing.UnitPrices{}, fmt.Errorf("output price: %w", err)
	}
	if result.CacheReadPerMillion, err = perMillion(cacheRead); err != nil {
		return pricing.UnitPrices{}, fmt.Errorf("cache read price: %w", err)
	}
	if cacheWrite == "" {
		cacheWrite = "0"
	}
	if result.CacheWritePerMillion, err = perMillion(cacheWrite); err != nil {
		return pricing.UnitPrices{}, fmt.Errorf("cache write price: %w", err)
	}
	if request == "" {
		request = "0"
	}
	// A request price is already absolute USD, not per token.
	if result.PerRequest, err = pricing.ParseDecimalUSD(request); err != nil {
		return pricing.UnitPrices{}, fmt.Errorf("request price: %w", err)
	}
	result.ReasoningPerMillion = pricing.MustDecimalUSD("0")
	return result, nil
}

// maxPrice returns the larger of two valid decimal strings; an empty string
// is unlisted and loses to any listed price.
func maxPrice(left, right string) string {
	if left == "" {
		return right
	}
	if right == "" {
		return left
	}
	leftValue, leftErr := pricing.ParseUSD(left)
	rightValue, rightErr := pricing.ParseUSD(right)
	if leftErr != nil || rightErr != nil {
		// Validation already rejected malformed prices; keep the left value
		// so an impossible error cannot lower a price.
		return left
	}
	if rightValue.Cmp(leftValue) > 0 {
		return right
	}
	return left
}
