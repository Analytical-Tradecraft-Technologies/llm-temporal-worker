package routing

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

type DeterministicPlanner struct {
	MaxRejections int
}

func (planner DeterministicPlanner) Plan(ctx context.Context, input Input) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	request, err := llm.NormalizeRequest(input.Request)
	if err != nil {
		return Plan{}, fmt.Errorf("normalize request: %w", err)
	}
	requested, err := llm.NormalizeServiceClass(request.ServiceClass)
	if err != nil {
		return Plan{}, err
	}
	if err := llm.ValidateServiceClassFallbacks(requested, request.ServiceClassFallbacks); err != nil {
		return Plan{}, err
	}
	if request.Model == "" {
		return Plan{}, fmt.Errorf("request model is required")
	}
	if input.Affinity != nil && input.Now.IsZero() {
		return Plan{}, fmt.Errorf("provider cache affinity planning time is required")
	}
	model, ok := input.Catalog.Models[request.Model]
	if !ok {
		return Plan{Model: request.Model, Version: input.Catalog.Version}, fmt.Errorf("no configured routes for logical model %q", request.Model)
	}
	classes := append([]llm.ServiceClass{requested}, request.ServiceClassFallbacks...)
	extensionDigest := extensionDigest(request)
	plan := Plan{Version: input.Catalog.Version, Model: request.Model}
	for fallbackIndex, class := range classes {
		for routeIndex, route := range model.Routes {
			if err := validateRouteShape(route); err != nil {
				if len(plan.Rejections) < planner.limit() {
					plan.Rejections = append(plan.Rejections, Rejection{Code: RejectInvalid, RouteID: route.ID, Detail: err.Error()})
				}
				continue
			}
			candidate, rejection, eligible := planner.evaluate(request, input.Continuation, input.Health, route, requested, class, fallbackIndex, routeIndex, extensionDigest)
			if !eligible {
				if len(plan.Rejections) < planner.limit() {
					plan.Rejections = append(plan.Rejections, rejection)
				}
				continue
			}
			plan.Candidates = append(plan.Candidates, candidate)
		}
	}
	sortedRejections(plan.Rejections)
	if input.Affinity != nil {
		preferred, err := PreferProviderCacheAffinity(plan, input.Affinity, input.Now)
		if err != nil {
			return Plan{}, err
		}
		plan = preferred
	}
	plan.Digest = digestPlan(plan)
	plan.DigestHex = fmt.Sprintf("%x", plan.Digest[:])
	if len(plan.Candidates) == 0 {
		return plan, fmt.Errorf("no eligible route candidates")
	}
	return plan, nil
}

func (planner DeterministicPlanner) limit() int {
	if planner.MaxRejections <= 0 {
		return 256
	}
	return planner.MaxRejections
}

func (planner DeterministicPlanner) evaluate(request llm.Request, continuation state.Constraints, health HealthView, route Route, requested, attempted llm.ServiceClass, fallbackIndex, routeIndex int, extensionDigest string) (Candidate, Rejection, bool) {
	reject := func(code, path, detail string) (Candidate, Rejection, bool) {
		return Candidate{}, Rejection{Code: code, RouteID: route.ID, Path: path, Detail: detail}, false
	}
	if len(route.AllowedTenants) > 0 {
		if request.Context.Tenant == "" {
			return reject(RejectTenant, "context.tenant", "route requires a request tenant")
		}
		if !containsString(route.AllowedTenants, request.Context.Tenant) {
			return reject(RejectTenant, "context.tenant", "route does not permit the request tenant")
		}
	}
	region := request.Context.Tags["region"]
	if region != "" && len(route.AllowedRegions) > 0 && !containsString(route.AllowedRegions, region) {
		return reject(RejectRegion, "context.tags.region", "route does not permit the requested region")
	}
	if route.Model == "" {
		return reject(RejectModel, "route.model", "route model is empty")
	}
	if !containsClass(route.Classes, attempted) {
		return reject(RejectClass, "route.classes", fmt.Sprintf("route is not authorized for service class %s", attempted))
	}
	status := health.forRoute(route.ID)
	if !status.Enabled || status.Open || status.AuthOpen {
		return reject(RejectHealth, "health", "route is disabled or health-open")
	}
	for feature, capability := range route.Capabilities.Features {
		_ = feature
		if !capability.State.Valid() {
			return reject(RejectCapability, "capabilities", "route contains an invalid capability state")
		}
	}
	for _, feature := range requiredFeatures(request, continuation) {
		capability, err := route.Capabilities.Resolve(feature, request.Portability != llm.PortabilityBestEffort)
		if err != nil || capability.State == CapabilityUnsupported || capability.State == CapabilityUnknown {
			return reject(RejectCapability, "capabilities."+string(feature), fmt.Sprintf("route cannot satisfy %s", feature))
		}
	}
	if request.Portability != llm.PortabilityBestEffort && len(request.Extensions) > 0 {
		for extension := range request.Extensions {
			if !containsString(route.ExtensionNames, extension) {
				return reject(RejectExtension, "extensions."+extension, "route does not advertise this extension")
			}
		}
	}
	if route.ContextBytes > 0 {
		requestBytes, _ := llm.CanonicalJSON(mustRequestJSON(request))
		if len(requestBytes) > route.ContextBytes {
			return reject(RejectContext, "request", "request exceeds route context limit")
		}
	}
	lineage := route.ModelLineage
	if lineage == "" {
		lineage = route.Model
	}
	revision := route.ModelRevision
	if revision == "" {
		revision = route.Model
	}
	pin := route.Pinning
	if pin.Provider == "" {
		pin = state.Pinning{Provider: route.Provider, EndpointID: route.EndpointID, AccountRegion: route.AccountRegion, Family: string(route.Family), ModelLineage: lineage}
	}
	compatibility := state.CheckPinning(continuation, pin)
	if compatibility.Decision == state.CompatibilityRejected {
		detail := "continuation is incompatible with route lineage"
		if len(compatibility.Diagnostics) > 0 {
			detail = compatibility.Diagnostics[0].Message
		}
		return reject(RejectContinuation, "continuation", detail)
	}
	tier := route.ProviderTiers[attempted]
	if tier == "" {
		return reject(RejectClass, "route.provider_tiers", "route has no provider tier mapping for the attempted class")
	}
	route.ModelLineage = lineage
	id, _, err := digestCandidate(route, requested, attempted, fallbackIndex, routeIndex, extensionDigest)
	if err != nil {
		return reject(RejectInvalid, "candidate", err.Error())
	}
	return Candidate{ID: id, RouteID: route.ID, EndpointID: route.EndpointID, Provider: route.Provider, Family: route.Family, Region: route.Region, EndpointAccountHMAC: route.EndpointAccountHMAC, Model: route.Model, ModelLineage: lineage, ModelRevision: revision, PriceAvailable: route.PriceAvailable, RequestedClass: requested, AttemptedClass: attempted, FallbackIndex: fallbackIndex, RouteIndex: routeIndex, ProviderTier: tier, CapabilityVersion: route.Capabilities.Version, PriceVersion: route.PriceVersion, ExtensionDigest: extensionDigest, Pinning: pin}, Rejection{}, true
}

func requiredFeatures(request llm.Request, continuation state.Constraints) []Feature {
	features := []Feature{FeatureText}
	if len(request.Tools) > 0 {
		features = append(features, FeatureToolCall)
	}
	if request.Output != nil && request.Output.Format.Kind != llm.OutputKindText {
		features = append(features, FeatureStructuredOutput)
	}
	if request.Reasoning != nil && request.Reasoning.Mode != llm.ReasoningModeProviderDefault {
		features = append(features, FeatureReasoning)
	}
	if continuation.Present {
		features = append(features, FeatureContinuation)
	}
	return features
}

func extensionDigest(request llm.Request) string {
	if len(request.Extensions) == 0 {
		return ""
	}
	data, _ := llm.CanonicalJSON(mustRequestJSON(request))
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest[:])
}

func mustRequestJSON(request llm.Request) []byte {
	data, _ := json.Marshal(request)
	return data
}

func digestPlan(plan Plan) [32]byte {
	data := make([]byte, 0, len(plan.Candidates)*64)
	for _, candidate := range plan.Candidates {
		data = append(data, candidate.ID...)
		data = append(data, 0)
	}
	return sha256.Sum256(data)
}

// CompileCatalog makes a defensive, deterministic route catalog and rejects
// duplicate identifiers before a snapshot is published.
func CompileCatalog(version string, models map[string]Model) (Catalog, error) {
	if version == "" {
		return Catalog{}, fmt.Errorf("catalog version is required")
	}
	copyModels := make(map[string]Model, len(models))
	for name, model := range models {
		if name == "" || len(model.Routes) == 0 {
			return Catalog{}, fmt.Errorf("model %q has no routes", name)
		}
		seen := make(map[string]struct{}, len(model.Routes))
		model.Routes = append([]Route(nil), model.Routes...)
		for index := range model.Routes {
			route := &model.Routes[index]
			if err := validateRouteShape(*route); err != nil {
				return Catalog{}, err
			}
			if route.ModelLineage == "" {
				route.ModelLineage = route.Model
			}
			if route.ModelRevision == "" {
				route.ModelRevision = route.Model
			}
			if _, exists := seen[route.ID]; exists {
				return Catalog{}, fmt.Errorf("model %q duplicate route %q", name, route.ID)
			}
			seen[route.ID] = struct{}{}
			route.Classes = append([]llm.ServiceClass(nil), route.Classes...)
			route.ProviderTiers = cloneClassMap(route.ProviderTiers)
			route.AllowedTenants = append([]string(nil), route.AllowedTenants...)
			route.AllowedRegions = append([]string(nil), route.AllowedRegions...)
			route.ExtensionNames = append([]string(nil), route.ExtensionNames...)
		}
		copyModels[name] = model
	}
	return Catalog{Version: version, Models: copyModels}, nil
}

func cloneClassMap(input map[llm.ServiceClass]string) map[llm.ServiceClass]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[llm.ServiceClass]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

var _ Planner = DeterministicPlanner{}

// Keep sort imported in this file for deterministic future extensions and to
// make accidental map iteration in callers obvious in review.
var _ = sort.Strings
