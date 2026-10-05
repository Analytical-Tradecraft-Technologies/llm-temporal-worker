package routing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

type Planner interface {
	Plan(context.Context, Input) (Plan, error)
}

type Input struct {
	Request      llm.Request
	Catalog      Catalog
	Continuation state.Constraints
	Health       HealthView
	// Affinity is an optional immutable checkpoint observation. It is applied
	// only after normal route eligibility has produced candidates; it cannot
	// authorize a route or change the requested service class.
	Affinity *AffinityPreferences
	// Now is captured by the engine's immutable snapshot clock so expiry
	// decisions remain deterministic for one plan and are testable.
	Now time.Time
}

// Catalog is an immutable route snapshot. The planner never reads process
// configuration or discovers endpoints; callers compile it once and publish
// a new value on reload.
type Catalog struct {
	Version string
	Models  map[string]Model
}

type Model struct {
	Name   string
	Routes []Route
}

type Route struct {
	ID            string
	EndpointID    string
	Provider      string
	Family        string
	Region        string
	AccountRegion string
	// EndpointAccountHMAC is a non-secret route identity digest used to match
	// persisted provider affinity without storing an account identifier.
	EndpointAccountHMAC [32]byte
	Model               string
	ModelLineage        string
	// ModelRevision identifies the provider model revision used to establish a
	// prompt-cache prefix. Empty revisions are normalized to Model when the
	// catalog is compiled.
	ModelRevision  string
	Classes        []llm.ServiceClass
	ProviderTiers  map[llm.ServiceClass]string
	AllowedTenants []string
	AllowedRegions []string
	Capabilities   CapabilitySet
	// ProviderFeatures carries the complete catalog capability declaration for
	// the endpoint adapter, including features such as usage, image and
	// document that route planning does not consume. Planning reads only
	// Capabilities.
	ProviderFeatures map[string]Capability
	PriceVersion     string
	// PriceAvailable reports whether every service class advertised by this
	// route had a current quote while the immutable snapshot was compiled. The
	// engine still resolves each selected candidate and applies its budgeted
	// unpriced policy before admission or dispatch.
	PriceAvailable bool
	ExtensionNames []string
	ContextBytes   int
	// OutputTokens is the model-specific output ceiling; zero means unspecified.
	OutputTokens int64
	// ContextTokens bounds estimated input plus reserved output and reasoning.
	// Zero means no declared model limit. Admission evaluates this per candidate.
	ContextTokens int64
	Pinning       state.Pinning
}

type Candidate struct {
	ID                  string
	RouteID             string
	EndpointID          string
	Provider            string
	Family              string
	Region              string
	EndpointAccountHMAC [32]byte
	Model               string
	ModelLineage        string
	ModelRevision       string
	PriceAvailable      bool
	RequestedClass      llm.ServiceClass
	AttemptedClass      llm.ServiceClass
	FallbackIndex       int
	RouteIndex          int
	ContextTokens       int64
	ProviderTier        string
	CapabilityVersion   string
	PriceVersion        string
	ExtensionDigest     string
	Pinning             state.Pinning
}

func (candidate Candidate) Class() llm.ServiceClass { return candidate.AttemptedClass }

type Plan struct {
	Version    string
	Model      string
	Candidates []Candidate
	Rejections []Rejection
	Digest     [32]byte
	DigestHex  string
}

func (plan Plan) Clone() Plan {
	plan.Candidates = append([]Candidate(nil), plan.Candidates...)
	plan.Rejections = append([]Rejection(nil), plan.Rejections...)
	return plan
}

type HealthView struct {
	Routes map[string]RouteHealth
}

type RouteHealth struct {
	Open       bool
	AuthOpen   bool
	Enabled    bool
	Reason     string
	SnapshotID string
}

func (health HealthView) forRoute(id string) RouteHealth {
	value, ok := health.Routes[id]
	if !ok {
		return RouteHealth{Enabled: true}
	}
	return value
}

type Rejection struct {
	Code    string
	RouteID string
	Path    string
	Detail  string
}

func (rejection Rejection) diagnostic() llm.Diagnostic {
	return llm.Diagnostic{Code: rejection.Code, Severity: llm.DiagnosticWarning, Path: rejection.Path, Message: rejection.Detail, Details: map[string]string{"route_id": rejection.RouteID}}
}

func digestCandidate(route Route, requested, attempted llm.ServiceClass, fallback, routeIndex int, extensionDigest string) (string, [32]byte, error) {
	value := struct {
		RouteID, EndpointID, Provider, Family, Model, Lineage, Revision string
		Requested, Attempted                                            llm.ServiceClass
		Tier                                                            string
		Fallback, RouteIndex                                            int
		Capability, Price, Extension, AccountRegion, Region             string
		EndpointAccountHMAC                                             string
	}{
		RouteID: route.ID, EndpointID: route.EndpointID, Provider: route.Provider, Family: string(route.Family), Model: route.Model, Lineage: route.ModelLineage, Revision: route.ModelRevision,
		Requested: requested, Attempted: attempted, Tier: route.ProviderTiers[attempted], Fallback: fallback, RouteIndex: routeIndex,
		Capability: route.Capabilities.Version, Price: route.PriceVersion, Extension: extensionDigest,
		AccountRegion: route.AccountRegion, Region: route.Region,
		EndpointAccountHMAC: hex.EncodeToString(route.EndpointAccountHMAC[:]),
	}
	data, err := llm.CanonicalJSON(mustJSON(value))
	if err != nil {
		return "", [32]byte{}, err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), digest, nil
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func validateRouteShape(route Route) error {
	if route.OutputTokens < 0 {
		return fmt.Errorf("route %q has a negative output token limit", route.ID)
	}
	if route.ContextTokens < 0 {
		return fmt.Errorf("route %q has a negative context token limit", route.ID)
	}
	if route.ID == "" || route.EndpointID == "" || route.Provider == "" || route.Model == "" || route.Family == "" {
		return fmt.Errorf("route %q is incomplete", route.ID)
	}
	if len(route.Classes) == 0 {
		return fmt.Errorf("route %q has no service classes", route.ID)
	}
	for _, class := range route.Classes {
		if !class.Valid() {
			return fmt.Errorf("route %q contains unknown public service class %q; want economy, standard, or priority", route.ID, class)
		}
	}
	invalidTierClasses := make([]string, 0)
	for class := range route.ProviderTiers {
		if !class.Valid() {
			invalidTierClasses = append(invalidTierClasses, string(class))
		}
	}
	if len(invalidTierClasses) > 0 {
		sort.Strings(invalidTierClasses)
		return fmt.Errorf("route %q has a provider tier for unknown public service class %q; want economy, standard, or priority", route.ID, invalidTierClasses[0])
	}
	if route.ModelLineage == "" {
		route.ModelLineage = route.Model
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsClass(values []llm.ServiceClass, target llm.ServiceClass) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sortedRejections(values []Rejection) {
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].RouteID != values[j].RouteID {
			return values[i].RouteID < values[j].RouteID
		}
		if values[i].Code != values[j].Code {
			return values[i].Code < values[j].Code
		}
		return values[i].Path < values[j].Path
	})
}

// SupportsOutputLimit requires an explicit positive cap when the route declares
// a ceiling. Runtime callers materialize the budget estimator's default before
// planning, so provider defaults cannot bypass the catalog limit.
func (route Route) SupportsOutputLimit(request llm.Request) bool {
	if route.OutputTokens == 0 {
		return true
	}
	return route.OutputTokens > 0 && request.Output != nil && request.Output.MaxTokens != nil && *request.Output.MaxTokens > 0 && int64(*request.Output.MaxTokens) <= route.OutputTokens
}
