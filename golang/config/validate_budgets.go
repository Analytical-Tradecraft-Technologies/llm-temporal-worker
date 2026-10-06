package config

import (
	"fmt"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// maxAdmissionHashTagBytes and the forbidden characters mirror the Redis key
// space, which wraps the tag in braces to pin admission keys to one slot.
const maxAdmissionHashTagBytes = 64

func validateAdmissionHashTag(tag string) error {
	if tag == "" || len(tag) > maxAdmissionHashTagBytes || strings.ContainsAny(tag, "{} \t\r\n") {
		return fmt.Errorf("state.redis.admission_hash_tag must be 1 to %d bytes without braces or whitespace", maxAdmissionHashTagBytes)
	}
	return nil
}

// budgetWindowBuckets is the bucket count the budget engine charges a window
// against limits.max_budget_buckets_per_window. It must stay equal to the
// count in budget.Window.Validate, which rejects the window at worker start.
func budgetWindowBuckets(duration, bucket Duration) int64 {
	if bucket <= 0 {
		return 0
	}
	return int64(duration/bucket) + 2
}

// validateBudgetLimit requires a window limit that Redis can hold exactly.
// Budget state is materialized as integer nano-USD: a limit above the safe
// integer range cannot be stored, and one below a nano-USD rounds down to a
// zero limit that rejects every matching request.
func validateBudgetLimit(window BudgetWindow, path string) error {
	limit, field := window.LimitUSD, "limit_usd"
	var err error
	if limit.IsZero() {
		field = "limit_micro_usd"
		limit, err = pricing.USDFromMicro(pricing.MicroUSD(window.LimitMicroUSD))
	}
	nano := pricing.NanoUSD(0)
	if err == nil {
		nano, err = pricing.FloorNanoUSD(limit)
	}
	if err != nil {
		largest, _ := pricing.USDFromNano(pricing.NanoUSDSafeLimit)
		return fmt.Errorf("%s.%s must not exceed %s USD, the largest limit Redis represents exactly", path, field, largest)
	}
	if nano <= 0 {
		return fmt.Errorf("%s.%s must be at least 0.000000001 USD; a smaller limit rounds down to zero", path, field)
	}
	return nil
}

// validateBudgetReferences rejects budget settings that are well formed in
// isolation but cannot work in this configuration: a window that needs more
// buckets than limits allow, and a matcher that names a model, endpoint,
// environment or authorization scope that no request can carry. Such a
// matcher never matches, so its policy silently never applies, or with
// require_match the affected routes silently disappear.
func (config Config) validateBudgetReferences() error {
	for index, policy := range config.Budgets.Policies {
		path := fmt.Sprintf("budgets.policies[%d]", index)
		for windowIndex, window := range policy.Windows {
			if buckets := budgetWindowBuckets(window.Duration, window.Bucket); buckets > int64(config.Limits.MaxBudgetBucketsPerWindow) {
				return fmt.Errorf("%s.windows[%d] needs %d buckets (duration/bucket + 2), limits.max_budget_buckets_per_window is %d", path, windowIndex, buckets, config.Limits.MaxBudgetBucketsPerWindow)
			}
		}
		match := policy.Match
		if budgetMatchIsExact(match.LogicalModel) {
			if _, exists := config.Models[match.LogicalModel]; !exists {
				return fmt.Errorf("%s.match.logical_model %q is not configured in models", path, match.LogicalModel)
			}
		}
		if budgetMatchIsExact(match.EndpointID) {
			if _, exists := config.Endpoints[match.EndpointID]; !exists {
				return fmt.Errorf("%s.match.endpoint %q is not configured in endpoints", path, match.EndpointID)
			}
			// A request only carries an endpoint through a route of its
			// logical model, so a declared endpoint no such route uses can
			// never match either.
			if !config.modelRoutesTo(match.LogicalModel, match.EndpointID) {
				if budgetMatchIsExact(match.LogicalModel) {
					return fmt.Errorf("%s.match.endpoint %q is not used by a route of model %q", path, match.EndpointID, match.LogicalModel)
				}
				return fmt.Errorf("%s.match.endpoint %q is not used by any model route", path, match.EndpointID)
			}
		}
		if budgetMatchIsExact(match.Environment) && match.Environment != config.Environment {
			return fmt.Errorf("%s.match.environment %q can never match environment %q", path, match.Environment, config.Environment)
		}
		if config.Authorization != nil {
			if err := validateBudgetMatchScope(match, config.Authorization.AllowedScopes, path); err != nil {
				return err
			}
		}
	}
	return nil
}

// modelRoutesTo reports whether a route of the named model, or of any model
// when the name is a wildcard, uses the endpoint.
func (config Config) modelRoutesTo(model, endpoint string) bool {
	for name, value := range config.Models {
		if budgetMatchIsExact(model) && name != model {
			continue
		}
		for _, route := range value.Routes {
			if route.Endpoint == endpoint {
				return true
			}
		}
	}
	return false
}

// validateBudgetMatchScope compares exact tenant and project restrictions
// with the authorization allowlist. Only allowlisted scopes reach budget
// matching, and the comparison is case-sensitive. Messages name the field
// without echoing tenant or project names.
func validateBudgetMatchScope(match BudgetMatch, scopes []AuthorizedScope, path string) error {
	tenant, project := budgetMatchIsExact(match.Tenant), budgetMatchIsExact(match.Project)
	if !tenant && !project {
		return nil
	}
	tenantAllowed, projectAllowed := !tenant, !project
	for _, scope := range scopes {
		tenantMatches := !tenant || scope.Tenant == match.Tenant
		projectMatches := !project || scope.Project == match.Project
		if tenantMatches && projectMatches {
			return nil
		}
		tenantAllowed = tenantAllowed || tenantMatches
		projectAllowed = projectAllowed || projectMatches
	}
	switch {
	case !tenantAllowed:
		return fmt.Errorf("%s.match.tenant is not a tenant in authorization.allowed_scopes", path)
	case !projectAllowed:
		return fmt.Errorf("%s.match.project is not a project in authorization.allowed_scopes", path)
	default:
		return fmt.Errorf("%s.match tenant and project are not paired in authorization.allowed_scopes", path)
	}
}

// budgetMatchIsExact reports whether a matcher field restricts requests to
// one value. Empty and "*" are wildcards.
func budgetMatchIsExact(value string) bool {
	return value != "" && value != "*"
}
