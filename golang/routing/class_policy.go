package routing

import (
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// ClassPolicy limits the service classes a matching request may be served
// at. A disabled class is never a failure: the planner serves the request at
// standard instead, so Classes must always include standard.
type ClassPolicy struct {
	ID string
	// Tenant and Project select the requests the policy applies to; an empty
	// value matches every request.
	Tenant  string
	Project string
	Classes []llm.ServiceClass
}

// Validate rejects a policy that could leave a request without a class.
func (policy ClassPolicy) Validate() error {
	if policy.ID == "" {
		return fmt.Errorf("service class policy ID is required")
	}
	seen := make(map[llm.ServiceClass]struct{}, len(policy.Classes))
	for _, class := range policy.Classes {
		if !class.Valid() {
			return fmt.Errorf("service class policy %q contains unknown class %q; want economy, standard, or priority", policy.ID, class)
		}
		if _, duplicate := seen[class]; duplicate {
			return fmt.Errorf("service class policy %q repeats class %q", policy.ID, class)
		}
		seen[class] = struct{}{}
	}
	if _, ok := seen[llm.ServiceClassStandard]; !ok {
		return fmt.Errorf("service class policy %q must enable standard", policy.ID)
	}
	return nil
}

func (policy ClassPolicy) matches(context llm.RequestContext) bool {
	return (policy.Tenant == "" || policy.Tenant == context.Tenant) && (policy.Project == "" || policy.Project == context.Project)
}

// ServiceClasses returns the classes a request is planned at, in fallback
// order: the requested class followed by its explicit fallbacks. A class that
// a matching policy disables, or that no route of the logical model offers,
// is served at standard instead, and a class that repeats an earlier one is
// dropped. Every policy that matches applies, so a class is enabled only when
// all of them enable it.
func (catalog Catalog) ServiceClasses(request llm.Request) ([]llm.ServiceClass, error) {
	requested, err := llm.NormalizeServiceClass(request.ServiceClass)
	if err != nil {
		return nil, err
	}
	model, known := catalog.Models[request.Model]
	classes := make([]llm.ServiceClass, 0, 1+len(request.ServiceClassFallbacks))
	for _, class := range append([]llm.ServiceClass{requested}, request.ServiceClassFallbacks...) {
		if !catalog.classEnabled(request.Context, class) || (known && !model.offers(class)) {
			class = llm.ServiceClassStandard
		}
		if !containsClass(classes, class) {
			classes = append(classes, class)
		}
	}
	return classes, nil
}

func (catalog Catalog) classEnabled(context llm.RequestContext, class llm.ServiceClass) bool {
	if class == llm.ServiceClassStandard {
		return true
	}
	for _, policy := range catalog.ClassPolicies {
		if policy.matches(context) && !containsClass(policy.Classes, class) {
			return false
		}
	}
	return true
}

// offers reports whether any route of the model can serve class at all.
func (model Model) offers(class llm.ServiceClass) bool {
	for _, route := range model.Routes {
		if containsClass(route.Classes, class) && route.ProviderTiers[class] != "" {
			return true
		}
	}
	return false
}

func cloneClassPolicies(source []ClassPolicy) []ClassPolicy {
	if source == nil {
		return nil
	}
	result := make([]ClassPolicy, len(source))
	for index, policy := range source {
		policy.Classes = append([]llm.ServiceClass(nil), policy.Classes...)
		result[index] = policy
	}
	return result
}
