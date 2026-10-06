package llm

import (
	"fmt"
)

// ServiceClass is the provider-neutral latency/cost class requested by a
// caller. It is intentionally closed: provider tier names never cross this
// boundary.
type ServiceClass string

const (
	ServiceClassEconomy  ServiceClass = "economy"
	ServiceClassStandard ServiceClass = "standard"
	ServiceClassPriority ServiceClass = "priority"
)

func (class ServiceClass) Valid() bool {
	switch class {
	case ServiceClassEconomy, ServiceClassStandard, ServiceClassPriority:
		return true
	default:
		return false
	}
}

// NormalizeServiceClass makes omission deterministic and rejects all values
// outside the public three-value enum. There is deliberately no
// provider-default value.
func NormalizeServiceClass(class ServiceClass) (ServiceClass, error) {
	if class == "" {
		return ServiceClassStandard, nil
	}
	if !class.Valid() {
		return "", fmt.Errorf("invalid service class %q: want economy, standard, or priority", class)
	}
	return class, nil
}

// ValidateServiceClassFallbacks validates an ordered explicit fallback list.
// A fallback authorizes another class but does not force the router to use it.
func ValidateServiceClassFallbacks(requested ServiceClass, fallbacks []ServiceClass) error {
	normalized, err := NormalizeServiceClass(requested)
	if err != nil {
		return err
	}
	seen := make(map[ServiceClass]struct{}, len(fallbacks))
	for index, class := range fallbacks {
		if !class.Valid() {
			return fmt.Errorf("service class fallback %d is invalid: %q", index, class)
		}
		if class == normalized {
			return fmt.Errorf("service class fallback %d repeats requested class %q", index, class)
		}
		if _, ok := seen[class]; ok {
			return fmt.Errorf("service class fallback %d repeats class %q", index, class)
		}
		seen[class] = struct{}{}
	}
	return nil
}

// DiagnosticServiceClassProviderDowngrade marks a response the provider served
// at a lower class than the worker attempted. It is distinct from a router
// fallback, which the response reports through fallback_index instead.
const DiagnosticServiceClassProviderDowngrade = "service_class_provider_downgrade"

// rank orders the public classes by the latency/capacity they buy. Provider
// labels are never compared; only the mapped classes are.
func (class ServiceClass) rank() int {
	switch class {
	case ServiceClassEconomy:
		return 0
	case ServiceClassStandard:
		return 1
	case ServiceClassPriority:
		return 2
	default:
		return -1
	}
}

// ProviderDowngradeDiagnostic reports the required diagnostic when the provider
// classified the response below the attempted class. A response with no
// mappable actual class carries no evidence of a downgrade and gets none.
func (service ServiceFacts) ProviderDowngradeDiagnostic() (Diagnostic, bool) {
	attempted, err := NormalizeServiceClass(service.Attempted)
	if err != nil || service.Actual == nil || !service.Actual.Valid() || service.Actual.rank() >= attempted.rank() {
		return Diagnostic{}, false
	}
	return Diagnostic{Code: DiagnosticServiceClassProviderDowngrade, Severity: DiagnosticWarning, Path: "/service/actual",
		Message: fmt.Sprintf("provider served %s after a %s attempt", *service.Actual, attempted),
		Details: map[string]string{"attempted": string(attempted), "actual": string(*service.Actual)}}, true
}
