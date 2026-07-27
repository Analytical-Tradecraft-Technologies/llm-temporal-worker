package control

import (
	"fmt"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// validateQueryResultOrdering protects the stable keyset contract at the
// typed boundary. Storage repositories already emit ordered pages, but the
// QueryService also accepts deployment-owned TypedHandlers; accepting an
// unordered page from one of those handlers would make a signed continuation
// cursor skip or repeat rows. The wire model deliberately does not prescribe
// storage keys, so this validator is the final common ordering gate.
func validateQueryResultOrdering(kind llm.QueryKind, result QueryResult) error {
	switch value := result.(type) {
	case ProviderStatusResult:
		if kind != llm.QueryProviderStatus {
			return fmt.Errorf("query result kind %q does not match provider status rows", kind)
		}
		previous := ""
		for index, row := range value.Routes {
			key := string(row.RouteID)
			if key == "" || (index > 0 && key <= previous) {
				return fmt.Errorf("provider status rows must be sorted and unique by route_id")
			}
			previous = key
		}
	case *ProviderStatusResult:
		if value == nil {
			return fmt.Errorf("query result is nil")
		}
		return validateQueryResultOrdering(kind, *value)
	case ModelInventoryResult:
		if kind != llm.QueryModelInventory {
			return fmt.Errorf("query result kind %q does not match model inventory rows", kind)
		}
		previous := ""
		for _, row := range value.Models {
			key := modelInventoryResultKey(row)
			if key == "" || (previous != "" && key <= previous) {
				return fmt.Errorf("model inventory rows must be sorted and unique by provider, endpoint, and model")
			}
			previous = key
		}
	case *ModelInventoryResult:
		if value == nil {
			return fmt.Errorf("query result is nil")
		}
		return validateQueryResultOrdering(kind, *value)
	case CreditStatusResult:
		if kind != llm.QueryCreditStatus {
			return fmt.Errorf("query result kind %q does not match credit status rows", kind)
		}
		previous := ""
		for _, row := range value.Endpoints {
			if row.Provider == "" || row.Endpoint == "" {
				return fmt.Errorf("credit status rows require provider and endpoint")
			}
			key := joinQueryResultKey(string(row.Provider), string(row.Endpoint))
			if previous != "" && key <= previous {
				return fmt.Errorf("credit status rows must be sorted and unique by provider and endpoint")
			}
			previous = key
		}
	case *CreditStatusResult:
		if value == nil {
			return fmt.Errorf("query result is nil")
		}
		return validateQueryResultOrdering(kind, *value)
	case BudgetStatusResult:
		if kind != llm.QueryBudgetStatus {
			return fmt.Errorf("query result kind %q does not match budget windows", kind)
		}
		previous := ""
		for _, row := range value.Windows {
			if row.PolicyKey == "" || row.WindowKey == "" {
				return fmt.Errorf("budget windows require policy and window")
			}
			key := joinQueryResultKey(string(row.PolicyKey), string(row.WindowKey))
			if previous != "" && key <= previous {
				return fmt.Errorf("budget windows must be sorted and unique by policy and window")
			}
			previous = key
		}
	case *BudgetStatusResult:
		if value == nil {
			return fmt.Errorf("query result is nil")
		}
		return validateQueryResultOrdering(kind, *value)
	case SpendSummaryResult:
		if kind != llm.QuerySpendSummary {
			return fmt.Errorf("query result kind %q does not match spend buckets", kind)
		}
		previous := ""
		for _, row := range value.Buckets {
			key := spendBucketResultKey(row)
			if previous != "" && key <= previous {
				return fmt.Errorf("spend buckets must be sorted and unique by operation, provider, and model")
			}
			previous = key
		}
	case *SpendSummaryResult:
		if value == nil {
			return fmt.Errorf("query result is nil")
		}
		return validateQueryResultOrdering(kind, *value)
	default:
		return fmt.Errorf("unknown typed query result")
	}
	return nil
}

func modelInventoryResultKey(row ModelInventoryRow) string {
	if row.Provider == "" || row.Endpoint == "" || row.ProviderModelID == "" {
		return ""
	}
	return joinQueryResultKey(string(row.Provider), string(row.Endpoint), string(row.ProviderModelID))
}

func spendBucketResultKey(bucket SpendBucket) string {
	if bucket.Group == nil || (bucket.Group.OperationKind == nil && bucket.Group.Provider == nil && bucket.Group.Model == nil) {
		// A nil (or all-NULL) group is the global aggregate and sorts before
		// grouped rows.
		return "\x00"
	}
	// Spend dimensions are canonicalized by their wire names before the
	// repository is called (model, operation_kind, provider). Keep the same
	// order here so the common typed boundary agrees with SQL's NULLS FIRST
	// ordering regardless of which dimensions the caller selected.
	return strings.Join([]string{
		spendGroupValue(bucket.Group.Model),
		spendGroupValue(bucket.Group.OperationKind),
		spendGroupValue(bucket.Group.Provider),
	}, "\x00")
}

func joinQueryResultKey(values ...string) string {
	return strings.Join(values, "\x00")
}

func spendGroupValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}
