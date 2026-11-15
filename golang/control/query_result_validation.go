package control

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
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
		const unsorted = "provider status rows must be sorted and unique by route_id"
		return validateResultRows(kind, llm.QueryProviderStatus, "provider status rows", unsorted, value.Routes, func(row ProviderStatusRow) (string, error) {
			if row.RouteID == "" {
				return "", errors.New(unsorted)
			}
			return string(row.RouteID), nil
		})
	case ModelInventoryResult:
		const unsorted = "model inventory rows must be sorted and unique by provider, endpoint, and model"
		return validateResultRows(kind, llm.QueryModelInventory, "model inventory rows", unsorted, value.Models, func(row ModelInventoryRow) (string, error) {
			key := modelInventoryResultKey(row)
			if key == "" {
				return "", errors.New(unsorted)
			}
			return key, nil
		})
	case CreditStatusResult:
		return validateResultRows(kind, llm.QueryCreditStatus, "credit status rows", "credit status rows must be sorted and unique by provider and endpoint", value.Endpoints, func(row CreditStatusRow) (string, error) {
			if row.Provider == "" || row.Endpoint == "" {
				return "", errors.New("credit status rows require provider and endpoint")
			}
			return joinQueryResultKey(string(row.Provider), string(row.Endpoint)), nil
		})
	case BudgetStatusResult:
		return validateResultRows(kind, llm.QueryBudgetStatus, "budget windows", "budget windows must be sorted and unique by policy and window", value.Windows, func(row BudgetWindow) (string, error) {
			if row.PolicyKey == "" || row.WindowKey == "" {
				return "", errors.New("budget windows require policy and window")
			}
			return joinQueryResultKey(string(row.PolicyKey), string(row.WindowKey)), nil
		})
	case SpendSummaryResult:
		return validateResultRows(kind, llm.QuerySpendSummary, "spend buckets", "spend buckets must be sorted and unique by operation, provider, and model", value.Buckets, func(row SpendBucket) (string, error) {
			return spendBucketResultKey(row), nil
		})
	case *ProviderStatusResult:
		return validateResultPointer(kind, value)
	case *ModelInventoryResult:
		return validateResultPointer(kind, value)
	case *CreditStatusResult:
		return validateResultPointer(kind, value)
	case *BudgetStatusResult:
		return validateResultPointer(kind, value)
	case *SpendSummaryResult:
		return validateResultPointer(kind, value)
	default:
		return fmt.Errorf("unknown typed query result")
	}
}

// validateResultPointer validates the result a pointer refers to; a typed nil
// pointer is rejected.
func validateResultPointer[T QueryResult](kind llm.QueryKind, value *T) error {
	if value == nil {
		return fmt.Errorf("query result is nil")
	}
	return validateQueryResultOrdering(kind, *value)
}

// validateResultRows checks that the result family matches kind (noun names
// the family's rows in the mismatch error) and that rows are strictly
// increasing by key. key reports a row that cannot be keyed; unsorted is the
// error for a row whose key does not sort after the previous row's.
func validateResultRows[R any](kind, want llm.QueryKind, noun, unsorted string, rows []R, key func(R) (string, error)) error {
	if kind != want {
		return fmt.Errorf("query result kind %q does not match %s", kind, noun)
	}
	previous := ""
	for index, row := range rows {
		current, err := key(row)
		if err != nil {
			return err
		}
		if index > 0 && current <= previous {
			return errors.New(unsorted)
		}
		previous = current
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
	// order here so the common typed boundary uses absent dimensions first
	// ordering regardless of which dimensions the caller selected.
	return strings.Join([]string{
		stringValue(bucket.Group.Model),
		stringValue(bucket.Group.OperationKind),
		stringValue(bucket.Group.Provider),
	}, "\x00")
}

func joinQueryResultKey(values ...string) string {
	return strings.Join(values, "\x00")
}
