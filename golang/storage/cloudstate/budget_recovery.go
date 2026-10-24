package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// BudgetRecoveryPage is one bounded page of durable paid executions. Scanned
// counts every discovery row read, including skipped ones, for progress logs.
type BudgetRecoveryPage struct {
	Executions    []SavedProviderExecution
	Scanned       int
	NextPageToken string
}

// ListBudgetRecovery is privileged, service-internal access for rebuilding
// lost Redis budget authority. It reads one bounded page of one discovery
// shard, including terminal requests, and returns every paid execution whose
// accounting may still count. A terminal request last updated before
// terminalNotBefore has settled spend that already left every window, so it is
// skipped without reading its payload. Corrupt records fail the page: recovery
// must never treat an unreadable record as unused budget.
func (r *Repository) ListBudgetRecovery(ctx context.Context, shard, pageSize int, token string, terminalNotBefore time.Time) (BudgetRecoveryPage, error) {
	if err := validContext(ctx); err != nil {
		return BudgetRecoveryPage{}, err
	}
	if shard < 0 || shard >= PendingShards || pageSize < 0 || pageSize > 1000 {
		return BudgetRecoveryPage{}, ErrInvalid
	}
	if pageSize == 0 {
		pageSize = 100
	}
	page, err := r.table.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: r.partition(shard), SortKeyPrefix: RequestIDPrefix, PageSize: pageSize, PageToken: token})
	if err != nil {
		return BudgetRecoveryPage{}, err
	}
	if len(page.Records) > pageSize || (page.NextPageToken != "" && page.NextPageToken == token) {
		return BudgetRecoveryPage{}, ErrCorrupt
	}
	result := BudgetRecoveryPage{Scanned: len(page.Records), NextPageToken: page.NextPageToken}
	lastKey := ""
	for _, row := range page.Records {
		value, err := r.decodeIndex(row)
		if err != nil {
			return BudgetRecoveryPage{}, err
		}
		if row.Item.PartitionKey != r.partition(shard) || row.Item.SortKey <= lastKey {
			return BudgetRecoveryPage{}, ErrCorrupt
		}
		lastKey = row.Item.SortKey
		// A terminal index row is written at the request's final revision,
		// after any settlement it carries.
		if value.Status.terminal() && value.UpdatedAt.Before(terminalNotBefore) {
			continue
		}
		record, err := r.ReadForRecovery(ctx, value.ID)
		if errors.Is(err, contracts.ErrNotFound) && value.Revision == 0 {
			continue // Create never published; nothing reached Redis.
		}
		if err != nil {
			return BudgetRecoveryPage{}, err
		}
		saved, ok, err := budgetRecoveryExecution(record)
		if err != nil {
			return BudgetRecoveryPage{}, err
		}
		if ok {
			result.Executions = append(result.Executions, saved)
		}
	}
	return result, nil
}

// budgetRecoveryExecution returns the paid execution of one record. A request
// without a plan, an execution or a reservation never reached Redis in a way
// that could authorize paid work.
func budgetRecoveryExecution(record Record) (SavedProviderExecution, bool, error) {
	var progress map[string]json.RawMessage
	if json.Unmarshal(record.Progress, &progress) != nil {
		return SavedProviderExecution{}, false, ErrCorrupt
	}
	if progress["budget_plan"] == nil || progress["provider_execution"] == nil {
		return SavedProviderExecution{}, false, nil
	}
	_, plan, execution, err := executionProgress(record)
	if err != nil {
		return SavedProviderExecution{}, false, err
	}
	if execution == nil || !plan.RequiresReservation() {
		return SavedProviderExecution{}, false, nil
	}
	return SavedProviderExecution{Plan: *plan, Execution: *execution}, true, nil
}
