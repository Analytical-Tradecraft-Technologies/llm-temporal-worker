package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// budgetRecoveryReadConcurrency bounds the record reads of one page in flight.
const budgetRecoveryReadConcurrency = 16

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
	candidates := make([]indexRecord, 0, len(page.Records))
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
		candidates = append(candidates, value)
	}
	// Each record is an independent event-stream and payload read, so a page
	// is read concurrently; sequential reads made a large history take hours.
	type outcome struct {
		saved SavedProviderExecution
		ok    bool
		err   error
	}
	outcomes := make([]outcome, len(candidates))
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	slots := make(chan struct{}, budgetRecoveryReadConcurrency)
	var wg sync.WaitGroup
	for index, value := range candidates {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer func() { <-slots; wg.Done() }()
			record, err := r.ReadForRecovery(readCtx, value.ID)
			if errors.Is(err, contracts.ErrNotFound) && value.Revision == 0 {
				return // Create never published; nothing reached Redis.
			}
			if err == nil {
				outcomes[index].saved, outcomes[index].ok, err = budgetRecoveryExecution(record)
			}
			if err != nil {
				outcomes[index].err = err
				cancel()
			}
		}()
	}
	wg.Wait()
	for _, outcome := range outcomes {
		if outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
			return BudgetRecoveryPage{}, outcome.err
		}
	}
	for _, outcome := range outcomes {
		if outcome.err != nil {
			return BudgetRecoveryPage{}, outcome.err
		}
		if outcome.ok {
			result.Executions = append(result.Executions, outcome.saved)
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
