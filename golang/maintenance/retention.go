// Package maintenance contains storage-neutral contracts for bounded worker
// maintenance.  Retention is deliberately separate from Activities: callers
// run it with a maintenance role and an implementation must recheck every
// reference while the candidate row is locked.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ResourceKind identifies the durable table whose retention policy is being
// evaluated.  The values mirror the independent retention horizons in the
// PostgreSQL design; a policy may leave any horizon unset.
type ResourceKind string

const (
	ResourceCache      ResourceKind = "cache"
	ResourceStatus     ResourceKind = "status"
	ResourceInventory  ResourceKind = "inventory"
	ResourceOperation  ResourceKind = "operation"
	ResourceBudget     ResourceKind = "budget"
	ResourceCheckpoint ResourceKind = "checkpoint"
	// ResourceQueryExecution identifies the bounded control-query audit ledger.
	// Query executions have no durable children, so their expiry can be
	// reclaimed independently from inference operations.
	ResourceQueryExecution ResourceKind = "query_execution"
)

// RetentionRecord is the small set of facts a backend must recheck inside its
// deletion transaction.  Active work and references always win over age.
// Backends should not populate these fields from an earlier, unlocked scan.
type RetentionRecord struct {
	ID                    string
	Kind                  ResourceKind
	State                 string
	ExpiresAt             time.Time
	LastUsedAt            time.Time
	Active                bool
	HasRetainedDescendant bool
	HasActiveFill         bool
	// HasActiveUse is true when a cache-use ledger row points at a
	// non-terminal operation. Cache maintenance must keep the entry usable
	// until that operation leaves retry/poll state.
	HasActiveUse     bool
	// HasCandidateBlobReference identifies a blob owned by this row. The
	// candidate's tombstone/delete lifecycle releases that reference, so it
	// must not make an otherwise eligible row ineligible.
	HasCandidateBlobReference bool
	// HasExternalBlobReference identifies a retained blob reference owned by a
	// different row. That reference must outlive this candidate and therefore
	// fences retention.
	HasExternalBlobReference bool
}

// RetentionPolicy bounds one maintenance pass.  Every non-zero cutoff is an
// independent policy.  A zero cutoff means that resource kind is not touched.
type RetentionPolicy struct {
	Now                         time.Time
	Limit                       int
	CacheUnusedBefore           time.Time
	StatusExpiresBefore         time.Time
	InventoryExpiresBefore      time.Time
	OperationExpiresBefore      time.Time
	BudgetExpiresBefore         time.Time
	CheckpointExpiresBefore     time.Time
	QueryExecutionExpiresBefore time.Time
}

func (policy RetentionPolicy) Validate() error {
	if policy.Now.IsZero() {
		return errors.New("retention policy time is required")
	}
	if policy.Limit <= 0 || policy.Limit > 10_000 {
		return errors.New("retention policy limit must be between 1 and 10000")
	}
	for name, cutoff := range map[string]time.Time{
		"cache": policy.CacheUnusedBefore, "status": policy.StatusExpiresBefore,
		"inventory": policy.InventoryExpiresBefore, "operation": policy.OperationExpiresBefore,
		"budget": policy.BudgetExpiresBefore, "checkpoint": policy.CheckpointExpiresBefore,
		"query_execution": policy.QueryExecutionExpiresBefore,
	} {
		if !cutoff.IsZero() && cutoff.After(policy.Now) {
			return fmt.Errorf("retention %s cutoff must not be after policy time", name)
		}
	}
	return nil
}

// Eligible applies the conservative, storage-neutral part of the policy.
// SQL adapters must repeat the same predicates in their locked query rather
// than relying on a previously returned RetentionRecord.
func (record RetentionRecord) Eligible(policy RetentionPolicy) bool {
	if record.ID == "" || record.Active || record.HasRetainedDescendant || record.HasActiveFill || record.HasActiveUse || record.HasExternalBlobReference {
		return false
	}
	var cutoff time.Time
	if record.Kind == ResourceCache {
		if record.State != "ready" {
			return false
		}
		if record.LastUsedAt.IsZero() {
			return false
		}
		cutoff = policy.CacheUnusedBefore
		return !cutoff.IsZero() && record.LastUsedAt.Before(cutoff)
	} else {
		if record.ExpiresAt.IsZero() {
			return false
		}
		switch record.Kind {
		case ResourceStatus:
			cutoff = policy.StatusExpiresBefore
		case ResourceInventory:
			cutoff = policy.InventoryExpiresBefore
		case ResourceOperation:
			cutoff = policy.OperationExpiresBefore
		case ResourceBudget:
			cutoff = policy.BudgetExpiresBefore
		case ResourceCheckpoint:
			cutoff = policy.CheckpointExpiresBefore
		case ResourceQueryExecution:
			cutoff = policy.QueryExecutionExpiresBefore
		default:
			return false
		}
	}
	return !cutoff.IsZero() && record.ExpiresAt.Before(cutoff)
}

// RetentionResult reports bounded progress, not an estimate of the whole
// table.  Skipped is useful for operators: a row was old enough but failed a
// reference/active check during the locked recheck.
type RetentionResult struct {
	Examined   int
	Tombstoned int
	Deleted    int
	Skipped    int
}

// RetentionStore is implemented by PostgreSQL and development adapters.  The
// call is intentionally one bounded unit so claim, recheck, tombstone/delete,
// and outbox publication can share one transaction where the backend supports
// it.
type RetentionStore interface {
	Prune(context.Context, RetentionPolicy) (RetentionResult, error)
}

// InMemoryRetention is a deterministic adapter for unit tests and the memory
// composition. It serializes the entire bounded pass, matching the atomic
// recheck guarantee of the PostgreSQL implementation without pretending to be
// a distributed lock.
type InMemoryRetention struct {
	mu      sync.Mutex
	records map[string]RetentionRecord
}

func NewInMemoryRetention(records []RetentionRecord) (*InMemoryRetention, error) {
	store := &InMemoryRetention{records: make(map[string]RetentionRecord, len(records))}
	for _, record := range records {
		if record.ID == "" {
			return nil, errors.New("retention record ID is required")
		}
		if _, exists := store.records[record.ID]; exists {
			return nil, fmt.Errorf("retention record %q is duplicated", record.ID)
		}
		store.records[record.ID] = record
	}
	return store, nil
}

func (store *InMemoryRetention) Snapshot() []RetentionRecord {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]RetentionRecord, 0, len(store.records))
	for _, record := range store.records {
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (store *InMemoryRetention) Prune(ctx context.Context, policy RetentionPolicy) (RetentionResult, error) {
	var result RetentionResult
	if store == nil {
		return result, errors.New("retention store is nil")
	}
	if err := policy.Validate(); err != nil {
		return result, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	candidates := make([]RetentionRecord, 0, len(store.records))
	for _, record := range store.records {
		if record.Eligible(policy) {
			candidates = append(candidates, record)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		return left.ID < right.ID
	})
	if len(candidates) > policy.Limit {
		candidates = candidates[:policy.Limit]
	}
	result.Examined = len(candidates)
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		// Re-read the map while holding the mutation lock. A production adapter
		// performs this same recheck after FOR UPDATE SKIP LOCKED.
		current, exists := store.records[candidate.ID]
		if !exists || !current.Eligible(policy) {
			result.Skipped++
			continue
		}
		if current.Kind == ResourceCache {
			current.State = "tombstoned"
			store.records[current.ID] = current
			result.Tombstoned++
		} else {
			delete(store.records, current.ID)
			result.Deleted++
		}
	}
	return result, nil
}
