package cloudstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

type indexRecord struct {
	Schema    int       `json:"schema"`
	ID        RequestID `json:"id"`
	ScopeTag  string    `json:"scope"`
	Binding   string    `json:"binding"`
	Revision  uint64    `json:"revision"`
	Status    Status    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PendingShard hashes the complete internal ID into one of eight partitions.
// The shard count is a persisted format constant, not a tunable runtime limit.
func PendingShard(id RequestID) (int, error) {
	if !id.valid() {
		return 0, ErrInvalid
	}
	digest := sha256.Sum256([]byte(id))
	return int(digest[len(digest)-1]) % PendingShards, nil
}

func (r *Repository) partition(shard int) string {
	return fmt.Sprintf("%s/pending/%d", r.namespace, shard)
}
func (r *Repository) indexKey(id RequestID) kv.KeyValueKey {
	shard, _ := PendingShard(id)
	return kv.KeyValueKey{PartitionKey: r.partition(shard), SortKey: string(id)}
}

func (r *Repository) indexItem(value indexRecord) kv.KeyValueItem {
	key := r.indexKey(value.ID)
	data, _ := json.Marshal(value)
	return kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"request": kv.Bytes(data)}}
}

func (r *Repository) decodeIndex(record kv.KeyValueRecord) (indexRecord, error) {
	var value indexRecord
	data, ok := record.Item.Fields["request"].(kv.KeyValueBytes)
	if !ok || len(data) > 2048 || json.Unmarshal(data, &value) != nil || value.Schema != 1 || !value.ID.valid() || !hexDigest(value.ScopeTag) || !hexDigest(value.Binding) || !value.Status.valid() || value.Revision > maxRevisions || !validTime(value.UpdatedAt) || record.Version == "" || record.Item.Key() != r.indexKey(value.ID) || (value.Revision == 0 && value.Status != StatusPending) {
		return indexRecord{}, ErrCorrupt
	}
	return value, nil
}

func hexDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func (r *Repository) ensureIndex(ctx context.Context, request CreateRequest) error {
	value := indexRecord{Schema: 1, ID: request.ID, ScopeTag: r.scopeTag(request.Scope), Binding: r.binding(request), Status: StatusPending, UpdatedAt: request.CreatedAt}
	_, err := r.table.Create(ctx, r.indexItem(value))
	if err == nil {
		return nil
	}
	if errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrAlreadyExists) {
		return err
	}
	stored, err := r.table.Get(ctx, r.indexKey(request.ID))
	if err != nil {
		return err
	}
	previous, err := r.decodeIndex(stored)
	if err != nil {
		return err
	}
	if previous.ScopeTag != value.ScopeTag || previous.Binding != value.Binding {
		return contracts.ErrConflict
	}
	return nil
}

func (r *Repository) advanceIndex(ctx context.Context, record Record) error {
	for attempt := 0; attempt < 16; attempt++ {
		stored, err := r.table.Get(ctx, r.indexKey(record.Request.ID))
		if err != nil {
			return err
		}
		previous, err := r.decodeIndex(stored)
		if err != nil {
			return err
		}
		if previous.Binding != r.binding(record.Request) || previous.ScopeTag != r.scopeTag(record.Request.Scope) {
			return ErrCorrupt
		}
		// A delayed acknowledgement must never regress a newer index record.
		if previous.Revision > record.Revision {
			return nil
		}
		if previous.Revision == record.Revision {
			if previous.Status != record.Status || !previous.UpdatedAt.Equal(record.UpdatedAt) {
				return ErrCorrupt
			}
			return nil
		}
		previous.Revision, previous.Status, previous.UpdatedAt = record.Revision, record.Status, record.UpdatedAt
		_, err = r.table.Replace(ctx, r.indexItem(previous), stored.Version)
		if err == nil {
			return nil
		}
		if errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrConflict) {
			return err
		}
	}
	return contracts.ErrConflict
}

// ListPending is a privileged recovery/diagnostic query, not a public activity.
// It reads one bounded page of one shard without scanning a table or using a
// secondary index. Complete all eight shards and follow empty pages with tokens.
// Status can conservatively lag a committed event; always ReadForRecovery before
// acting. No pagination snapshot or automatic recovery/cleanup is promised.
// Terminal discovery rows are retained for now and filtered from returned pages.
func (r *Repository) ListPending(ctx context.Context, shard, pageSize int, token string) (PendingPage, error) {
	if err := validContext(ctx); err != nil {
		return PendingPage{}, err
	}
	if shard < 0 || shard >= PendingShards || pageSize < 0 || pageSize > 1000 {
		return PendingPage{}, ErrInvalid
	}
	if pageSize == 0 {
		pageSize = 100
	}
	page, err := r.table.QueryPartition(ctx, kv.KeyValueQuery{PartitionKey: r.partition(shard), SortKeyPrefix: RequestIDPrefix, PageSize: pageSize, PageToken: token})
	if err != nil {
		return PendingPage{}, err
	}
	if len(page.Records) > pageSize || (page.NextPageToken != "" && page.NextPageToken == token) {
		return PendingPage{}, ErrCorrupt
	}
	result := PendingPage{Requests: make([]PendingRequest, 0, len(page.Records)), NextPageToken: page.NextPageToken}
	lastKey := ""
	for _, row := range page.Records {
		value, err := r.decodeIndex(row)
		if err != nil {
			return PendingPage{}, err
		}
		if row.Item.PartitionKey != r.partition(shard) || row.Item.SortKey <= lastKey {
			return PendingPage{}, ErrCorrupt
		}
		lastKey = row.Item.SortKey
		if value.Status.terminal() {
			continue
		}
		result.Requests = append(result.Requests, PendingRequest{ID: value.ID, Status: value.Status, Revision: value.Revision, UpdatedAt: value.UpdatedAt, Initializing: value.Revision == 0})
	}
	return result, nil
}
