package cloudstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/budget"
)

var _ budget.InitializationStore = (*Repository)(nil)

// The key excludes every secret and policy version so changing them cannot
// select a new, empty budget authority. Receipts have no TTL or delete method.
func (r *Repository) budgetInitializationKey(namespace string) kv.KeyValueKey {
	digest := sha256.Sum256([]byte(namespace))
	return kv.KeyValueKey{PartitionKey: r.namespace + "/budget-initialization", SortKey: hex.EncodeToString(digest[:])}
}

func (r *Repository) budgetInitializationItem(value budget.Initialization) kv.KeyValueItem {
	key := r.budgetInitializationKey(value.Identity.Namespace)
	data, _ := json.Marshal(value)
	return kv.KeyValueItem{PartitionKey: key.PartitionKey, SortKey: key.SortKey, Fields: kv.KeyValueDocument{"initialization": kv.Bytes(data)}}
}

func (r *Repository) decodeBudgetInitialization(row kv.KeyValueRecord, namespace string) (budget.Initialization, error) {
	var value budget.Initialization
	data, ok := row.Item.Fields["initialization"].(kv.KeyValueBytes)
	if !ok || len(data) > 2048 || len(row.Item.Fields) != 1 || json.Unmarshal(data, &value) != nil || value.Validate() != nil || row.Version == "" || value.Identity.Namespace != namespace || row.Item.Key() != r.budgetInitializationKey(namespace) {
		return budget.Initialization{}, ErrCorrupt
	}
	return value, nil
}

func (r *Repository) ReadBudgetInitialization(ctx context.Context, namespace string) (budget.Initialization, error) {
	if err := validContext(ctx); err != nil {
		return budget.Initialization{}, err
	}
	row, err := r.table.Get(ctx, r.budgetInitializationKey(namespace))
	if err != nil {
		return budget.Initialization{}, err
	}
	return r.decodeBudgetInitialization(row, namespace)
}

func (r *Repository) PrepareBudgetInitialization(ctx context.Context, identity budget.InitializationIdentity, now time.Time) (budget.InitializationPreparation, error) {
	if err := validContext(ctx); err != nil {
		return budget.InitializationPreparation{}, err
	}
	if identity.Validate() != nil || !validTime(now) {
		return budget.InitializationPreparation{}, ErrInvalid
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return budget.InitializationPreparation{}, err
	}
	value := budget.Initialization{Schema: budget.InitializationSchema, Identity: identity, Epoch: id.String(), CreatedAt: now.UTC()}
	_, err = r.table.Create(ctx, r.budgetInitializationItem(value))
	if err == nil {
		return budget.InitializationPreparation{Receipt: value, Created: true}, nil
	}
	if errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrAlreadyExists) {
		return budget.InitializationPreparation{}, err
	}
	previous, err := r.ReadBudgetInitialization(ctx, identity.Namespace)
	if err != nil {
		return budget.InitializationPreparation{}, err
	}
	if previous.Identity != identity {
		return budget.InitializationPreparation{}, contracts.ErrConflict
	}
	return budget.InitializationPreparation{Receipt: previous}, nil
}

func (r *Repository) CompleteBudgetInitialization(ctx context.Context, expected budget.Initialization) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if expected.Validate() != nil {
		return ErrInvalid
	}
	for attempt := 0; attempt < 8; attempt++ {
		row, err := r.table.Get(ctx, r.budgetInitializationKey(expected.Identity.Namespace))
		if err != nil {
			return err
		}
		previous, err := r.decodeBudgetInitialization(row, expected.Identity.Namespace)
		if err != nil {
			return err
		}
		if previous.Identity != expected.Identity || previous.Epoch != expected.Epoch || !previous.CreatedAt.Equal(expected.CreatedAt) {
			return contracts.ErrConflict
		}
		if previous.Ready {
			return nil
		}
		previous.Ready = true
		_, err = r.table.Replace(ctx, r.budgetInitializationItem(previous), row.Version)
		if err == nil || errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrConflict) {
			return err
		}
	}
	return contracts.ErrConflict
}
