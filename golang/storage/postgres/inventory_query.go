package postgres

// This file contains the bounded read side of persisted provider model
// inventory.  It deliberately stops at a typed storage page: authorization,
// signed public cursors, and conversion to the llm.ModelInventoryPage wire
// shape belong to the control/query layer.  A page is pinned to one snapshot
// horizon so a later cursor cannot silently switch to a newly persisted
// inventory while it is being consumed.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

// These aliases retain the legacy SQL adapter while query contracts live in control.
type InventoryModelPosition = control.InventoryModelPosition
type InventoryModelListOptions = control.InventoryModelListOptions
type InventorySnapshotInfo = control.InventorySnapshotInfo
type InventoryModelRecord = control.InventoryModelRecord
type InventoryModelPage = control.InventoryModelPage

const DefaultInventoryPageSize = control.DefaultInventoryPageSize
const MaxInventoryPageSize = control.MaxInventoryPageSize

func validInventoryLifecycle(value control.Lifecycle) bool {
	switch value {
	case control.LifecycleAvailable, control.LifecycleDeprecated,
		control.LifecycleUnavailable, control.LifecycleUnknown:
		return true
	default:
		return false
	}
}

// ListInventoryModels reads the latest immutable snapshot for each matching
// provider/endpoint and then applies a stable (provider, endpoint, model-id)
// keyset page.  Both reads run in one repeatable-read transaction.  The first
// read computes the horizon, while the second read is pinned to that horizon;
// this prevents a new snapshot from changing page membership between calls.
func (repository InventoryRepository) ListInventoryModels(ctx context.Context, options InventoryModelListOptions) (InventoryModelPage, error) {
	var page InventoryModelPage
	if err := repository.validateRead(); err != nil {
		return page, err
	}
	if err := options.Normalize(); err != nil {
		return page, err
	}
	snapshots, err := repository.Namespace.Render("provider_inventory_snapshots")
	if err != nil {
		return page, err
	}
	models, err := repository.Namespace.Render("provider_inventory_models")
	if err != nil {
		return page, err
	}
	tx, err := repository.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, redactPostgresError(fmt.Errorf("begin inventory model read: %w", err))
	}
	defer tx.Rollback(ctx)

	var horizon *time.Time
	if options.SnapshotHorizon.IsZero() {
		query := latestInventoryHorizonQuery(snapshots)
		if err := tx.QueryRow(ctx, query, options.ConfigDigest[:], options.Provider, options.EndpointID).Scan(&horizon); err != nil {
			return page, redactPostgresError(fmt.Errorf("find inventory snapshot horizon: %w", err))
		}
		if horizon == nil {
			if err := tx.Commit(ctx); err != nil {
				return page, redactPostgresError(fmt.Errorf("commit empty inventory model read: %w", err))
			}
			return page, nil
		}
		page.SnapshotHorizon = horizon.UTC()
	} else {
		page.SnapshotHorizon = options.SnapshotHorizon.UTC()
	}

	query := latestInventoryModelsQuery(snapshots, models)
	rows, err := tx.Query(ctx, query,
		options.ConfigDigest[:], options.Provider, options.EndpointID,
		page.SnapshotHorizon, options.ModelPrefix, options.Lifecycle,
		options.After.Provider, options.After.EndpointID, options.After.SnapshotID, options.After.ProviderModelID,
		options.Limit+1,
	)
	if err != nil {
		return page, redactPostgresError(fmt.Errorf("list inventory models: %w", err))
	}
	defer rows.Close()
	for rows.Next() {
		var record InventoryModelRecord
		var snapshotID uuid.UUID
		var configDigest, inventoryDigest, capabilityDigest []byte
		var source, lifecycle string
		var displayName, ownedBy string
		var metadata []byte
		var createdAt *time.Time
		if err := rows.Scan(
			&snapshotID, &configDigest, &inventoryDigest, &record.Snapshot.Provider,
			&record.Snapshot.EndpointID, &source, &record.Snapshot.ObservedAt,
			&record.Snapshot.ExpiresAt, &record.Snapshot.Complete,
			&record.Model.ProviderModelID, &displayName, &ownedBy, &createdAt,
			&lifecycle, &capabilityDigest, &metadata,
		); err != nil {
			return InventoryModelPage{}, redactPostgresError(fmt.Errorf("scan inventory model: %w", err))
		}
		record.Snapshot.ID = snapshotID
		if len(configDigest) != len(record.Snapshot.ConfigDigest) || len(inventoryDigest) != len(record.Snapshot.InventoryDigest) || len(capabilityDigest) != len(record.Model.CapabilityDigest) {
			return InventoryModelPage{}, errors.New("PostgreSQL inventory model has invalid digest length")
		}
		copy(record.Snapshot.ConfigDigest[:], configDigest)
		copy(record.Snapshot.InventoryDigest[:], inventoryDigest)
		copy(record.Model.CapabilityDigest[:], capabilityDigest)
		record.Snapshot.Source = control.InventorySource(source)
		record.Model.Lifecycle = control.Lifecycle(lifecycle)
		record.Model.DisplayName = displayName
		record.Model.OwnedBy = ownedBy
		record.Snapshot.ObservedAt = record.Snapshot.ObservedAt.UTC()
		record.Snapshot.ExpiresAt = record.Snapshot.ExpiresAt.UTC()
		if createdAt != nil {
			record.Model.CreatedAt = createdAt.UTC()
		}
		if err := validateInventoryModelRecord(record, options.ConfigDigest); err != nil {
			return InventoryModelPage{}, err
		}
		if string(metadata) == "null" {
			return InventoryModelPage{}, errors.New("PostgreSQL inventory model metadata is null")
		}
		if err := json.Unmarshal(metadata, &record.Model.SafeMetadata); err != nil {
			return InventoryModelPage{}, fmt.Errorf("decode inventory model metadata: %w", err)
		}
		if err := validateInventoryModelMetadata(record.Model.SafeMetadata); err != nil {
			return InventoryModelPage{}, err
		}
		if len(page.Models) == options.Limit {
			last := page.Models[len(page.Models)-1]
			page.Next = &InventoryModelPosition{Provider: last.Snapshot.Provider, EndpointID: last.Snapshot.EndpointID, SnapshotID: last.Snapshot.ID, ProviderModelID: last.Model.ProviderModelID}
			break
		}
		page.Models = append(page.Models, record)
	}
	if err := rows.Err(); err != nil {
		return InventoryModelPage{}, redactPostgresError(fmt.Errorf("read inventory models: %w", err))
	}
	// pgx keeps the connection busy while a result set is open, even after
	// Next has returned false (or after we stopped at limit+1).  Close the
	// result before committing the read-only transaction.
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return InventoryModelPage{}, redactPostgresError(fmt.Errorf("commit inventory model read: %w", err))
	}
	return page, nil
}

func (repository InventoryRepository) validateRead() error {
	if repository.Pool == nil {
		return errors.New("inventory repository pool is nil")
	}
	return repository.Namespace.Validate()
}

func latestInventoryHorizonQuery(snapshots string) string {
	return "SELECT MAX(observed_at) FROM (SELECT DISTINCT ON (provider, endpoint_id, endpoint_account_hmac) observed_at FROM " + snapshots + " WHERE config_digest=$1 AND ($2='' OR provider=$2) AND ($3='' OR endpoint_id=$3) ORDER BY provider, endpoint_id, endpoint_account_hmac, observed_at DESC, inventory_snapshot_id DESC) latest"
}

func latestInventoryModelsQuery(snapshots, models string) string {
	return "WITH latest AS (SELECT DISTINCT ON (provider, endpoint_id, endpoint_account_hmac) inventory_snapshot_id, config_digest, inventory_digest, provider, endpoint_id, source, observed_at, expires_at, complete FROM " + snapshots + " WHERE config_digest=$1 AND ($2='' OR provider=$2) AND ($3='' OR endpoint_id=$3) AND observed_at <= $4 ORDER BY provider, endpoint_id, endpoint_account_hmac, observed_at DESC, inventory_snapshot_id DESC) SELECT latest.inventory_snapshot_id, latest.config_digest, latest.inventory_digest, latest.provider, latest.endpoint_id, latest.source, latest.observed_at, latest.expires_at, latest.complete, model.provider_model_id, COALESCE(model.display_name,''), COALESCE(model.owned_by,''), model.created_at_provider, model.lifecycle_state, model.capability_digest, model.safe_metadata FROM latest JOIN " + models + " model ON model.inventory_snapshot_id=latest.inventory_snapshot_id WHERE ($5='' OR LEFT(model.provider_model_id, char_length($5))=$5) AND ($6='' OR model.lifecycle_state=$6) AND ($7='' OR (latest.provider, latest.endpoint_id, latest.inventory_snapshot_id, model.provider_model_id) > ($7,$8,$9,$10)) ORDER BY latest.provider, latest.endpoint_id, latest.inventory_snapshot_id, model.provider_model_id LIMIT $11"
}

func validateInventoryModelRecord(record InventoryModelRecord, expectedConfig [32]byte) error {
	if record.Snapshot.ConfigDigest != expectedConfig || record.Snapshot.ID == uuid.Nil || record.Snapshot.InventoryDigest == ([32]byte{}) {
		return errors.New("PostgreSQL inventory model has invalid snapshot identity")
	}
	if record.Snapshot.Provider == "" || record.Snapshot.EndpointID == "" || strings.TrimSpace(record.Snapshot.Provider) != record.Snapshot.Provider || strings.TrimSpace(record.Snapshot.EndpointID) != record.Snapshot.EndpointID {
		return errors.New("PostgreSQL inventory model has unsafe provider identity")
	}
	if record.Snapshot.Source != control.InventoryProviderAPI && record.Snapshot.Source != control.InventoryConfiguredOnly && record.Snapshot.Source != control.InventoryUnsupported {
		return fmt.Errorf("PostgreSQL inventory model has unknown source %q", record.Snapshot.Source)
	}
	if record.Snapshot.Source == control.InventoryUnsupported {
		return errors.New("PostgreSQL unsupported inventory snapshot contains model rows")
	}
	if record.Snapshot.ObservedAt.IsZero() || record.Snapshot.ExpiresAt.IsZero() || !record.Snapshot.ExpiresAt.After(record.Snapshot.ObservedAt) {
		return errors.New("PostgreSQL inventory model has invalid snapshot interval")
	}
	if record.Model.ProviderModelID == "" || len(record.Model.ProviderModelID) > 256 || strings.TrimSpace(record.Model.ProviderModelID) != record.Model.ProviderModelID || strings.ContainsAny(record.Model.ProviderModelID, "\x00\r\n") {
		return errors.New("PostgreSQL inventory model has unsafe model id")
	}
	if !validInventoryLifecycle(record.Model.Lifecycle) {
		return fmt.Errorf("PostgreSQL inventory model has unknown lifecycle %q", record.Model.Lifecycle)
	}
	for name, value := range map[string]string{"display_name": record.Model.DisplayName, "owned_by": record.Model.OwnedBy} {
		if len(value) > 512 || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("PostgreSQL inventory model %s is too long or unsafe", name)
		}
	}
	return nil
}

func validateInventoryModelMetadata(metadata map[string]string) error {
	if len(metadata) > 32 {
		return errors.New("PostgreSQL inventory model metadata exceeds limit")
	}
	for key, value := range metadata {
		if key == "" || len(key) > 128 || strings.TrimSpace(key) != key || strings.ContainsAny(key, "\x00\r\n") {
			return errors.New("PostgreSQL inventory model metadata has an unsafe key")
		}
		if len(value) > 512 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("PostgreSQL inventory model metadata has an unsafe value")
		}
	}
	return nil
}
