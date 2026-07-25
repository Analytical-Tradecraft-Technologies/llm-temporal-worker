package postgres

// Provider status persistence is the durable hand-off for the provider-control
// domain. A validated event is inserted into the append-only ledger and its
// current route projection is updated while holding one PostgreSQL transaction
// and route row lock.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

var ErrProviderStatusNotFound = errors.New("provider route status not found")

type ProviderStatusRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	// Observer is optional. When absent, PersistStatusEvent uses the metrics
	// collector bound to its Activity context, if any.
	Observer ProviderStatusObserver
}

// providerStatusTestHooks coordinate the deterministic missing-row race test
// without adding synchronization controls to the production API.
type providerStatusTestHooks struct {
	beforeRouteLock     func(control.StatusEvent)
	afterProjectionRead func(control.StatusEvent, bool)
}

func DefaultProviderStatusRepository(pool *pgxpool.Pool, namespace Namespace) ProviderStatusRepository {
	return ProviderStatusRepository{Pool: pool, Namespace: namespace}
}

func (repository ProviderStatusRepository) validate() error {
	if repository.Pool == nil {
		return errors.New("provider status repository pool is nil")
	}
	return repository.Namespace.Validate()
}

func validateStatusEvent(event control.StatusEvent) error {
	if event.EventDigest == ([32]byte{}) {
		return errors.New("provider status event digest is required")
	}
	validated, err := control.NewStatusEvent(event.StatusObservation)
	if err != nil {
		return err
	}
	if !bytes.Equal(validated.EventDigest[:], event.EventDigest[:]) {
		return errors.New("provider status event digest does not match its observation")
	}
	return nil
}

// providerRouteAdvisoryLockKey derives the transaction lock used to serialize
// the first projection row for a route. PostgreSQL cannot lock a missing row;
// without this lock, two first events can both read an empty projection and
// race to write projection_version=1. The namespace is included so separate
// worker schemas in one database do not unnecessarily share route locks.
func providerRouteAdvisoryLockKey(namespace Namespace, configDigest [32]byte, routeID string) int64 {
	hashInput := make([]byte, 0, len(namespace.String())+1+len(configDigest)+1+len(routeID))
	hashInput = append(hashInput, namespace.String()...)
	hashInput = append(hashInput, 0)
	hashInput = append(hashInput, configDigest[:]...)
	hashInput = append(hashInput, 0)
	hashInput = append(hashInput, routeID...)
	digest := sha256.Sum256(hashInput)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

// PersistStatusEvent appends event to the ledger and applies it to the route
// projection. A duplicate event digest is an idempotent no-op. Events that
// are valid but stale or for a different endpoint remain in the ledger for
// auditability and return applied=false.
func (repository ProviderStatusRepository) PersistStatusEvent(ctx context.Context, event control.StatusEvent) (applied bool, err error) {
	return repository.persistStatusEvent(ctx, event, providerStatusTestHooks{})
}

func (repository ProviderStatusRepository) persistStatusEvent(ctx context.Context, event control.StatusEvent, hooks providerStatusTestHooks) (applied bool, err error) {
	if err := repository.validate(); err != nil {
		return false, err
	}
	if err := validateStatusEvent(event); err != nil {
		return false, err
	}
	events, err := repository.Namespace.Render("provider_status_events")
	if err != nil {
		return false, err
	}
	routes, err := repository.Namespace.Render("provider_route_status")
	if err != nil {
		return false, err
	}
	tx, err := repository.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, redactPostgresError(fmt.Errorf("begin provider status transaction: %w", err))
	}
	defer tx.Rollback(ctx)
	if hooks.beforeRouteLock != nil {
		hooks.beforeRouteLock(event)
	}
	lockKey := providerRouteAdvisoryLockKey(repository.Namespace, event.ConfigDigest, event.RouteID)
	lockStarted := time.Now()
	_, lockErr := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey)
	repository.observeProviderStatusLock(ctx, lockStarted)
	if lockErr != nil {
		return false, redactPostgresError(fmt.Errorf("lock provider route projection: %w", lockErr))
	}

	var eventID int64
	insertEvent := "INSERT INTO " + events + " (event_digest, config_digest, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, observed_at, source, availability, credit_state, billing_state, safe_error_code, provider_code, evidence_digest, config_epoch, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''),NULLIF($14,''),$15,$16,$17) ON CONFLICT (event_digest) DO NOTHING RETURNING event_id"
	if err := tx.QueryRow(ctx, insertEvent,
		event.EventDigest[:], event.ConfigDigest[:], event.RouteID, event.EndpointID,
		event.EndpointAccountHMAC[:], event.Provider, event.EndpointFamily,
		event.ObservedAt, event.Source, event.Availability, event.Credit,
		event.Billing, event.SafeErrorCode, event.ProviderCode,
		event.EvidenceDigest[:], event.ConfigEpoch, event.ExpiresAt,
	).Scan(&eventID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return false, redactPostgresError(fmt.Errorf("commit duplicate provider status event: %w", err))
			}
			return false, nil
		}
		return false, redactPostgresError(fmt.Errorf("append provider status event: %w", err))
	}

	var current control.RouteStatus
	var currentConfigDigest, currentAccountHMAC, currentEventDigest []byte
	var currentSuccessAt, currentFailureAt *time.Time
	var currentCreditConfirmedAt *time.Time
	var currentProjectionVersion int64
	selectCurrent := "SELECT config_digest, config_epoch, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_digest, last_success_at, last_failure_at, observed_at, stale_after, credit_confirmed_at, projection_version FROM " + routes + " WHERE config_digest = $1 AND route_id = $2 FOR UPDATE"
	rowErr := tx.QueryRow(ctx, selectCurrent, event.ConfigDigest[:], event.RouteID).Scan(
		&currentConfigDigest, &current.ConfigEpoch, &current.RouteID, &current.EndpointID,
		&currentAccountHMAC, &current.Provider, &current.EndpointFamily,
		&current.Availability, &current.Credit, &current.Billing, &current.Circuit,
		&current.ConsecutiveDefiniteFailures, &currentEventDigest, &currentSuccessAt,
		&currentFailureAt, &current.ObservedAt, &current.StaleAfter,
		&currentCreditConfirmedAt, &currentProjectionVersion,
	)
	exists := rowErr == nil
	if rowErr != nil && !errors.Is(rowErr, pgx.ErrNoRows) {
		return false, redactPostgresError(fmt.Errorf("lock provider route status: %w", rowErr))
	}
	if hooks.afterProjectionRead != nil {
		hooks.afterProjectionRead(event, exists)
	}
	if exists {
		if len(currentConfigDigest) != len(current.ConfigDigest) || len(currentAccountHMAC) != len(current.EndpointAccountHMAC) || len(currentEventDigest) != len(current.LastEventDigest) {
			return false, errors.New("PostgreSQL provider route status has invalid digest length")
		}
		copy(current.ConfigDigest[:], currentConfigDigest)
		copy(current.EndpointAccountHMAC[:], currentAccountHMAC)
		copy(current.LastEventDigest[:], currentEventDigest)
		if currentCreditConfirmedAt != nil {
			current.CreditConfirmedAt = currentCreditConfirmedAt.UTC()
		}
		if current.ConfigEpoch != event.ConfigEpoch {
			currentSuccessAt, currentFailureAt, currentCreditConfirmedAt = nil, nil, nil
			current.CreditConfirmedAt = time.Time{}
		}
		if !current.Apply(event) {
			if err := tx.Commit(ctx); err != nil {
				return false, redactPostgresError(fmt.Errorf("commit stale provider status event: %w", err))
			}
			return false, nil
		}
	} else if !current.Apply(event) {
		return false, errors.New("apply first provider status event failed")
	}

	healthy := current.Availability == control.AvailabilityAvailable && current.Credit == control.CreditOK && current.Billing == control.BillingOK
	if healthy {
		current.ConsecutiveDefiniteFailures = 0
		observed := event.ObservedAt.UTC()
		currentSuccessAt = &observed
	} else {
		if exists {
			current.ConsecutiveDefiniteFailures++
		}
		if current.ConsecutiveDefiniteFailures < 1 {
			current.ConsecutiveDefiniteFailures = 1
		}
		observed := event.ObservedAt.UTC()
		currentFailureAt = &observed
	}
	projectionVersion := int64(1)
	if exists {
		projectionVersion = currentProjectionVersion + 1
	}
	upsert := "INSERT INTO " + routes + " (config_digest, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, config_epoch, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_id, last_event_digest, last_success_at, last_failure_at, credit_confirmed_at, observed_at, stale_after, projection_version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) ON CONFLICT (config_digest, route_id) DO UPDATE SET endpoint_id=EXCLUDED.endpoint_id, endpoint_account_hmac=EXCLUDED.endpoint_account_hmac, provider=EXCLUDED.provider, endpoint_family=EXCLUDED.endpoint_family, config_epoch=EXCLUDED.config_epoch, availability=EXCLUDED.availability, credit_state=EXCLUDED.credit_state, billing_state=EXCLUDED.billing_state, circuit_state=EXCLUDED.circuit_state, consecutive_definite_failures=EXCLUDED.consecutive_definite_failures, last_event_id=EXCLUDED.last_event_id, last_event_digest=EXCLUDED.last_event_digest, last_success_at=EXCLUDED.last_success_at, last_failure_at=EXCLUDED.last_failure_at, credit_confirmed_at=EXCLUDED.credit_confirmed_at, observed_at=EXCLUDED.observed_at, stale_after=EXCLUDED.stale_after, projection_version=EXCLUDED.projection_version"
	if _, err := tx.Exec(ctx, upsert,
		current.ConfigDigest[:], current.RouteID, current.EndpointID, current.EndpointAccountHMAC[:],
		current.Provider, current.EndpointFamily, current.ConfigEpoch, current.Availability,
		current.Credit, current.Billing, current.Circuit, current.ConsecutiveDefiniteFailures,
		eventID, current.LastEventDigest[:], currentSuccessAt, currentFailureAt,
		nilTime(current.CreditConfirmedAt), current.ObservedAt, current.StaleAfter,
		projectionVersion,
	); err != nil {
		return false, redactPostgresError(fmt.Errorf("update provider route status: %w", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return false, redactPostgresError(fmt.Errorf("commit provider status event: %w", err))
	}
	return true, nil
}

func nilTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}

// GetRouteStatus reads the current projection for one configuration and route.
func (repository ProviderStatusRepository) GetRouteStatus(ctx context.Context, configDigest [32]byte, routeID string) (control.RouteStatus, error) {
	var status control.RouteStatus
	if err := repository.validate(); err != nil {
		return status, err
	}
	if configDigest == ([32]byte{}) || routeID == "" {
		return status, errors.New("provider route status config digest and route id are required")
	}
	relation, err := repository.Namespace.Render("provider_route_status")
	if err != nil {
		return status, err
	}
	var digest, account, eventDigest []byte
	var creditConfirmedAt *time.Time
	query := "SELECT config_digest, config_epoch, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_digest, observed_at, stale_after, credit_confirmed_at FROM " + relation + " WHERE config_digest = $1 AND route_id = $2"
	err = repository.Pool.QueryRow(ctx, query, configDigest[:], routeID).Scan(
		&digest, &status.ConfigEpoch, &status.RouteID, &status.EndpointID, &account,
		&status.Provider, &status.EndpointFamily, &status.Availability, &status.Credit,
		&status.Billing, &status.Circuit, &status.ConsecutiveDefiniteFailures,
		&eventDigest, &status.ObservedAt, &status.StaleAfter, &creditConfirmedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return control.RouteStatus{}, ErrProviderStatusNotFound
	}
	if err != nil {
		return status, redactPostgresError(fmt.Errorf("get provider route status: %w", err))
	}
	if len(digest) != len(status.ConfigDigest) || len(account) != len(status.EndpointAccountHMAC) || len(eventDigest) != len(status.LastEventDigest) {
		return control.RouteStatus{}, errors.New("PostgreSQL provider route status has invalid digest length")
	}
	copy(status.ConfigDigest[:], digest)
	copy(status.EndpointAccountHMAC[:], account)
	copy(status.LastEventDigest[:], eventDigest)
	if creditConfirmedAt != nil {
		status.CreditConfirmedAt = creditConfirmedAt.UTC()
	}
	return status, nil
}
