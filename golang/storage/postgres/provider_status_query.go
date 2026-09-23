package postgres

// This file contains the set-based read side of the provider status
// projection.  Query Activities own authorization, signed cursors, and wire
// models; this repository deliberately exposes only a bounded, unsigned
// database page so those concerns cannot leak into storage.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
)

// These aliases retain the legacy SQL adapter while query contracts live in control.
type ProviderStatusListOptions = control.ProviderStatusListOptions
type ProviderStatusPage = control.ProviderStatusPage

const DefaultProviderStatusPageSize = control.DefaultProviderStatusPageSize
const MaxProviderStatusPageSize = control.MaxProviderStatusPageSize

func validateProviderStatusQueryIdentifier(name, value string) error {
	if len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("provider status %s is empty or unsafe", name)
	}
	return nil
}

func validProviderStatusAvailability(value control.Availability) bool {
	switch value {
	case control.AvailabilityAvailable, control.AvailabilityDegraded,
		control.AvailabilityUnavailable, control.AvailabilityUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusCredit(value control.CreditState) bool {
	switch value {
	case control.CreditOK, control.CreditLow, control.CreditExhausted, control.CreditUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusBilling(value control.BillingState) bool {
	switch value {
	case control.BillingOK, control.BillingIssue, control.BillingUnknown:
		return true
	default:
		return false
	}
}

func validProviderStatusCircuit(value control.CircuitState) bool {
	switch value {
	case control.CircuitClosed, control.CircuitOpen, control.CircuitHalfOpen:
		return true
	default:
		return false
	}
}

// ListRouteStatuses reads the current provider route projection in stable
// route-id order.  It performs one bounded SQL query and never reads the
// append-only event ledger or invokes a provider management API.
func (repository ProviderStatusRepository) ListRouteStatuses(ctx context.Context, options ProviderStatusListOptions) (ProviderStatusPage, error) {
	var page ProviderStatusPage
	if err := repository.validate(); err != nil {
		return page, err
	}
	if err := options.Normalize(); err != nil {
		return page, err
	}
	relation, err := repository.Namespace.Render("provider_route_status")
	if err != nil {
		return page, err
	}
	query := "SELECT config_digest, config_epoch, route_id, endpoint_id, endpoint_account_hmac, provider, endpoint_family, availability, credit_state, billing_state, circuit_state, consecutive_definite_failures, last_event_digest, observed_at, stale_after, credit_confirmed_at FROM " + relation + " WHERE config_digest = $1 AND ($2 = '' OR provider = $2) AND ($3 = '' OR endpoint_id = $3) AND ($4 = '' OR availability = $4) AND ($5 OR availability <> 'available' OR credit_state <> 'ok' OR billing_state <> 'ok' OR circuit_state <> 'closed') AND ($6::timestamptz IS NULL OR observed_at <= $6) AND ($7 = '' OR route_id > $7) ORDER BY route_id LIMIT $8"
	var horizon any
	if !options.SnapshotHorizon.IsZero() {
		horizon = options.SnapshotHorizon
	}
	rows, err := repository.Pool.Query(ctx, query, options.ConfigDigest[:], options.Provider, options.EndpointID, options.Availability, options.IncludeHealthy, horizon, options.AfterRouteID, options.Limit+1)
	if err != nil {
		return page, redactPostgresError(fmt.Errorf("list provider route statuses: %w", err))
	}
	defer rows.Close()
	for rows.Next() {
		var status control.RouteStatus
		var configDigest, account, eventDigest []byte
		var creditConfirmedAt *time.Time
		if err := rows.Scan(
			&configDigest, &status.ConfigEpoch, &status.RouteID, &status.EndpointID,
			&account, &status.Provider, &status.EndpointFamily, &status.Availability,
			&status.Credit, &status.Billing, &status.Circuit,
			&status.ConsecutiveDefiniteFailures, &eventDigest, &status.ObservedAt,
			&status.StaleAfter, &creditConfirmedAt,
		); err != nil {
			return ProviderStatusPage{}, redactPostgresError(fmt.Errorf("scan provider route status: %w", err))
		}
		if len(configDigest) != len(status.ConfigDigest) || len(account) != len(status.EndpointAccountHMAC) || len(eventDigest) != len(status.LastEventDigest) {
			return ProviderStatusPage{}, errors.New("PostgreSQL provider route status has invalid digest length")
		}
		copy(status.ConfigDigest[:], configDigest)
		copy(status.EndpointAccountHMAC[:], account)
		copy(status.LastEventDigest[:], eventDigest)
		if !validProviderStatusAvailability(status.Availability) || !validProviderStatusCredit(status.Credit) || !validProviderStatusBilling(status.Billing) || !validProviderStatusCircuit(status.Circuit) {
			return ProviderStatusPage{}, errors.New("PostgreSQL provider route status has an unknown enum")
		}
		status.ObservedAt = status.ObservedAt.UTC()
		status.StaleAfter = status.StaleAfter.UTC()
		if creditConfirmedAt != nil {
			status.CreditConfirmedAt = creditConfirmedAt.UTC()
		}
		page.Routes = append(page.Routes, status)
	}
	if err := rows.Err(); err != nil {
		return ProviderStatusPage{}, redactPostgresError(fmt.Errorf("read provider route statuses: %w", err))
	}
	if len(page.Routes) > options.Limit {
		page.NextRouteID = page.Routes[options.Limit-1].RouteID
		page.Routes = page.Routes[:options.Limit]
	}
	return page, nil
}
