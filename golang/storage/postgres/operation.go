package postgres

// Operation persistence is intentionally a small, one-shot boundary. It
// stores content-free request metadata, an envelope-encrypted canonical
// request payload, and every route attempt while the provider adapter remains
// responsible for the actual network call.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const (
	defaultOperationKind = "generate"
	defaultAPIVersion    = "llm.generate.v1"
	defaultCostMethod    = "provider_reported"
)

// OperationRepository implements admission.AdmissionStore for PostgreSQL.
// UUID operation IDs remain the durable foreign-reference key. Non-UUID IDs
// are converted only at this storage boundary; logical replay is resolved by
// the immutable scope/kind/API/actor/operation-key tuple and request digest.
type OperationRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Keys      Keyring
	Scopes    ScopeRepository
	Retention time.Duration
	Now       func() time.Time
}

func (r OperationRepository) validate() error {
	if r.Pool == nil {
		return errors.New("operation repository pool is nil")
	}
	if err := r.Namespace.Validate(); err != nil {
		return err
	}
	if _, _, err := r.Keys.activeKey(); err != nil {
		return err
	}
	if err := r.Scopes.validate(); err != nil {
		return err
	}
	if r.Retention <= 0 {
		return errors.New("operation repository retention must be positive")
	}
	return nil
}

func DefaultOperationRepository(pool *pgxpool.Pool, namespace Namespace, keys Keyring, scopes ScopeRepository) OperationRepository {
	return OperationRepository{Pool: pool, Namespace: namespace, Keys: keys, Scopes: scopes, Retention: 24 * time.Hour, Now: time.Now}
}

func operationUUID(id string) uuid.UUID {
	if parsed, err := uuid.Parse(id); err == nil {
		return parsed
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("llmtw/operation/v1\x00"+id))
}

func operationHMAC(key []byte, domain string, value []byte) [32]byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("llmtw/" + domain + "/v1\x00"))
	_, _ = mac.Write(value)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func (r OperationRepository) activeKey() ([]byte, error) {
	_, key, err := r.Keys.activeKey()
	return key, err
}

func splitScope(value string) (string, string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", errors.New("operation scope is empty")
	}
	// The engine's canonical scope key is tenant + NUL + operation key.
	// Preserve that format while deriving the HMAC lookup components; passing
	// the complete key as a tenant would be rejected by ScopeHMAC because it
	// contains a control character.
	if strings.Contains(value, "\x00") {
		parts := strings.SplitN(value, "\x00", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return "", "", errors.New("operation scope has an empty component")
		}
		return parts[0], parts[1], nil
	}
	parts := strings.SplitN(value, "/", 2)
	if len(parts) == 1 {
		return parts[0], "default", nil
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", errors.New("operation scope has an empty component")
	}
	return parts[0], parts[1], nil
}

func normalizeManifest(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return []byte(`{}`), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("request manifest is invalid JSON: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("request manifest must be a JSON object")
	}
	return json.Marshal(value)
}
func canonicalOperationRequestManifest(payload []byte, digest [32]byte) ([]byte, error) {
	return json.Marshal(map[string]any{
		"schema_version":    2,
		"payload_sha256":    hex.EncodeToString(digest[:]),
		"payload_bytes":     len(payload),
		"payload_reference": "inline",
	})
}

func normalizeImmutableFacts(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	normalized, err := normalizeManifest(raw)
	if err != nil {
		return nil, fmt.Errorf("immutable reservation facts: %w", err)
	}
	return normalized, nil
}

func (r OperationRepository) beginKeyring() ([]byte, error) {
	key, err := r.activeKey()
	if err != nil {
		return nil, err
	}
	if len(key) != keyDigestBytes {
		return nil, errors.New("operation HMAC key must be exactly 32 bytes")
	}
	return key, nil
}

func usdText(value pricing.USD) (string, error) { return EncodeUSD(value) }

func (r OperationRepository) Begin(ctx context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	var result admission.BeginResult
	if err := r.validate(); err != nil {
		return result, err
	}
	if request.ID == "" {
		return result, errors.New("operation id is empty")
	}
	if request.OperationKey == "" || strings.TrimSpace(request.OperationKey) != request.OperationKey ||
		strings.ContainsRune(request.OperationKey, '\x00') {
		return result, errors.New("operation key is empty or not canonical")
	}
	if request.Actor == "" || strings.TrimSpace(request.Actor) != request.Actor ||
		strings.ContainsRune(request.Actor, '\x00') {
		return result, errors.New("operation actor is empty or not canonical")
	}
	if request.ExpiresAt.IsZero() {
		request.ExpiresAt = r.clock().Add(r.Retention)
	}
	if !request.ExpiresAt.After(r.clock()) {
		return result, errors.New("operation expiry must be in the future")
	}
	kind := request.OperationKind
	if kind == "" {
		kind = defaultOperationKind
	}
	if kind != "generate" && kind != "compact" {
		return result, errors.New("unsupported operation kind")
	}
	apiVersion := request.APIVersion
	if apiVersion == "" {
		apiVersion = defaultAPIVersion
	}
	version := request.RequestSchemaVersion
	if version == 0 {
		version = 1
	}
	suppliedManifest, err := normalizeManifest(request.RequestManifest)
	if err != nil {
		return result, err
	}
	payload := request.RequestPayload
	hasDistinctPayload := len(payload) != 0
	if !hasDistinctPayload {
		// Legacy callers supplied one value for both columns. Keep their
		// encrypted replay bytes, but derive safe metadata rather than copying
		// those bytes into request_manifest_jsonb.
		payload = suppliedManifest
	}
	manifest, err := canonicalOperationRequestManifest(payload, request.RequestDigest)
	if err != nil {
		return result, err
	}
	if hasDistinctPayload {
		if sha256.Sum256(payload) != request.RequestDigest {
			return result, errors.New("request payload digest does not match request digest")
		}
		if !hmac.Equal(suppliedManifest, manifest) {
			return result, errors.New("request manifest does not match canonical content-free metadata")
		}
	}
	immutableFacts, err := normalizeImmutableFacts(request.ImmutableFacts)
	if err != nil {
		return result, err
	}
	scopeTenant, scopeProject, err := splitScope(request.ScopeKey)
	if err != nil {
		return result, err
	}
	scope, err := r.Scopes.Ensure(ctx, scopeTenant, scopeProject)
	if err != nil {
		return result, err
	}
	key, err := r.beginKeyring()
	if err != nil {
		return result, err
	}
	opID := operationUUID(request.ID)
	var releasedOpID uuid.UUID
	if request.ReleasedID != "" {
		releasedOpID = operationUUID(request.ReleasedID)
	}
	actorHMAC := operationHMAC(key, "operation-actor", []byte(request.Actor))
	operationKeyMaterial := request.Actor + "\x00" + apiVersion + "\x00" + request.OperationKey
	operationKey := operationHMAC(key, "operation-key-v2", []byte(operationKeyMaterial))
	var releasedOperationKey [32]byte
	if request.ReleasedID != "" {
		releasedOperationKey = operationHMAC(key, "operation-key", []byte(request.ReleasedID))
	}
	fingerprint := operationHMAC(key, "request-fingerprint", request.RequestDigest[:])
	sealed, err := r.Keys.Seal(EnvelopeContext{ScopeID: scope.ID, OperationID: opID, PayloadKind: "operation-request", Digest: request.RequestDigest}, payload)
	if err != nil {
		return result, err
	}
	// Bind the encrypted scope key to the deterministic operation identity so
	// readers can reopen it without retaining the caller's raw scope key.
	scopeDigest := operationHMAC(key, "scope-key", opID[:])
	scopeSealed, err := r.Keys.Seal(EnvelopeContext{ScopeID: scope.ID, OperationID: opID, PayloadKind: "operation-scope-key", Digest: scopeDigest}, []byte(request.ScopeKey))
	if err != nil {
		return result, err
	}
	cost, err := usdText(request.ReservationUSD)
	if err != nil {
		return result, err
	}
	configDigest := request.ConfigDigest
	if configDigest == [32]byte{} {
		configDigest = sha256.Sum256([]byte(request.ConfigVersion))
	}
	if request.ConfigVersion == "" {
		request.ConfigVersion = "unknown"
	}
	if request.LeaseUntil.IsZero() {
		request.LeaseUntil = r.clock().Add(5 * time.Minute)
	}
	tokenForID := func(id uuid.UUID) string {
		digest := operationHMAC(key, "dispatch-token", id[:])
		return hex.EncodeToString(digest[:])
	}
	tokenValue := tokenForID(opID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return result, err
	}
	configs, err := r.Namespace.Render("configuration_snapshots")
	if err != nil {
		return result, err
	}
	currentQuery := "SELECT operation_id::text, request_digest, request_fingerprint_hmac, immutable_reservation_facts FROM " + operations +
		" WHERE scope_id=$1 AND operation_kind=$2 AND api_version=$3 AND operation_actor_hmac=$4" +
		" AND operation_key_hmac=$5 AND operation_identity_version=2 FOR UPDATE"
	releasedQuery := "SELECT operation_id::text, scope_id, operation_kind, api_version, operation_key_hmac, request_digest, request_fingerprint_hmac, immutable_reservation_facts FROM " +
		operations + " WHERE operation_id=$1 AND operation_identity_version=1 FOR UPDATE"
	insert := "INSERT INTO " + operations + " (operation_id, scope_id, operation_kind, api_version, operation_actor_hmac, operation_identity_version, operation_key_hmac, request_fingerprint_hmac, request_digest, request_schema_version, request_manifest_jsonb, request_payload_sha256, request_payload_byte_length, request_payload_reference, request_inline_ciphertext, request_key_id, scope_key_ciphertext, scope_key_key_id, scope_key_context_digest, config_digest, immutable_reservation_facts, state, lease_expires_at, operation_expires_at, reserved_cost_usd, incurred_cost_usd, cost_status, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,2,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20::jsonb,'reserved',$21,$22,$23,$24,'pending',clock_timestamp(),clock_timestamp()) ON CONFLICT DO NOTHING"
	err = WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		acceptExisting := func(existingID string, existingDigest, existingFingerprint, existingFacts []byte) (bool, error) {
			if !hmac.Equal(existingDigest, request.RequestDigest[:]) ||
				!hmac.Equal(existingFingerprint, fingerprint[:]) {
				return false, admission.ErrOperationConflict
			}
			if len(immutableFacts) != 0 {
				if len(existingFacts) == 0 {
					updated, updateErr := tx.Exec(ctx, "UPDATE "+operations+" SET immutable_reservation_facts=$2::jsonb, updated_at=clock_timestamp() WHERE operation_id=$1 AND state='reserved' AND immutable_reservation_facts IS NULL", operationUUID(existingID), immutableFacts)
					if updateErr != nil {
						return false, redactPostgresError(fmt.Errorf("persist immutable reservation facts: %w", updateErr))
					}
					if updated.RowsAffected() != 1 {
						return false, admission.ErrOperationConflict
					}
				} else {
					storedFacts, factsErr := normalizeImmutableFacts(existingFacts)
					if factsErr != nil || !hmac.Equal(storedFacts, immutableFacts) {
						return false, admission.ErrOperationConflict
					}
				}
			}
			if operationUUID(existingID) == opID {
				result.Operation.ID = request.ID
			} else {
				result.Operation.ID = existingID
			}
			result.Existing = true
			return true, nil
		}
		lookupCurrent := func() (bool, error) {
			var existingID string
			var existingDigest, existingFingerprint, existingFacts []byte
			scanErr := tx.QueryRow(ctx, currentQuery, scope.ID, kind, apiVersion, actorHMAC[:], operationKey[:]).
				Scan(&existingID, &existingDigest, &existingFingerprint, &existingFacts)
			if errors.Is(scanErr, pgx.ErrNoRows) {
				return false, nil
			}
			if scanErr != nil {
				return false, redactPostgresError(fmt.Errorf("lookup strict PostgreSQL operation identity: %w", scanErr))
			}
			return acceptExisting(existingID, existingDigest, existingFingerprint, existingFacts)
		}

		found, lookupErr := lookupCurrent()
		if lookupErr != nil || found {
			return lookupErr
		}
		if releasedOpID != uuid.Nil {
			var existingID, existingKind, existingAPIVersion string
			var existingScope uuid.UUID
			var existingOperationKey, existingDigest, existingFingerprint, existingFacts []byte
			scanErr := tx.QueryRow(ctx, releasedQuery, releasedOpID).Scan(
				&existingID, &existingScope, &existingKind, &existingAPIVersion,
				&existingOperationKey, &existingDigest, &existingFingerprint, &existingFacts,
			)
			switch {
			case scanErr == nil:
				if existingScope != scope.ID || existingKind != kind || existingAPIVersion != apiVersion ||
					!hmac.Equal(existingOperationKey, releasedOperationKey[:]) ||
					!hmac.Equal(existingDigest, request.RequestDigest[:]) ||
					!hmac.Equal(existingFingerprint, fingerprint[:]) {
					return admission.ErrOperationConflict
				}
				updated, updateErr := tx.Exec(ctx, "UPDATE "+operations+
					" SET operation_actor_hmac=$2, operation_key_hmac=$3, operation_identity_version=2, updated_at=clock_timestamp()"+
					" WHERE operation_id=$1 AND operation_identity_version=1",
					releasedOpID, actorHMAC[:], operationKey[:])
				if updateErr != nil {
					return redactPostgresError(fmt.Errorf("bind released PostgreSQL operation identity: %w", updateErr))
				}
				if updated.RowsAffected() != 1 {
					return admission.ErrOperationConflict
				}
				_, acceptErr := acceptExisting(existingID, existingDigest, existingFingerprint, existingFacts)
				return acceptErr
			case !errors.Is(scanErr, pgx.ErrNoRows):
				return redactPostgresError(fmt.Errorf("lookup released PostgreSQL operation identity: %w", scanErr))
			}
		}

		// Keep the referenced configuration digest self-contained for callers
		// that do not yet have a separate configuration snapshot repository.
		if _, err := tx.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$1,'{}'::jsonb) ON CONFLICT DO NOTHING", configDigest[:], request.ConfigVersion); err != nil {
			return redactPostgresError(fmt.Errorf("persist operation configuration: %w", err))
		}
		inserted, err := tx.Exec(ctx, insert,
			opID, scope.ID, kind, apiVersion, actorHMAC[:], operationKey[:], fingerprint[:],
			request.RequestDigest[:], version, manifest, request.RequestDigest[:],
			int64(len(payload)), "inline", sealed.Ciphertext, sealed.KeyID,
			scopeSealed.Ciphertext, scopeSealed.KeyID, scopeSealed.ContextHash[:],
			configDigest[:], nullableJSON(immutableFacts), request.LeaseUntil,
			request.ExpiresAt, cost, cost,
		)
		if err != nil {
			return redactPostgresError(fmt.Errorf("insert PostgreSQL operation: %w", err))
		}
		if inserted.RowsAffected() == 0 {
			found, lookupErr := lookupCurrent()
			if lookupErr != nil || found {
				return lookupErr
			}
			return admission.ErrOperationConflict
		}
		result.Operation = admission.Operation{
			ID: request.ID, ScopeKey: request.ScopeKey,
			RequestDigest: request.RequestDigest, State: admission.StateReserved,
			ReservedCostUSD: &request.ReservationUSD,
			ImmutableFacts:  append([]byte(nil), immutableFacts...),
			DispatchToken:   tokenValue, LeaseUntil: request.LeaseUntil,
			ExpiresAt: request.ExpiresAt, ConfigVersion: request.ConfigVersion,
			PriceVersion: request.PriceVersion, CreatedAt: r.clock(), UpdatedAt: r.clock(),
		}
		return nil
	})
	if err != nil {
		return admission.BeginResult{}, err
	}
	if result.Existing {
		hydrated, getErr := r.Get(ctx, result.Operation.ID)
		if getErr != nil {
			return admission.BeginResult{}, getErr
		}
		result.Operation = hydrated
		if len(immutableFacts) != 0 {
			storedFacts, factsErr := normalizeImmutableFacts(hydrated.ImmutableFacts)
			if factsErr != nil || !hmac.Equal(storedFacts, immutableFacts) {
				return admission.BeginResult{}, admission.ErrOperationConflict
			}
		}
	}
	return result, nil
}

func (r OperationRepository) clock() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r OperationRepository) MarkDispatching(ctx context.Context, request admission.DispatchRequest) error {
	return r.transitionDispatch(ctx, request)
}

func (r OperationRepository) transitionDispatch(ctx context.Context, request admission.DispatchRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	opID := operationUUID(request.OperationID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return err
	}
	attempts, err := r.Namespace.Render("operation_attempts")
	if err != nil {
		return err
	}
	now := r.clock()
	lease := request.LeaseUntil
	if lease.IsZero() {
		lease = now.Add(5 * time.Minute)
	}
	if !lease.After(now) {
		return admission.ErrInvalidTransition
	}
	return WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var stateValue string
		var persistedLeaseLive bool
		var number int
		// poll_count tracks provider polling, not route attempts. Derive the
		// next attempt from the durable attempt rows while holding the operation
		// lock so retries cannot reuse attempt number one.
		selectQ := "SELECT state, COALESCE(lease_expires_at > clock_timestamp(), false), COALESCE((SELECT MAX(attempt_number) FROM " + attempts + " WHERE operation_id=$1),0) FROM " + operations + " WHERE operation_id=$1 FOR UPDATE"
		if err := tx.QueryRow(ctx, selectQ, opID).Scan(&stateValue, &persistedLeaseLive, &number); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return admission.ErrOperationNotFound
			}
			return redactPostgresError(err)
		}
		if stateValue != string(admission.StateReserved) {
			return admission.ErrInvalidTransition
		}
		if !persistedLeaseLive {
			return admission.ErrInvalidTransition
		}
		if request.DispatchToken == "" {
			return admission.ErrInvalidToken
		}
		expected := operationHMAC(mustKey(r.Keys), "dispatch-token", opID[:])
		if !hmac.Equal([]byte(request.DispatchToken), []byte(hex.EncodeToString(expected[:]))) {
			return admission.ErrInvalidToken
		}
		facts := request.Attempt
		if facts.RouteID == "" {
			facts.RouteID = "unknown"
		}
		if facts.EndpointID == "" {
			facts.EndpointID = "unknown"
		}
		if facts.Provider == "" {
			facts.Provider = "unknown"
		}
		if facts.ServiceClass == "" {
			facts.ServiceClass = "standard"
		}
		if facts.AttemptNumber <= 0 {
			facts.AttemptNumber = number + 1
		}
		disposition := string(facts.Dispatch)
		if disposition == "" {
			disposition = string(admission.Accepted)
		}
		resolvedModel := facts.ResolvedModel
		if resolvedModel == "" {
			resolvedModel = "unknown"
		}
		updated, err := tx.Exec(ctx, "UPDATE "+operations+" SET state='dispatching', lease_expires_at=$2, route_id=$3, endpoint_id=$4, provider=$5, resolved_model=$6, attempted_service_class=$7, updated_at=clock_timestamp() WHERE operation_id=$1 AND state='reserved' AND lease_expires_at > clock_timestamp() AND $2 > clock_timestamp()", opID, lease, facts.RouteID, facts.EndpointID, facts.Provider, resolvedModel, facts.ServiceClass)
		if err != nil {
			return redactPostgresError(err)
		}
		if updated.RowsAffected() != 1 {
			return admission.ErrInvalidTransition
		}
		_, err = tx.Exec(ctx, "INSERT INTO "+attempts+" (attempt_id,operation_id,attempt_number,route_index,fallback_index,route_id,endpoint_id,provider,endpoint_family,resolved_model,route_model_revision,state,dispatch_disposition,reserved_cost_usd,cost_status,safe_diagnostics) VALUES ($1,$2,$3,0,0,$4,$5,$6,'unknown',$7,'unknown','submitted',$8,0,'pending','{}'::jsonb) ON CONFLICT (operation_id,attempt_number) DO NOTHING", uuid.New(), opID, facts.AttemptNumber, facts.RouteID, facts.EndpointID, facts.Provider, resolvedModel, disposition)
		return err
	})
}

// FailBeforeDispatch records a route attempt and terminalizes its reservation
// in one transaction. It is deliberately limited to outcomes for which the
// caller has proved that no provider write was possible.
func (r OperationRepository) FailBeforeDispatch(ctx context.Context, request admission.FailRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	if request.PostResponse || (request.Certainty != admission.NotDispatched && request.Certainty != admission.Rejected) {
		return errors.New("pre-write failure requires a proven not-dispatched or rejected outcome")
	}
	zero := pricing.MustUSD("0")
	if request.Incurred != 0 || request.IncurredCostUSD.Cmp(zero) != 0 {
		return errors.New("pre-write failure cannot persist incurred cost")
	}
	opID := operationUUID(request.OperationID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return err
	}
	attempts, err := r.Namespace.Render("operation_attempts")
	if err != nil {
		return err
	}
	failureReason := safeReason(request.Reason)
	return WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var current, persistedReason, costStatus, costMethod, serviceClass string
		var number int
		selectQ := "SELECT state, COALESCE(failure_reason_code,''), cost_status, COALESCE(cost_method,''), COALESCE(attempted_service_class,''), COALESCE((SELECT MAX(attempt_number) FROM " + attempts + " WHERE operation_id=$1),0) FROM " + operations + " WHERE operation_id=$1 FOR UPDATE"
		if err := tx.QueryRow(ctx, selectQ, opID).Scan(&current, &persistedReason, &costStatus, &costMethod, &serviceClass, &number); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return admission.ErrOperationNotFound
			}
			return redactPostgresError(err)
		}
		if !r.validToken(opID, request.DispatchToken) {
			return admission.ErrInvalidToken
		}

		facts := request.Attempt
		if facts.Dispatch == "" {
			facts.Dispatch = request.Certainty
		}
		if facts.Dispatch != request.Certainty {
			return errors.New("pre-write attempt disposition conflicts with failure certainty")
		}
		if facts.AttemptNumber <= 0 {
			if current == string(admission.StateReserved) {
				facts.AttemptNumber = number + 1
			} else {
				facts.AttemptNumber = number
			}
		}

		if current == string(admission.StateDefiniteFailed) {
			if costStatus != "exact" || costMethod != "worker_cache_zero" || persistedReason != failureReason || serviceClass != facts.ServiceClass || facts.AttemptNumber <= 0 {
				return admission.ErrOperationConflict
			}
			var attemptState, routeID, endpointID, providerName, resolvedModel, disposition string
			replayQ := "SELECT state, route_id, endpoint_id, provider, resolved_model, COALESCE(dispatch_disposition,'') FROM " + attempts + " WHERE operation_id=$1 AND attempt_number=$2"
			if err := tx.QueryRow(ctx, replayQ, opID, facts.AttemptNumber).Scan(&attemptState, &routeID, &endpointID, &providerName, &resolvedModel, &disposition); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return admission.ErrOperationConflict
				}
				return redactPostgresError(err)
			}
			if (attemptState != "pre_write_failed" && attemptState != string(admission.StateDefiniteFailed)) || routeID != facts.RouteID || endpointID != facts.EndpointID || providerName != facts.Provider || resolvedModel != facts.ResolvedModel || disposition != string(facts.Dispatch) {
				return admission.ErrOperationConflict
			}
			return nil
		}
		if current != string(admission.StateReserved) {
			return admission.ErrInvalidTransition
		}
		if facts.AttemptNumber <= number {
			return admission.ErrOperationConflict
		}

		insertQ := "INSERT INTO " + attempts + " (attempt_id,operation_id,attempt_number,route_index,fallback_index,route_id,endpoint_id,provider,endpoint_family,resolved_model,route_model_revision,state,safe_error_code,dispatch_disposition,reserved_cost_usd,actual_cost_usd,cost_status,cost_method,started_at,finished_at,safe_diagnostics) VALUES ($1,$2,$3,0,0,$4,$5,$6,'',$7,'','pre_write_failed',$8,$9,0,0,'exact','definite_uncharged_zero',clock_timestamp(),clock_timestamp(),'{}'::jsonb)"
		if _, err := tx.Exec(ctx, insertQ, uuid.New(), opID, facts.AttemptNumber, facts.RouteID, facts.EndpointID, facts.Provider, facts.ResolvedModel, failureReason, string(facts.Dispatch)); err != nil {
			return redactPostgresError(err)
		}
		updateQ := "UPDATE " + operations + " SET state='definite_failed', route_id=NULLIF($2,''), endpoint_id=NULLIF($3,''), provider=NULLIF($4,''), resolved_model=NULLIF($5,''), attempted_service_class=NULLIF($6,''), incurred_cost_usd=0, actual_cost_usd=0, cost_status='exact', cost_method='worker_cache_zero', cost_unknown_reason_code=NULL, failure_reason_code=$7, completed_at=clock_timestamp(), retention_expires_at=clock_timestamp()+$8 * interval '1 second', lease_expires_at=NULL, updated_at=clock_timestamp() WHERE operation_id=$1 AND state='reserved'"
		updated, err := tx.Exec(ctx, updateQ, opID, facts.RouteID, facts.EndpointID, facts.Provider, facts.ResolvedModel, facts.ServiceClass, failureReason, r.Retention.Seconds())
		if err != nil {
			return redactPostgresError(err)
		}
		if updated.RowsAffected() != 1 {
			return admission.ErrInvalidTransition
		}
		return nil
	})
}

func mustKey(r Keyring) []byte { _, key, _ := r.activeKey(); return key }

func (r OperationRepository) validToken(operationID uuid.UUID, supplied string) bool {
	if supplied == "" {
		return false
	}
	expected := operationHMAC(mustKey(r.Keys), "dispatch-token", operationID[:])
	return hmac.Equal([]byte(supplied), []byte(hex.EncodeToString(expected[:])))
}

// MarkProviderPending records the provider's durable id before the activity
// returns. The provider id is stored only as an HMAC plus an encrypted
// envelope; it is never emitted in SQL errors or logs.
func (r OperationRepository) MarkProviderPending(ctx context.Context, request admission.ProviderPendingRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	if request.ProviderOperationID == "" || request.EndpointID == "" {
		return errors.New("provider operation id and endpoint are required")
	}
	opID := operationUUID(request.OperationID)
	relation, err := r.Namespace.Render("operations")
	if err != nil {
		return err
	}
	key := mustKey(r.Keys)
	providerHMAC := operationHMAC(key, "provider-operation", []byte(request.ProviderOperationID))
	// The operation scope is intentionally not accepted from the caller. The
	// ciphertext column is opaque at this bounded repository layer; provider
	// adapters that need to decrypt it use their scoped result repository.
	return WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var current string
		var scopeID uuid.UUID
		var existingProviderHMAC []byte
		if err := tx.QueryRow(ctx, "SELECT state, scope_id, provider_operation_id_hmac FROM "+relation+" WHERE operation_id=$1 FOR UPDATE", opID).Scan(&current, &scopeID, &existingProviderHMAC); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return admission.ErrOperationNotFound
			}
			return err
		}
		if current == string(admission.StateProviderPending) {
			if hmac.Equal(existingProviderHMAC, providerHMAC[:]) {
				return nil
			}
			return admission.ErrOperationConflict
		}
		if current != string(admission.StateDispatching) {
			return admission.ErrInvalidTransition
		}
		if !r.validToken(opID, request.DispatchToken) {
			return admission.ErrInvalidToken
		}
		sealed, err := r.Keys.Seal(EnvelopeContext{ScopeID: scopeID, OperationID: opID, PayloadKind: "provider-operation", Digest: providerHMAC}, []byte(request.ProviderOperationID))
		if err != nil {
			return err
		}
		poll := any(nil)
		if !request.PollAfter.IsZero() {
			poll = request.PollAfter
		}
		updated, err := tx.Exec(ctx, "UPDATE "+relation+" SET state='provider_pending', endpoint_id=$2, provider=COALESCE(NULLIF($3,''),provider), provider_operation_id_hmac=$4, provider_operation_id_ciphertext=$5, provider_reference_key_id=$6, provider_pending_at=clock_timestamp(), poll_after=$7, updated_at=clock_timestamp() WHERE operation_id=$1 AND state='dispatching'", opID, request.EndpointID, request.Provider, providerHMAC[:], sealed.Ciphertext, sealed.KeyID, poll)
		if err != nil {
			return err
		}
		if updated.RowsAffected() != 1 {
			return admission.ErrInvalidTransition
		}
		return nil
	})
}

func (r OperationRepository) Continue(ctx context.Context, request admission.ContinueRequest) (admission.ContinueResult, error) {
	var result admission.ContinueResult
	if err := r.validate(); err != nil {
		return result, err
	}
	opID := operationUUID(request.OperationID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return result, err
	}
	remaining, err := usdText(request.RemainingUSD)
	if err != nil {
		return result, err
	}
	now := r.clock()
	lease := request.LeaseUntil
	if lease.IsZero() {
		lease = now.Add(5 * time.Minute)
	}
	if !lease.After(now) {
		return result, admission.ErrInvalidTransition
	}
	err = WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var stateValue string
		if err := tx.QueryRow(ctx, "SELECT state FROM "+operations+" WHERE operation_id=$1 FOR UPDATE", opID).Scan(&stateValue); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return admission.ErrOperationNotFound
			}
			return err
		}
		if stateValue != string(admission.StateDispatching) && stateValue != string(admission.StateProviderPending) {
			return admission.ErrInvalidTransition
		}
		if !r.validToken(opID, request.DispatchToken) {
			return admission.ErrInvalidToken
		}
		_, err := tx.Exec(ctx, "UPDATE "+operations+" SET state='reserved', reserved_cost_usd=$2, lease_expires_at=$3, updated_at=clock_timestamp() WHERE operation_id=$1", opID, remaining, lease)
		return err
	})
	if err != nil {
		return result, err
	}
	result.Operation, err = r.Get(ctx, request.OperationID)
	return result, err
}

func (r OperationRepository) Complete(ctx context.Context, request admission.CompleteRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	return WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		return r.completeTx(ctx, tx, request)
	})
}

func (r OperationRepository) completeTx(ctx context.Context, tx pgx.Tx, request admission.CompleteRequest) error {
	if request.ResultRef == nil || !request.ResultRef.Valid() {
		return errors.New("completed operation requires a valid result reference")
	}
	opID := operationUUID(request.OperationID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return err
	}
	attempts, err := r.Namespace.Render("operation_attempts")
	if err != nil {
		return err
	}
	actualUSD := request.ActualCostUSD
	actual, err := usdText(actualUSD)
	if err != nil {
		return err
	}
	method := request.CostMethod
	if method == "" {
		method = defaultCostMethod
	}
	status := request.CostStatus
	if status == "" {
		status = "exact"
	}
	if status != "exact" && status != "unknown" {
		return errors.New("completed operation cost status must be exact or unknown")
	}
	catalogVersion := request.CostCatalogVersion
	if status == "exact" && (method == "catalog_usage" || method == "reconstructed_usage") && catalogVersion == "" {
		return errors.New("catalog-derived completed operation cost requires a catalog version")
	}
	var stateValue string
	if err := tx.QueryRow(ctx, "SELECT state FROM "+operations+" WHERE operation_id=$1 FOR UPDATE", opID).Scan(&stateValue); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return admission.ErrOperationNotFound
		}
		return err
	}
	if stateValue != string(admission.StateDispatching) && stateValue != string(admission.StateProviderPending) {
		return admission.ErrInvalidTransition
	}
	if !r.validToken(opID, request.DispatchToken) {
		return admission.ErrInvalidToken
	}
	if status == "unknown" {
		actual = ""
		method = ""
		catalogVersion = ""
		request.UnknownReason = safeReason(request.UnknownReason)
	}
	resultCipher := []byte{0}
	resultKey := r.Keys.Active
	if _, err := tx.Exec(ctx, "UPDATE "+operations+" SET state='completed', result_inline_ciphertext=$2, result_key_id=$3, result_digest=$4, result_byte_length=$5, result_media_type=$6, actual_cost_usd=$7, cost_status=$8, cost_method=$9, cost_catalog_version=$10, cost_unknown_reason_code=$11, completed_at=clock_timestamp(), retention_expires_at=clock_timestamp()+$12 * interval '1 second', lease_expires_at=NULL, updated_at=clock_timestamp() WHERE operation_id=$1 AND state IN ('dispatching','provider_pending')", opID, resultCipher, resultKey, request.ResultRef.Digest[:], request.ResultRef.Size, request.ResultRef.Media, nullableText(actual), status, nullableText(method), nullableText(catalogVersion), nullableText(request.UnknownReason), r.Retention.Seconds()); err != nil {
		return err
	}
	return updateTerminalAttempt(ctx, tx, attempts, opID, request.Attempt, "completed", status, actualUSD, method, catalogVersion, request.UnknownReason)
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func (r OperationRepository) Fail(ctx context.Context, request admission.FailRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	opID := operationUUID(request.OperationID)
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return err
	}
	attempts, err := r.Namespace.Render("operation_attempts")
	if err != nil {
		return err
	}
	stateValue, status, method := failurePersistence(request.Certainty)
	actual := pricing.MustUSD("0")
	incurred := pricing.MustUSD("0")
	costReason := ""
	catalogVersion := ""
	if request.PostResponse {
		if request.Certainty != admission.Accepted {
			return errors.New("post-response failure requires accepted dispatch")
		}
		stateValue = string(admission.StateDefiniteFailed)
		incurred = request.IncurredCostUSD
		switch request.CostStatus {
		case "exact":
			status = "exact"
			actual = request.IncurredCostUSD
			method = request.CostMethod
			if method == "" {
				method = "provider_reported"
			}
			catalogVersion = request.CostCatalogVersion
			if (method == "catalog_usage" || method == "reconstructed_usage") && catalogVersion == "" {
				return errors.New("catalog-derived failed operation cost requires a catalog version")
			}
		case "unknown":
			status = "unknown"
			method = ""
			costReason = safeReason(request.UnknownReason)
		default:
			return errors.New("post-response failure cost status must be exact or unknown")
		}
	} else if status == "unknown" {
		costReason = safeReason(request.Reason)
	}
	incurredText, err := usdText(incurred)
	if err != nil {
		return err
	}
	actualText := ""
	if status == "exact" {
		actualText, err = usdText(actual)
		if err != nil {
			return err
		}
	}
	failureReason := safeReason(request.Reason)
	retentionSQL := "NULL"
	retentionArgs := []any{opID, stateValue, incurredText, nullableText(actualText), status, nullableText(method), nullableText(catalogVersion), nullableText(costReason), failureReason}
	if stateValue != "ambiguous" {
		retentionSQL = "clock_timestamp()+$10 * interval '1 second'"
		retentionArgs = append(retentionArgs, r.Retention.Seconds())
	}
	attempt := request.Attempt
	if attempt.Dispatch == "" {
		attempt.Dispatch = request.Certainty
	}
	return WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var current string
		if err := tx.QueryRow(ctx, "SELECT state FROM "+operations+" WHERE operation_id=$1 FOR UPDATE", opID).Scan(&current); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return admission.ErrOperationNotFound
			}
			return err
		}
		if current != string(admission.StateDispatching) && current != string(admission.StateProviderPending) {
			return admission.ErrInvalidTransition
		}
		if !r.validToken(opID, request.DispatchToken) {
			return admission.ErrInvalidToken
		}
		if _, err := tx.Exec(ctx, "UPDATE "+operations+" SET state=$2, incurred_cost_usd=$3, actual_cost_usd=$4, cost_status=$5, cost_method=$6, cost_catalog_version=$7, cost_unknown_reason_code=$8, failure_reason_code=$9, completed_at=clock_timestamp(), retention_expires_at="+retentionSQL+", lease_expires_at=NULL, updated_at=clock_timestamp() WHERE operation_id=$1", retentionArgs...); err != nil {
			return err
		}
		return updateTerminalAttempt(ctx, tx, attempts, opID, attempt, stateValue, status, actual, method, catalogVersion, costReason)
	})
}

func failurePersistence(certainty admission.DispatchCertainty) (stateValue, status, method string) {
	if certainty == admission.Accepted || certainty == admission.Ambiguous {
		return "ambiguous", "unknown", ""
	}
	return "definite_failed", "exact", "worker_cache_zero"
}

func safeReason(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "provider_outcome_unknown"
	}
	var b strings.Builder
	for _, c := range value {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' {
			b.WriteRune(c)
		}
	}
	value = b.String()
	if len(value) == 0 {
		return "provider_outcome_unknown"
	}
	if len(value) > 64 {
		return value[:64]
	}
	return value
}

func (r OperationRepository) Get(ctx context.Context, id string) (admission.Operation, error) {
	var operation admission.Operation
	if err := r.validate(); err != nil {
		return operation, err
	}
	operations, err := r.Namespace.Render("operations")
	if err != nil {
		return operation, err
	}
	var scopeID uuid.UUID
	var requestFingerprint, requestDigest, resultDigest, scopeCiphertext, scopeContextHash, immutableFacts []byte
	var stateValue, apiVersion, costStatus, costMethod, costReason, failureReason, endpointID, providerName string
	var scopeKeyID *string
	var reserved, incurred, actual *string
	var completed, retention, lease, operationExpiry *time.Time
	var resultSize *int64
	var resultMedia *string
	opID := operationUUID(id)
	err = r.Pool.QueryRow(ctx, "SELECT scope_id, state, api_version, request_fingerprint_hmac, request_digest, reserved_cost_usd::text, incurred_cost_usd::text, actual_cost_usd::text, cost_status, COALESCE(cost_method,''), COALESCE(cost_unknown_reason_code,''), COALESCE(failure_reason_code,''), COALESCE(endpoint_id,''), COALESCE(provider,''), created_at, updated_at, completed_at, retention_expires_at, lease_expires_at, operation_expires_at, result_digest, result_byte_length, result_media_type, scope_key_ciphertext, scope_key_key_id, scope_key_context_digest, immutable_reservation_facts FROM "+operations+" WHERE operation_id=$1", opID).Scan(&scopeID, &stateValue, &apiVersion, &requestFingerprint, &requestDigest, &reserved, &incurred, &actual, &costStatus, &costMethod, &costReason, &failureReason, &endpointID, &providerName, &operation.CreatedAt, &operation.UpdatedAt, &completed, &retention, &lease, &operationExpiry, &resultDigest, &resultSize, &resultMedia, &scopeCiphertext, &scopeKeyID, &scopeContextHash, &immutableFacts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return operation, admission.ErrOperationNotFound
		}
		return operation, redactPostgresError(err)
	}
	operation.ImmutableFacts = append([]byte(nil), immutableFacts...)
	operation.ID = id
	operation.State = admission.OperationState(stateValue)
	if operation.State == admission.StateDispatching || operation.State == admission.StateProviderPending {
		operation.Attempt.EndpointID = endpointID
		operation.Attempt.Provider = providerName
		if operation.State == admission.StateProviderPending {
			operation.Attempt.Dispatch = admission.Accepted
		}
	}
	operationUUIDValue := operationUUID(id)
	tokenDigest := operationHMAC(mustKey(r.Keys), "dispatch-token", operationUUIDValue[:])
	operation.DispatchToken = hex.EncodeToString(tokenDigest[:])
	operation.ConfigVersion = apiVersion
	if len(requestDigest) == len(operation.RequestDigest) {
		copy(operation.RequestDigest[:], requestDigest)
	} else {
		operation.RequestDigest = requestFingerprintDigest(requestFingerprint)
	}
	if lease != nil {
		operation.LeaseUntil = *lease
	}
	if retention != nil {
		operation.ExpiresAt = *retention
	} else if operationExpiry != nil {
		operation.ExpiresAt = *operationExpiry
	}
	if completed != nil {
		operation.CompletedAt = *completed
	}
	if reserved != nil {
		if v, e := DecodeUSD(*reserved); e == nil {
			operation.ReservedCostUSD = &v
		}
	}
	if incurred != nil {
		if v, e := DecodeUSD(*incurred); e == nil {
			operation.IncurredCostUSD = &v
		}
	}
	if actual != nil {
		if v, e := DecodeUSD(*actual); e == nil {
			operation.ActualCostUSD = &v
		}
	}
	if len(resultDigest) == len(operation.RequestDigest) && resultSize != nil && resultMedia != nil && *resultMedia != "" {
		var digest [32]byte
		copy(digest[:], resultDigest)
		operation.ResultRef = &state.BlobRef{Digest: digest, Size: *resultSize, Media: *resultMedia}
	}
	if scopeID != uuid.Nil && len(scopeCiphertext) != 0 && scopeKeyID != nil && *scopeKeyID != "" && len(scopeContextHash) == len(operation.RequestDigest) {
		scopeDigest := operationHMAC(mustKey(r.Keys), "scope-key", opID[:])
		plaintext, openErr := r.Keys.Open(EnvelopeContext{ScopeID: scopeID, OperationID: opID, PayloadKind: "operation-scope-key", Digest: scopeDigest}, SealedValue{KeyID: *scopeKeyID, Ciphertext: scopeCiphertext, ContextHash: bytesToDigest(scopeContextHash)})
		if openErr != nil {
			return admission.Operation{}, redactPostgresError(fmt.Errorf("open operation scope key: %w", openErr))
		}
		operation.ScopeKey = string(plaintext)
	}
	operation.CostStatus = costStatus
	operation.CostMethod = costMethod
	operation.CostUnknownReason = costReason
	operation.FailureReason = failureReason
	attempts, attemptErr := r.Attempts(ctx, id)
	if attemptErr != nil {
		return admission.Operation{}, fmt.Errorf("load durable operation attempts: %w", attemptErr)
	}
	if len(attempts) > 0 {
		operation.Attempt = attempts[len(attempts)-1]
	}
	_ = apiVersion
	return operation, nil
}

func bytesToDigest(value []byte) [32]byte {
	var digest [32]byte
	copy(digest[:], value)
	return digest
}

func requestFingerprintDigest(value []byte) [32]byte { return sha256.Sum256(value) }

// Attempts returns the durable attempt count. A separate result/attempt file
// keeps this query reusable by conformance tests without exposing SQL.
func (r OperationRepository) Attempts(ctx context.Context, id string) ([]admission.AttemptFacts, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	relation, err := r.Namespace.Render("operation_attempts")
	if err != nil {
		return nil, err
	}
	rows, err := r.Pool.Query(ctx, "SELECT attempt_number, route_id, endpoint_id, provider, resolved_model, COALESCE(dispatch_disposition,'not_dispatched') FROM "+relation+" WHERE operation_id=$1 ORDER BY attempt_number", operationUUID(id))
	if err != nil {
		return nil, redactPostgresError(err)
	}
	defer rows.Close()
	var attempts []admission.AttemptFacts
	for rows.Next() {
		var a admission.AttemptFacts
		var d string
		if err := rows.Scan(&a.AttemptNumber, &a.RouteID, &a.EndpointID, &a.Provider, &a.ResolvedModel, &d); err != nil {
			return nil, err
		}
		a.Dispatch = admission.DispatchCertainty(d)
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

// updateTerminalAttempt closes the route-attempt row that produced a terminal
// operation. Operation state and route-attempt state are deliberately kept in
// the same transaction: a replay/reconciliation reader must never observe a
// completed operation whose last provider attempt is still pending. The
// attempt table uses its own zero-cost method name because it predates the
// operation-level worker_cache_zero vocabulary.
func updateTerminalAttempt(ctx context.Context, tx pgx.Tx, relation string, operationID uuid.UUID, attempt admission.AttemptFacts, state, status string, actual pricing.USD, method, catalogVersion, reason string) error {
	if status != "exact" && status != "unknown" {
		return fmt.Errorf("terminal attempt cost status %q is invalid", status)
	}
	dispatch := attempt.Dispatch
	if dispatch == "" {
		switch state {
		case string(admission.StateCompleted):
			dispatch = admission.Accepted
		case string(admission.StateAmbiguous):
			dispatch = admission.Ambiguous
		default:
			dispatch = admission.Rejected
		}
	}
	if dispatch != admission.Accepted && dispatch != admission.Rejected && dispatch != admission.Ambiguous && dispatch != admission.NotDispatched {
		return fmt.Errorf("terminal attempt dispatch certainty %q is invalid", dispatch)
	}
	var actualValue any
	var attemptMethod any
	var attemptCatalogVersion any
	var unknownReason any
	if status == "exact" {
		encoded, err := EncodeUSD(actual)
		if err != nil {
			return err
		}
		actualValue = encoded
		if method == "worker_cache_zero" {
			method = "definite_uncharged_zero"
		}
		if method != "" {
			attemptMethod = method
		}
		if catalogVersion != "" {
			attemptCatalogVersion = catalogVersion
		}
	} else {
		// Unknown actual cost is represented by SQL NULL plus a bounded reason;
		// never coerce it to zero while closing the attempt.
		unknownReason = nullableText(safeReason(reason))
	}
	attemptNumber := attempt.AttemptNumber
	if attemptNumber < 0 {
		attemptNumber = 0
	}
	query := "UPDATE " + relation + " SET state=$2, dispatch_disposition=$3, provider=CASE WHEN $4='' THEN provider ELSE $4 END, resolved_model=CASE WHEN $5='' THEN resolved_model ELSE $5 END, actual_cost_usd=$6, cost_status=$7, cost_method=$8, cost_catalog_version=$9, cost_unknown_reason_code=$10, finished_at=clock_timestamp() WHERE operation_id=$1 AND attempt_number=COALESCE(NULLIF($11,0),(SELECT MAX(attempt_number) FROM " + relation + " WHERE operation_id=$1))"
	updated, err := tx.Exec(ctx, query, operationID, state, string(dispatch), attempt.Provider, attempt.ResolvedModel, actualValue, status, attemptMethod, attemptCatalogVersion, unknownReason, attemptNumber)
	if err != nil {
		return err
	}
	if updated.RowsAffected() != 1 {
		return admission.ErrInvalidTransition
	}
	return nil
}

// ProviderOperation opens the encrypted provider reference for reconciliation
// workers. The operation HMAC is checked as the AEAD digest, so a copied
// ciphertext or mismatched provider id fails closed.
func (r OperationRepository) ProviderOperation(ctx context.Context, id string) (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	relation, err := r.Namespace.Render("operations")
	if err != nil {
		return "", err
	}
	opID := operationUUID(id)
	var scopeID uuid.UUID
	var providerHMAC, ciphertext []byte
	var keyID string
	if err := r.Pool.QueryRow(ctx, "SELECT scope_id, provider_operation_id_hmac, provider_operation_id_ciphertext, provider_reference_key_id FROM "+relation+" WHERE operation_id=$1", opID).Scan(&scopeID, &providerHMAC, &ciphertext, &keyID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", admission.ErrOperationNotFound
		}
		return "", redactPostgresError(err)
	}
	if len(providerHMAC) != keyDigestBytes || len(ciphertext) == 0 || keyID == "" {
		return "", errors.New("provider operation envelope is incomplete")
	}
	var digest [32]byte
	copy(digest[:], providerHMAC)
	plaintext, err := r.Keys.Open(EnvelopeContext{ScopeID: scopeID, OperationID: opID, PayloadKind: "provider-operation", Digest: digest}, SealedValue{KeyID: keyID, Ciphertext: ciphertext, ContextHash: contextHashForProvider(scopeID, opID, digest)})
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// ProviderPollAfter returns the provider's persisted next-poll guidance for a
// pending operation. A missing value means the provider supplied no delay;
// the encrypted provider operation itself remains behind ProviderOperation.
func (r OperationRepository) ProviderPollAfter(ctx context.Context, id string) (time.Time, error) {
	if err := r.validate(); err != nil {
		return time.Time{}, err
	}
	relation, err := r.Namespace.Render("operations")
	if err != nil {
		return time.Time{}, err
	}
	opID := operationUUID(id)
	var pollAfter *time.Time
	if err := r.Pool.QueryRow(ctx, "SELECT poll_after FROM "+relation+" WHERE operation_id=$1", opID).Scan(&pollAfter); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, admission.ErrOperationNotFound
		}
		return time.Time{}, redactPostgresError(err)
	}
	if pollAfter == nil {
		return time.Time{}, nil
	}
	return pollAfter.UTC(), nil
}

func contextHashForProvider(scopeID, operationID uuid.UUID, digest [32]byte) [32]byte {
	hash, _ := contextDigest(EnvelopeContext{ScopeID: scopeID, OperationID: operationID, PayloadKind: "provider-operation", Digest: digest})
	return hash
}
