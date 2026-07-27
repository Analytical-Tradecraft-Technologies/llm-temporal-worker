-- PostgreSQL worker-state migration template.
-- __SCHEMA__ and __PREFIX__ are replaced only after Namespace validation.
-- This template is intentionally schema-qualified and does not alter session
-- namespace resolution. The installer creates a missing schema and revokes
-- PUBLIC access only for that newly created, worker-owned schema. An existing
-- schema may be operator-owned/shared and must retain its schema-level ACL;
-- the installer creates only the prefixed worker objects in that case.

CREATE TABLE __SCHEMA__.__PREFIX__schema_contract (
    contract_name text PRIMARY KEY,
    contract_version text NOT NULL,
    migration_digest bytea NOT NULL CHECK (octet_length(migration_digest) = 32),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE __SCHEMA__.__PREFIX__scopes (
    scope_id uuid PRIMARY KEY,
    tenant_hmac bytea NOT NULL CHECK (octet_length(tenant_hmac) = 32),
    project_hmac bytea NOT NULL CHECK (octet_length(project_hmac) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    UNIQUE (tenant_hmac, project_hmac)
);

CREATE TABLE __SCHEMA__.__PREFIX__configuration_snapshots (
    config_digest bytea PRIMARY KEY CHECK (octet_length(config_digest) = 32),
    config_version text NOT NULL,
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    sanitized_config jsonb NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    UNIQUE (config_version, source_digest)
);

CREATE TABLE __SCHEMA__.__PREFIX__blobs (
    blob_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    store_id text NOT NULL,
    locator_ciphertext bytea NOT NULL,
    locator_key_id text NOT NULL,
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    media_type text NOT NULL,
    encryption_context_digest bytea NOT NULL
        CHECK (octet_length(encryption_context_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz,
    deletion_state text NOT NULL DEFAULT 'retained'
        CHECK (deletion_state IN ('retained', 'eligible', 'deleting', 'deleted')),
    UNIQUE (scope_id, store_id, sha256, byte_length, media_type)
);

CREATE INDEX __PREFIX__blobs_scope_created_idx
    ON __SCHEMA__.__PREFIX__blobs (scope_id, created_at DESC, blob_id);

CREATE INDEX __PREFIX__blobs_expiry_idx
    ON __SCHEMA__.__PREFIX__blobs (expires_at, blob_id)
    WHERE expires_at IS NOT NULL AND deletion_state = 'retained';
CREATE TABLE __SCHEMA__.__PREFIX__operations (
    operation_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    operation_kind text NOT NULL
        CHECK (operation_kind IN ('generate', 'compact')),
    api_version text NOT NULL,
    operation_key_hmac bytea NOT NULL
        CHECK (octet_length(operation_key_hmac) = 32),
    request_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(request_fingerprint_hmac) = 32),
    request_digest bytea NOT NULL
        CHECK (octet_length(request_digest) = 32),
    request_schema_version integer NOT NULL
        CHECK (request_schema_version > 0),
    request_manifest_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(request_manifest_jsonb) = 'object'),
    request_inline_ciphertext bytea,
    request_blob_id uuid
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    request_key_id text,
    scope_key_ciphertext bytea,
    scope_key_key_id text,
    scope_key_context_digest bytea
        CHECK (
            scope_key_context_digest IS NULL OR
            octet_length(scope_key_context_digest) = 32
        ),
    config_digest bytea NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    state text NOT NULL CHECK (
        state IN (
            'reserved',
            'dispatching',
            'provider_pending',
            'completed',
            'definite_failed',
            'ambiguous',
            'canceled'
        )
    ),
    parent_operation_id uuid
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    parent_checkpoint_id uuid,
    result_inline_ciphertext bytea,
    result_blob_id uuid
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    result_key_id text,
    result_digest bytea CHECK (
        result_digest IS NULL OR octet_length(result_digest) = 32
    ),
    result_byte_length bigint CHECK (
        result_byte_length IS NULL OR result_byte_length >= 0
    ),
    result_media_type text,
    cache_entry_id uuid,
    route_id text,
    endpoint_id text,
    provider text,
    endpoint_family text,
    resolved_model text,
    route_model_revision text,
    requested_service_class text CHECK (
        requested_service_class IS NULL OR
        requested_service_class IN ('economy', 'standard', 'priority')
    ),
    attempted_service_class text CHECK (
        attempted_service_class IS NULL OR
        attempted_service_class IN ('economy', 'standard', 'priority')
    ),
    actual_service_class text CHECK (
        actual_service_class IS NULL OR
        actual_service_class IN ('economy', 'standard', 'priority')
    ),
    provider_idempotency_key_hmac bytea CHECK (
        provider_idempotency_key_hmac IS NULL OR
        octet_length(provider_idempotency_key_hmac) = 32
    ),
    provider_idempotency_key_ciphertext bytea,
    provider_operation_id_hmac bytea CHECK (
        provider_operation_id_hmac IS NULL OR
        octet_length(provider_operation_id_hmac) = 32
    ),
    provider_operation_id_ciphertext bytea,
    provider_reference_key_id text,
    provider_submit_started_at timestamptz,
    provider_accepted_at timestamptz,
    provider_pending_at timestamptz,
    poll_after timestamptz,
    last_polled_at timestamptz,
    poll_count integer NOT NULL DEFAULT 0 CHECK (poll_count >= 0),
    lease_owner uuid,
    lease_expires_at timestamptz,
    operation_expires_at timestamptz NOT NULL,
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    incurred_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (incurred_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL DEFAULT 'pending' CHECK (
        cost_status IN ('pending', 'exact', 'unknown')
    ),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'provider_reported',
            'catalog_usage',
            'reconstructed_usage',
            'worker_cache_zero'
        )
    ),
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    cost_catalog_version text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    retention_expires_at timestamptz,
    UNIQUE (scope_id, operation_kind, operation_key_hmac),
    CHECK (
        state <> 'provider_pending' OR (
            provider_operation_id_ciphertext IS NOT NULL AND
            provider_operation_id_hmac IS NOT NULL AND
            provider_reference_key_id IS NOT NULL AND
            provider_pending_at IS NOT NULL
        )
    ),
    CHECK (
        state <> 'completed' OR (
            completed_at IS NOT NULL AND
            ((result_inline_ciphertext IS NOT NULL)::integer +
             (result_blob_id IS NOT NULL)::integer = 1) AND
            result_digest IS NOT NULL AND
            cost_status IN ('exact', 'unknown')
        )
    ),
    CHECK (
        (request_inline_ciphertext IS NOT NULL)::integer +
        (request_blob_id IS NOT NULL)::integer = 1
    ),
    CHECK (
        request_inline_ciphertext IS NULL = (request_key_id IS NULL)
    ),
    CHECK (
        scope_key_ciphertext IS NULL = (scope_key_key_id IS NULL)
    ),
    CHECK (
        scope_key_ciphertext IS NULL = (scope_key_context_digest IS NULL)
    ),
    CHECK (
        result_inline_ciphertext IS NULL = (result_key_id IS NULL)
    ),
    CHECK (
        (cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        provider_operation_id_ciphertext IS NULL =
        (provider_operation_id_hmac IS NULL)
    ),
    CHECK (
        provider_idempotency_key_ciphertext IS NULL =
        (provider_idempotency_key_hmac IS NULL)
    ),
    CHECK (
        provider_idempotency_key_ciphertext IS NULL OR
        provider_reference_key_id IS NOT NULL
    ),
    CHECK (
        provider_operation_id_ciphertext IS NULL OR
        endpoint_id IS NOT NULL
    ),
    CHECK (
        cost_method <> 'worker_cache_zero' OR
        actual_cost_usd = 0
    ),
    CHECK (
        cost_method NOT IN ('catalog_usage', 'reconstructed_usage') OR
        cost_catalog_version IS NOT NULL
    ),
    CHECK (completed_at IS NULL OR completed_at >= created_at),
    CHECK (
        (state IN ('completed', 'definite_failed', 'canceled') AND
            retention_expires_at IS NOT NULL) OR
        (state IN ('reserved', 'dispatching', 'provider_pending', 'ambiguous') AND
            retention_expires_at IS NULL)
    )
);

CREATE TABLE __SCHEMA__.__PREFIX__operation_attempts (
    attempt_id uuid PRIMARY KEY,
    operation_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    route_index integer NOT NULL CHECK (route_index >= 0),
    fallback_index integer NOT NULL CHECK (fallback_index >= 0),
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    resolved_model text NOT NULL,
    route_model_revision text NOT NULL,
    state text NOT NULL CHECK (
        state IN (
            'planned',
            'pre_write_failed',
            'submitted',
            'provider_pending',
            'completed',
            'definite_failed',
            'ambiguous',
            'canceled'
        )
    ),
    provider_request_id_hmac bytea CHECK (
        provider_request_id_hmac IS NULL OR
        octet_length(provider_request_id_hmac) = 32
    ),
    provider_request_id_ciphertext bytea,
    provider_reference_key_id text,
    safe_error_code text,
    dispatch_disposition text CHECK (
        dispatch_disposition IS NULL OR
        dispatch_disposition IN (
            'not_dispatched',
            'rejected',
            'accepted',
            'ambiguous'
        )
    ),
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL DEFAULT 'pending' CHECK (
        cost_status IN ('pending', 'exact', 'unknown')
    ),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'provider_reported',
            'catalog_usage',
            'reconstructed_usage',
            'definite_uncharged_zero'
        )
    ),
    cost_catalog_version text,
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    possible_write_at timestamptz,
    finished_at timestamptz,
    safe_diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (operation_id, attempt_number),
    CHECK (
        provider_request_id_ciphertext IS NULL =
        (provider_request_id_hmac IS NULL)
    ),
    CHECK (
        provider_request_id_ciphertext IS NULL OR
        provider_reference_key_id IS NOT NULL
    ),
    CHECK (
        (cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND
            cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            cost_method IS NULL AND
            cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (finished_at IS NULL OR cost_status IN ('exact', 'unknown')),
    CHECK (
        cost_method <> 'definite_uncharged_zero' OR actual_cost_usd = 0
    ),
    CHECK (
        cost_method NOT IN ('catalog_usage', 'reconstructed_usage') OR
        cost_catalog_version IS NOT NULL
    ),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE UNIQUE INDEX __PREFIX__operations_provider_operation_uidx
    ON __SCHEMA__.__PREFIX__operations (endpoint_id, provider_operation_id_hmac)
    WHERE provider_operation_id_hmac IS NOT NULL;

CREATE INDEX __PREFIX__operations_parent_operation_idx
    ON __SCHEMA__.__PREFIX__operations (parent_operation_id)
    WHERE parent_operation_id IS NOT NULL;

CREATE INDEX __PREFIX__operations_parent_checkpoint_idx
    ON __SCHEMA__.__PREFIX__operations (parent_checkpoint_id)
    WHERE parent_checkpoint_id IS NOT NULL;

CREATE INDEX __PREFIX__operations_cache_entry_idx
    ON __SCHEMA__.__PREFIX__operations (cache_entry_id)
    WHERE cache_entry_id IS NOT NULL;

CREATE INDEX __PREFIX__operations_config_idx
    ON __SCHEMA__.__PREFIX__operations (config_digest, operation_id);

CREATE INDEX __PREFIX__operations_result_blob_idx
    ON __SCHEMA__.__PREFIX__operations (result_blob_id)
    WHERE result_blob_id IS NOT NULL;

CREATE INDEX __PREFIX__operations_request_blob_idx
    ON __SCHEMA__.__PREFIX__operations (request_blob_id)
    WHERE request_blob_id IS NOT NULL;

CREATE INDEX __PREFIX__operations_lease_recovery_idx
    ON __SCHEMA__.__PREFIX__operations (lease_expires_at, operation_id)
    WHERE state IN ('reserved', 'dispatching');

CREATE INDEX __PREFIX__operations_poll_due_idx
    ON __SCHEMA__.__PREFIX__operations (poll_after, operation_id)
    INCLUDE (endpoint_id, provider)
    WHERE state = 'provider_pending';

CREATE INDEX __PREFIX__operations_scope_spend_idx
    ON __SCHEMA__.__PREFIX__operations (scope_id, completed_at DESC, operation_id)
    INCLUDE (
        operation_kind,
        actual_cost_usd,
        cost_status,
        cost_method,
        endpoint_id,
        resolved_model,
        route_model_revision
    )
    WHERE state = 'completed';

CREATE INDEX __PREFIX__operations_unknown_cost_idx
    ON __SCHEMA__.__PREFIX__operations (scope_id, completed_at DESC, operation_id)
    INCLUDE (operation_kind, endpoint_id, resolved_model, cost_unknown_reason_code)
    WHERE state = 'completed' AND cost_status = 'unknown';

CREATE INDEX __PREFIX__operations_terminal_expiry_idx
    ON __SCHEMA__.__PREFIX__operations (retention_expires_at, operation_id)
    WHERE state IN ('completed', 'definite_failed', 'canceled');

-- Operation orphan retention only deletes exact settled-cost rows. Keep its
-- candidate scan bounded when unresolved terminal history accumulates.
CREATE INDEX __PREFIX__operations_exact_terminal_expiry_idx
    ON __SCHEMA__.__PREFIX__operations (retention_expires_at, operation_id)
    WHERE state IN ('completed', 'definite_failed', 'canceled')
      AND cost_status = 'exact';

CREATE INDEX __PREFIX__operations_completed_brin_idx
    ON __SCHEMA__.__PREFIX__operations USING brin (completed_at)
    WITH (pages_per_range = 64)
    WHERE completed_at IS NOT NULL;

CREATE INDEX __PREFIX__operation_attempts_provider_request_idx
    ON __SCHEMA__.__PREFIX__operation_attempts (endpoint_id, provider_request_id_hmac)
    WHERE provider_request_id_hmac IS NOT NULL;

CREATE INDEX __PREFIX__operation_attempts_route_time_idx
    ON __SCHEMA__.__PREFIX__operation_attempts
        (endpoint_id, resolved_model, started_at DESC, attempt_id)
    INCLUDE (state, safe_error_code, actual_cost_usd, cost_status);
CREATE TABLE __SCHEMA__.__PREFIX__conversation_checkpoints (
    checkpoint_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    public_id_hmac bytea NOT NULL UNIQUE
        CHECK (octet_length(public_id_hmac) = 32),
    handle_key_id text NOT NULL,
    parent_checkpoint_id uuid
        REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    checkpoint_kind text NOT NULL CHECK (
        checkpoint_kind IN ('generation', 'compaction', 'cache_replay')
    ),
    depth integer NOT NULL CHECK (depth >= 0),
    origin_operation_id uuid NOT NULL UNIQUE
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    origin_cache_entry_id uuid,
    delta_blob_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    response_blob_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    settings_patch_blob_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    materialized_snapshot_blob_id uuid
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    canonical_lineage_digest bytea NOT NULL
        CHECK (octet_length(canonical_lineage_digest) = 32),
    materialized_settings_digest bytea NOT NULL
        CHECK (octet_length(materialized_settings_digest) = 32),
    tool_frontier_digest bytea NOT NULL
        CHECK (octet_length(tool_frontier_digest) = 32),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    compiler_epoch text NOT NULL,
    compaction_policy_version text,
    compaction_prompt_version text,
    compacted_through_checkpoint_id uuid
        REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    CHECK (
        (parent_checkpoint_id IS NULL AND depth = 0) OR
        (parent_checkpoint_id IS NOT NULL AND depth > 0)
    ),
    CHECK (parent_checkpoint_id IS DISTINCT FROM checkpoint_id)
);

CREATE TABLE __SCHEMA__.__PREFIX__checkpoint_provider_state (
    checkpoint_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    provider text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    region text NOT NULL,
    endpoint_family text NOT NULL,
    model_lineage text NOT NULL,
    state_kind text NOT NULL,
    state_blob_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    state_digest bytea NOT NULL CHECK (octet_length(state_digest) = 32),
    required boolean NOT NULL,
    immutable_fork_safe boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz,
    PRIMARY KEY (checkpoint_id, ordinal)
);

CREATE TABLE __SCHEMA__.__PREFIX__checkpoint_provider_affinities (
    checkpoint_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    affinity_rank smallint NOT NULL CHECK (affinity_rank >= 0),
    provider text NOT NULL,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    region text NOT NULL,
    endpoint_family text NOT NULL,
    model_lineage text NOT NULL,
    route_model_revision text NOT NULL,
    provider_cache_key_hmac bytea CHECK (
        provider_cache_key_hmac IS NULL OR
        octet_length(provider_cache_key_hmac) = 32
    ),
    cache_epoch text NOT NULL,
    hard_pinned boolean NOT NULL,
    observed_cache_read_tokens bigint NOT NULL DEFAULT 0
        CHECK (observed_cache_read_tokens >= 0),
    observed_cache_write_tokens bigint NOT NULL DEFAULT 0
        CHECK (observed_cache_write_tokens >= 0),
    last_success_at timestamptz NOT NULL,
    expires_at timestamptz,
    PRIMARY KEY (checkpoint_id, affinity_rank)
);

CREATE INDEX __PREFIX__checkpoints_parent_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints
        (parent_checkpoint_id, created_at, checkpoint_id)
    WHERE parent_checkpoint_id IS NOT NULL;

CREATE INDEX __PREFIX__checkpoints_scope_created_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints
        (scope_id, created_at DESC, checkpoint_id);

CREATE INDEX __PREFIX__checkpoints_retention_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (expires_at, checkpoint_id);

CREATE INDEX __PREFIX__checkpoints_compacted_through_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (compacted_through_checkpoint_id)
    WHERE compacted_through_checkpoint_id IS NOT NULL;

CREATE INDEX __PREFIX__checkpoints_delta_blob_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (delta_blob_id);

CREATE INDEX __PREFIX__checkpoints_response_blob_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (response_blob_id);

CREATE INDEX __PREFIX__checkpoints_settings_blob_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (settings_patch_blob_id);

CREATE INDEX __PREFIX__checkpoints_snapshot_blob_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (materialized_snapshot_blob_id)
    WHERE materialized_snapshot_blob_id IS NOT NULL;

CREATE INDEX __PREFIX__checkpoint_provider_state_blob_idx
    ON __SCHEMA__.__PREFIX__checkpoint_provider_state (state_blob_id);

CREATE INDEX __PREFIX__checkpoint_provider_state_expiry_idx
    ON __SCHEMA__.__PREFIX__checkpoint_provider_state (expires_at, checkpoint_id, ordinal)
    WHERE expires_at IS NOT NULL;

CREATE INDEX __PREFIX__checkpoint_provider_state_route_idx
    ON __SCHEMA__.__PREFIX__checkpoint_provider_state
        (endpoint_id, endpoint_family, model_lineage, checkpoint_id);

CREATE INDEX __PREFIX__checkpoint_affinity_route_idx
    ON __SCHEMA__.__PREFIX__checkpoint_provider_affinities
        (checkpoint_id, hard_pinned DESC, affinity_rank)
    INCLUDE (route_id, endpoint_id, route_model_revision, expires_at);

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD CONSTRAINT __PREFIX__operations_parent_checkpoint_fk
    FOREIGN KEY (parent_checkpoint_id)
    REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE __SCHEMA__.__PREFIX__response_cache_entries (
    cache_entry_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    fingerprint_version integer NOT NULL CHECK (fingerprint_version > 0),
    semantic_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(semantic_fingerprint_hmac) = 32),
    canonical_request_digest bytea NOT NULL
        CHECK (octet_length(canonical_request_digest) = 32),
    canonical_request_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(canonical_request_jsonb) = 'object'),
    variant integer NOT NULL CHECK (variant >= 0),
    cache_route_identity_hmac bytea NOT NULL
        CHECK (octet_length(cache_route_identity_hmac) = 32),
    semantic_profile_version text NOT NULL,
    cache_epoch text NOT NULL,
    response_inline_ciphertext bytea,
    response_key_id text,
    response_blob_id uuid
        REFERENCES __SCHEMA__.__PREFIX__blobs(blob_id) ON DELETE RESTRICT,
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    origin_operation_id uuid NOT NULL UNIQUE
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    origin_checkpoint_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__conversation_checkpoints(checkpoint_id)
        ON DELETE RESTRICT,
    origin_provider text NOT NULL,
    origin_endpoint_id text NOT NULL,
    origin_resolved_model text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz NOT NULL,
    last_used_at timestamptz NOT NULL,
    use_count integer NOT NULL DEFAULT 1
        CHECK (use_count BETWEEN 1 AND 2147483647),
    state text NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'tombstoned')),
    CHECK (
        (response_inline_ciphertext IS NOT NULL)::integer +
        (response_blob_id IS NOT NULL)::integer = 1
    ),
    CHECK (
        response_inline_ciphertext IS NULL = (response_key_id IS NULL)
    ),
    CHECK (completed_at >= created_at),
    CHECK (last_used_at >= completed_at)
);

CREATE UNIQUE INDEX __PREFIX__response_cache_reusable_key_uidx
    ON __SCHEMA__.__PREFIX__response_cache_entries (
        scope_id,
        fingerprint_version,
        semantic_fingerprint_hmac,
        variant,
        cache_route_identity_hmac
    )
    WHERE state = 'ready';

CREATE TABLE __SCHEMA__.__PREFIX__response_cache_uses (
    cache_entry_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__response_cache_entries(cache_entry_id)
        ON DELETE RESTRICT,
    operation_id uuid NOT NULL UNIQUE
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    first_used_at timestamptz NOT NULL,
    PRIMARY KEY (cache_entry_id, operation_id)
);

CREATE TABLE __SCHEMA__.__PREFIX__response_cache_fills (
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    fingerprint_version integer NOT NULL CHECK (fingerprint_version > 0),
    semantic_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(semantic_fingerprint_hmac) = 32),
    variant integer NOT NULL CHECK (variant >= 0),
    cache_route_identity_hmac bytea NOT NULL
        CHECK (octet_length(cache_route_identity_hmac) = 32),
    owner_operation_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    state text NOT NULL CHECK (state IN ('filling', 'completed', 'failed')),
    lease_expires_at timestamptz NOT NULL,
    cache_entry_id uuid
        REFERENCES __SCHEMA__.__PREFIX__response_cache_entries(cache_entry_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        scope_id,
        fingerprint_version,
        semantic_fingerprint_hmac,
        variant,
        cache_route_identity_hmac
    ),
    CHECK (state <> 'completed' OR cache_entry_id IS NOT NULL)
);

CREATE INDEX __PREFIX__response_cache_gc_idx
    ON __SCHEMA__.__PREFIX__response_cache_entries (last_used_at, cache_entry_id)
    WHERE state = 'ready';

CREATE INDEX __PREFIX__response_cache_route_invalidation_idx
    ON __SCHEMA__.__PREFIX__response_cache_entries
        (cache_route_identity_hmac, completed_at DESC, cache_entry_id)
    WHERE state = 'ready';

CREATE INDEX __PREFIX__response_cache_origin_checkpoint_idx
    ON __SCHEMA__.__PREFIX__response_cache_entries (origin_checkpoint_id);

CREATE INDEX __PREFIX__response_cache_response_blob_idx
    ON __SCHEMA__.__PREFIX__response_cache_entries (response_blob_id)
    WHERE response_blob_id IS NOT NULL;

CREATE INDEX __PREFIX__response_cache_fill_owner_idx
    ON __SCHEMA__.__PREFIX__response_cache_fills (owner_operation_id);

CREATE INDEX __PREFIX__response_cache_fill_entry_idx
    ON __SCHEMA__.__PREFIX__response_cache_fills (cache_entry_id)
    WHERE cache_entry_id IS NOT NULL;

CREATE INDEX __PREFIX__response_cache_fill_lease_idx
    ON __SCHEMA__.__PREFIX__response_cache_fills (lease_expires_at, owner_operation_id)
    WHERE state = 'filling';

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD CONSTRAINT __PREFIX__operations_cache_entry_fk
    FOREIGN KEY (cache_entry_id)
    REFERENCES __SCHEMA__.__PREFIX__response_cache_entries(cache_entry_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE __SCHEMA__.__PREFIX__conversation_checkpoints
    ADD CONSTRAINT __PREFIX__checkpoints_origin_cache_entry_fk
    FOREIGN KEY (origin_cache_entry_id)
    REFERENCES __SCHEMA__.__PREFIX__response_cache_entries(cache_entry_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX __PREFIX__checkpoints_origin_cache_entry_idx
    ON __SCHEMA__.__PREFIX__conversation_checkpoints (origin_cache_entry_id)
    WHERE origin_cache_entry_id IS NOT NULL;
CREATE TABLE __SCHEMA__.__PREFIX__budget_policies (
    policy_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    policy_key text NOT NULL,
    config_digest bytea NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    selector_digest bytea NOT NULL CHECK (octet_length(selector_digest) = 32),
    sanitized_selector jsonb NOT NULL
        CHECK (jsonb_typeof(sanitized_selector) = 'object'),
    priority integer NOT NULL,
    enabled boolean NOT NULL,
    effective_from timestamptz NOT NULL,
    effective_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (scope_id, policy_key, config_digest),
    CHECK (
        effective_until IS NULL OR effective_until > effective_from
    )
);

CREATE TABLE __SCHEMA__.__PREFIX__budget_windows (
    window_id uuid PRIMARY KEY,
    policy_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_policies(policy_id) ON DELETE RESTRICT,
    window_key text NOT NULL,
    duration_seconds bigint NOT NULL CHECK (duration_seconds > 0),
    bucket_seconds bigint NOT NULL CHECK (bucket_seconds > 0),
    limit_usd numeric(38,18) NOT NULL CHECK (limit_usd > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (policy_id, window_key),
    CHECK (bucket_seconds <= duration_seconds),
    CHECK (duration_seconds % bucket_seconds = 0)
);

CREATE TABLE __SCHEMA__.__PREFIX__budget_redis_generations (
    generation_id uuid PRIMARY KEY,
    generation_slot smallint NOT NULL DEFAULT 1
        CHECK (generation_slot = 1),
    reason text NOT NULL CHECK (
        reason IN ('initial_cold_start', 'full_service_restart', 'redis_data_loss')
    ),
    state text NOT NULL CHECK (
        state IN ('building', 'active', 'superseded', 'failed')
    ),
    source_journal_id bigint NOT NULL CHECK (source_journal_id >= 0),
    coverage_start timestamptz NOT NULL,
    coverage_end timestamptz NOT NULL,
    manifest_digest bytea NOT NULL
        CHECK (octet_length(manifest_digest) = 32),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    safe_failure_code text,
    CHECK (coverage_end > coverage_start),
    CHECK (
        (state IN ('building') AND completed_at IS NULL AND
            safe_failure_code IS NULL) OR
        (state IN ('active', 'superseded') AND completed_at IS NOT NULL AND
            safe_failure_code IS NULL) OR
        (state = 'failed' AND completed_at IS NOT NULL AND
            safe_failure_code IS NOT NULL)
    )
);

CREATE TABLE __SCHEMA__.__PREFIX__budget_journal_events (
    journal_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    redis_generation_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_redis_generations(generation_id)
        ON DELETE RESTRICT,
    operation_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    window_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    reservation_revision integer NOT NULL
        CHECK (reservation_revision >= 0),
    event_kind text NOT NULL CHECK (
        event_kind IN (
            'reserve', 'finalize_exact', 'finalize_unknown',
            'retain_ambiguous', 'resolve_unknown_exact', 'release'
        )
    ),
    reserved_increase_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_increase_usd >= 0),
    reserved_decrease_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_decrease_usd >= 0),
    accounted_increase_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_increase_usd >= 0),
    accounted_decrease_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_decrease_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    actual_cost_status text NOT NULL CHECK (
        actual_cost_status IN ('pending', 'exact', 'unknown')
    ),
    actual_cost_unknown_reason_code text CHECK (
        actual_cost_unknown_reason_code IS NULL OR
        actual_cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    occurred_at timestamptz NOT NULL,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (operation_id, window_id, reservation_revision),
    CHECK (
        reserved_increase_usd > 0 OR reserved_decrease_usd > 0 OR
        accounted_increase_usd > 0 OR accounted_decrease_usd > 0 OR
        event_kind = 'retain_ambiguous'
    ),
    CHECK (
        (actual_cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        (event_kind = 'reserve' AND
            reserved_increase_usd > 0 AND reserved_decrease_usd = 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'pending') OR
        (event_kind = 'finalize_exact' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = actual_cost_usd AND
            accounted_decrease_usd = 0 AND
            actual_cost_status = 'exact') OR
        (event_kind = 'finalize_unknown' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = reserved_decrease_usd AND
            accounted_decrease_usd = 0 AND
            actual_cost_status = 'unknown') OR
        (event_kind = 'retain_ambiguous' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd = 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'unknown') OR
        (event_kind = 'resolve_unknown_exact' AND
            reserved_increase_usd = 0 AND
            ((reserved_decrease_usd > 0 AND
                accounted_decrease_usd = 0) OR
             (reserved_decrease_usd = 0 AND
                accounted_decrease_usd > 0)) AND
            accounted_increase_usd = actual_cost_usd AND
            actual_cost_status = 'exact') OR
        (event_kind = 'release' AND
            reserved_increase_usd = 0 AND reserved_decrease_usd > 0 AND
            accounted_increase_usd = 0 AND accounted_decrease_usd = 0 AND
            actual_cost_status = 'exact' AND actual_cost_usd = 0)
    )
);

CREATE TABLE __SCHEMA__.__PREFIX__budget_buckets (
    window_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    reserved_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (reserved_cost_usd >= 0),
    accounted_cost_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (accounted_cost_usd >= 0),
    last_journal_id bigint NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_journal_events(journal_id)
        ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (window_id, bucket_start)
);

CREATE TABLE __SCHEMA__.__PREFIX__operation_budget_reservations (
    operation_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__operations(operation_id) ON DELETE RESTRICT,
    window_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_windows(window_id) ON DELETE RESTRICT,
    bucket_start timestamptz NOT NULL,
    state text NOT NULL CHECK (
        state IN ('reserved', 'finalized', 'retained_ambiguous', 'released')
    ),
    reserved_cost_usd numeric(38,18) NOT NULL
        CHECK (reserved_cost_usd >= 0),
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    actual_cost_status text NOT NULL DEFAULT 'pending' CHECK (
        actual_cost_status IN ('pending', 'exact', 'unknown')
    ),
    actual_cost_unknown_reason_code text CHECK (
        actual_cost_unknown_reason_code IS NULL OR
        actual_cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    budget_charge_usd numeric(38,18) NOT NULL DEFAULT 0
        CHECK (budget_charge_usd >= 0),
    budget_charge_basis text NOT NULL DEFAULT 'reserved' CHECK (
        budget_charge_basis IN (
            'reserved', 'exact_actual', 'retained_bound', 'released'
        )
    ),
    reservation_revision integer NOT NULL
        CHECK (reservation_revision >= 0),
    last_journal_id bigint NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__budget_journal_events(journal_id)
        ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finalized_at timestamptz,
    PRIMARY KEY (operation_id, window_id),
    FOREIGN KEY (window_id, bucket_start)
        REFERENCES __SCHEMA__.__PREFIX__budget_buckets(window_id, bucket_start)
        ON DELETE RESTRICT,
    CHECK (
        state = 'reserved' OR finalized_at IS NOT NULL
    ),
    CHECK (
        (actual_cost_status = 'pending' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'exact' AND
            actual_cost_usd IS NOT NULL AND
            actual_cost_unknown_reason_code IS NULL) OR
        (actual_cost_status = 'unknown' AND
            actual_cost_usd IS NULL AND
            actual_cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (
        (budget_charge_basis = 'reserved' AND state = 'reserved' AND
            budget_charge_usd = 0 AND actual_cost_status = 'pending') OR
        (budget_charge_basis = 'exact_actual' AND state = 'finalized' AND
            actual_cost_status = 'exact' AND
            budget_charge_usd = actual_cost_usd) OR
        (budget_charge_basis = 'retained_bound' AND
            state IN ('finalized', 'retained_ambiguous') AND
            actual_cost_status = 'unknown' AND
            budget_charge_usd = reserved_cost_usd) OR
        (budget_charge_basis = 'released' AND state = 'released' AND
            budget_charge_usd = 0 AND
            actual_cost_status = 'exact' AND actual_cost_usd = 0)
    )
);

CREATE INDEX __PREFIX__budget_policies_scope_active_idx
    ON __SCHEMA__.__PREFIX__budget_policies
        (scope_id, enabled, priority, effective_from, policy_id)
    INCLUDE (effective_until, selector_digest);

CREATE INDEX __PREFIX__budget_policies_config_idx
    ON __SCHEMA__.__PREFIX__budget_policies (config_digest, policy_id);

CREATE INDEX __PREFIX__budget_windows_policy_idx
    ON __SCHEMA__.__PREFIX__budget_windows (policy_id, window_id)
    INCLUDE (duration_seconds, bucket_seconds, limit_usd);

CREATE INDEX __PREFIX__budget_buckets_retention_idx
    ON __SCHEMA__.__PREFIX__budget_buckets (bucket_start, window_id);

CREATE INDEX __PREFIX__budget_journal_window_rebuild_idx
    ON __SCHEMA__.__PREFIX__budget_journal_events
        (window_id, bucket_start, journal_id)
    INCLUDE (
        operation_id,
        event_kind,
        reserved_increase_usd,
        reserved_decrease_usd,
        accounted_increase_usd,
        accounted_decrease_usd,
        actual_cost_usd,
        actual_cost_status
    );

CREATE INDEX __PREFIX__budget_journal_operation_idx
    ON __SCHEMA__.__PREFIX__budget_journal_events (operation_id, journal_id);

CREATE INDEX __PREFIX__budget_journal_generation_idx
    ON __SCHEMA__.__PREFIX__budget_journal_events
        (redis_generation_id, journal_id);

CREATE UNIQUE INDEX __PREFIX__budget_redis_generation_active_idx
    ON __SCHEMA__.__PREFIX__budget_redis_generations (generation_slot)
    WHERE state = 'active';

CREATE INDEX __PREFIX__budget_redis_generation_history_idx
    ON __SCHEMA__.__PREFIX__budget_redis_generations (started_at DESC, generation_id)
    INCLUDE (state, reason, source_journal_id, completed_at);

CREATE INDEX __PREFIX__budget_journal_time_brin_idx
    ON __SCHEMA__.__PREFIX__budget_journal_events USING brin (persisted_at)
    WITH (pages_per_range = 64);

CREATE INDEX __PREFIX__budget_buckets_last_journal_idx
    ON __SCHEMA__.__PREFIX__budget_buckets
        (last_journal_id, window_id, bucket_start);

CREATE INDEX __PREFIX__operation_budget_window_idx
    ON __SCHEMA__.__PREFIX__operation_budget_reservations
        (window_id, bucket_start, state, operation_id)
    INCLUDE (
        reserved_cost_usd,
        budget_charge_usd,
        budget_charge_basis,
        actual_cost_usd,
        actual_cost_status
    );

CREATE INDEX __PREFIX__operation_budget_open_idx
    ON __SCHEMA__.__PREFIX__operation_budget_reservations
        (window_id, bucket_start, operation_id)
    INCLUDE (reserved_cost_usd, reservation_revision, last_journal_id)
    WHERE state IN ('reserved', 'retained_ambiguous');

CREATE INDEX __PREFIX__operation_budget_last_journal_idx
    ON __SCHEMA__.__PREFIX__operation_budget_reservations
        (last_journal_id, operation_id, window_id);
CREATE TABLE __SCHEMA__.__PREFIX__price_catalogs (
    price_catalog_id uuid PRIMARY KEY,
    catalog_version text NOT NULL UNIQUE,
    source_digest bytea NOT NULL CHECK (octet_length(source_digest) = 32),
    compiled_digest bytea NOT NULL UNIQUE
        CHECK (octet_length(compiled_digest) = 32),
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    effective_from timestamptz NOT NULL,
    retired_at timestamptz,
    status text NOT NULL CHECK (status IN ('active', 'retired', 'rejected'))
);

CREATE TABLE __SCHEMA__.__PREFIX__price_entries (
    price_entry_id uuid PRIMARY KEY,
    price_catalog_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__price_catalogs(price_catalog_id)
        ON DELETE RESTRICT,
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    endpoint_id text NOT NULL,
    region text NOT NULL,
    resolved_model text NOT NULL,
    provider_tier text NOT NULL,
    usage_formula_version text NOT NULL,
    price_status text NOT NULL CHECK (
        price_status IN ('exact', 'partial', 'unknown')
    ),
    input_per_million_usd numeric(38,18)
        CHECK (input_per_million_usd IS NULL OR input_per_million_usd >= 0),
    output_per_million_usd numeric(38,18)
        CHECK (output_per_million_usd IS NULL OR output_per_million_usd >= 0),
    cache_read_per_million_usd numeric(38,18)
        CHECK (
            cache_read_per_million_usd IS NULL OR
            cache_read_per_million_usd >= 0
        ),
    cache_write_per_million_usd numeric(38,18)
        CHECK (
            cache_write_per_million_usd IS NULL OR
            cache_write_per_million_usd >= 0
        ),
    reasoning_per_million_usd numeric(38,18)
        CHECK (
            reasoning_per_million_usd IS NULL OR
            reasoning_per_million_usd >= 0
        ),
    per_request_usd numeric(38,18)
        CHECK (per_request_usd IS NULL OR per_request_usd >= 0),
    unknown_component_codes text[] NOT NULL DEFAULT '{}'::text[] CHECK (
        array_position(unknown_component_codes, NULL) IS NULL AND
        unknown_component_codes <@ ARRAY[
            'input_tokens',
            'output_tokens',
            'cache_read_tokens',
            'cache_write_tokens',
            'reasoning_tokens',
            'request'
        ]::text[]
    ),
    price_unknown_reason_code text CHECK (
        price_unknown_reason_code IS NULL OR
        price_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    source_price_digest bytea NOT NULL CHECK (octet_length(source_price_digest) = 32),
    effective_from timestamptz NOT NULL,
    effective_until timestamptz,
    UNIQUE (
        price_catalog_id,
        provider,
        endpoint_family,
        endpoint_id,
        region,
        resolved_model,
        provider_tier,
        effective_from
    ),
    CHECK (
        effective_until IS NULL OR effective_until > effective_from
    ),
    CHECK (
        (price_status = 'exact' AND
            input_per_million_usd IS NOT NULL AND
            output_per_million_usd IS NOT NULL AND
            cache_read_per_million_usd IS NOT NULL AND
            cache_write_per_million_usd IS NOT NULL AND
            reasoning_per_million_usd IS NOT NULL AND
            per_request_usd IS NOT NULL AND
            cardinality(unknown_component_codes) = 0 AND
            price_unknown_reason_code IS NULL) OR
        (price_status = 'partial' AND
            num_nonnulls(
                input_per_million_usd,
                output_per_million_usd,
                cache_read_per_million_usd,
                cache_write_per_million_usd,
                reasoning_per_million_usd,
                per_request_usd
            ) BETWEEN 1 AND 5 AND
            cardinality(unknown_component_codes) > 0 AND
            price_unknown_reason_code IS NOT NULL) OR
        (price_status = 'unknown' AND
            num_nonnulls(
                input_per_million_usd,
                output_per_million_usd,
                cache_read_per_million_usd,
                cache_write_per_million_usd,
                reasoning_per_million_usd,
                per_request_usd
            ) = 0 AND
            cardinality(unknown_component_codes) > 0 AND
            price_unknown_reason_code IS NOT NULL)
    )
);

CREATE INDEX __PREFIX__price_entries_resolution_idx
    ON __SCHEMA__.__PREFIX__price_entries (
        endpoint_id,
        region,
        resolved_model,
        provider_tier,
        price_status,
        effective_from DESC,
        price_entry_id
    )
    INCLUDE (
        effective_until,
        usage_formula_version,
        input_per_million_usd,
        output_per_million_usd,
        cache_read_per_million_usd,
        cache_write_per_million_usd,
        reasoning_per_million_usd,
        per_request_usd,
        price_catalog_id
    );

CREATE INDEX __PREFIX__price_entries_unresolved_idx
    ON __SCHEMA__.__PREFIX__price_entries
        (price_status, effective_from DESC, price_entry_id)
    INCLUDE (
        endpoint_id,
        resolved_model,
        provider_tier,
        unknown_component_codes,
        price_unknown_reason_code
    )
    WHERE price_status IN ('partial', 'unknown');

CREATE TABLE __SCHEMA__.__PREFIX__provider_status_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_digest bytea NOT NULL UNIQUE
        CHECK (octet_length(event_digest) = 32),
    config_digest bytea NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    observed_at timestamptz NOT NULL,
    source text NOT NULL CHECK (
        source IN (
            'inference',
            'management_api',
            'startup_probe',
            'operator'
        )
    ),
    availability text NOT NULL CHECK (
        availability IN ('available', 'degraded', 'unavailable', 'unknown')
    ),
    credit_state text NOT NULL CHECK (
        credit_state IN ('ok', 'low', 'exhausted', 'unknown')
    ),
    billing_state text NOT NULL CHECK (
        billing_state IN ('ok', 'issue', 'unknown')
    ),
    safe_error_code text,
    provider_code text,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    config_epoch text NOT NULL,
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > observed_at)
);

CREATE TABLE __SCHEMA__.__PREFIX__provider_route_status (
    config_digest bytea NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    route_id text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    provider text NOT NULL,
    endpoint_family text NOT NULL,
    config_epoch text NOT NULL,
    availability text NOT NULL CHECK (
        availability IN ('available', 'degraded', 'unavailable', 'unknown')
    ),
    credit_state text NOT NULL CHECK (
        credit_state IN ('ok', 'low', 'exhausted', 'unknown')
    ),
    billing_state text NOT NULL CHECK (
        billing_state IN ('ok', 'issue', 'unknown')
    ),
    circuit_state text NOT NULL CHECK (
        circuit_state IN ('closed', 'open', 'half_open')
    ),
    consecutive_definite_failures integer NOT NULL DEFAULT 0
        CHECK (consecutive_definite_failures >= 0),
    last_event_id bigint NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__provider_status_events(event_id)
        ON DELETE RESTRICT,
    last_event_digest bytea NOT NULL
        CHECK (octet_length(last_event_digest) = 32),
    last_success_at timestamptz,
    last_failure_at timestamptz,
    credit_confirmed_at timestamptz,
    observed_at timestamptz NOT NULL,
    stale_after timestamptz NOT NULL,
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    PRIMARY KEY (config_digest, route_id)
);

CREATE TABLE __SCHEMA__.__PREFIX__provider_inventory_snapshots (
    inventory_snapshot_id uuid PRIMARY KEY,
    config_digest bytea NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__configuration_snapshots(config_digest)
        ON DELETE RESTRICT,
    provider text NOT NULL,
    endpoint_id text NOT NULL,
    endpoint_account_hmac bytea NOT NULL
        CHECK (octet_length(endpoint_account_hmac) = 32),
    endpoint_family text NOT NULL,
    region text NOT NULL,
    source text NOT NULL CHECK (
        source IN ('provider_api', 'configured_only', 'unsupported')
    ),
    observed_at timestamptz NOT NULL,
    complete boolean NOT NULL,
    next_cursor_ciphertext bytea,
    next_cursor_key_id text,
    inventory_digest bytea NOT NULL CHECK (octet_length(inventory_digest) = 32),
    expires_at timestamptz NOT NULL,
    UNIQUE (
        config_digest,
        endpoint_id,
        endpoint_account_hmac,
        observed_at
    ),
    CHECK (expires_at > observed_at),
    CHECK (
        next_cursor_ciphertext IS NULL =
        (next_cursor_key_id IS NULL)
    )
);

CREATE TABLE __SCHEMA__.__PREFIX__provider_inventory_models (
    inventory_snapshot_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__provider_inventory_snapshots(inventory_snapshot_id)
        ON DELETE RESTRICT,
    provider_model_id text NOT NULL,
    display_name text,
    owned_by text,
    created_at_provider timestamptz,
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN ('available', 'deprecated', 'unavailable', 'unknown')
    ),
    capability_digest bytea NOT NULL CHECK (octet_length(capability_digest) = 32),
    safe_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(safe_metadata) = 'object'),
    CHECK (length(provider_model_id) BETWEEN 1 AND 256),
    PRIMARY KEY (inventory_snapshot_id, provider_model_id)
);

CREATE INDEX __PREFIX__provider_status_route_time_idx
    ON __SCHEMA__.__PREFIX__provider_status_events
        (config_digest, route_id, observed_at DESC, event_id DESC)
    INCLUDE (
        availability,
        credit_state,
        billing_state,
        safe_error_code,
        source
    );

CREATE INDEX __PREFIX__provider_status_credit_incident_idx
    ON __SCHEMA__.__PREFIX__provider_status_events
        (observed_at DESC, endpoint_id, event_id)
    WHERE credit_state IN ('low', 'exhausted')
       OR billing_state = 'issue';

CREATE INDEX __PREFIX__provider_status_event_brin_idx
    ON __SCHEMA__.__PREFIX__provider_status_events USING brin (observed_at)
    WITH (pages_per_range = 64);

CREATE INDEX __PREFIX__provider_status_expiry_idx
    ON __SCHEMA__.__PREFIX__provider_status_events (expires_at, event_id);

CREATE INDEX __PREFIX__provider_route_current_problem_idx
    ON __SCHEMA__.__PREFIX__provider_route_status
        (availability, credit_state, billing_state, observed_at DESC, route_id)
    WHERE availability <> 'available'
       OR credit_state <> 'ok'
       OR billing_state <> 'ok';

CREATE INDEX __PREFIX__provider_route_last_event_idx
    ON __SCHEMA__.__PREFIX__provider_route_status (last_event_id);

-- Query Activities read the current projection in deterministic route-id
-- order.  Keep the page key and all filter columns covered so a persisted
-- status query does not fall back to the append-only event ledger.
CREATE INDEX __PREFIX__provider_route_query_idx
    ON __SCHEMA__.__PREFIX__provider_route_status
        (config_digest, route_id)
    INCLUDE (
        provider,
        endpoint_id,
        availability,
        credit_state,
        billing_state,
        observed_at,
        stale_after
    );

-- Credit-status Query Activities collapse route projections by provider and
-- endpoint, selecting the newest observation. Keep that DISTINCT ON shape
-- indexable and include the event join/state columns used by the bounded read.
CREATE INDEX __PREFIX__provider_route_credit_query_idx
    ON __SCHEMA__.__PREFIX__provider_route_status
        (config_digest, provider, endpoint_id, observed_at DESC, route_id DESC)
    INCLUDE (credit_state, billing_state, credit_confirmed_at, last_event_id);

CREATE INDEX __PREFIX__provider_inventory_latest_idx
    ON __SCHEMA__.__PREFIX__provider_inventory_snapshots
        (config_digest, endpoint_id, observed_at DESC, inventory_snapshot_id)
    INCLUDE (source, complete, expires_at);

-- The query page partitions latest selection by endpoint-account identity.
-- Keep that HMAC internal while making the account-epoch ordering indexable.
CREATE INDEX __PREFIX__provider_inventory_latest_account_idx
    ON __SCHEMA__.__PREFIX__provider_inventory_snapshots
        (config_digest, provider, endpoint_id, endpoint_account_hmac,
         observed_at DESC, inventory_snapshot_id)
    INCLUDE (source, complete, expires_at, inventory_digest);

CREATE INDEX __PREFIX__provider_inventory_expiry_idx
    ON __SCHEMA__.__PREFIX__provider_inventory_snapshots
        (expires_at, inventory_snapshot_id);

CREATE INDEX __PREFIX__provider_inventory_model_lookup_idx
    ON __SCHEMA__.__PREFIX__provider_inventory_models
        (provider_model_id, inventory_snapshot_id)
    INCLUDE (lifecycle_state, display_name, capability_digest);
CREATE TABLE __SCHEMA__.__PREFIX__maintenance_outbox (
    outbox_id uuid PRIMARY KEY,
    event_kind text NOT NULL CHECK (
        event_kind IN (
            'delete_blob',
            'delete_provider_state',
            'refresh_inventory'
        )
    ),
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    dedupe_key bytea NOT NULL CHECK (octet_length(dedupe_key) = 32),
    safe_payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'processing', 'completed', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL,
    lease_expires_at timestamptz,
    -- A fresh opaque fence is written for every claim. It remains after a
    -- terminal transition so duplicate completion/failure calls with the
    -- same token are idempotent, while a reclaim always replaces it.
    lease_token uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (event_kind, dedupe_key),
    CHECK (state <> 'processing' OR (lease_expires_at IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK (state = 'processing' OR lease_expires_at IS NULL)
);

CREATE INDEX __PREFIX__maintenance_outbox_ready_idx
    ON __SCHEMA__.__PREFIX__maintenance_outbox (available_at, outbox_id)
    WHERE state IN ('pending', 'failed');

CREATE INDEX __PREFIX__maintenance_outbox_lease_idx
    ON __SCHEMA__.__PREFIX__maintenance_outbox (lease_expires_at, outbox_id)
    WHERE state = 'processing';
ALTER TABLE __SCHEMA__.__PREFIX__operations SET (fillfactor = 80);
ALTER TABLE __SCHEMA__.__PREFIX__budget_buckets SET (fillfactor = 80);
ALTER TABLE __SCHEMA__.__PREFIX__response_cache_entries SET (fillfactor = 80);
ALTER TABLE __SCHEMA__.__PREFIX__response_cache_fills SET (fillfactor = 80);
ALTER TABLE __SCHEMA__.__PREFIX__provider_route_status SET (fillfactor = 80);
ALTER TABLE __SCHEMA__.__PREFIX__maintenance_outbox SET (fillfactor = 80);
CREATE TABLE __SCHEMA__.__PREFIX__query_executions (
    query_execution_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL
        REFERENCES __SCHEMA__.__PREFIX__scopes(scope_id) ON DELETE RESTRICT,
    api_version text NOT NULL,
    operation_key_hmac bytea NOT NULL
        CHECK (octet_length(operation_key_hmac) = 32),
    request_fingerprint_hmac bytea NOT NULL
        CHECK (octet_length(request_fingerprint_hmac) = 32),
    query_kind text NOT NULL CHECK (
        query_kind IN (
            'provider_status', 'model_inventory', 'credit_status',
            'budget_status', 'spend_summary'
        )
    ),
    request_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(request_jsonb) = 'object'),
    response_jsonb jsonb NOT NULL
        CHECK (jsonb_typeof(response_jsonb) = 'object'),
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    source text NOT NULL,
    actual_cost_usd numeric(38,18)
        CHECK (actual_cost_usd IS NULL OR actual_cost_usd >= 0),
    cost_status text NOT NULL CHECK (cost_status IN ('exact', 'unknown')),
    cost_method text CHECK (
        cost_method IS NULL OR
        cost_method IN (
            'control_query_zero', 'provider_reported', 'catalog_usage'
        )
    ),
    cost_unknown_reason_code text CHECK (
        cost_unknown_reason_code IS NULL OR
        cost_unknown_reason_code ~ '^[a-z0-9_]{1,64}$'
    ),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    retention_expires_at timestamptz NOT NULL,
    UNIQUE (scope_id, operation_key_hmac),
    CHECK (completed_at >= started_at),
    CHECK (retention_expires_at > completed_at),
    CHECK (
        (cost_status = 'exact' AND actual_cost_usd IS NOT NULL AND
            cost_method IS NOT NULL AND cost_unknown_reason_code IS NULL) OR
        (cost_status = 'unknown' AND actual_cost_usd IS NULL AND
            cost_method IS NULL AND cost_unknown_reason_code IS NOT NULL)
    ),
    CHECK (cost_method <> 'control_query_zero' OR actual_cost_usd = 0)
);

CREATE INDEX __PREFIX__query_executions_scope_time_idx
    ON __SCHEMA__.__PREFIX__query_executions
        (scope_id, completed_at DESC, query_execution_id)
    INCLUDE (query_kind, actual_cost_usd, cost_status, source);

CREATE INDEX __PREFIX__query_executions_retention_idx
    ON __SCHEMA__.__PREFIX__query_executions
        (retention_expires_at, query_execution_id);

CREATE INDEX __PREFIX__query_executions_unknown_cost_idx
    ON __SCHEMA__.__PREFIX__query_executions
        (scope_id, completed_at DESC, query_execution_id)
    INCLUDE (query_kind, cost_unknown_reason_code)
    WHERE cost_status = 'unknown';

-- Blob deletion claims are short-lived SQL fences, but physical object-store
-- deletion occurs after the claim transaction commits. These triggers reject
-- new direct or cache-mediated references once a blob is deleting/deleted.
-- The deleter therefore cannot observe an unreferenced object and then race a
-- writer that adds a reference before the external delete completes.
CREATE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    referenced_id uuid;
    referenced_state text;
BEGIN
    IF TG_ARGV[1] = 'cache_entry_state' THEN
        IF NEW.state <> 'ready' OR NEW.response_blob_id IS NULL THEN
            RETURN NEW;
        END IF;
        referenced_id := NEW.response_blob_id;
    ELSE
        referenced_id := NULLIF(to_jsonb(NEW)->>TG_ARGV[0], '')::uuid;
    END IF;
    IF referenced_id IS NULL THEN
        RETURN NEW;
    END IF;

    IF TG_ARGV[1] = 'cache_entry' THEN
        SELECT b.deletion_state INTO referenced_state
        FROM __SCHEMA__.__PREFIX__response_cache_entries c
        JOIN __SCHEMA__.__PREFIX__blobs b ON b.blob_id = c.response_blob_id
        WHERE c.cache_entry_id = referenced_id
        FOR UPDATE OF b;
    ELSE
        SELECT b.deletion_state INTO referenced_state
        FROM __SCHEMA__.__PREFIX__blobs b
        WHERE b.blob_id = referenced_id
        FOR UPDATE OF b;
    END IF;

    IF referenced_state IN ('deleting', 'deleted') THEN
        RAISE EXCEPTION 'blob % is not accepting new references while %', referenced_id, referenced_state
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER __PREFIX__op_req_blob_guard
    BEFORE INSERT OR UPDATE OF request_blob_id ON __SCHEMA__.__PREFIX__operations
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('request_blob_id');
CREATE TRIGGER __PREFIX__op_res_blob_guard
    BEFORE INSERT OR UPDATE OF result_blob_id ON __SCHEMA__.__PREFIX__operations
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('result_blob_id');
CREATE TRIGGER __PREFIX__op_cache_guard
    BEFORE INSERT OR UPDATE OF cache_entry_id ON __SCHEMA__.__PREFIX__operations
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('cache_entry_id', 'cache_entry');
CREATE TRIGGER __PREFIX__cp_delta_blob_guard
    BEFORE INSERT OR UPDATE OF delta_blob_id ON __SCHEMA__.__PREFIX__conversation_checkpoints
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('delta_blob_id');
CREATE TRIGGER __PREFIX__cp_response_blob_guard
    BEFORE INSERT OR UPDATE OF response_blob_id ON __SCHEMA__.__PREFIX__conversation_checkpoints
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('response_blob_id');
CREATE TRIGGER __PREFIX__cp_settings_blob_guard
    BEFORE INSERT OR UPDATE OF settings_patch_blob_id ON __SCHEMA__.__PREFIX__conversation_checkpoints
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('settings_patch_blob_id');
CREATE TRIGGER __PREFIX__cp_snapshot_blob_guard
    BEFORE INSERT OR UPDATE OF materialized_snapshot_blob_id ON __SCHEMA__.__PREFIX__conversation_checkpoints
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('materialized_snapshot_blob_id');
CREATE TRIGGER __PREFIX__cp_origin_cache_guard
    BEFORE INSERT OR UPDATE OF origin_cache_entry_id ON __SCHEMA__.__PREFIX__conversation_checkpoints
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('origin_cache_entry_id', 'cache_entry');
CREATE TRIGGER __PREFIX__provider_state_blob_guard
    BEFORE INSERT OR UPDATE OF state_blob_id ON __SCHEMA__.__PREFIX__checkpoint_provider_state
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('state_blob_id');
CREATE TRIGGER __PREFIX__cache_response_blob_guard
    BEFORE INSERT OR UPDATE OF response_blob_id ON __SCHEMA__.__PREFIX__response_cache_entries
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('response_blob_id');
CREATE TRIGGER __PREFIX__cache_state_guard
    BEFORE INSERT OR UPDATE OF state ON __SCHEMA__.__PREFIX__response_cache_entries
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('state', 'cache_entry_state');
CREATE TRIGGER __PREFIX__cache_use_guard
    BEFORE INSERT OR UPDATE OF cache_entry_id ON __SCHEMA__.__PREFIX__response_cache_uses
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('cache_entry_id', 'cache_entry');
CREATE TRIGGER __PREFIX__cache_fill_guard
    BEFORE INSERT OR UPDATE OF cache_entry_id ON __SCHEMA__.__PREFIX__response_cache_fills
    FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.__PREFIX__reject_blob_gc_reference('cache_entry_id', 'cache_entry');
