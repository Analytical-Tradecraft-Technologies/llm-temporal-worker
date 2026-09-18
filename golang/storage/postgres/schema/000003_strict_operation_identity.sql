-- PostgreSQL worker-state v3 upgrade template.
-- Existing rows retain their operation_id and all durable references. They are
-- bound lazily to the strict v2 identity by OperationRepository.Begin after the
-- released request digest and scope have been verified.

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD COLUMN operation_actor_hmac bytea,
    ADD COLUMN operation_identity_version integer NOT NULL DEFAULT 1;

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD CHECK (operation_identity_version IN (1, 2)),
    ADD CHECK (
        (operation_identity_version = 1 AND operation_actor_hmac IS NULL) OR
        (operation_identity_version = 2 AND
            operation_actor_hmac IS NOT NULL AND
            octet_length(operation_actor_hmac) = 32)
    );
-- Rows present when the column was added keep version 1. Every later insert
-- defaults to version 2 and therefore fails closed unless actor identity is
-- supplied.
ALTER TABLE __SCHEMA__.__PREFIX__operations
    ALTER COLUMN operation_identity_version SET DEFAULT 2;


-- The released uniqueness constraint remains valid because current
-- operation_key_hmac values bind actor, API version, and caller operation key.
-- This explicit partial index makes the physical v2 lookup contract
-- independently verifiable while leaving released rows untouched.
CREATE UNIQUE INDEX __PREFIX__operations_strict_identity_idx
    ON __SCHEMA__.__PREFIX__operations
        (scope_id, operation_kind, api_version,
         operation_actor_hmac, operation_key_hmac)
    WHERE operation_identity_version = 2;

CREATE INDEX __PREFIX__operations_strict_request_idx
    ON __SCHEMA__.__PREFIX__operations
        (scope_id, operation_kind, api_version,
         operation_actor_hmac, operation_key_hmac,
         request_digest, request_fingerprint_hmac, operation_id)
    WHERE operation_identity_version = 2;
