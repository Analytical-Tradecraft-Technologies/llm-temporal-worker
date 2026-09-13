-- PostgreSQL worker-state v2 upgrade template.
-- This migration is applied only after the immutable worker_state_v1 template
-- checksum has been verified by the installer.

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD COLUMN immutable_reservation_facts jsonb,
    ADD COLUMN failure_reason_code text,
    ADD COLUMN request_payload_sha256 bytea,
    ADD COLUMN request_payload_byte_length bigint,
    ADD COLUMN request_payload_reference text;

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ADD CHECK (
        immutable_reservation_facts IS NULL OR
        jsonb_typeof(immutable_reservation_facts) = 'object'
    ),
    ADD CHECK (
        failure_reason_code IS NULL OR
        failure_reason_code ~ '^[a-z0-9_]{1,64}$'
    );

-- Released v1 inline request envelopes use AES-GCM with a 12-byte nonce and a
-- 16-byte authentication tag. Blob-backed requests already carry their exact
-- plaintext length in the immutable blob descriptor. These are the only two
-- storage representations admitted by the v1 operations-table constraint.
UPDATE __SCHEMA__.__PREFIX__operations AS operation
SET request_payload_sha256 = operation.request_digest,
    request_payload_byte_length = CASE
        WHEN operation.request_inline_ciphertext IS NOT NULL
            THEN octet_length(operation.request_inline_ciphertext) - 28
        ELSE blob.byte_length
    END,
    request_payload_reference = CASE
        WHEN operation.request_inline_ciphertext IS NOT NULL THEN 'inline'
        ELSE operation.request_blob_id::text
    END
FROM __SCHEMA__.__PREFIX__blobs AS blob
WHERE operation.request_blob_id = blob.blob_id;

UPDATE __SCHEMA__.__PREFIX__operations AS operation
SET request_payload_sha256 = operation.request_digest,
    request_payload_byte_length = octet_length(operation.request_inline_ciphertext) - 28,
    request_payload_reference = 'inline'
WHERE operation.request_inline_ciphertext IS NOT NULL;

-- Destroy the legacy content-bearing manifest before advancing the contract
-- marker. The canonical request remains available only through its encrypted
-- inline envelope or encrypted blob object.
UPDATE __SCHEMA__.__PREFIX__operations
SET request_manifest_jsonb = jsonb_build_object(
        'schema_version', 2,
        'payload_sha256', encode(request_payload_sha256, 'hex'),
        'payload_bytes', request_payload_byte_length,
        'payload_reference', request_payload_reference
    );

ALTER TABLE __SCHEMA__.__PREFIX__operations
    ALTER COLUMN request_payload_sha256 SET NOT NULL,
    ALTER COLUMN request_payload_byte_length SET NOT NULL,
    ALTER COLUMN request_payload_reference SET NOT NULL,
    ADD CHECK (octet_length(request_payload_sha256) = 32),
    ADD CHECK (request_payload_sha256 = request_digest),
    ADD CHECK (request_payload_byte_length >= 0),
    ADD CHECK (length(request_payload_reference) BETWEEN 1 AND 64),
    ADD CHECK (
        (request_inline_ciphertext IS NOT NULL AND
            request_payload_reference = 'inline') OR
        (request_blob_id IS NOT NULL AND
            request_payload_reference = request_blob_id::text)
    ),
    ADD CHECK (
        request_manifest_jsonb = jsonb_build_object(
            'schema_version', 2,
            'payload_sha256', encode(request_payload_sha256, 'hex'),
            'payload_bytes', request_payload_byte_length,
            'payload_reference', request_payload_reference
        )
    );

CREATE INDEX __PREFIX__operations_request_identity_idx
    ON __SCHEMA__.__PREFIX__operations
        (scope_id, operation_kind, request_digest, operation_id);
