# Blob garbage-collection boundary

Durable payloads use the configured cloud blob store and immutable encrypted
references. Publication writes blobs before conditionally publishing their
request, checkpoint or cache metadata. Interrupted publication may leave orphan
objects. Automatic production blob cleanup is not implemented.

The reverse case, a committed reference whose object is gone (manual deletion,
a bucket lifecycle rule, a partial restore), is never read as an absent or
expired record. Readers fail it as `state_corrupt`; see the
[cloud request repository](cloud-request-repository.md#request-state).

The generic provider delete operation is only a storage primitive. Safe worker
cleanup must prove that an object has no live request, attempt, checkpoint,
cache-success or cache-fill reference, including references created concurrently
with the scan. It must preserve ancestors needed by live descendants and fence
writers before external deletion. A successful metadata transition alone does
not prove the object was physically deleted.

These requirements are tracked in [#818](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/818).
See [cloud checkpoint publication](cloud-request-repository.md#checkpoint-persistence)
and [maintenance](maintenance.md). There is no supported worker command that
sweeps production blobs today.
