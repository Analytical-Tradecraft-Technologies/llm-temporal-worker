# Rotating runtime credentials

This runbook rotates the credentials and keys a running worker uses, and says
which need a restart. The design, what each compromise exposes, and the
proposals not yet implemented are in
[key compromise and credential rotation](../architecture/key-compromise-and-credential-rotation.md).
The cloud request storage keyring is a separate design:
[durable-storage key management](../architecture/durable-storage-key-management.md).
The [threat model](../architecture/threat-model.md) describes who could obtain
each key. A rolling restart or drain here is the same operation as in the
[fleet upgrade and rollback runbook](fleet-upgrade-and-rollback.md); rotate
credentials separately from an upgrade so a failure has one cause.

## Summary

| Credential | Configuration | Applied by | Overlap the dependency must allow |
| --- | --- | --- | --- |
| Provider API key | `endpoints.*.auth` (`bearer_env`, `header_env`) | Rolling restart | Two valid keys at the provider |
| Redis ACL password | `state.redis.password` (`kind: file`) | Reload | Two passwords on the ACL user |
| Redis TLS CA | `state.redis.tls.ca_file` | Reload | CA bundle with old and new CA |
| Langfuse key pair | `langfuse.public_key`, `langfuse.secret_key` (`kind: file`) | Reload | Two key pairs in the Langfuse project |
| Continuation handle keys | `continuation.handle_keys` | Reload, in stages | Old and new key listed together |
| Temporal API key | `temporal.api_key_file` | Rolling restart | Two valid API keys |
| Temporal mTLS | `temporal.tls.cert_file`, `key_file`, `ca_file` | Rolling restart | Server trusts old and new client CA |
| Payload codec keys | `temporal.payload_codec.keys` | Rolling restart, in stages | Old and new key listed together |
| Redis `key_secret` | `state.redis.key_secret` | New namespace and full drain | None: not rotatable in place |
| Cloud request storage secret | `state.requests.secret` | Not rotatable yet | See #861 |

A credential configured as `kind: env` always needs a restart, because a
process cannot see a new value for its own environment. In production
`validate-config` prints a `warning:` line, and the worker logs a warning with
`config_field`, for each reloadable credential configured that way.

## Applying a change

### Reload

A reload re-reads every secret file and rebuilds every snapshot client, even
when the configuration file is unchanged. Replacing a secret file does **not**
trigger one: the worker watches only its configuration file. After the new
file is in place on every worker, trigger the reload with one of:

- `SIGHUP` to the worker process;
- any change to the content of the `--config` file (a comment is enough),
  which the watcher reloads about two seconds after it lands;
- a restart.

The shipped Kubernetes manifests mount the configuration from an immutable,
hash-named ConfigMap and run a distroless image with no `kill` binary, so a
configuration change rolls the Deployment and `kubectl exec ... kill` is not
available. With those manifests, use `kubectl rollout restart` unless the
deployment adds a way to signal the process. Kubernetes updates a projected
Secret volume in place only after the kubelet's next sync (typically under two
minutes); a `subPath` mount never updates.

### Verify a reload

On each worker, after the secret file changed:

- a `configuration reloaded` record with `outcome=success`, and
  `llmtw_config_reload_total{outcome="success"}` increased;
- readiness still ready.

A rejected reload logs `configuration reload failed` with a `cause`. With
`cause="dependency"` the new credential failed its dependency check (for
example Redis refused the password); with `cause="secret"` the file could not
be read. Either way the previous snapshot, and the old credential, stay
active. Fix the file and reload again. Do not revoke the old credential until
every worker has reloaded successfully.

### Rolling restart

`kubectl rollout restart deployment/<name>` (or the platform's equivalent)
replaces workers one at a time; each drains its Activities within
`temporal.worker.graceful_stop_timeout`. Wait for the rollout to finish and
every new worker to be ready before revoking the old credential. A new worker
that cannot authenticate to Temporal or resolve a secret fails at startup and
the rollout stalls, which leaves the old workers running.

## Provider API keys

Provider endpoints accept only `bearer_env` and `header_env`, so rotation is
always a rolling restart.

1. Create the new key at the provider. Keep the old one active.
2. Update the environment source (the base manifests use the
   `llmtw-provider-credentials` Secret through `envFrom`).
3. Rolling restart, and wait for it to finish.
4. Revoke the old key at the provider.
5. Verify: `llmtw_provider_attempt_total{outcome="failure"}` for the endpoint
   does not rise after revocation.

Under compromise, revoke the old key as soon as the rollout finishes. A
provider call still made with the revoked key fails with `authentication`.

## Redis ACL password

Keep the ACL user and add a second password to it, so the username never
changes.

1. Add the new password to the worker's ACL user (`ACL SETUSER <user>
   >new-password`, or the managed service's equivalent). Both passwords now
   authenticate.
2. Replace the file `state.redis.password` points at.
3. Reload every worker and [verify](#verify-a-reload). The reload's
   dependency check connects with the new password before publishing.
4. Remove the old password (`ACL SETUSER <user> <old-password`).
5. Verify: readiness stays ready on every worker.

Connections opened by the previous snapshot close once its in-flight
Activities finish. If `state.redis.password` is `kind: env`, replace step 3
with a rolling restart.

## Redis TLS CA

1. Publish a CA bundle containing the old and new CA to the file
   `state.redis.tls.ca_file` names, and reload.
2. Move Redis to a certificate from the new CA.
3. Publish a bundle with only the new CA, and reload.

## Langfuse key pair

1. Create a new API key pair in the Langfuse project. Keep the old pair.
2. Replace both files (`langfuse.public_key`, `langfuse.secret_key`).
3. Reload every worker and [verify](#verify-a-reload).
4. Revoke the old pair in Langfuse.
5. Verify: `llmtw_langfuse_export_failures_total{reason="rejected"}` does not
   rise.

Exports still running on the previous snapshot use the old pair. One rejected
after revocation counts as `rejected` and is dropped without retry; it does
not change the operation's result.

## Temporal API key

`temporal.api_key_file` is read once when the Temporal client is created.

1. Create a new API key in Temporal. Keep the old one.
2. Replace the file and rolling restart.
3. Revoke the old key once every worker has restarted and is polling
   (`llmtw_worker_polling`).

## Temporal mTLS client certificate

The certificate, key and CA are read once when the Temporal client is created.

1. If the new certificate comes from a new CA, add that CA to the namespace's
   accepted client CAs first.
2. Replace `temporal.tls.cert_file` and `key_file` together (and `ca_file`
   if the server certificate's CA changes), then rolling restart.
3. Remove the old client CA from the namespace after every worker restarted.

## Continuation handle keys

Never change the bytes behind an existing key ID: every handle and internal
reference signed with that ID would stop verifying. Rotate by adding a new ID.

1. **Add** the new key as `primary: false` on every worker, with its own file
   and a new ID. Reload (or restart) and verify on every worker. With the
   shipped manifests the new file is a new projected Secret item, which
   changes the Deployment and so restarts the workers.
2. **Promote** the new key to `primary: true` and set the old key to
   `primary: false`. Reload and verify on every worker.
3. **Retire** the old key by removing it, once `state.continuation_retention`
   has passed since step 2 and every workflow open before step 2 has closed.

Do not promote before every worker has the new key: a worker without it
rejects the handles other workers sign. Under compromise, run steps 1 and 2
back to back and decide whether to retire at once; the
[design](../architecture/key-compromise-and-credential-rotation.md#revocation-procedure)
lists what callers and open workflows see in each case.

## Payload codec keys

Every codec change is a rolling restart, and callers and any codec server are
part of each stage.

1. **Add** the new key as `primary: false` to every worker, every caller's
   codec and every codec server. Restart the workers.
2. **Promote** it on every worker and caller. Restart the workers.
3. **Retire** the old key only when no open workflow used it and every closed
   workflow that used it is past the namespace retention period.

Removing a key early stops open workflows from replaying and makes closed
histories unreadable in the UI.

## Redis `key_secret`

`state.redis.key_secret` names Redis keys and, where the Responses API
listener runs, seals the hour-long Redis result copies; it does not
authenticate to Redis. If only it leaked, rotate the Redis ACL password first.
Replace the key too when the listener is in use and Redis read access cannot
be ruled out. Replacing it means a new namespace:

1. Generate at least 32 random bytes as the new secret, and choose a new
   `state.redis.key_prefix` (or `state.redis.admission_hash_tag`).
2. Run `budget-initialize --apply` with the new configuration. As for any
   first initialization, the admission Function or script must already be
   provisioned. No worker uses the new namespace yet.
3. Stop callers from starting workflows on the task queue, and wait until no
   workflow is open
   ([drain](fleet-upgrade-and-rollback.md#4-drain-when-the-change-requires-it)).
4. Stop every worker. Start workers with the new configuration.
5. Lower budget limits for one window length if a reset window is not
   acceptable: the new namespace starts with no recorded spend.
6. After the longest budget window has passed, delete the old namespace's
   keys. Match the complete old namespace,
   `<old key_prefix>:{<old admission_hash_tag>}:*`, never the bare
   `<key_prefix>:*`: if only the hash tag changed, the bare prefix also
   matches every key of the new namespace. In Redis Cluster all the old keys
   share the old hash tag's slot.

Never change `key_secret` under the same prefix and hash tag. A production
durable worker detects it, because the budget receipt binds the secret's
fingerprint: startup fails and a reload is rejected with the budget
initialization error.

## Cloud request storage secret

`state.requests.secret` derives both the payload encryption key and every
durable identifier. It cannot be rotated today. Replacing it means a new
`state.requests.namespace` and is a migration; see
[durable-storage key management](../architecture/durable-storage-key-management.md).
