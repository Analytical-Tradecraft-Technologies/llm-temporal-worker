# Configuration and credentials

Runbooks for failed configuration reloads, provider credential rotation and
rolling back a change to a process-lifetime setting. How reload works, and
which settings it may change, is in
[configuration reload](../reference/cli.md#configuration-reload). Start from
[on-call escalation and severity](on-call.md) for the rules that apply to
every incident.

The worker reloads its `--config` file on `SIGHUP` and when its file watcher
sees a complete replacement. The watcher watches only the configuration file:
a changed secret file, or a changed environment variable, is not noticed on
its own. Environment variables are not re-read during reload.

## Failed configuration reload

- **Signal:** `llmtw_config_reload_total{outcome="failure"}` increases and the
  `configuration reload failed` error log names a bounded `cause` (`read`,
  `yaml`, `validation`, `secret`, `process_lifetime`, `catalog`, `dependency`,
  `canceled` or `internal`) and, for `validation` and `process_lifetime`, the
  `config_field` schema path, such as `state.redis.key_prefix`.
- **Readiness reason:** none. A failed reload leaves the active snapshot and
  readiness unchanged; the worker keeps serving the previous configuration.
- **Default severity:** SEV3 when nothing urgent depended on the change; SEV2
  when the change was needed to fix an incident; SEV1 when the cause is
  `process_lifetime` on a durable identity field (see
  [configuration rollback](#configuration-rollback-for-process-lifetime-fields)).
- **Safe action:**
  1. Read `cause` and `config_field` from the log.
  2. `yaml` or `validation`: run
     `llm-temporal-worker validate-config --config <file>` on the same file for
     the full message, fix it, and replace the file again.
  3. `secret`: a referenced environment variable or secret file could not be
     read. Check the mounted Secret and its path.
  4. `catalog`: a catalog file could not be read, does not match its configured
     `sha256`, or does not decode. Recompute the digest of the reviewed file.
  5. `dependency`: replacement Redis, cloud request or blob clients failed
     their checks. Fix the dependency, then send `SIGHUP`.
  6. `process_lifetime`: the change needs a rollout, not a reload. Either
     revert the file, or plan a rollout that is allowed for that field (see
     the next sections).
  7. Confirm success with `llmtw_config_reload_total{outcome="success"}` and a
     `configuration reloaded` log carrying the new `config_version`; compare
     it with `validate-config`'s `config version` for the intended file.
- **Forbidden:** working around a `process_lifetime` rejection by restarting
  pods one at a time with the new value. That runs replicas with different
  durable identities against the same traffic.

## Provider credential rotation

Rotate a provider credential so the old and new values overlap: the provider
must accept both until every replica uses the new one.

- **Signal:** a planned rotation, or `provider attempt failed` warnings with
  `error_code=authentication` (Temporal type `llm_authentication`) for one
  `endpoint`, often with `llmtw_route_held_total{endpoint,reason="circuit_open"}`
  as route health holds the route.
- **Readiness reason:** none. Provider endpoints are not readiness
  dependencies.
- **Default severity:** SEV2 when a credential is already rejected; planned
  rotations are not incidents.
- **Safe action:** follow the procedure for the endpoint's `auth.kind`.
  1. `bearer_env` or `header_env` (the endpoint reads an environment
     variable): environment variables are fixed for the life of the process
     and a reload does not re-read them, so `SIGHUP` does not rotate these.
     Issue the new key at the provider, update the Kubernetes Secret behind
     the variable, and do a rolling restart of the Deployment. Revoke the old
     key only after every replica has restarted.
  2. `aws_default_chain` or `azure_default_credential`: the SDK credential
     chain refreshes its own temporary credentials. Rotate the role or
     identity in the cloud provider; no worker action is needed.
  3. File secrets (`kind: file`) that are reloadable, such as
     `state.redis.username` and `state.redis.password`: update the mounted
     Secret, wait until the kubelet has projected the new file into every pod
     (a Secret mounted with `subPath` is never updated in place and needs a
     restart instead), then send `SIGHUP` to each worker process. The reload reads the file
     again, checks Redis with the new credential, and only then publishes the
     snapshot; a failure keeps the old snapshot and logs `cause=secret` or
     `cause=dependency`. Remove the old Redis credential only after every
     replica logged `configuration reloaded`.
  4. Verify: `configuration reloaded` (for `SIGHUP`) or a clean start (for a
     restart) on every replica, no new `error_code=authentication` warnings for
     the endpoint, and `llmtw_provider_attempt_total{endpoint,outcome="success"}`
     rising. Publishing a replacement configuration snapshot also clears an
     authentication hold on the route for planning; otherwise that hold lasts
     until the configuration changes, bounded at 24 hours.
- **Forbidden:** rotating key material that names state, not access:
  `state.redis.key_secret`, `state.requests.secret` and
  `continuation.handle_keys` entries still in use. Changing them selects a
  different namespace or invalidates every handle; they need a migration, not
  a rotation. Temporal payload codec keys and the Temporal mTLS certificate
  or API key file are read at startup and rotate only through their own
  procedures with a rolling restart (see
  [Temporal payload encryption](../reference/configuration.md#temporal-payload-encryption)).
  Never paste a credential into a ticket, a log query or the configuration
  file itself.

## Configuration rollback for process-lifetime fields

Process-lifetime settings are read at startup. A reload that changes one is
rejected (`cause=process_lifetime`, with `config_field`), so running replicas
keep the old value while any replica that restarts picks up the new one.
The full list is in
[configuration reload](../reference/cli.md#configuration-reload). They fall
into two groups:

- **Process wiring:** the `server` listeners, shutdown and
  monitor settings, the inline Activity payload limit, Temporal connection and
  worker settings, telemetry wiring, the endpoint set and `outbound_hosts`.
  Rolling these forward or back is an ordinary rollout.
- **Durable identity:** `environment`, `temporal.namespace`, `state.kind`,
  `state.redis.key_prefix`, `state.redis.admission_hash_tag`,
  `state.redis.key_secret`, the `state.requests` provider, `request_table`,
  `payload_store`, `namespace` and `secret`, and the result blob store's kind,
  root, bucket, region and prefix. Requests, reservations, results and
  checkpoints exist only under the identity they were written with.

- **Signal:** `configuration reload failed` with `cause=process_lifetime` and
  a `config_field`, or replicas reporting different `config_version` values
  after some of them restarted.
- **Readiness reason:** none; replicas on either value can be ready.
- **Default severity:** SEV3 for process wiring; SEV1 for durable identity
  once any replica has served traffic with the new value.
- **Safe action:**
  1. Stop further restarts: pause the rollout and any autoscaling or node
     drains that would start new pods.
  2. Find which replicas run which value from their `config_version`.
  3. Process wiring: either complete the rollout with the new value or revert
     the file and roll the replicas that already restarted. A revert reloads
     cleanly on replicas that never took the change.
  4. Durable identity, no replica has served traffic on the new value yet:
     revert the file to the previous content and roll back the restarted
     replicas so every replica uses the original identity.
  5. Durable identity, some replicas served traffic on the new value:
     escalate to the service owner before rolling anything. Work started
     under the new identity exists only there; flipping back strands it, and
     flipping forward strands the old work. Keep both sets of state, record
     the time window and the affected replicas, and decide with the owner.
     Never delete either side.
  6. Lowering `server.inline_payload_bytes` is safe only after draining open
     Generate and Compact workflows; raising it is safe at any time.
- **Forbidden:** toggling a durable identity field back and forth, copying or
  renaming Redis keys, tables or buckets to "merge" two identities, or
  deleting the state written under either identity. A deployment that needs
  to move durable state needs its own migration and rollback design.
