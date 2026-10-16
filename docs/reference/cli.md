# Command-line reference

The repository builds one binary, `llm-temporal-worker`. The production entry
point accepts a subcommand followed by flags:

```sh
llm-temporal-worker <command> [flags]
```

Every command that reads configuration accepts `--config PATH`. If it is not
provided, the binary reads `/etc/llmtw/config.yaml`.

## `help`

Print the command summary without reading configuration or starting runtime
dependencies. `help`, `-h`, and `--help` are equivalent when used as the first
argument:

```sh
llm-temporal-worker --help
```

The command exits with status `0`.

## `version`

Print the image and binary build metadata as JSON without reading configuration
or starting runtime dependencies:

```sh
llm-temporal-worker version
```

The result contains the version, revision, build time, Go version, and source
URL stamped into the binary. It contains no configuration or credentials. The
`make image-verify` gate compares this output with the final image labels.

## `health-server`

Start only the probe listener used by hardened-image verification:

```sh
llm-temporal-worker health-server --address 0.0.0.0:8080
```

It reports `/health/live` while its listener is running and keeps
`/health/ready` unavailable because it does not construct the worker or check
dependencies. It is not a production worker mode; deployment manifests must
continue to use `worker --config /etc/llmtw/config.yaml`.

## `healthcheck`

Probe one or more HTTP(S) health endpoints and exit non-zero unless every one
is healthy. It does not read configuration or start runtime dependencies, so
it can run as a container healthcheck inside the minimal image, which has no
shell or HTTP client:

```sh
llm-temporal-worker healthcheck \
  --url http://127.0.0.1:8080/health/live \
  --url http://127.0.0.1:8080/health/ready
```

`--url` is required and may be repeated; each value must be an absolute
`http` or `https` URL. The command sends a `GET` to each URL in order and
stops at the first failure. Each request has a two-second timeout, redirects
are not followed, and only a `2xx` response counts as healthy. The Compose
worker service uses it with the live and ready endpoints shown above.

On success it prints `healthcheck passed` and exits with status `0`. A
connection error, timeout, or non-`2xx` response prints `healthcheck failed`
and exits with status `1`. A missing `--url`, an extra argument, or a URL that
is not an absolute HTTP(S) URL exits with status `2`. Response bodies are not
read or printed.

## `worker`

Attempt to start the production composition, including the configured provider
and state backends, Temporal client, health and metrics listeners, and Activity
worker. The CLI installs the cloud runtime using the explicit
`authorization.mode: trusted_temporal` policy and its tenant/project allowlist.
Missing policy or incomplete durable storage configuration fails closed before
listeners or polling:

```sh
llm-temporal-worker worker --config /etc/llmtw/config.yaml
```

The command validates the YAML before constructing runtime dependencies. It
then blocks while the worker polls its configured Temporal task queue. A
`SIGINT` or `SIGTERM` begins graceful shutdown: readiness is withdrawn, polling
stops, in-flight Activities receive their configured grace period, and the
runtime drains its snapshot clients before exiting. `SIGHUP` requests a
configuration reload without stopping polling; the worker also watches the
same `--config` file for atomic replacement or in-place metadata changes.

The worker resolves referenced secrets and catalog files while constructing the
production runtime. A failure in that phase is fatal; the process must not
start polling with an incomplete provider or state configuration.

## `budget-initialize`

Inspect or initialize the Redis budget namespace before starting a production
durable worker. It uses the same configuration, IAM cloud storage access, Redis
TLS/CA settings, and file/environment secret references as the worker. It does
not connect to Temporal, resolve provider credentials, load provider catalogs,
or submit LLM requests. It requires the worker's trusted Temporal authorization
configuration and durable cloud storage settings.

```sh
llm-temporal-worker budget-initialize --config /etc/llmtw/config.yaml
llm-temporal-worker budget-initialize --config /etc/llmtw/config.yaml --apply --timeout 30s
```

Without `--apply`, the command performs reads only and reports JSON with
`status` equal to `initialization_required`, `incomplete`, or `ready` and
`applied: false`. The default overall timeout is 30 seconds; `--timeout` must
be positive and at most five minutes. Inspection still requires the pinned
Redis code, persistence policy, and access to the cloud receipt.

`--apply` conditionally creates a permanent cloud receipt, verifies that the
Redis namespace is unused, and writes a persistent Redis marker and initial
coordination event. Repeating a completed initialization only verifies it;
it never resets balances or re-creates a lost marker. An incomplete installation
can resume only if its preparing or ready Redis marker remains present.
Keep all workers stopped during first initialization. Provision the exact
admission Function or Lua script separately before running the command.

There is no reset flag or automatic data-loss recovery. A lost cloud-create
reply or a crash before the first Redis write requires operator investigation;
do not delete the receipt or change namespaces to get past the error. See
[budget initialization](redis-budget-leases.md#initialization-and-readiness)
for ordering, permissions, and recovery limits.

## `generation-recover`

Restore the original public planning binding of a historical compacted
Generate operation from its retained Temporal execution history. It reads the
same configuration as `worker`, requires durable cloud storage and the trusted
Temporal authorization configuration, and constructs only the cloud storage
and Temporal clients. It does not start a worker, load provider catalogs,
acquire budget, or submit LLM requests.

```sh
llm-temporal-worker generation-recover --config /etc/llmtw/config.yaml \
  --workflow-id ORIGINAL_WORKFLOW_ID --run-id ORIGINAL_RUN_ID
llm-temporal-worker generation-recover --config /etc/llmtw/config.yaml \
  --workflow-id ORIGINAL_WORKFLOW_ID --run-id ORIGINAL_RUN_ID --apply
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config PATH` | `/etc/llmtw/config.yaml` | worker configuration YAML |
| `--workflow-id ID` | required | original public Generate workflow ID |
| `--run-id ID` | required | explicit original run ID; the latest run is never substituted |
| `--apply` | `false` | persist the verified planning decision and binding |
| `--timeout DURATION` | `2m` | overall timeout; must be positive and at most two minutes |

Without `--apply` the command is a dry run: every storage operation is a read.
It prints `recovery_required` when the retained history and the durable
Generate manifest agree but the public binding is missing, and `ready` when
the same binding already exists. `--apply` writes the planning decision and
binding with conditional immutable writes, reads the binding back, and reports
`ready` with `applied: true`. An existing matching binding is left unchanged
and reported as `ready` with `applied: false`; a conflicting or corrupt one
fails rather than being replaced. Retry an interrupted or uncertain apply with
the same workflow and run IDs.

Successful output is one JSON object containing only `status` and `applied`,
for example `{"status":"recovery_required","applied":false}`. It omits
identifiers and payloads, and error messages never echo the supplied IDs.

The command exits with status `0` on success. It exits with status `2`, before
reading configuration, when `--workflow-id` or `--run-id` is missing, a flag is
invalid, an extra argument is supplied, or `--timeout` is out of range. It
exits with status `1` when the configuration cannot be read or is invalid, or
when history, decryption keys, storage, authorization, or the binding cannot
be verified. See
[durable preparation and recovery](cloud-request-preparation.md#durable-preparation-and-recovery)
for which operations need recovery and the history limits.

## `validate-config`

Parse and validate the strict YAML shape without starting the worker:

```sh
llm-temporal-worker validate-config --config ./config.yaml
```

On success it prints the effective configuration digest as a `config version`.
In the production binary this command performs schema/default validation only;
it does not read secret values, load catalog file contents, dial Temporal,
connect to Redis or S3, or contact a provider. Use `worker` startup as the
full dependency and reference-validation gate.

## `print-effective-config`

Print the canonical effective configuration as JSON without starting runtime
dependencies:

```sh
llm-temporal-worker print-effective-config --config ./config.yaml
```

The output contains configuration and secret-reference metadata, not resolved
secret values. It is canonical JSON rather than the source YAML, so it is
appropriate for comparing configuration snapshots but should still be handled
as deployment-sensitive material.

## Configuration reload

`worker` reloads when it receives `SIGHUP` or observes a change to the supplied
`--config` path. It reads a complete replacement, validates it, resolves
references, constructs and verifies replacement state clients, atomically
publishes the new snapshot, then drains clients captured by in-flight
Activities. At startup the watcher also compares the path with the bytes that
were already validated, covering a replacement that lands before the watcher
is initialized. Repeated notifications are coalesced.

The watcher polls the path every second and reloads a change only once two
consecutive polls see the same file identity, size, modification time and
content; the reload then publishes only that content. A file that is still
being written in place is therefore not reloaded until the write has finished,
and an atomic rename or ConfigMap symlink swap is reloaded about two seconds
after it lands. `SIGHUP` is an explicit request and reloads the file as it is
at that moment, without waiting for it to settle.

An unreadable or invalid replacement leaves the active snapshot and readiness
state unchanged. The worker records `llmtw_config_reload_total{outcome="failure"}`
and emits only a safe error classification; configuration text, paths, resolved
secrets, and provider payloads are not logged. A successful reload records
`outcome="success"`.

The `configuration reload failed` log record carries a bounded `cause` so a
rejection can be diagnosed without restarting a pod:

| `cause` | Meaning |
| --- | --- |
| `read` | the file could not be opened or read |
| `yaml` | the file is not decodable: malformed YAML, an unknown or duplicate key, or a value of the wrong type |
| `validation` | the decoded configuration failed validation |
| `secret` | a secret reference could not be resolved, including a provider API-key variable or key secret read while clients are built |
| `process_lifetime` | the replacement changes a setting that requires a restart (listed below) |
| `catalog` | a capability or pricing catalog could not be read, did not match its configured SHA-256, or could not be decoded |
| `dependency` | replacement clients could not be built, or a Redis, cloud request or blob dependency check failed |
| `canceled` | the reload was canceled or timed out before a dependency check |
| `internal` | anything else |

For `validation` and `process_lifetime` the record also carries `config_field`,
the schema path of the offending field, such as `state.redis.key_prefix` or
`models.*.routes[0].model`. Operator-chosen map keys are written as `*` and no
configured value, file path or reference name is logged. `validate-config` on
the same file prints the full validation message; it cannot report a
`process_lifetime`, `catalog` or `dependency` rejection, which depend on the
running worker. The cause is a log attribute only;
`llmtw_config_reload_total` keeps its single `outcome` label.

Reload changes the dynamic request snapshot (routes, catalogs, budgets, and
provider/state clients). The following are established at startup and require
a restart. A replacement that changes one is rejected before replacement
clients are built, with the field in `config_field`:

- `environment`, the `server` listener addresses, shutdown and
  dependency-monitor settings and inline Activity payload limit;
- the `temporal` connection, task queue and worker settings, and `telemetry`
  process wiring;
- `endpoints.*.outbound_hosts` and the set of endpoints;
- the identity of durable state: `state.kind`, `state.redis.key_prefix`,
  `state.redis.admission_hash_tag`, the `state.redis.key_secret` reference,
  and in `state.requests` the `provider` block (type, AWS region, profile and
  temp directory, and the table and blob alias maps), `request_table`,
  `payload_store`, `namespace` and the `secret` reference;
- where results are stored: `blob_store.kind`, `blob_store.file.root`, and
  `blob_store.s3.bucket`, `region` and `prefix`.

State and result clients are rebuilt for every snapshot, so these could be
swapped mechanically, but requests, budget reservations and results already in
flight exist only under the old identity; after a swap they would read as
absent and a retried operation could be dispatched and charged again.

`state.redis.addresses`, the Redis username and password references, TLS,
connection limits and timeouts, and `blob_store.s3.auth` remain reloadable:
they change how the same data is reached. The replacement must still pass the
dependency checks before it is published. The two key-material references are
compared as references only. The secret behind an unchanged reference is read
again on reload and must not change. With `state.kind: memory` the Redis
section is ignored, so `state.redis.admission_hash_tag` and
`state.redis.key_secret` can change there.

Environment variables are not re-read during reload.

## Exit status and diagnostics

- `0` means the command completed successfully.
- `1` means a file, configuration, dependency, or runtime operation failed.
- `2` means the invocation was invalid, such as a missing command, unknown
  command, or invalid flag.

Command errors are reduced to a single safe line. Credential, token, prompt,
output, and provider-body terms are redacted from the CLI error boundary.
