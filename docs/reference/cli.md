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

## `worker`

Start the production composition, including the configured provider and state
backends, Temporal client, health and metrics listeners, and Activity worker:

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
Activities. Repeated notifications are coalesced.

An unreadable or invalid replacement leaves the active snapshot and readiness
state unchanged. The worker records `llmtw_config_reload_total{outcome="failure"}`
and emits only a safe error classification; configuration text, paths, resolved
secrets, and provider payloads are not logged. A successful reload records
`outcome="success"`.

Reload changes the dynamic request snapshot (routes, catalogs, budgets, and
provider/state clients). The environment, listener addresses, shutdown and
dependency-monitor settings, inline Activity payload limit, Temporal
connection/task-queue and worker settings, telemetry process wiring, Redis key
prefix, PostgreSQL database/schema/table prefix, and endpoint outbound-host
allowlists are established at startup and require a restart. A replacement
that changes one is rejected before replacement clients are built.
Environment variables are not re-read during reload.

## Exit status and diagnostics

- `0` means the command completed successfully.
- `1` means a file, configuration, dependency, or runtime operation failed.
- `2` means the invocation was invalid, such as a missing command, unknown
  command, or invalid flag.

Command errors are reduced to a single safe line. Credential, token, prompt,
output, and provider-body terms are redacted from the CLI error boundary.
