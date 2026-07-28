# Deployment assets

The worker image is built by the repository [Dockerfile](../Dockerfile) as a
static, non-root binary on a digest-pinned Go builder and a digest-pinned
Distroless runtime. It has no shell, writes no files by default, and expects an
orchestrator-provided `/tmp` volume when the root filesystem is read-only. The
Kubernetes base deliberately contains a `REPLACE_WITH_RELEASE_DIGEST` marker;
release automation must substitute the signed digest before applying it.

Kubernetes manifests live under `kubernetes/base` and include:

- two replicas, rolling-update safety, a disruption budget, resource bounds,
  dropped capabilities, RuntimeDefault seccomp, and read-only root storage;
- fail-closed `/health/live` and `/health/ready` probes plus `/metrics`;
- ConfigMap-mounted non-secret configuration/catalogs and externally provisioned
  Secret volumes for Redis/TLS/continuation material;
- an ingress/egress NetworkPolicy that permits only probe/metrics traffic and
  DNS, Redis, worker PostgreSQL, Temporal, and TLS egress. PostgreSQL is
  explicitly allowed on TCP 5432 because the durable worker configuration
  uses it as the authoritative state store;
- AWS and Azure workload-identity examples that opt into service-account token
  mounting only in the selected overlay.

Render and structurally inspect every manifest offline with:

```sh
make deployment-policy-verify
```

For an explicit, reviewed `kubectl` binary, run the companion check with its
path pinned by the invoking environment:

```sh
KUBECTL=/path/to/pinned/kubectl make kustomize-verify
```

Both commands use only `kubectl kustomize`; neither can apply a resource or
contact a cluster. The rendered policy rejects root or string identities,
unbounded writable storage, missing CPU/memory constraints, unsafe service
account token mounting, and probe paths that diverge from the worker contract.
Replace the example image/config/catalog/identity values in a reviewed overlay
before production use; no credentials belong in this tree.

## Hardened image verification

Run the Docker-backed image gate from a clean checkout:

```sh
make image-verify
```

The target builds a local image stamped with the checked-out revision and its
commit time, then verifies the OCI labels and the binary's `version` JSON for
the version, revision, build time, Go version, and source URL. It starts the
image directly—without a shell or root override—as UID/GID `65532:65532` with
a read-only root filesystem and exactly one writable mount: `/tmp` as a
`rw,nosuid,nodev,noexec,size=64m` tmpfs. The verification-only
`health-server` command reports liveness but deliberately remains unready
because it does not construct worker dependencies.

`make image-verify` requires a running Docker daemon. The image build context
continues to exclude local credentials and runtime state through
[`.dockerignore`](../.dockerignore).
