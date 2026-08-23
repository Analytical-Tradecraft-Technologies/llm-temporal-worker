# syntax=docker/dockerfile:1.7

# Both build and runtime roots are reviewed immutable manifest-list digests.
# Buildx still selects only linux/amd64; a dependency update changes the
# readable patch tag and digest together.
ARG GO_IMAGE=docker.io/library/golang:1.26.7@sha256:45a5f7a810238aabcbad211d70b9ae082022d96f7c7259e94041ad1b933575ac
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown
ARG SOURCE=https://github.com/mfow/llm-temporal-worker
ARG GO_VERSION=unknown

FROM ${GO_IMAGE} AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG REVISION
ARG BUILD_TIME
ARG SOURCE
ARG GO_VERSION

WORKDIR /src

# Keep dependency downloads in a cacheable layer and never copy credentials
# into the build context. BuildKit cache mounts are optional but harmless when
# the builder does not enable them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test "${TARGETOS:-linux}" = "linux" && \
    test "${TARGETARCH:-amd64}" = "amd64" && \
    test -n "${VERSION}" && test "${VERSION}" != "dev" && \
    test -n "${REVISION}" && test "${REVISION}" != "unknown" && \
    test -n "${BUILD_TIME}" && test "${BUILD_TIME}" != "unknown" && \
    test -n "${SOURCE}" && \
    actual_go_version="$(go env GOVERSION)" && \
    if [ "${GO_VERSION}" = "unknown" ]; then \
      go_version="${actual_go_version}"; \
    else \
      test "${actual_go_version}" = "${GO_VERSION}"; \
      go_version="${GO_VERSION}"; \
    fi && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -buildvcs=false \
      -ldflags="-s -w \
        -X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Version=${VERSION} \
        -X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Revision=${REVISION} \
        -X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.BuildTime=${BUILD_TIME} \
        -X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Source=${SOURCE} \
        -X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.GoVersion=${go_version}" \
      -o /out/llm-temporal-worker ./cmd/llm-temporal-worker

# Distroless static includes CA roots, time-zone data, and uid 65532, but no
# shell, package manager, or toolchain. The reviewed digest is the same
# manifest list for which Buildx selects linux/amd64.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35

ARG VERSION
ARG REVISION
ARG BUILD_TIME
ARG SOURCE
ARG GO_VERSION

LABEL org.opencontainers.image.title="llm-temporal-worker" \
      org.opencontainers.image.description="Replay-safe Temporal LLM inference worker" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.source="${SOURCE}" \
      io.github.mfow.llm-temporal-worker.go.version="${GO_VERSION}"

ENV LLMTW_BUILD_VERSION="${VERSION}" \
    LLMTW_BUILD_GIT_SHA="${REVISION}" \
    LLMTW_BUILD_TIMESTAMP="${BUILD_TIME}" \
    LLMTW_BUILD_SOURCE="${SOURCE}" \
    LLMTW_BUILD_GO_VERSION="${GO_VERSION}"

COPY --from=build /out/llm-temporal-worker /usr/local/bin/llm-temporal-worker

USER 65532:65532
WORKDIR /home/nonroot

# Health is provided by the orchestrator. Distroless intentionally has no
# shell/curl healthcheck and the root filesystem is expected to be read-only.
# The production deployment supplies `worker --config ...`; keeping CMD empty
# makes the native binary the only image entrypoint.
ENTRYPOINT ["/usr/local/bin/llm-temporal-worker"]
