SHELL := /bin/sh

GO ?= go
# Keep the security scan on the reviewed Go patch release. A moving `go env
# GOVERSION` default can select an older runner toolchain than the documented
# baseline and reintroduce fixed standard-library vulnerabilities.
SECURITY_GO_TOOLCHAIN ?= go1.26.7
SECURITY_BASE_DIR ?=
COMPOSE ?= docker compose
KUBECTL ?= kubectl
READINESS_REDIS_IMAGE ?= redis:7.4.2-alpine@sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952
READINESS_REDIS_CONTAINER_PREFIX ?= llmtw-readiness-integration
REDIS_INTEGRATION_IMAGE ?= redis:7.4.2-alpine@sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952
REDIS_INTEGRATION_CONTAINER_PREFIX ?= llmtw-redis-integration
POSTGRES_INTEGRATION_IMAGE ?= postgres:17.5-alpine@sha256:6567bca8d7bc8c82c5922425a0baee57be8402df92bae5eacad5f01ae9544daa
POSTGRES_INTEGRATION_CONTAINER_PREFIX ?= llmtw-postgres-integration
IMAGE_VERIFY_TAG ?= llm-temporal-worker:image-verify
IMAGE_VERIFY_VERSION ?= image-verify
IMAGE_VERIFY_SOURCE ?= https://github.com/mfow/llm-temporal-worker
IMAGE_VERIFY_GO_VERSION ?= $(shell $(GO) env GOVERSION)
IMAGE_VERIFY_OCI_LAYOUT ?=

.PHONY: fmt-check schema-verify docs-verify traceability-verify workflow-verify vet test build benchmark redis-benchmark-compile redis-benchmark slo-evidence-verify integration readiness-integration redis-integration postgres-integration image-verify compose-smoke compose-live-integration deployment-policy-verify kustomize-verify adapter-contracts routing-contracts routing-fuzz live-contract-verify security-pr-verify security-verify fuzz-smoke mutation-verify release-verify verify

fmt-check:
	@bash scripts/check-go-format.sh

schema-verify:
	$(GO) test ./llm/schema ./config

docs-verify:
	$(GO) test ./internal/documentationtest

traceability-verify:
	$(GO) test ./internal/architecturetest -run '^TestV1Traceability' -count=1

workflow-verify:
	bash scripts/check-workflow-policy.sh

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

# Executes the build-tagged live-provider contract unit suite without making a
# provider request. Force all live gates off so this deterministic target cannot
# become a credentialed run through inherited environment values.
live-contract-verify:
	env \
		LLMTW_LIVE_TESTS=0 \
		LLMTW_LIVE_AUTHORIZED=0 \
		LLMTW_LIVE_OPENAI_RESPONSES=0 \
		LLMTW_LIVE_AZURE_RESPONSES=0 \
		LLMTW_LIVE_OPENAI_CHAT=0 \
		LLMTW_LIVE_OPENROUTER_CHAT=0 \
		LLMTW_LIVE_EXA_CHAT=0 \
		LLMTW_LIVE_ANTHROPIC_DIRECT=0 \
		LLMTW_LIVE_ANTHROPIC_AWS=0 \
		LLMTW_LIVE_BEDROCK_ANTHROPIC=0 \
		LLMTW_LIVE_BEDROCK_CONVERSE=0 \
		$(GO) test -tags=live ./integration/live -count=1

build:
	$(GO) build ./...

# Compiles the opt-in Redis benchmark without running its tests or benchmark.
# It has no operator gates and cannot connect to Redis or a provider.
redis-benchmark-compile:
	$(GO) test -tags=redisbenchmark ./engine -run '^$$' -bench '^$$'

# Runs an opt-in local performance proxy. It has no Redis server or provider
# network request, so its p99 output is not an SLO proof.
benchmark:
	$(GO) test ./engine -run '^$$' -bench '^BenchmarkGenerateMemoryAdmissionAndCompile$$' -benchmem -count=1

# Measures Generate against an explicitly addressed, already-provisioned Redis
# deployment. This target is deliberately operator-only: it refuses CI, never
# starts Redis, and never loads Functions or invokes a live provider.
redis-benchmark:
	@test "$${LLMTW_REDIS_BENCHMARK:-}" = "1" || { echo "redis-benchmark requires LLMTW_REDIS_BENCHMARK=1" >&2; exit 2; }
	@test "$${LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION:-}" = "1" || { echo "redis-benchmark requires LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION=1" >&2; exit 2; }
	@test -n "$${LLMTW_REDIS_BENCHMARK_ADDR:-}" || { echo "redis-benchmark requires LLMTW_REDIS_BENCHMARK_ADDR" >&2; exit 2; }
	@test -z "$${CI:-}" || { echo "redis-benchmark refuses CI" >&2; exit 2; }
	LLMTW_REDIS_BENCHMARK=1 LLMTW_REDIS_BENCHMARK_ALLOW_MUTATION=1 LLMTW_REDIS_BENCHMARK_ADDR="$${LLMTW_REDIS_BENCHMARK_ADDR}" $(GO) test -tags=redisbenchmark ./engine -run '^$$' -bench '^BenchmarkGenerateRedisAdmissionAndCompile$$' -benchmem -count=1 -benchtime=10s

# Runs the offline SLO evidence recorder/verifier contract tests without
# requiring a protected measurement, provider credentials, Redis, or Docker.
# This target is intentionally separate from release-verify: it validates the
# standalone SLO evidence binding contract, while release-verify validates a
# complete retained release bundle.
slo-evidence-verify:
	$(GO) test ./internal/architecturetest -run '^TestSLOEvidence' -count=1

integration:
	$(GO) test -race ./integration/...

# Runs the readiness recovery gate against exactly one fresh, digest-pinned
# Redis daemon. The test itself explicitly provisions the immutable Functions
# and compatibility script; the worker only verifies them and never writes
# shared Redis code.
readiness-integration:
	@command -v docker >/dev/null 2>&1 || { \
		echo "readiness-integration requires Docker" >&2; \
		exit 2; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "readiness-integration requires a running Docker daemon" >&2; \
		exit 2; \
	}
	@set -eu; \
		container="$(READINESS_REDIS_CONTAINER_PREFIX)-$$(date +%s)-$$$$"; \
		cleanup() { \
			status=$$?; \
			if [ "$$status" -ne 0 ]; then docker logs "$$container" >&2 || true; fi; \
			docker rm -f "$$container" >/dev/null 2>&1 || true; \
			exit "$$status"; \
		}; \
		trap cleanup EXIT; \
		trap 'exit 130' HUP INT TERM; \
		docker run --detach --name "$$container" --publish 127.0.0.1::6379 "$(READINESS_REDIS_IMAGE)" redis-server --appendonly yes --appendfsync always --save 60 1 --maxmemory-policy noeviction >/dev/null; \
		port="$$(docker inspect --format '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}' "$$container")"; \
		case "$$port" in \
			''|*[!0-9]*) echo "readiness-integration could not discover its loopback Redis port" >&2; exit 1 ;; \
		esac; \
		LLMTW_READINESS_REDIS_ADDR="127.0.0.1:$$port" LLMTW_READINESS_REDIS_CONTAINER="$$container" $(GO) test -count=1 -tags=readinessintegration ./integration; \
		LLMTW_REDIS_ADDR="127.0.0.1:$$port" LLMTW_REDIS_CONTAINER="$$container" LLMTW_REDIS_CONTAINER_PREFIX="$(READINESS_REDIS_CONTAINER_PREFIX)" LLMTW_REDIS_TEST_PROVISION=1 $(GO) test -count=1 -tags=integration ./storage/redis -run '^TestLiveRedis'

# Runs the black-box StoreFactory contract against memory and one fresh,
# digest-pinned Redis daemon. The target discovers an ephemeral loopback port
# for its own uniquely named container, explicitly enables durable AOF/RDB
# settings, and removes only that container. Redis output is shown only on
# failure and always passes through the repository redactor.
redis-integration:
	@command -v docker >/dev/null 2>&1 || { \
		echo "redis-integration requires Docker" >&2; \
		exit 2; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "redis-integration requires a running Docker daemon" >&2; \
		exit 2; \
	}
	@set -eu; \
		container="$(REDIS_INTEGRATION_CONTAINER_PREFIX)-$$(date +%s)-$$$$"; \
		cleanup() { \
			status=$$?; \
			if [ "$$status" -ne 0 ]; then \
				docker logs "$$container" 2>&1 | LLMTW_LOG_REDACT_REDIS_PASSWORD=not-configured LLMTW_LOG_REDACT_POSTGRES_PASSWORD=not-configured LLMTW_LOG_REDACT_MOCK_API_KEY=not-configured LLMTW_LOG_REDACT_CONTINUATION_HMAC=not-configured bash ./scripts/redact-compose-logs.sh >&2 || true; \
			fi; \
			docker rm -f "$$container" >/dev/null 2>&1 || true; \
			exit "$$status"; \
		}; \
		trap cleanup EXIT; \
		trap 'exit 130' HUP INT TERM; \
		docker run --detach --name "$$container" --publish 127.0.0.1::6379 "$(REDIS_INTEGRATION_IMAGE)" redis-server --appendonly yes --appendfsync always --save "60 1" --maxmemory-policy noeviction >/dev/null; \
		port="$$(docker inspect --format '{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}' "$$container")"; \
		case "$$port" in \
			''|*[!0-9]*) echo "redis-integration could not discover its loopback Redis port" >&2; exit 1 ;; \
		esac; \
		$(GO) test -count=1 ./storage/conformance ./storage/memory ./storage/redis; \
		LLMTW_REDIS_ADDR="127.0.0.1:$$port" LLMTW_REDIS_CONTAINER="$$container" LLMTW_REDIS_CONTAINER_PREFIX="$(REDIS_INTEGRATION_CONTAINER_PREFIX)" LLMTW_REDIS_TEST_PROVISION=1 $(GO) test -count=1 -tags=integration ./storage/redis -run '^TestLiveRedis'; \
		echo "redis-integration passed (isolated pinned Redis)"

# Runs the PostgreSQL namespace/schema gate against a fresh PostgreSQL 17
# service. The schema installer is the only mutating operation; the container
# and its temporary data are removed on every exit.
postgres-integration:
	@command -v docker >/dev/null 2>&1 || { echo "postgres-integration requires Docker" >&2; exit 2; }
	@docker info >/dev/null 2>&1 || { echo "postgres-integration requires a running Docker daemon" >&2; exit 2; }
	@set -eu; \
		container="$(POSTGRES_INTEGRATION_CONTAINER_PREFIX)-$$(date +%s)-$$$$"; \
		cleanup() { docker rm -f "$$container" >/dev/null 2>&1 || true; }; \
		trap cleanup EXIT HUP INT TERM; \
		docker run --detach --name "$$container" --env POSTGRES_USER=llmtw --env POSTGRES_PASSWORD=llmtw --env POSTGRES_DB=llm_worker --publish 127.0.0.1::5432 "$(POSTGRES_INTEGRATION_IMAGE)" >/dev/null; \
		port="$$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$$container")"; \
		for attempt in $$(seq 1 30); do docker exec "$$container" pg_isready -U llmtw -d llm_worker >/dev/null 2>&1 && break; sleep 1; done; \
		LLMTW_POSTGRES_ADDR="127.0.0.1:$$port" LLMTW_POSTGRES_DATABASE=llm_worker LLMTW_POSTGRES_SCHEMA=llm_worker $(GO) test -count=1 ./storage/postgres

# Builds a fresh local image from the checked-out revision, then delegates all
# runtime assertions to the Docker-backed integration test. The test runs the
# image directly as its numeric non-root user with an explicitly read-only
# root filesystem and the sole writable /tmp tmpfs. When release evidence
# requests an OCI layout, one Docker-compatible archive with explicit OCI media
# types is loaded for runtime testing and extracted for the evidence consumers.
# Its private archive staging directory is trap-cleaned; the caller retains the
# extracted layout for the downstream evidence steps.
image-verify:
	@command -v docker >/dev/null 2>&1 || { \
		echo "image-verify requires Docker" >&2; \
		exit 2; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "image-verify requires a running Docker daemon" >&2; \
		exit 2; \
	}
	@set -eu; \
		revision="$$(git rev-parse HEAD)"; \
		build_time="$$(git show -s --format=%cI HEAD)"; \
		if [ -n "$(IMAGE_VERIFY_OCI_LAYOUT)" ]; then \
			layout="$(IMAGE_VERIFY_OCI_LAYOUT)"; \
			case "$$layout" in \
				/*) ;; \
				*) layout="$$PWD/$$layout" ;; \
			esac; \
			mkdir -p "$$(dirname "$$layout")"; \
			test ! -e "$$layout" && test ! -L "$$layout" || { echo "image-verify OCI layout already exists: $$layout" >&2; exit 2; }; \
			archive_directory="$$(mktemp -d "$${TMPDIR:-/tmp}/llmtw-image-verify.XXXXXX")"; \
			archive="$$archive_directory/image.tar"; \
			cleanup_archive() { rm -rf -- "$$archive_directory"; }; \
			trap cleanup_archive EXIT HUP INT TERM; \
			docker buildx build --platform linux/amd64 --provenance=false --sbom=false \
				--tag "$(IMAGE_VERIFY_TAG)" \
				--output "type=docker,oci-mediatypes=true,dest=$$archive,tar=true,name=$(IMAGE_VERIFY_TAG)" \
				--build-arg VERSION="$(IMAGE_VERIFY_VERSION)" \
				--build-arg REVISION="$$revision" \
				--build-arg BUILD_TIME="$$build_time" \
				--build-arg SOURCE="$(IMAGE_VERIFY_SOURCE)" \
				--build-arg GO_VERSION="$(IMAGE_VERIFY_GO_VERSION)" \
				.; \
			docker image load --input "$$archive" >/dev/null; \
			mkdir "$$layout"; \
			tar -xf "$$archive" -C "$$layout"; \
			docker image inspect "$(IMAGE_VERIFY_TAG)" >/dev/null; \
		else \
			docker build --tag "$(IMAGE_VERIFY_TAG)" \
				--build-arg VERSION="$(IMAGE_VERIFY_VERSION)" \
				--build-arg REVISION="$$revision" \
				--build-arg BUILD_TIME="$$build_time" \
				--build-arg SOURCE="$(IMAGE_VERIFY_SOURCE)" \
				--build-arg GO_VERSION="$(IMAGE_VERIFY_GO_VERSION)" \
				.; \
		fi; \
		LLMTW_IMAGE="$(IMAGE_VERIFY_TAG)" \
		LLMTW_IMAGE_VERSION="$(IMAGE_VERIFY_VERSION)" \
		LLMTW_IMAGE_REVISION="$$revision" \
		LLMTW_IMAGE_BUILD_TIME="$$build_time" \
		LLMTW_IMAGE_SOURCE="$(IMAGE_VERIFY_SOURCE)" \
		LLMTW_IMAGE_GO_VERSION="$(IMAGE_VERIFY_GO_VERSION)" \
		$(GO) test -count=1 -tags=imageintegration ./integration -run '^TestHardenedImageRuntimeAndMetadata$$'

compose-smoke:
	@command -v "$(firstword $(COMPOSE))" >/dev/null 2>&1 || { \
		echo "compose-smoke requires '$(firstword $(COMPOSE))'; install Docker Compose or set COMPOSE to an equivalent command" >&2; \
		exit 2; \
	}
	$(GO) test ./integration/compose
	$(COMPOSE) config --quiet
	@if [ "$${LLMTW_COMPOSE_LIVE:-0}" = "1" ]; then \
		echo "LLMTW_COMPOSE_LIVE=1 requested, but the worker live gate is intentionally opt-in and is not started by this offline-safe target." >&2; \
		echo "Run the documented Compose commands only in an authorized environment after supplying a continuation key and provider/state runtime." >&2; \
		exit 2; \
	else \
		echo "compose smoke passed (Compose model and offline fixture checks; live services not started)"; \
	fi

# The local live gate is deliberately separate from compose-smoke. It requires
# an explicit opt-in, starts a uniquely named Docker Compose project, and
# removes only the project resources, generated continuation key, temporary Go
# cache, and images it creates. It never sends a provider request: the Temporal
# recovery test injects a content-free adapter in the Go test process.
# The Compose wait timeout is five minutes: the Temporal and worker
# healthchecks allow up to 212s (20s start period plus 24 checks at 5s
# intervals with 3s timeouts), so a slow but healthy CI runner is not cut off.
compose-live-integration:
	@if [ "$${LLMTW_COMPOSE_LIVE:-0}" != "1" ]; then \
		echo "compose-live-integration requires LLMTW_COMPOSE_LIVE=1; it starts local Docker services and is intentionally opt-in" >&2; \
		exit 2; \
	fi
	@command -v "$(firstword $(COMPOSE))" >/dev/null 2>&1 || { \
		echo "compose-live-integration requires '$(firstword $(COMPOSE))'; install Docker Compose or set COMPOSE to an equivalent command" >&2; \
		exit 2; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "compose-live-integration requires a running Docker daemon" >&2; \
		exit 2; \
	}
	@set -e; \
	tmpdir="$$(mktemp -d "$${TMPDIR:-/tmp}/llmtw-compose-live.XXXXXX")"; \
	project="llmtw-compose-live-$$$$"; \
	worker_image="llmtw/worker:compose-live-$$$$"; \
	mock_image="llmtw/provider-mock:compose-live-$$$$"; \
	temporal_port=0; \
	temporal_ui_port=0; \
	redis_port=0; \
	redis_username="$${LLMTW_REDIS_USERNAME:-local}"; \
	redis_password="$${LLMTW_REDIS_PASSWORD:-local-only}"; \
	redis_key_prefix="$${LLMTW_REDIS_KEY_PREFIX:-llmtw}"; \
	postgres_password="$${LLMTW_POSTGRES_PASSWORD:-local-only}"; \
	mock_api_key=local-only; \
	continuation_hmac="$$(LC_ALL=C od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')"; \
	health_port=0; \
	metrics_port=0; \
	key="$$tmpdir/continuation-hmac"; \
	go_cache="$$tmpdir/gocache"; \
	cleanup() { \
		COMPOSE_PROJECT_NAME="$$project" LLMTW_CONTINUATION_KEY_FILE="$$key" LLMTW_WORKER_IMAGE="$$worker_image" LLMTW_PROVIDER_MOCK_IMAGE="$$mock_image" LLMTW_COMPOSE_TEMPORAL_PORT="$$temporal_port" LLMTW_COMPOSE_TEMPORAL_UI_PORT="$$temporal_ui_port" LLMTW_COMPOSE_REDIS_PORT="$$redis_port" LLMTW_REDIS_USERNAME="$$redis_username" LLMTW_REDIS_PASSWORD="$$redis_password" LLMTW_REDIS_KEY_PREFIX="$$redis_key_prefix" LLMTW_POSTGRES_PASSWORD="$$postgres_password" LLMTW_COMPOSE_HEALTH_PORT="$$health_port" LLMTW_COMPOSE_METRICS_PORT="$$metrics_port" $(COMPOSE) --profile worker down --volumes --remove-orphans --timeout 10 >/dev/null 2>&1 || true; \
		docker image rm -f "$$worker_image" "$$mock_image" >/dev/null 2>&1 || true; \
		rm -rf "$$tmpdir"; \
	}; \
	trap cleanup EXIT HUP INT TERM; \
	umask 077; \
	if [ "$${#continuation_hmac}" -ne 64 ]; then \
		echo "compose-live-integration could not generate a continuation HMAC" >&2; \
		exit 1; \
	fi; \
	printf '%s' "$$continuation_hmac" > "$$key"; \
	chmod 0444 "$$key"; \
	export COMPOSE_PROJECT_NAME="$$project" LLMTW_CONTINUATION_KEY_FILE="$$key" LLMTW_WORKER_IMAGE="$$worker_image" LLMTW_PROVIDER_MOCK_IMAGE="$$mock_image" LLMTW_COMPOSE_TEMPORAL_PORT="$$temporal_port" LLMTW_COMPOSE_TEMPORAL_UI_PORT="$$temporal_ui_port" LLMTW_COMPOSE_REDIS_PORT="$$redis_port" LLMTW_REDIS_USERNAME="$$redis_username" LLMTW_REDIS_PASSWORD="$$redis_password" LLMTW_REDIS_KEY_PREFIX="$$redis_key_prefix" LLMTW_POSTGRES_PASSWORD="$$postgres_password" LLMTW_COMPOSE_HEALTH_PORT="$$health_port" LLMTW_COMPOSE_METRICS_PORT="$$metrics_port"; \
	$(COMPOSE) --profile worker build --no-cache --quiet worker provider-mock; \
	if ! $(COMPOSE) --profile worker up --wait --wait-timeout 300; then \
		echo "compose-live-integration Temporal healthcheck output (redacted):" >&2; \
		temporal_container="$$( $(COMPOSE) ps -q temporal 2>/dev/null || true )"; \
		if [ -n "$$temporal_container" ]; then \
			docker inspect --format '{{range .State.Health.Log}}{{.Output}}{{end}}' "$$temporal_container" 2>&1 | \
				LLMTW_LOG_REDACT_REDIS_PASSWORD="$$redis_password" \
				LLMTW_LOG_REDACT_POSTGRES_PASSWORD="$$postgres_password" \
				LLMTW_LOG_REDACT_MOCK_API_KEY="$$mock_api_key" \
				LLMTW_LOG_REDACT_CONTINUATION_HMAC="$$continuation_hmac" \
				sh ./scripts/redact-compose-logs.sh >&2 || true; \
		fi; \
		echo "compose-live-integration service logs (redacted; service output only; no environment inspection):" >&2; \
		$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker 2>&1 | \
			LLMTW_LOG_REDACT_REDIS_PASSWORD="$$redis_password" \
			LLMTW_LOG_REDACT_POSTGRES_PASSWORD="$$postgres_password" \
			LLMTW_LOG_REDACT_MOCK_API_KEY="$$mock_api_key" \
			LLMTW_LOG_REDACT_CONTINUATION_HMAC="$$continuation_hmac" \
			sh ./scripts/redact-compose-logs.sh >&2 || true; \
		exit 1; \
	fi; \
	temporal_address="$$( $(COMPOSE) port temporal 7233 )"; \
	temporal_ui_address="$$( $(COMPOSE) port temporal 8233 )"; \
	redis_address="$$( $(COMPOSE) port redis 6379 )"; \
	health_address="$$( $(COMPOSE) port worker 8080 )"; \
	metrics_address="$$( $(COMPOSE) port worker 9090 )"; \
	for address in "$$temporal_address" "$$temporal_ui_address" "$$redis_address" "$$health_address" "$$metrics_address"; do \
		if [ -z "$$address" ]; then \
			echo "compose-live-integration could not discover a Docker-assigned host port" >&2; \
			exit 1; \
		fi; \
	done; \
	redis_container="$$( $(COMPOSE) ps -q redis )"; \
	if [ -z "$$redis_container" ]; then \
		echo "compose-live-integration could not find the isolated Redis container" >&2; \
		exit 1; \
	fi; \
	if ! GOCACHE="$$go_cache" LLMTW_COMPOSE_WORKER_HEALTH_ADDR="$$health_address" LLMTW_COMPOSE_WORKER_METRICS_ADDR="$$metrics_address" LLMTW_COMPOSE_REDIS_CONTAINER="$$redis_container" $(GO) test -count=1 -tags=composeliveintegration ./integration/compose -run '^TestComposeWorkerReadinessTracksRedis$$'; then \
		echo "compose-live-integration Redis lifecycle test service logs (redacted; service output only; no environment inspection):" >&2; \
		$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker 2>&1 | \
			LLMTW_LOG_REDACT_REDIS_PASSWORD="$$redis_password" \
			LLMTW_LOG_REDACT_POSTGRES_PASSWORD="$$postgres_password" \
			LLMTW_LOG_REDACT_MOCK_API_KEY="$$mock_api_key" \
			LLMTW_LOG_REDACT_CONTINUATION_HMAC="$$continuation_hmac" \
			sh ./scripts/redact-compose-logs.sh >&2 || true; \
		exit 1; \
	fi; \
	redis_address="$$( $(COMPOSE) port redis 6379 )"; \
	if [ -z "$$redis_address" ]; then \
		echo "compose-live-integration could not rediscover the Redis host port after lifecycle recovery" >&2; \
		exit 1; \
	fi; \
	if ! GOCACHE="$$go_cache" LLMTW_TEMPORAL_ADDRESS="$$temporal_address" LLMTW_REDIS_ADDR="$$redis_address" LLMTW_REDIS_USERNAME="$$redis_username" LLMTW_REDIS_PASSWORD="$$redis_password" LLMTW_REDIS_KEY_PREFIX="$$redis_key_prefix" $(GO) test -count=1 -tags=composeliveintegration ./integration/temporal -run '^(TestTemporalRecoveryWithSharedRedis|TestTemporalKeepaliveCompletesLongOneShotProviderCall)$$'; then \
		echo "compose-live-integration Temporal recovery test service logs (redacted; service output only; no environment inspection):" >&2; \
		$(COMPOSE) logs --no-color temporal postgres redis redis-function-provisioner blob-volume-provisioner provider-mock worker 2>&1 | \
			LLMTW_LOG_REDACT_REDIS_PASSWORD="$$redis_password" \
			LLMTW_LOG_REDACT_POSTGRES_PASSWORD="$$postgres_password" \
			LLMTW_LOG_REDACT_MOCK_API_KEY="$$mock_api_key" \
			LLMTW_LOG_REDACT_CONTINUATION_HMAC="$$continuation_hmac" \
			sh ./scripts/redact-compose-logs.sh >&2 || true; \
		exit 1; \
	fi

deployment-policy-verify:
	KUBECTL="$(KUBECTL)" ./deploy/verify.sh
	KUBECTL="$(KUBECTL)" $(GO) test ./integration/kubernetes

kustomize-verify:
	@command -v "$(KUBECTL)" >/dev/null 2>&1 || { \
		echo "kustomize-verify requires '$(KUBECTL)'; install kubectl or set KUBECTL to a pinned executable" >&2; \
		exit 2; \
	}
	KUBECTL="$(KUBECTL)" ./deploy/verify.sh
	KUBECTL="$(KUBECTL)" $(GO) test ./integration/kubernetes

adapter-contracts:
	$(GO) test -v ./llm/provider/contracttest ./llm/provider/...

# Exercises the route/continuation security contract without contacting a
# provider or external state service. Longer fuzzing remains opt-in through
# routing-fuzz so pull-request checks stay bounded and deterministic.
routing-contracts:
	$(GO) test -race ./routing ./state ./storage/memory -run '^Test(PlannerNeverMovesAcrossRequiredContinuationPin|ProviderCacheAffinityCannotBypassHealthEligibility|ContinuationSecurity|ContinuationConstraintsMarkOnlyRequiredProviderStateAsPinned|CheckPinningDropsOnlyOptionalStateForBestEffortPortableTranscript|CanonicalTranscript)'

routing-fuzz:
	$(GO) test ./routing -run '^$$' -fuzz '^FuzzPlannerNeverSelectsUnauthorizedClasses$$' -fuzztime "$${ROUTING_FUZZ_TIME:-60s}" -parallel=1
	$(GO) test ./state -run '^$$' -fuzz '^FuzzCanonicalTranscriptNeverPanics$$' -fuzztime "$${ROUTING_FUZZ_TIME:-60s}" -parallel=1

security-pr-verify:
	@test -n "$(SECURITY_BASE_DIR)" || { echo "SECURITY_BASE_DIR is required" >&2; exit 2; }
	@workspace="$$(mktemp -d)"; \
	cleanup() { rm -rf "$$workspace"; }; \
	trap cleanup EXIT HUP INT TERM; \
	status=0; \
	go_mod_status=pass; \
	vulnerability_status=pass; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) build -o "$$workspace/govulncheck" golang.org/x/vuln/cmd/govulncheck || { status=1; vulnerability_status=fail; }; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) mod edit -json >"$$workspace/go-mod.json" 2>&1 || { status=1; go_mod_status=fail; }; \
	if [ "$$vulnerability_status" = pass ]; then \
		(cd "$(SECURITY_BASE_DIR)" && GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) "$$workspace/govulncheck" -format json ./...) >"$$workspace/govulncheck-base.json" 2>"$$workspace/govulncheck-base.stderr" || { status=1; vulnerability_status=fail; }; \
		GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) "$$workspace/govulncheck" -format json ./... >"$$workspace/govulncheck-head.json" 2>"$$workspace/govulncheck-head.stderr" || { status=1; vulnerability_status=fail; }; \
	fi; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) run ./tools/supplychainverify -baseline tools/supplychainverify/baseline.json -go-mod "$$workspace/go-mod.json" -base-vulnerability-output "$$workspace/govulncheck-base.json" -vulnerability-output "$$workspace/govulncheck-head.json" -report "$$workspace/security-verify.json" -go-mod-status "$$go_mod_status" -vulnerability-status "$$vulnerability_status" || status=1; \
	exit "$$status"

security-verify:
	@workspace="$$(mktemp -d)"; \
	cleanup() { rm -rf "$$workspace"; }; \
	trap cleanup EXIT HUP INT TERM; \
	status=0; \
	test_status=pass; \
	source_status=pass; \
	go_mod_status=pass; \
	vulnerability_status=pass; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) test -json ./... >"$$workspace/test-output.json" 2>&1 || { status=1; test_status=fail; }; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) run ./tools/sourceverify -test-output "$$workspace/test-output.json" || { status=1; source_status=fail; }; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) mod edit -json >"$$workspace/go-mod.json" 2>&1 || { status=1; go_mod_status=fail; }; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) tool govulncheck -format json ./... >"$$workspace/govulncheck.json" 2>"$$workspace/govulncheck.stderr" || { status=1; vulnerability_status=fail; }; \
	GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN) $(GO) run ./tools/supplychainverify -baseline tools/supplychainverify/baseline.json -go-mod "$$workspace/go-mod.json" -vulnerability-output "$$workspace/govulncheck.json" -report "$$workspace/security-verify.json" -test-status "$$test_status" -source-status "$$source_status" -go-mod-status "$$go_mod_status" -vulnerability-status "$$vulnerability_status" || status=1; \
	if [ -n "$${SECURITY_REPORT:-}" ] && [ -f "$$workspace/security-verify.json" ]; then \
		mkdir -p "$$(dirname "$$SECURITY_REPORT")"; \
		cp "$$workspace/security-verify.json" "$$SECURITY_REPORT"; \
	fi; \
	exit "$$status"

# Replays every checked-in fuzz seed without nondeterministic mutation. The
# longer, bounded fuzz shards run only in the trusted master workflow.
fuzz-smoke:
	bash scripts/run-fuzz.sh smoke

# Compiles reviewed semantic mutants through Go overlays; it never changes the
# checked-out source tree.
mutation-verify:
	bash scripts/run-mutation.sh

# Validates a previously collected local evidence bundle only. It cannot sign,
# publish, push an image, or contact an LLM provider.
release-verify:
	bash ../scripts/release/verify.sh --artifact-dir "release-artifacts" --evidence "release-artifacts/evidence.json"

verify: fmt-check schema-verify docs-verify vet test build redis-benchmark-compile
