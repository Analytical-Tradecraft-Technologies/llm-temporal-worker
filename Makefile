# Repository entry point for the Go Temporal worker module.
# The implementation and its build assets live under golang/ so this repository
# can host additional clients without coupling their build roots.
.PHONY: help release-verify
help:
	@echo "Go worker targets are available through: make -C golang <target>"

release-verify:
	bash scripts/release/verify.sh --artifact-dir "release-artifacts" --evidence "release-artifacts/evidence.json"

%:
	$(MAKE) -C golang $@

# Explicit paid local gate. The dotenv contents never become Make variables.
OPENAI_SMOKE_ENV_FILE ?= $(CURDIR)/.env
.PHONY: openai-smoke
openai-smoke:
	python3 scripts/openai-smoke.py --env-file "$(OPENAI_SMOKE_ENV_FILE)"

.PHONY: openai-smoke-check
openai-smoke-check:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_openai_smoke.py'
	$(MAKE) -C golang openai-smoke-compile
