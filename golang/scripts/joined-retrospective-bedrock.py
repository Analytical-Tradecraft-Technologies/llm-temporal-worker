#!/usr/bin/env python3
"""Run one LOCAL archive-backed historical ATT evaluation; never publish.

Requires Python 3 + PyYAML, openssl, and a local Docker/Podman Compose runtime.
All required paths are explicit: see --help. --validate-only is offline and
never reads the credential contents, creates output, or starts containers.
A new output directory is mandatory; an interrupted pilot cannot be resumed
or reissued by this launcher. Corpus coverage and model eligibility are bounded.
"""

import argparse
import configparser
import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

import yaml

from historical_research_trace import TraceInvalid, verify_research_trace


ROOT = Path(__file__).resolve().parents[1]
METHODS = {"direct": "direct_categorical_v1", "fixed": "fixed_ordinal_summary_stage2_v1", "irt": "ordinal_irt_stage2_v1"}
TOTAL_MICRO_USD = 5_000_000
MAX_ARM_MICRO_USD = 1_500_000
QUESTION_FIELDS = {"question_id", "source", "source_id", "historical_forecast_date", "question", "resolution_criteria", "background", "url", "source_intro", "market_info_open_datetime", "market_info_close_datetime", "market_info_resolution_criteria"}
NOTICE = "LOCAL bounded-corpus historical ATT evaluation, not licensed BTF or an official submission. Publisher metadata is not a cryptographic timestamp authority; pre-outcome model eligibility does not prove absence of contamination or competitive advantage. No publication."
RELEASE_FILES = {"release-attestation.json": "release_attestation", "release-trust-root.json": "release_trust_root", "resource-capacity.json": "resource_capacity"}
FROZEN_INPUTS = ("input", "config", "capabilities", "prices", "direct_profile", "fixed_profile", "irt_profile", "historical_corpus", "question_proof", "question_projection", "model_evidence", *RELEASE_FILES.values())
SOURCE_FIELDS = {"question", "resolution_criteria", "background", "url", "source_intro", "market_info_open_datetime", "market_info_close_datetime", "market_info_resolution_criteria"}
CUTOFF = "2025-12-07T00:00:00Z"
MODEL_ID = "amazon.nova-2-lite-v1:0"
ARCHIVE_MODE = "immutable_archive_snapshot"
RELEASE_IDENTITY_ENV = {"worker_revision": "REVISION", "image_digest": "IMAGE_DIGEST", "release_version": "RELEASE_VERSION", "source_sha256": "SOURCE_SHA256", "config_sha256": "CONFIG_SHA256", "environment_sha256": "ENVIRONMENT_SHA256"}


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate document key")
        result[key] = value
    return result


class StrictYAML(yaml.SafeLoader):
    pass


def yaml_mapping(loader, node):
    loader.flatten_mapping(node)
    return unique_pairs((loader.construct_object(key), loader.construct_object(value)) for key, value in node.value)


StrictYAML.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, yaml_mapping)


def load_json(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs, parse_constant=lambda _: (_ for _ in ()).throw(Invalid("nonfinite JSON number")))


def load_yaml(path):
    return yaml.load(path.read_text(), Loader=StrictYAML)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def seconds(value):
    require(isinstance(value, str), "duration must use Go duration string syntax")
    parts = re.findall(r"([0-9]+(?:\.[0-9]+)?)(h|m|s)", value)
    require(parts and "".join(number + unit for number, unit in parts) == value, "unsupported duration")
    return sum(float(number) * {"h": 3600, "m": 60, "s": 1}[unit] for number, unit in parts)


def positive_int(value, maximum, name):
    require(type(value) is int and 0 < value <= maximum, name + " is missing, noninteger, or exceeds its allowance")
    return value


def utc_time(value):
    require(isinstance(value, str), "timestamp must be explicit UTC text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0, "timestamp must be timezone-aware UTC")
    return parsed


def confined_file(root, relative):
    require(isinstance(relative, str) and relative and not Path(relative).is_absolute(), "provenance body path must be relative")
    path = root / relative
    require(".." not in Path(relative).parts and all(not parent.is_symlink() for parent in [path, *path.parents] if parent != root.parent), "provenance body path cannot traverse symlinks")
    require(path.resolve().is_relative_to(root.resolve()) and path.is_file(), "provenance body escapes its manifest directory or is missing")
    return path


def validate_historical_inputs(args, question):
    require(question["question_id"] == "infer:1554" and question["source"] == "infer" and question["source_id"] == "1554" and question["historical_forecast_date"] == "2025-12-07", "only the frozen infer:1554 question and original cutoff are authorized")
    proof, projection = load_json(args.question_proof), load_json(args.question_projection)
    require(proof.get("schema_version") == "att_historical_question_version_proof/v1" and proof.get("question_id") == "infer:1554", "question proof identity is invalid")
    require(proof.get("source_repository") == "forecastingresearch/forecastbench-datasets" and proof.get("source_commit") == "66801b37101fc8684b5c13fbaa2862089f30f721" and proof.get("source_file") == "datasets/question_sets/2025-11-23-llm.json", "question proof is not the admitted publisher revision")
    require(proof.get("source_commit_publication_time") == "2025-11-23T00:05:00Z" and proof.get("cutoff_utc") == CUTOFF and proof.get("cutoff_changed") is False, "question proof availability/cutoff changed")
    require(proof.get("source_url") == "https://raw.githubusercontent.com/forecastingresearch/forecastbench-datasets/66801b37101fc8684b5c13fbaa2862089f30f721/datasets/question_sets/2025-11-23-llm.json" and utc_time(CUTOFF) < utc_time(proof["retrieved_at_utc"]) <= datetime.now(timezone.utc), "question proof publisher location or actual retrieval time differs")
    require(proof.get("source_file_sha256") == "784ac0f8c7b0a87270f0302b8bee5680a0bce9b1cc041c3d6f5d7ab58cc84960", "publisher source bytes differ from the reviewed proof")
    require(proof.get("answer_free_projection_sha256") == digest(args.question_projection) == "bc354a8bc55e53d187bfc4ce0d63dd6a0ea81bd8f4ab6ac0857b30818e780ad4", "question source projection bytes differ from the verified publisher projection")
    require(set(projection) == SOURCE_FIELDS == set(proof.get("fields_compared", [])) and projection == {key: question[key] for key in SOURCE_FIELDS}, "all eight source fields must exactly match the pre-cutoff publisher projection")
    require(proof.get("all_model_input_source_fields_identical") is True and proof.get("outcomes_and_crowd_in_projection") is False and proof.get("availability_basis"), "question provenance is incomplete or outcome-bearing")
    model = load_json(args.model_evidence)
    require(model.get("schema_version") == "att_historical_model_evidence/v1" and model.get("model_id") == MODEL_ID and model.get("launch_date") == "2025-12-02" and model.get("knowledge_cutoff") == "2025-10", "fixed pre-outcome Nova 2 Lite identity/eligibility evidence is required")
    source_url = urlsplit(model.get("source_url", ""))
    require(source_url.geturl() == "https://docs.aws.amazon.com/bedrock/latest/userguide/model-card-amazon-nova-2-lite.html", "model eligibility must cite the exact reviewed official AWS model card")
    source = confined_file(args.model_evidence.parent, model.get("source_path"))
    require(0 < source.stat().st_size <= 16 << 20 and digest(source) == model.get("source_sha256") == "90415bdecd940a954af85fe85d5b433a10a3fe482a661e0d6997cd821fa971c6", "retained model-card bytes differ from the reviewed pre-outcome model evidence")
    require(utc_time(CUTOFF) < utc_time(model["retrieved_at_utc"]) <= datetime.now(timezone.utc), "model-card retrieval must record actual current operations, not a backdated/future capture")
    args.model_source = source
    args.model_source_relative = model["source_path"]
    args.frozen_sha256[str(source)] = digest(source)
    corpus = load_json(args.historical_corpus)
    require(corpus.get("schema_version") == "att_historical_corpus/v1" and corpus.get("source_cutoff_utc") == CUTOFF and corpus.get("selection_policy") and corpus.get("documents"), "a real, explicitly selected pre-cutoff archive corpus is required")
    require(utc_time(CUTOFF) < utc_time(corpus["created_at_utc"]) <= datetime.now(timezone.utc), "corpus construction must retain honest operational time")
    require(args.victoria_validator.is_file() and os.access(args.victoria_validator, os.X_OK), "provide a prebuilt Victoria competition-worker validator; validation never builds")
    require(1 <= seconds(args.request_timeout) <= 60, "archive request timeout must be between 1s and 60s")
    # This pure Go command verifies corpus bytes and uses the runtime's exact
    # binding helper. Never duplicate Go policy serialization/canonical hashes.
    environment = {"PATH": os.environ.get("PATH", ""), "HOME": str(args.victoria_root)}
    result = subprocess.run([str(args.victoria_validator), "retrospective-forecast-eval", "--historical-corpus", str(args.historical_corpus), "--historical-corpus-sha256", digest(args.historical_corpus), "--request-timeout", args.request_timeout, "--archive-proxy-url", "http://archive-egress:8080", "--print-collection-binding"], env=environment, capture_output=True, text=True, timeout=180)
    require(result.returncode == 0, "Victoria offline corpus/binding validation refused admission")
    args.collection_binding = json.loads(result.stdout, object_pairs_hook=unique_pairs)
    require(args.collection_binding.get("mode") == ARCHIVE_MODE and args.collection_binding.get("search_provider") == "historical_corpus_v1" and args.collection_binding.get("corpus_sha256") == digest(args.historical_corpus), "Victoria returned an unexpected historical collection binding")
    args.corpus = corpus
    args.corpus_bodies = {}
    for document in corpus["documents"]:
        body = confined_file(args.historical_corpus.parent, document["body_path"])
        require(digest(body) == document["body_sha256"], "corpus body changed after canonical validation")
        args.corpus_bodies[document["body_path"]] = body
        args.frozen_sha256[str(body)] = digest(body)


def validate_release(args):
    attestation = load_json(args.release_attestation)
    root = load_json(args.release_trust_root)
    require(isinstance(attestation, dict) and set(attestation) == {"schema_version", "key_id", "identity", "signature"} and attestation["schema_version"] == "competition_worker_release_attestation/v1", "release attestation shape/schema is invalid")
    require(isinstance(root, dict) and set(root) == {"schema_version", "keys"} and root["schema_version"] == "competition_worker_release_trust_root/v1", "release trust root shape/schema is invalid")
    identity = attestation["identity"]
    require(isinstance(identity, dict) and set(identity) == set(RELEASE_IDENTITY_ENV) and all(isinstance(value, str) for value in identity.values()), "release identity must contain exactly the six canonical string fields")
    require(bool(re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", identity["worker_revision"])), "release worker revision is invalid")
    require(bool(re.fullmatch(r"sha256:[0-9a-f]{64}", identity["image_digest"])), "release image digest is invalid")
    for field in ("source_sha256", "config_sha256", "environment_sha256"):
        require(bool(re.fullmatch(r"[0-9a-f]{64}", identity[field])), "release identity hash is invalid: " + field)
    identifier = r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"
    require(bool(re.fullmatch(r"[1-9][0-9]*", identity["release_version"])), "release version must match the runtime image's positive integer version")
    require(isinstance(attestation["key_id"], str) and bool(re.fullmatch(identifier, attestation["key_id"])), "release attestation key ID is invalid")
    require(isinstance(attestation["signature"], str) and bool(re.fullmatch(r"[A-Za-z0-9+/]{86}==", attestation["signature"])), "release signature encoding is invalid")
    require(isinstance(root["keys"], list) and 0 < len(root["keys"]) <= 32, "release trust root must contain 1-32 keys")
    keys = {}
    for key in root["keys"]:
        require(isinstance(key, dict) and set(key) == {"key_id", "public_key"}, "release trust key shape is invalid")
        require(isinstance(key["key_id"], str) and bool(re.fullmatch(identifier, key["key_id"])) and key["key_id"] not in keys, "release trust key ID is invalid or repeated")
        require(isinstance(key["public_key"], str) and bool(re.fullmatch(r"[A-Za-z0-9+/]{43}=", key["public_key"])), "release public key encoding is invalid")
        keys[key["key_id"]] = key["public_key"]
    fixture_keys = load_json(ROOT / "integration/joined/release-trust-root.json")["keys"]
    fixture_public_keys = {key["public_key"] for key in fixture_keys}
    require(attestation["key_id"] in keys and keys[attestation["key_id"]] not in fixture_public_keys, "release attestation requires a task-local signing key, not any alias of a public fixture key")
    args.release_key_id = attestation["key_id"]
    # Structural checks only: canonical encoding and Ed25519 verification remain
    # mandatory in the real Go worker before any inference can be dispatched.
    args.release_identity = identity


def validate_config(config, profiles, args):
    require(config.get("environment") == "development", "local development worker required")
    require(config["state"]["kind"] == "durable", "durable PostgreSQL/Redis budget state is mandatory")
    require(config["state"]["redis"]["addresses"] == ["redis:6379"] and config["state"]["redis"]["key_prefix"] == "joined_smoke", "use task-owned joined Redis only")
    pg = config["state"]["postgres"]
    require(pg["addresses"] == ["worker-postgres:5432"] and pg["database"] == "llmtw_worker" and pg["schema"] == "llmtw_state" and pg["table_prefix"] == "llmtw_", "use task-owned joined PostgreSQL only")
    require(config["blob_store"]["kind"] == "file" and config["blob_store"]["file"]["root"] == "/var/lib/llmtw/blobs", "LLM artifacts must use local file storage")
    require(config["temporal"]["target"] == "joined-smoke:7243" and config["temporal"]["namespace"] == "default" and config["temporal"]["task_queue"] == "llm-inference", "use local joined Temporal only")
    require(config["temporal"]["worker"]["max_concurrent_activities"] == 1, "pilot requires one LLM activity at a time")
    require(config["limits"]["route_attempts"] == 1, "provider route retries are prohibited")
    require(config["resource_capacity"]["limits"]["provider_call_maximum_attempts"] == 1, "provider activity retries are prohibited")
    require(config["resource_capacity"]["trust_root_file"] == "/run/release-trust-root.json" and config["resource_capacity"]["trust_root_sha256"] == args.frozen_sha256[str(args.release_trust_root)], "capacity trust root path/hash does not match the supplied release trust root")
    require(config["continuation"]["allow_provider_hosted_state"] is False and config["continuation"]["retain_canonical_transcript"] is True, "provider-hosted state must be disabled; retain local transcript")
    require(config["telemetry"]["content_logging"] == "disabled" and config["telemetry"]["tracing"]["enabled"] is False, "disable content logging and external tracing")
    host = "bedrock-runtime." + args.aws_region + ".amazonaws.com"
    endpoints = config["endpoints"]
    require(bool(endpoints), "at least one approved Bedrock endpoint is required")
    for endpoint in endpoints.values():
        url = urlsplit(endpoint["base_url"])
        require(endpoint["family"] in {"bedrock_converse", "bedrock_anthropic_messages"}, "only existing Bedrock adapters are allowed")
        require(url.scheme == "https" and url.hostname == host and url.port in {None, 443} and url.path in {"", "/"} and not url.query and not url.fragment and not url.username and not url.password, "endpoint must be the approved regional public Bedrock HTTPS origin")
        require(endpoint["outbound_hosts"] == [host] and endpoint["region"] == args.aws_region and endpoint["account_region"] == args.aws_region, "Bedrock outbound host/region must agree with approval")
        require(endpoint["auth"]["kind"] == "aws_default_chain" and not any(endpoint["auth"].get(key) for key in ("name", "path", "audience")), "Bedrock must use aws_default_chain, never an API key")
        require(endpoint["provider_storage"]["permitted"] is False, "provider storage must be disabled")
    budgets = config["budgets"]
    require(budgets["require_match"] is True and len(budgets["policies"]) == 1, "one mandatory aggregate budget policy is required")
    policy = budgets["policies"][0]
    match = {key: value for key, value in policy["match"].items() if value not in (None, "")}
    require(match == {"tenant": "att", "project": "competition", "environment": "development"}, "aggregate budget must match every pilot request, not an arm/model/endpoint subset")
    require(len(policy["windows"]) == 1, "one durable aggregate budget window is required")
    window = policy["windows"][0]
    usd = Decimal(str(window.get("limit_usd", 0)))
    micro = window.get("limit_micro_usd", 0)
    require(type(micro) is int and micro in {0, TOTAL_MICRO_USD} and usd in {Decimal(0), Decimal(5)} and (usd == 5 or micro == TOTAL_MICRO_USD), "durable aggregate allowance must be exactly $5 total")
    duration, bucket = seconds(window["duration"]), seconds(window["bucket"])
    require(duration == 86400 and bucket == 300, "pilot requires the approved 24h window with 5m buckets")
    require(args.run_timeout_seconds + 600 < duration - bucket, "runtime could outlive the aggregate budget window")
    require(config["pricing"]["require_price_when_budgeted"] is True, "all billable routes must have prices")
    for field, path in (("capabilities", args.capabilities), ("pricing", args.prices)):
        catalogs = config[field]["catalogs"]
        target = "/etc/llmtw/" + ("capabilities.yaml" if field == "capabilities" else "prices.yaml")
        require(len(catalogs) == 1 and catalogs[0]["file"] == target and catalogs[0]["sha256"].lower() == digest(path), "catalog path/hash does not match supplied frozen file")
    for arm, profile in profiles.items():
        inference = profile["inference"]
        context = inference["context"]
        require(context["tenant"] == "att" and context["project"] == "competition", "arm is outside the aggregate budget scope")
        settings = inference["settings"]
        model = config["models"].get(settings["model"])
        require(model is not None and "att" in model["allowed_tenants"] and len(model["routes"]) == 1, "each arm must select one approved, non-fallback route")
        route = model["routes"][0]
        require(route["endpoint"] in endpoints and route["model"] in {MODEL_ID, "us." + MODEL_ID, "eu." + MODEL_ID, "global." + MODEL_ID}, "only the fixed pre-outcome Nova 2 Lite revision or its system routing profile is admitted")
        require(profile["core_provider_route_id"] == route["id"] and all(item == route["id"] for item in profile["rater_provider_route_ids"]), "profile routes must match the approved model route")
        for frozen in [profile["run_profile"]["core_model"], *profile["run_profile"]["rater_models"]]:
            require(frozen["model"] == settings["model"] and frozen["resolved_revision"] == route["model"], "frozen model identity must match worker route")
        require(profile["resource_capacity"]["provider_call_maximum_attempts"] == 1, "arm enables provider activity retry")
        require(profile["run_profile"]["code_commit"] == args.release_identity["worker_revision"] and profile["run_profile"]["container_digest"] == args.release_identity["image_digest"], "frozen profile revision/image does not match the supplied release identity")
        capacity = profile["resource_capacity"]
        require(capacity["manifest_sha256"] == config["resource_capacity"]["manifest_sha256"] and capacity["artifact"]["sha256"] == capacity["manifest_sha256"], "profile capacity manifest does not match the configured signed manifest")
        require(capacity["manifest_sha256"] == args.frozen_sha256[str(args.resource_capacity)], "capacity bytes differ from the approved task-local signed manifest")
        positive_int(capacity["search_fetch_max_inflight"], 64, "signed archive search/fetch concurrency")
    require(len({config["models"][profile["inference"]["settings"]["model"]]["routes"][0]["model"] for profile in profiles.values()}) == 1, "all methods must use identical frozen model routing")


def validate(args):
    for name in FROZEN_INPUTS:
        path = getattr(args, name)
        require(path.is_file() and path.stat().st_size > 0, name + " must be a nonempty regular file")
        if name in RELEASE_FILES.values():
            require(path.stat().st_size <= 16 << 10, name + " exceeds the canonical Go release file size bound")
    args.frozen_sha256 = {str(getattr(args, name)): digest(getattr(args, name)) for name in FROZEN_INPUTS}
    validate_release(args)
    require(args.victoria_root.joinpath("research/ai_ach/go.mod").is_file(), "Victoria root is invalid")
    credential_stat = args.aws_shared_credentials_file.lstat()
    require(stat.S_ISREG(credential_stat.st_mode) and credential_stat.st_uid == os.getuid() and not credential_stat.st_mode & 0o077, "credentials must be a caller-owned private regular file (0600 or 0400), not a symlink")
    require(bool(re.fullmatch(r"[A-Za-z0-9_-]+", args.aws_profile)), "invalid AWS profile name")
    require(bool(re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]*", args.aws_region)), "AWS commercial region is required")
    require(bool(re.fullmatch(r"[0-9]{12}", args.aws_account_id)), "approved AWS account ID must contain twelve digits")
    require(not args.output_root.exists(), "output directory already exists; never resume or overwrite a pilot")
    require(0 < args.run_timeout_seconds <= 21600, "run timeout must be between 1s and 6h")
    inputs = load_json(args.input)
    require(set(inputs) == {"schema_version", "questions"} and inputs["schema_version"] == "att_retrospective_screen_inputs/v1", "answer-free input envelope is invalid")
    require(len(inputs["questions"]) == 1, "provide Main's frozen one-question projection; automatic sampling/expansion is prohibited")
    question = inputs["questions"][0]
    require(set(question) == QUESTION_FIELDS and all(isinstance(value, str) for value in question.values()), "input must contain only the contracted answer-free string fields")
    require(all(question[field].strip() for field in ("question_id", "question", "historical_forecast_date", "url")), "question identity/text/date/url is required")
    datetime.strptime(question["historical_forecast_date"], "%Y-%m-%d")
    validate_historical_inputs(args, question)
    profiles = {}
    for arm, method in METHODS.items():
        profile = load_json(getattr(args, arm + "_profile"))
        profiles[arm] = profile
        require(profile["schema_version"] == "ai_ach.retrospective_eval_profile/v1", "wrong retrospective profile schema")
        run = profile["run_profile"]
        require(run["estimator_policy"]["categorical_method"] == method, "profile method does not match " + arm)
        cap = positive_int(profile["max_cost_microunits"], MAX_ARM_MICRO_USD, arm + " reservation")
        require(run["limits"]["max_cost_microunits"] == cap, "run profile and arm reservation disagree")
        positive_int(profile["inference"]["budget"]["max_cost_microunits"], cap, "inference budget")
        positive_int(profile["max_runtime_seconds"], args.run_timeout_seconds, "arm runtime")
        require(profile["inference"]["settings"].get("tools", []) == [], "hosted tools/search are prohibited")
        require(profile["inference"]["settings"]["tool_policy"]["mode"] == "none", "provider tools must be disabled")
        collection = run["collection"]
        binding = {"mode": collection.get("evidence_mode"), **{key: collection.get(key) for key in ("search_provider", "search_config_sha256", "fetch_config_sha256", "corpus_sha256")}}
        require(binding == args.collection_binding, "frozen profile differs from the actual archive provider/fetcher/corpus binding")
        queries = positive_int(collection["max_queries"], 32, "archive search queries")
        pages = positive_int(collection["max_fetched_pages"], 64, "archive fetched pages")
        passes = positive_int(collection["max_initial_remote_evidence_passes"], 1, "initial archive research passes")
        results = positive_int(collection["max_results_per_query"], 20, "archive results per query")
        positive_int(collection["max_page_bytes"], 8 << 20, "archive page bytes")
        require(passes <= queries and pages <= queries * results, "archive pass/query/page allowances are incoherent")
        require(profile["limits"]["max_search_queries"] == queries and profile["limits"]["max_fetched_pages"] == pages, "outer search/page allowances differ from the frozen collection")
        for settings in (profile["limits"], run["limits"]):
            require(settings["max_initial_remote_evidence_passes"] == passes, "outer/nested initial research pass limits disagree")
            require(type(settings["max_priority_augmentation_turns"]) is int and 1 <= settings["max_priority_augmentation_turns"] <= 8, "priority augmentation requires 1-8 bounded turns")
        require(profile["limits"]["max_priority_augmentation_turns"] == run["limits"]["max_priority_augmentation_turns"], "outer/nested priority research allowances differ")
    require(all(profile["run_profile"]["collection"] == profiles["direct"]["run_profile"]["collection"] for profile in profiles.values()), "all methods must share the identical frozen corpus, policy, and research allowances")
    require(all(profile["resource_capacity"] == profiles["direct"]["resource_capacity"] for profile in profiles.values()), "all methods must use identical signed capacity")
    build_times = {utc_time(profile["run_profile"]["created_at"]) for profile in profiles.values()}
    require(len(build_times) == 1, "all methods must share the same runtime build creation time")
    build_time = next(iter(build_times))
    require(build_time.microsecond == 0 and build_time.timestamp() >= 1, "runtime build creation time must use whole positive UTC seconds")
    signed_capacity = load_json(args.resource_capacity)
    require(signed_capacity.get("schema_version") == "competition_resource_capacity_manifest/v1", "signed task-local capacity schema is invalid")
    fixture_capacity = load_json(ROOT / "integration/joined/resource-capacity.json")
    require(signed_capacity.get("key_id") == args.release_key_id, "capacity requires the same task-local release signer, not a joined fixture signer")
    require(signed_capacity["manifest"]["generation_id"] != fixture_capacity["manifest"]["generation_id"], "capacity requires a distinct task-local generation, not the checked-in smoke generation")
    require(digest(args.resource_capacity) != digest(ROOT / "integration/joined/resource-capacity.json"), "checked-in smoke capacity is not task-local approval")
    for key, value in signed_capacity["manifest"]["limits"].items():
        require(profiles["direct"]["resource_capacity"].get(key) == value, "frozen arm capacity differs from signed limits: " + key)
    require(sum(profile["max_cost_microunits"] for profile in profiles.values()) <= TOTAL_MICRO_USD, "arm reservations exceed $5 total")
    require(sum(profile["max_runtime_seconds"] for profile in profiles.values()) + 600 <= args.run_timeout_seconds, "run timeout must include all serial arm runtimes and 600s cleanup reserve")
    # The existing joined bootstrap coverage is deliberately finite; never run
    # past it or silently regenerate an allowance with a new coverage epoch.
    require(datetime.now(timezone.utc).timestamp() + args.run_timeout_seconds < datetime(2027, 1, 1, tzinfo=timezone.utc).timestamp(), "joined durable bootstrap coverage expires before this pilot can finish")
    config = load_yaml(args.config)
    validate_config(config, profiles, args)
    load_yaml(args.capabilities)
    load_yaml(args.prices)
    require(all(digest(Path(path)) == sha for path, sha in args.frozen_sha256.items()), "input/config/profile changed during validation")
    return inputs, profiles


def bind(source, target, read_only=True):
    return {"type": "bind", "source": str(source), "target": target, "read_only": read_only, "bind": {"selinux": "z"}}


def interpolate(value, environment):
    if isinstance(value, dict):
        return {key: interpolate(item, environment) for key, item in value.items()}
    if isinstance(value, list):
        return [interpolate(item, environment) for item in value]
    if not isinstance(value, str):
        return value
    def replace(match):
        text = match.group(1)
        key = re.split(r":[-?]", text, maxsplit=1)[0]
        if key in environment:
            return environment[key]
        if ":-" in text:
            return text.split(":-", 1)[1]
        raise Invalid("missing local composition variable: " + key)
    return re.sub(r"(?<!\$)\$\{([^}]+)\}", replace, value)


def merge_service(base, override):
    value = copy.deepcopy(base)
    for key, item in override.items():
        if key == "build" and isinstance(item, dict):
            build = value.setdefault(key, {})
            for field, setting in item.items():
                if field == "args" and isinstance(setting, dict):
                    build.setdefault(field, {}).update(copy.deepcopy(setting))
                else:
                    build[field] = copy.deepcopy(setting)
        elif key == "environment" and isinstance(item, dict):
            value.setdefault(key, {}).update(copy.deepcopy(item))
        elif key == "volumes":
            mounts = {mount["target"] if isinstance(mount, dict) else mount.split(":")[1]: mount for mount in value.get("volumes", [])}
            mounts.update({mount["target"] if isinstance(mount, dict) else mount.split(":")[1]: copy.deepcopy(mount) for mount in item})
            value[key] = list(mounts.values())
        else:
            value[key] = copy.deepcopy(item)
    return value


class Pilot:
    def __init__(self, args, inputs, profiles):
        self.args, self.inputs, self.profiles = args, inputs, profiles
        self.project = "att-bedrock-" + secrets.token_hex(8)
        self.private = None
        self.output = args.output_root
        self.services = {}
        self.engine_name = ""
        self.started = False
        self.ledger_ready = False
        self.engine_started = False
        self.deadline = None
        self.environment = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "XDG_RUNTIME_DIR", "DOCKER_HOST", "CONTAINER_HOST", "DOCKER_CONFIG"}}
        for key in ("DOCKER_HOST", "CONTAINER_HOST"):
            require(not self.environment.get(key) or self.environment[key].startswith("unix://"), "only a local Unix-socket container engine is allowed")
        self.private = Path(tempfile.mkdtemp(prefix=self.project + "-"))
        self.command = ["docker", "compose", "-p", self.project, "-f", str(self.private / "compose.json")]

    def run(self, command, timeout=120, capture=False, check=True, stdout=None):
        return subprocess.run(command, env=self.environment, cwd=ROOT, timeout=timeout, check=check, text=True, stdout=subprocess.PIPE if capture else stdout, stderr=subprocess.PIPE if capture else None)

    def compose(self, *args, **kwargs):
        return self.run(self.command + list(args), **kwargs)

    def prepare(self):
        self.output.mkdir(mode=0o700)
        for name in ("smoke", "fake-secrets", "provider", "inputs", "corpus", "model-evidence"):
            (self.private / name).mkdir(mode=0o755)
            # mkdir's mode is filtered by the caller's umask. These directories
            # are bind roots read by UID 65532; their host parent stays 0700.
            (self.private / name).chmod(0o755)
        # The engine owns creation of each arm root and refuses existing roots.
        (self.output / "arms").mkdir(mode=0o755)
        (self.output / "arms").chmod(0o755)
        (self.output / "plumbing").mkdir(mode=0o755)
        frozen = {"input.json": self.args.input, "config.yaml": self.args.config, "capabilities.yaml": self.args.capabilities, "prices.yaml": self.args.prices}
        frozen.update({"question-proof.json": self.args.question_proof, "question-projection.json": self.args.question_projection, "model-evidence.json": self.args.model_evidence})
        frozen.update({arm + ".json": getattr(self.args, arm + "_profile") for arm in METHODS})
        frozen.update({name: getattr(self.args, argument) for name, argument in RELEASE_FILES.items()})
        for name, source in frozen.items():
            shutil.copyfile(source, self.private / "inputs" / name)
            (self.private / "inputs" / name).chmod(0o444)
            require(digest(self.private / "inputs" / name) == self.args.frozen_sha256[str(source)], "approved input changed before freezing")
        shutil.copytree(self.private / "inputs", self.output / "frozen")
        shutil.copyfile(self.args.historical_corpus, self.private / "corpus/manifest.json")
        require(digest(self.private / "corpus/manifest.json") == self.args.collection_binding["corpus_sha256"], "corpus changed before freezing")
        for relative, source in self.args.corpus_bodies.items():
            destination = self.private / "corpus" / relative
            require(destination != self.private / "corpus/manifest.json", "body path conflicts with the mounted manifest")
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            for parent in destination.parents:
                if parent == self.private / "corpus":
                    break
                parent.chmod(0o755)
            shutil.copyfile(source, destination)
            destination.chmod(0o444)
            require(digest(destination) == self.args.frozen_sha256[str(source)], "archive body changed before freezing")
        (self.private / "corpus/manifest.json").chmod(0o444)
        shutil.copytree(self.private / "corpus", self.output / "frozen/corpus")
        model_source = self.private / "model-evidence" / self.args.model_source_relative
        model_source.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.args.model_source, model_source)
        require(digest(model_source) == self.args.frozen_sha256[str(self.args.model_source)], "model-card evidence changed before freezing")
        shutil.copyfile(self.private / "inputs/model-evidence.json", self.private / "model-evidence/model-evidence.json")
        shutil.copytree(self.private / "model-evidence", self.output / "frozen/model-evidence")
        # Only this opaque temporary copy is mounted into the provider worker.
        # It is not exposed to Compose interpolation, build args, logs, or ATT.
        credential = self.private / "provider" / "credentials"
        shutil.copyfile(self.args.aws_shared_credentials_file, credential)
        # The host parent is private (0700); only the provider worker receives
        # this read-only bind. No ownership helper ever mounts real credentials.
        credential.chmod(0o444)
        parsed = configparser.ConfigParser(interpolation=None)
        parsed.read(credential)
        require(parsed.sections() == [self.args.aws_profile] and not parsed.defaults(), "credential file must contain only the selected temporary profile")
        require(set(parsed[self.args.aws_profile]) == {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} and all(parsed[self.args.aws_profile].values()), "temporary session credentials are required; static keys and credential processes are prohibited")
        (self.private / "provider" / "config").write_text("")
        (self.private / "provider" / "config").chmod(0o444)
        fake = {"temporal-api-key": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJqb2luZWQtc21va2UifQ.c2ln", "att-token": "joined-smoke-att-token", "metaculus-token": "joined-smoke-metaculus-token", "aws-token": "joined-smoke-credential-token"}
        for name, value in fake.items():
            target = self.private / "fake-secrets" / name
            target.write_text(value)
            target.chmod(0o400)
        smoke = self.private / "smoke"
        (smoke / "continuation-hmac").write_text(secrets.token_hex(32))
        for arguments in (
            ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=att-local-evaluation-ca", "-keyout", str(smoke / "ca-key.pem"), "-out", str(smoke / "ca.pem")],
            ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=joined-smoke", "-addext", "subjectAltName=DNS:joined-smoke,DNS:joined-backend", "-keyout", str(smoke / "server-key.pem"), "-out", str(smoke / "server.csr")],
            ["x509", "-req", "-days", "1", "-in", str(smoke / "server.csr"), "-CA", str(smoke / "ca.pem"), "-CAkey", str(smoke / "ca-key.pem"), "-CAcreateserial", "-copy_extensions", "copy", "-out", str(smoke / "server.pem")],
        ):
            self.run(["openssl", *arguments], capture=True)
        for name in ("ca-key.pem", "server-key.pem", "continuation-hmac"):
            (smoke / name).chmod(0o400)
        for name in ("ca.pem", "server.pem"):
            (smoke / name).chmod(0o444)
        self.generate_compose()
        manifest = {"schema_version": "att_retrospective_local_pilot/v1", "project": self.project, "notice": NOTICE, "aggregate_budget_micro_usd": TOTAL_MICRO_USD, "arm_reservations_micro_usd": {arm: profile["max_cost_microunits"] for arm, profile in self.profiles.items()}, "run_timeout_seconds": self.args.run_timeout_seconds, "input_sha256": digest(self.private / "inputs/input.json"), "frozen_sha256": {name: digest(self.private / "inputs" / name) for name in frozen}, "aws_region": self.args.aws_region, "status": "prepared_no_inference"}
        manifest["release_provenance"] = {"authority": "task_local_only", "production_release_approval": False, "worker_revision_semantics": "base_commit_with_retained_working_tree_source", "identity": self.args.release_identity, "attestation_sha256": self.args.frozen_sha256[str(self.args.release_attestation)], "trust_root_sha256": self.args.frozen_sha256[str(self.args.release_trust_root)], "signature_verification": "required_by_real_go_worker_before_inference"}
        manifest.update(evaluation_mode="bounded_archive_backed_full", source_cutoff_utc=CUTOFF, collection_binding=self.args.collection_binding, corpus_selection_policy=self.args.corpus["selection_policy"], pre_outcome_model_id=MODEL_ID, historical_success=False, quality_or_competitive_edge_proven=False)
        (self.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def generate_compose(self):
        base = yaml.safe_load((ROOT / "compose.yaml").read_text())
        joined = yaml.safe_load((ROOT / "integration/joined/compose.yaml").read_text())
        variables = {"VICTORIA_ROOT": str(self.args.victoria_root), "JOINED_FIXTURE_ROOT": str(ROOT / "integration/joined"), "LLMTW_WORKER_POSTGRES_PASSWORD": secrets.token_hex(24), "LLMTW_POSTGRES_PASSWORD": secrets.token_hex(24), "LLMTW_REDIS_PASSWORD": secrets.token_hex(24)}
        variables.update({"LLMTW_COMPOSE_WORKER_" + key + "_FILE": str(self.private / "inputs" / name) for key, name in (("CONFIG", "config.yaml"), ("CAPABILITIES", "capabilities.yaml"), ("PRICES", "prices.yaml"))})
        names = ["postgres", "worker-postgres", "temporal", "redis", "redis-function-provisioner", "blob-volume-provisioner", "schema-install", "budget-bootstrap", "worker", "victoria-worker", "joined-smoke"]
        for name in names:
            service = interpolate(merge_service(base["services"].get(name, {}), joined["services"].get(name, {})), variables)
            for key in ("depends_on", "profiles", "ports", "secrets"):
                service.pop(key, None)
            service["restart"] = "no"
            service["networks"] = ["local"]
            if "build" in service:
                service["build"]["context"] = str((ROOT / service["build"]["context"]).resolve())
                service["image"] = "localhost/att-eval/" + self.project + "-" + name
            for mount in service.get("volumes", []):
                if isinstance(mount, dict) and mount["type"] == "bind":
                    if mount["target"] in {"/run/" + filename for filename in RELEASE_FILES}:
                        mount["source"] = str(self.private / "inputs" / Path(mount["target"]).name)
                        mount["read_only"] = True
                        continue
                    source = mount["source"]
                    if source.startswith("./.local/joined-smoke"):
                        mount["source"] = str(self.private / "smoke") + source[len("./.local/joined-smoke"):]
                    elif source.startswith("./.local/joined-secrets"):
                        mount["source"] = str(self.private / "fake-secrets") + source[len("./.local/joined-secrets"):]
                    else:
                        mount["source"] = str((ROOT / source).resolve())
            self.services[name] = service
        # ATT has no direct internet route. Its only archive path is a CONNECT
        # tunnel restricted to web.archive.org:443; local storage stays local.
        for name in ("postgres", "temporal", "joined-smoke"):
            self.services[name]["networks"] = ["backend"]
        self.services["joined-backend"] = self.services.pop("joined-smoke")
        worker = self.services["worker"]
        worker["build"]["args"]["GO_BUILD_TAGS"] = ""
        worker["networks"] = ["local", "egress"]
        for key in ("MOCK_API_KEY", "JOINED_SMOKE_PROVIDER_KEY", "BTF25_REAL_PROVIDER_API_KEY", "SSL_CERT_FILE"):
            worker["environment"].pop(key, None)
        worker["environment"].update({"AWS_SHARED_CREDENTIALS_FILE": "/run/provider/credentials", "AWS_CONFIG_FILE": "/run/provider/config", "AWS_PROFILE": self.args.aws_profile, "AWS_REGION": self.args.aws_region, "AWS_DEFAULT_REGION": self.args.aws_region, "AWS_EC2_METADATA_DISABLED": "true", "AWS_MAX_ATTEMPTS": "1"})
        worker["volumes"].append(bind(self.private / "provider", "/run/provider"))
        # Environment secrets are opaque bytes, not hex-decoded by the store.
        envelope_key, scope_key = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
        worker["environment"].update({"WORKER_POSTGRES_ENVELOPE_KEY": envelope_key, "WORKER_POSTGRES_SCOPE_KEY": scope_key, "CONTINUATION_HMAC": (self.private / "smoke/continuation-hmac").read_text()})
        self.services["budget-bootstrap"]["environment"]["WORKER_POSTGRES_SCOPE_KEY"] = scope_key
        victoria = self.services["victoria-worker"]
        build_time = utc_time(self.profiles["direct"]["run_profile"]["created_at"])
        victoria["build"]["args"].update({"REVISION": self.args.release_identity["worker_revision"], "VERSION": self.args.release_identity["release_version"], "CREATED": build_time.strftime("%Y-%m-%dT%H:%M:%SZ"), "SOURCE_DATE_EPOCH": str(int(build_time.timestamp()))})
        for key in ("AI_ACH_PROPHET_PROFILE_FILE", "AI_ACH_SEARCH_API_URL", "AI_ACH_SEARCH_ALLOWED_HOSTS", "AI_ACH_SEARCH_BEARER_TOKEN_FILE", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy", "all_proxy"):
            victoria["environment"].pop(key, None)
        victoria["environment"].update({"AI_ACH_EVIDENCE_MODE": ARCHIVE_MODE, "AI_ACH_HISTORICAL_CORPUS_FILE": "/run/historical-corpus/manifest.json", "AI_ACH_HISTORICAL_CORPUS_SHA256": self.args.collection_binding["corpus_sha256"], "AI_ACH_SEARCH_REQUEST_TIMEOUT": self.args.request_timeout, "AI_ACH_ARCHIVE_PROXY_URL": "http://archive-egress:8080"})
        capacity = self.profiles["direct"]["resource_capacity"]
        victoria["environment"].update({"AI_ACH_RESOURCE_CAPACITY_MANIFEST_SHA256": capacity["manifest_sha256"], "AI_ACH_RESOURCE_CAPACITY_ARTIFACT_ID": capacity["artifact"]["artifact_id"], "AI_ACH_RESOURCE_CAPACITY_ARTIFACT_LOCATOR": capacity["artifact"]["locator"]})
        victoria["volumes"].append(bind(self.private / "corpus", "/run/historical-corpus"))
        victoria["environment"]["AWS_EC2_METADATA_DISABLED"] = "true"
        victoria["volumes"] = [mount for mount in victoria["volumes"] if mount.get("target") != "/fixtures"]
        victoria["environment"]["AI_ACH_PRICING_IDENTITY_FILE"] = "/run/smoke/pricing-identity.json"
        victoria["environment"].update({"COMPETITION_WORKER_" + suffix: self.args.release_identity[field] for field, suffix in RELEASE_IDENTITY_ENV.items()})
        victoria["environment"].update({"COMPETITION_WORKER_RELEASE_ATTESTATION_FILE": "/run/release-attestation.json", "COMPETITION_WORKER_RELEASE_TRUST_ROOT_FILE": "/run/release-trust-root.json", "COMPETITION_WORKER_RELEASE_TRUST_ROOT_SHA256": self.args.frozen_sha256[str(self.args.release_trust_root)]})
        engine = copy.deepcopy(victoria)
        engine.pop("build")
        engine["image"] = "localhost/att-eval/" + self.project + "-engine"
        engine["entrypoint"] = ["/opt/ai-ach/bin/retrospective-forecast-eval"]
        engine["healthcheck"] = {"disable": True}
        engine["volumes"] += [bind(self.private / "inputs", "/run/retrospective-input"), bind(self.output, "/run/retrospective-output", False)]
        self.services["retrospective-engine"] = engine
        self.services["archive-egress"] = {"image": victoria["image"], "entrypoint": ["/opt/venv/bin/python", "-I", "-B", "/archive_tunnel.py"], "user": "65532:65532", "read_only": True, "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"], "networks": ["local", "archive"], "restart": "no", "volumes": [bind(ROOT / "scripts/historical_archive_tunnel.py", "/archive_tunnel.py")], "healthcheck": {"test": ["CMD", "/opt/venv/bin/python", "-I", "-c", "import socket; socket.create_connection(('127.0.0.1',8080),2).close()"], "interval": "2s", "timeout": "3s", "retries": 30}}
        self.services["joined-smoke"] = {"image": victoria["image"], "entrypoint": ["/opt/venv/bin/python", "-I", "-B", "/retrospective_plumbing.py"], "user": "0:0", "read_only": True, "cap_drop": ["ALL"], "cap_add": ["NET_BIND_SERVICE"], "security_opt": ["no-new-privileges:true"], "tmpfs": ["/tmp"], "networks": ["local", "backend"], "restart": "no", "volumes": [bind(ROOT / "integration/joined/retrospective_plumbing.py", "/retrospective_plumbing.py"), bind(self.private / "smoke", "/run/smoke"), bind(self.output, "/retained", False)], "healthcheck": {"test": ["CMD", "/opt/venv/bin/python", "-I", "-c", "import ssl,urllib.request; urllib.request.urlopen('https://joined-smoke/readiness',context=ssl.create_default_context(cafile='/run/smoke/ca.pem'),timeout=3)"], "interval": "2s", "timeout": "5s", "retries": 60}}
        document = {"services": self.services, "volumes": base["volumes"], "networks": {"local": {"internal": True}, "backend": {"internal": True}, "egress": {}, "archive": {}}}
        (self.private / "compose.json").write_text(json.dumps(document, indent=2))
        (self.private / "compose.json").chmod(0o600)

    def wait_healthy(self, service):
        result = self.run([
            "docker", "ps", "--all",
            "--filter", "label=com.docker.compose.project=" + self.project,
            "--filter", "label=com.docker.compose.service=" + service,
            "--format", "{{.ID}}",
        ], capture=True)
        ids = result.stdout.split()
        require(len(ids) == 1, "expected one owned container for " + service)
        for _ in range(120):
            value = json.loads(self.run(["docker", "inspect", ids[0]], capture=True).stdout)[0]["State"]
            health = value.get("Health", value.get("Healthcheck", {})).get("Status")
            if value["Status"] == "running" and health == "healthy":
                return
            require(value["Status"] not in {"exited", "dead"}, service + " exited before readiness")
            time.sleep(2)
        raise Invalid(service + " failed readiness")

    def start(self, service):
        self.started = True
        self.compose("up", "-d", "--no-deps", "--no-build", service)
        self.wait_healthy(service)

    def ledger(self):
        query = "SELECT json_build_object('operations', (SELECT COALESCE(json_agg(row_to_json(o)), '[]'::json) FROM (SELECT operation_id,state,cost_status,reserved_cost_usd,actual_cost_usd,provider_submit_started_at IS NOT NULL AS provider_submit_started FROM llmtw_state.llmtw_operations) o), 'attempts', (SELECT COALESCE(json_agg(row_to_json(a)), '[]'::json) FROM (SELECT operation_id,state,cost_status,safe_error_code,attempt_number FROM llmtw_state.llmtw_operation_attempts) a));"
        result = self.compose("exec", "-T", "worker-postgres", "psql", "-U", "llmtw_worker", "-d", "llmtw_worker", "-At", "-c", query, capture=True, timeout=30)
        return json.loads(result.stdout, parse_float=Decimal)

    def check_boundary(self, ledger, terminal=False):
        for name in ("blocked.jsonl", "errors.jsonl"):
            path = self.output / "plumbing" / name
            require(not path.exists() or path.stat().st_size == 0, "local evidence/storage boundary failed; no billable retry")
        for operation in ledger["operations"]:
            require(operation["state"] not in {"ambiguous", "definite_failed", "canceled"} and operation["cost_status"] != "unknown", "provider failure or ambiguous charge; stopping without retry")
            if terminal and operation["provider_submit_started"]:
                require(operation["state"] == "completed" and operation["cost_status"] == "exact", "unfinished/unknown provider operation remains")
        for attempt in ledger["attempts"]:
            require(attempt["attempt_number"] == 1 and attempt["state"] not in {"ambiguous", "pre_write_failed", "definite_failed", "canceled"} and not attempt["safe_error_code"], "provider attempt failed or was retried")
        # Batch escrow and unused descriptors are reservations, not additional
        # provider charges; the durable policy enforces those bounds itself.
        accounted = sum(Decimal(str(row["actual_cost_usd"] if row["actual_cost_usd"] is not None else row["reserved_cost_usd"])) for row in ledger["operations"] if row["provider_submit_started"])
        require(accounted <= 5, "durable accounted cost exceeded approved total")

    def authorize_existing_models(self, config):
        # This is the entire AWS control-plane allowlist. Inference is not an
        # access probe: Bedrock can otherwise subscribe a Marketplace model.
        cli = self.args.aws_cli or shutil.which("aws")
        if not cli:
            candidate = Path.home() / ".local/bin/aws"
            cli = str(candidate) if candidate.is_file() else None
        require(cli is not None, "AWS CLI is required for read-only preexisting-access verification")
        aws_environment = {"PATH": self.environment.get("PATH", ""), "HOME": str(self.private), "AWS_SHARED_CREDENTIALS_FILE": str(self.private / "provider/credentials"), "AWS_CONFIG_FILE": str(self.private / "provider/config"), "AWS_PROFILE": self.args.aws_profile, "AWS_REGION": self.args.aws_region, "AWS_DEFAULT_REGION": self.args.aws_region, "AWS_EC2_METADATA_DISABLED": "true", "AWS_MAX_ATTEMPTS": "1", "AWS_PAGER": "", "AWS_CLI_AUTO_PROMPT": "off"}
        evidence = {"schema_version": "att_existing_bedrock_authorization/v1", "checked_at": datetime.now(timezone.utc).isoformat(), "status": "checking", "models": [], "system_profiles": []}
        path = self.output / "bedrock-authorization.json"
        def retain():
            path.write_text(json.dumps(evidence, indent=2) + "\n")
        def request(service, operation, region, *parameters):
            require((service, operation) in {("sts", "get-caller-identity"), ("bedrock", "get-inference-profile"), ("bedrock", "get-foundation-model-availability")}, "AWS operation is outside the read-only allowlist")
            result = subprocess.run([str(cli), service, operation, *parameters, "--region", region, "--output", "json", "--no-cli-pager"], env=aws_environment, timeout=60, capture_output=True, text=True)
            if result.returncode:
                evidence.update(status="refused", failed_read_only_operation=operation, exit_code=result.returncode)
                retain()
                raise Invalid("read-only AWS availability verification failed; no inference or automatic enablement")
            return json.loads(result.stdout)
        retain()
        try:
            caller = request("sts", "get-caller-identity", self.args.aws_region)
            require(bool(re.fullmatch(r"[0-9]{12}", caller.get("Account", ""))) and caller.get("Arn", "").startswith("arn:aws:sts::" + caller["Account"] + ":assumed-role/"), "temporary assumed-role caller identity is required")
            evidence.update(account_id=caller["Account"], principal_arn=caller["Arn"])
            retain()
            require(caller["Account"] == self.args.aws_account_id, "AWS caller account differs from the explicitly approved account")
            models = {config["models"][profile["inference"]["settings"]["model"]]["routes"][0]["model"] for profile in self.profiles.values()}
            foundation_models = set()
            for model in sorted(models):
                if model.startswith(("us.", "eu.", "apac.", "global.")) or ":inference-profile/" in model:
                    profile = request("bedrock", "get-inference-profile", self.args.aws_region, "--inference-profile-identifier", model)
                    require(profile.get("type") == "SYSTEM_DEFINED" and profile.get("status") == "ACTIVE" and profile.get("models"), "only existing ACTIVE SYSTEM_DEFINED inference profiles are allowed")
                    evidence["system_profiles"].append({"requested_id": model, "profile_id": profile.get("inferenceProfileId"), "type": profile["type"], "status": profile["status"]})
                    for target in profile["models"]:
                        match = re.fullmatch(r"arn:aws:bedrock:([a-z0-9-]+)::foundation-model/([a-zA-Z0-9.:-]+)", target.get("modelArn", ""))
                        require(match is not None, "system profile contains an unknown/non-foundation target")
                        foundation_models.add((match.group(1), match.group(2)))
                else:
                    require(not model.startswith("arn:") and bool(re.fullmatch(r"[a-z0-9-]+\.[a-zA-Z0-9.:-]+", model)), "only foundation model IDs or system inference profiles are approved")
                    foundation_models.add((self.args.aws_region, model))
            for region, model in sorted(foundation_models):
                require(model == MODEL_ID, "system routing profile resolves to a model other than the approved pre-outcome revision")
                availability = request("bedrock", "get-foundation-model-availability", region, "--model-id", model)
                record = {"region": region, "model_id": model, "returned_model_id": availability.get("modelId"), "agreement": availability.get("agreementAvailability", {}).get("status"), "entitlement": availability.get("entitlementAvailability"), "authorization": availability.get("authorizationStatus"), "region_availability": availability.get("regionAvailability")}
                evidence["models"].append(record)
                retain()
                # Bedrock availability canonicalizes the default ":0" revision
                # to its model-level ID. Other revisions must still match.
                require(record["returned_model_id"] in {model, model.removesuffix(":0")}, "availability response identifies a different foundation model")
                require(record["agreement"] == "AVAILABLE" and record["entitlement"] == "AVAILABLE" and record["authorization"] == "AUTHORIZED" and record["region_availability"] == "AVAILABLE", "model lacks an existing available agreement/entitlement/authorization; refusing invocation and all enablement/subscription APIs")
            evidence["status"] = "existing_access_verified"
        except Exception:
            evidence["status"] = "refused"
            raise
        finally:
            retain()

    def execute(self):
        for binary in ("docker", "openssl"):
            require(shutil.which(binary), "missing prerequisite: " + binary)
        self.run(["docker", "compose", "version"], capture=True)
        self.prepare()
        self.started = True
        for service in ("worker", "schema-install", "budget-bootstrap", "joined-backend", "victoria-worker"):
            self.compose("build", service, timeout=1800)
        self.run(["docker", "build", "-f", str(ROOT / "integration/joined/Dockerfile.retrospective"), "--build-arg", "BASE_IMAGE=" + self.services["victoria-worker"]["image"], "-t", self.services["retrospective-engine"]["image"], str(ROOT)], timeout=120)
        inspected = json.loads(self.run(["docker", "image", "inspect", self.services["victoria-worker"]["image"]], capture=True).stdout)[0]
        image_id, manifest_digest, repo_digests = inspected.get("Id") or "", inspected.get("Digest") or "", inspected.get("RepoDigests") or []
        actual_digests = {value.removeprefix("sha256:") for value in (image_id, manifest_digest, *(value.rsplit("@", 1)[1] for value in repo_digests if "@" in value))}
        matches = self.args.release_identity["image_digest"].removeprefix("sha256:") in actual_digests
        (self.output / "victoria-image-identity.json").write_text(json.dumps({"image_id": image_id, "manifest_digest": manifest_digest, "repo_digests": repo_digests, "attested_image_digest": self.args.release_identity["image_digest"], "matches_attested_image": matches}, indent=2) + "\n")
        require(matches, "built Victoria image does not match the supplied release attestation; no inference permitted")
        # Only fake local tokens and output directories need container-UID
        # ownership. Real AWS credentials are not mounted into this helper.
        ownership = "import os; from pathlib import Path; paths=[Path('/smoke/continuation-hmac'),*Path('/fake-secrets').iterdir(),Path('/output/arms')]; [os.chown(p,65532,65532) for p in paths]; os.chmod('/output',0o755)"
        self.run(["docker", "run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "/opt/venv/bin/python", "--volume", str(self.private / "smoke") + ":/smoke:z", "--volume", str(self.private / "fake-secrets") + ":/fake-secrets:z", "--volume", str(self.output) + ":/output:z", self.services["victoria-worker"]["image"], "-I", "-c", ownership])
        # Compose providers may prepend container IDs to stdout. Invoke the
        # JSON-producing CLI directly, with no network or credential mounts.
        effective = self.run([
            "docker", "run", "--rm", "--pull=never", "--network", "none",
            "--volume", str(self.private / "inputs/config.yaml") + ":/etc/llmtw/config.yaml:ro,z",
            self.services["worker"]["image"], "print-effective-config", "--config", "/etc/llmtw/config.yaml",
        ], capture=True)
        compiled_config = json.loads(effective.stdout)
        validate_config(compiled_config, self.profiles, self.args)
        (self.output / "effective-config.json").write_text(json.dumps(compiled_config, indent=2) + "\n")
        for service in ("postgres", "worker-postgres", "redis", "temporal", "joined-backend", "joined-smoke"):
            self.start(service)
        for service in ("schema-install", "redis-function-provisioner", "blob-volume-provisioner", "budget-bootstrap"):
            self.compose("run", "--rm", "-T", "--no-deps", service, timeout=120)
        self.ledger_ready = True
        identity = self.private / "smoke/pricing-identity.json"
        require(identity.is_file() and identity.stat().st_size > 0, "durable budget bootstrap did not emit pricing identity")
        shutil.copyfile(identity, self.output / "pricing-identity.json")
        self.check_boundary(self.ledger(), terminal=True)
        # Compile every real method before any worker can dispatch inference.
        # The engine's validation mode does not connect to Temporal/providers.
        for arm in METHODS:
            result = self.run([
                "docker", "run", "--rm", "--pull=never", "--network", "none",
                "--entrypoint", "/opt/ai-ach/bin/retrospective-forecast-eval",
                "--volume", str(self.private / "inputs") + ":/run/retrospective-input:ro,z",
                "--volume", str(identity) + ":/run/pricing-identity.json:ro,z",
                "--volume", str(self.private / "corpus") + ":/run/historical-corpus:ro,z",
                self.services["retrospective-engine"]["image"],
                "--validate-only", "--input", "/run/retrospective-input/input.json",
                "--profile", "/run/retrospective-input/" + arm + ".json",
                "--pricing-identity", "/run/pricing-identity.json",
                "--arm-id", arm, "--max-questions", "1", "--max-concurrency", "1",
                "--historical-corpus", "/run/historical-corpus/manifest.json", "--historical-corpus-sha256", self.args.collection_binding["corpus_sha256"], "--request-timeout", self.args.request_timeout, "--archive-proxy-url", "http://archive-egress:8080",
            ], capture=True)
            (self.output / (arm + "-profile-validation.json")).write_text(result.stdout)
            checked = json.loads(result.stdout, object_pairs_hook=unique_pairs)
            require(checked.get("collection_binding") == self.args.collection_binding and checked.get("external_research_enabled") is True and checked.get("provider_calls") == 0, "actual Victoria profile validation did not admit the exact archive binding without inference")
        self.authorize_existing_models(compiled_config)
        self.deadline = time.monotonic() + self.args.run_timeout_seconds
        self.start("archive-egress")
        self.start("worker")
        self.start("victoria-worker")
        for arm in METHODS:
            self.engine_name = self.project + "-" + arm
            command = self.command + ["run", "--name", self.engine_name, "-T", "--no-deps", "retrospective-engine", "--input", "/run/retrospective-input/input.json", "--profile", "/run/retrospective-input/" + arm + ".json", "--arm-id", arm, "--output-root", "/run/retrospective-output/arms/" + arm, "--max-questions", "1", "--max-concurrency", "1", "--historical-corpus", "/run/historical-corpus/manifest.json", "--historical-corpus-sha256", self.args.collection_binding["corpus_sha256"], "--request-timeout", self.args.request_timeout, "--archive-proxy-url", "http://archive-egress:8080"]
            with (self.output / (arm + "-engine.log")).open("w") as log:
                self.engine_started = True
                process = subprocess.Popen(command, env=self.environment, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                try:
                    while process.poll() is None:
                        require(time.monotonic() < self.deadline, "aggregate runtime deadline reached; no automatic retry")
                        self.check_boundary(self.ledger())
                        time.sleep(2)
                    require(process.returncode == 0, arm + " engine failed; no automatic retry")
                finally:
                    if process.poll() is None:
                        self.compose("kill", "worker", "victoria-worker", check=False)
                        self.run(["docker", "rm", "-f", self.engine_name], check=False)
                        process.terminate()
                        try:
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
            self.make_results_readable()
            ledger = self.ledger()
            self.check_boundary(ledger, terminal=True)
            (self.output / (arm + "-ledger.json")).write_text(json.dumps(ledger, default=str, indent=2) + "\n")
            self.verify_arm(arm)
            self.run(["docker", "rm", self.engine_name], check=False)
            self.engine_name = ""
        with (self.output / "forecasts.jsonl").open("w") as result:
            for arm in METHODS:
                result.write((self.output / "arms" / arm / "forecasts.jsonl").read_text())
        manifest = load_json(self.output / "manifest.json")
        manifest["status"] = "returned_three_archive_evidence_verified_arms"
        manifest["historical_success"] = True
        manifest["research_trace_sha256"] = {arm: digest(self.output / (arm + "-historical-research-trace.json")) for arm in METHODS}
        (self.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def verify_arm(self, arm):
        path = self.output / "arms" / arm / "forecasts.jsonl"
        require(path.is_file(), arm + " engine omitted failure/forecast record")
        rows = [json.loads(line, object_pairs_hook=unique_pairs) for line in path.read_text().splitlines() if line.strip()]
        require(len(rows) == 1, "engine must retain exactly one requested question record")
        row = rows[0]
        require(row["schema_version"] == "att_retrospective_forecast/v1" and row["question_id"] == self.inputs["questions"][0]["question_id"] and row["arm_id"] == arm and row["method_id"] == METHODS[arm], "forecast record identity mismatch")
        require(row["input_sha256"] == digest(self.private / "inputs/input.json") and row["workflow_id"] and row["run_id"], "forecast workflow/input lineage is missing")
        require(row["status"] == "returned", "arm failed/abstained; retained record, stopping remaining arms")
        probability = row["probability_yes"]
        require(type(probability) in (int, float) and 0 <= probability <= 1, "returned binary probability is invalid")
        require(row.get("external_research_enabled") is True and row.get("platform_submit_enabled") is False, "returned result is not archive research with publication disabled")
        try:
            trace = verify_research_trace(self.output, arm, row, self.profiles[arm], self.args.collection_binding, self.args.corpus)
        except (TraceInvalid, KeyError, TypeError, ValueError, OSError) as error:
            reason = str(error) if isinstance(error, TraceInvalid) else type(error).__name__
            failure = {"schema_version": "att_historical_research_trace/v1", "arm_id": arm, "status": "refused", "downstream_remote_evidence_use_verified": False, "reason": reason}
            (self.output / (arm + "-historical-research-trace.json")).write_text(json.dumps(failure, indent=2) + "\n")
            raise Invalid(arm + " historical research trace refused: " + reason) from error
        (self.output / (arm + "-historical-research-trace.json")).write_text(json.dumps(trace, indent=2) + "\n")

    def make_results_readable(self):
        # UID 65532 is a subordinate UID under rootless Podman. Restore host
        # readability of nonsecret results without mounting any private input.
        program = "import os; from pathlib import Path; root=Path('/output/arms'); [(os.chmod(Path(parent)/name,0o755 if (Path(parent)/name).is_dir() else 0o644)) for parent,dirs,files in os.walk(root,followlinks=False) for name in dirs+files if not (Path(parent)/name).is_symlink()]"
        self.run(["docker", "run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "/opt/venv/bin/python", "--volume", str(self.output) + ":/output:z", self.services["victoria-worker"]["image"], "-I", "-c", program])

    def cleanup(self):
        errors = []
        def attempt(action):
            try:
                action()
            except Exception as error:
                errors.append(type(error).__name__)
        try:
            if self.started:
                # Stop dispatch before inspecting/exporting results. A failure
                # cannot bypass teardown or trigger a replacement runtime.
                attempt(lambda: self.compose("kill", "worker", "victoria-worker", check=False))
                if self.engine_name:
                    attempt(lambda: self.run(["docker", "rm", "-f", self.engine_name], check=False))
                if self.engine_started:
                    attempt(self.make_results_readable)
                def retain_ledger():
                    (self.output / "final-ledger.json").write_text(json.dumps(self.ledger(), default=str, indent=2) + "\n")
                if self.ledger_ready:
                    attempt(retain_ledger)
                def retain_log(service):
                    with (self.output / (service + ".log")).open("w") as stream:
                        self.compose("logs", "--no-color", service, check=False, stdout=stream)
                for service in ("worker", "victoria-worker", "temporal", "joined-backend", "joined-smoke", "archive-egress"):
                    attempt(lambda service=service: retain_log(service))
                attempt(lambda: self.compose("down", "--volumes", "--remove-orphans", timeout=180))
                images = sorted({service["image"] for service in self.services.values() if service.get("image", "").startswith("localhost/att-eval/" + self.project)})
                attempt(lambda: self.run(["docker", "image", "rm", *images], check=False))
                manifest_path = self.output / "manifest.json"
                if manifest_path.exists():
                    manifest = load_json(manifest_path)
                    if manifest["status"] != "returned_three_archive_evidence_verified_arms":
                        manifest["status"] = "stopped_without_retry"
                    manifest["unstarted_arms"] = [arm for arm in METHODS if not (self.output / "arms" / arm).exists()]
                    manifest["engine_started"] = self.engine_started
                    manifest["ledger_initialized"] = self.ledger_ready
                    manifest["cleanup_errors"] = errors
                    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
                    # Preserve every emitted record, including interrupted or
                    # failed arms; never invent results for unstarted workflows.
                    with (self.output / "forecasts.jsonl").open("w") as result:
                        for arm in METHODS:
                            path = self.output / "arms" / arm / "forecasts.jsonl"
                            if path.is_file():
                                result.write(path.read_text())
        finally:
            # Never delete caller credentials, fixed .local paths, or results.
            # All credential/continuation/encryption copies live only here.
            shutil.rmtree(self.private)
        require(not errors, "local cleanup/export incomplete; owned project is " + self.project + "; consult retained manifest, do not retry inference")


def arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("input", "config", "capabilities", "prices", "direct-profile", "fixed-profile", "irt-profile", "release-attestation", "release-trust-root", "resource-capacity", "historical-corpus", "question-proof", "question-projection", "model-evidence", "victoria-validator", "output-root", "aws-shared-credentials-file", "victoria-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--aws-profile", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--aws-account-id", required=True, help="approved account; STS must match before model discovery or invocation")
    parser.add_argument("--aws-cli", help="AWS CLI executable for read-only authorization checks (default: PATH, then ~/.local/bin/aws)")
    parser.add_argument("--run-timeout-seconds", type=int, default=14400)
    parser.add_argument("--request-timeout", default="30s", help="frozen archive fetch timeout; must match Victoria canonical binding")
    parser.add_argument("--approve-total-usd", choices=["5"], required=True)
    parser.add_argument("--acknowledge-real-bedrock-costs-no-publication", action="store_true", required=True)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    for key, value in vars(args).items():
        if isinstance(value, Path):
            # Preserve the original credential path for lstat symlink checks.
            setattr(args, key, value.absolute() if key == "aws_shared_credentials_file" else value.resolve())
    return args


def main():
    pilot = None
    status = 0
    try:
        args = arguments()
        inputs, profiles = validate(args)
        if args.validate_only:
            print("OFFLINE_ARCHIVE_PREFLIGHT=passed; actual Victoria local corpus/binding validated; no credential contents read, network calls, services, inference, or quality claims")
        else:
            pilot = Pilot(args, inputs, profiles)
            def interrupted(signum, _frame):
                raise Invalid("pilot interrupted; no automatic retry (signal " + str(signum) + ")")
            signal.signal(signal.SIGTERM, interrupted)
            signal.signal(signal.SIGINT, interrupted)
            pilot.execute()
            print("ATT_RETROSPECTIVE_OUTPUT=" + str(args.output_root))
            print(NOTICE)
    except Exception as error:
        # Avoid printing subprocess arguments, config text, or secret material.
        message = str(error) if isinstance(error, Invalid) else type(error).__name__
        print("joined-retrospective-bedrock: " + message, file=sys.stderr)
        status = 2
    finally:
        if pilot is not None:
            try:
                pilot.cleanup()
            except Exception as error:
                message = str(error) if isinstance(error, Invalid) else type(error).__name__
                print("joined-retrospective-bedrock cleanup: " + message, file=sys.stderr)
                status = 2
    return status


if __name__ == "__main__":
    sys.exit(main())
