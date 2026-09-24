"""Verify historical research use from retained production artifacts, offline.

This is a consumer-level cross-artifact verifier, not a substitute for Victoria's
canonical Go/profile/graph/Stage2 validators. No network or model calls occur.
"""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import stat


class TraceInvalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise TraceInvalid(message)


def pairs(values):
    result = {}
    for key, value in values:
        require(key not in result, "duplicate retained JSON key")
        result[key] = value
    return result


def decode(body):
    return json.loads(body, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(TraceInvalid("nonfinite retained JSON")))


def nodes(value, depth=0):
    require(depth <= 128, "artifact nesting exceeds safety bound")
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from nodes(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            yield from nodes(child, depth + 1)


def timestamp(value):
    require(isinstance(value, str), "missing evidence timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TraceInvalid("invalid evidence timestamp") from error
    require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0, "evidence timestamp is not exact UTC")
    return parsed


def sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid artifact digest")
    return value


class Artifacts:
    def __init__(self, output, arm, row):
        self.paths, self.cache, self.bytes_read = {}, {}, 0
        self.journals = set()
        self.arm_root = output / "arms" / arm / "artifacts"
        for node in nodes(row):
            if node.get("locator", "").startswith("artifact://sha256/") and node.get("sha256"):
                identity = sha(node["sha256"])
                require(node["locator"] == "artifact://sha256/" + identity, "local artifact locator differs from hash")
                self.paths[identity] = self.arm_root / "sha256" / identity[:2] / identity[2:]
        # These append-only records identify exact immutable bodies retained by
        # the local S3/ledger boundary, including final Activity journal seals.
        for filename in ("artifacts.jsonl", "ledger.jsonl"):
            path = output / "plumbing" / filename
            require(path.is_file() and not path.is_symlink(), "retained artifact/ledger index is missing")
            with path.open() as stream:
                for line in stream:
                    require(len(line) <= 1 << 20, "retained artifact index line exceeds bound")
                    record = decode(line)
                    if filename == "artifacts.jsonl" and "/activity-journal/" in record.get("path", "") and 200 <= record.get("status", 0) < 300:
                        self.journals.add(sha(record["sha256"]))
                    for field in ("sha256", "request_sha256", "response_sha256"):
                        if record.get(field):
                            identity = sha(record[field])
                            self.paths.setdefault(identity, output / "plumbing/objects" / identity)
                    require(len(self.paths) <= 10000, "retained artifact count exceeds bound")

    def raw(self, identity):
        identity = sha(identity)
        require(identity in self.paths, "required immutable artifact was not retained: " + identity)
        path = self.paths[identity]
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and 0 <= info.st_size <= 64 << 20 and not any(parent.is_symlink() for parent in path.parents), "retained artifact is not a bounded regular file")
        body = path.read_bytes()
        require(hashlib.sha256(body).hexdigest() == identity, "retained artifact hash mismatch")
        return body

    def json(self, reference):
        identity = sha(reference["sha256"])
        if identity not in self.cache:
            body = self.raw(identity)
            self.bytes_read += len(body)
            require(self.bytes_read <= 512 << 20, "retained JSON closure exceeds bound")
            try:
                self.cache[identity] = decode(body)
            except (UnicodeDecodeError, json.JSONDecodeError):
                self.cache[identity] = None
        require(self.cache[identity] is not None, "expected retained JSON artifact")
        return self.cache[identity]

    def documents(self):
        for identity in self.paths:
            body = self.raw(identity)
            if body.lstrip().startswith((b"{", b"[")):
                try:
                    value = self.json({"sha256": identity})
                except TraceInvalid as error:
                    if str(error) == "expected retained JSON artifact":
                        continue
                    raise
                yield identity, value

    def closure(self, value):
        seen, pending = set(), [value]
        while pending:
            current = pending.pop()
            for node in nodes(current):
                if node.get("artifact_id") and node.get("locator") and node.get("sha256"):
                    identity = sha(node["sha256"])
                    if identity in seen:
                        continue
                    seen.add(identity)
                    require(len(seen) <= 10000, "provenance closure exceeds bound")
                    body = self.raw(identity)
                    if body.lstrip().startswith((b"{", b"[")):
                        try:
                            pending.append(self.json(node))
                        except TraceInvalid as error:
                            if str(error) != "expected retained JSON artifact":
                                raise
        return seen


def verify_research_trace(output, arm, row, profile, binding, corpus):
    store = Artifacts(Path(output), arm, row)
    require(row.get("collection_binding") == binding and row.get("execution_selection") == "primary_only_v1", "forecast record changed the collection binding or primary-only execution")
    cutoff = timestamp(corpus["source_cutoff_utc"])
    now = datetime.now(timezone.utc)
    workflow = store.json(row["workflow_result"])
    require(workflow.get("forecast") == row.get("forecast") and workflow.get("input_sha256") == row.get("workflow_input_sha256") and not workflow.get("submission") and not workflow.get("comment"), "final workflow seal/input/publication lineage differs")
    forecast_ref = row["forecast"]["document"]
    require(row["forecast"]["sha256"] == forecast_ref["sha256"], "sealed forecast digest differs")
    final_forecast = store.json(forecast_ref)
    require(final_forecast.get("question_revision_id") == row["question"]["question_revision_id"], "sealed forecast belongs to another question")
    require(final_forecast.get("run_id") == row["semantic_run_id"] and final_forecast.get("forecast_id") == row["forecast"]["forecast_id"], "sealed forecast run/identity differs")
    if arm == "direct":
        require(final_forecast.get("option_ids") == ["yes", "no"], "direct outcome order differs")
        probabilities = final_forecast["submitted_probabilities"]
        require(probabilities == final_forecast["final_probabilities"], "direct scored vector differs from sealed vector")
    else:
        vector = final_forecast["probability_vector"]
        require([point["outcome_id"] for point in vector] == ["yes", "no"] and vector == final_forecast["platform_submitted_vector"], "Stage2 scored outcomes differ from sealed vector")
        probabilities = [point["probability"] for point in vector]
    require(len(probabilities) == 2 and all(type(value) in (int, float) and 0 <= value <= 1 for value in probabilities) and abs(sum(probabilities) - 1) <= 1e-12 and probabilities[0] == row["probability_yes"], "returned probability does not match the final sealed simplex")
    documents = list(store.documents())
    seals = [(identity, value) for identity, value in documents if identity in store.journals and isinstance(value, dict) and value.get("schema_version") == "ai_ach.temporal_execution/v1" and value.get("forecast") == row["forecast"] and isinstance(value.get("provenance"), dict) and value.get("operation", {}).get("run_id") == row["semantic_run_id"] and value["operation"].get("kind") == "forecast_seal"]
    require(seals, "no retained production seal binds final forecast provenance")
    seal_sha, seal = seals[0]
    provenance = seal["provenance"]
    require(all(value["provenance"] == provenance for _, value in seals), "conflicting final provenance for this seal")
    require(provenance["forecast"] == forecast_ref and provenance["profile"]["sha256"] == row["profile_artifact"]["sha256"] and provenance["question"] == row["question"]["document"], "sealed provenance differs from admitted question/profile/forecast")
    require(provenance["estimator"]["method"] == row["method_id"], "sealed estimator method differs")
    closure_hashes = store.closure(provenance)
    graph_ref = provenance["context_graph"]
    graph = store.json(graph_ref)
    require(graph.get("schema_version") == "context_graph_manifest/v1" and graph.get("validation", {}).get("status") == "validated" and graph.get("question_revision_id") == row["question"]["question_revision_id"], "final graph validation/identity missing")
    require(timestamp(graph["evidence_cutoff_utc"]) == cutoff and timestamp(graph["question_cutoff_utc"]) == cutoff, "graph cutoff changed")
    context = store.json(graph["context_output"])
    require(timestamp(context["evidence_cutoff_utc"]) == cutoff, "context cutoff changed")
    manifests = [(identity, value) for identity, value in documents if identity in closure_hashes and isinstance(value, dict) and value.get("schema_version") == "ai_ach.evidence_manifest/v2" and value.get("request", {}).get("operation", {}).get("run_id") == row["semantic_run_id"]]
    require(manifests, "final provenance does not retain an admitted archive collection manifest")
    corpus_by_url = {document["archive_url"]: document for document in corpus["documents"]}
    queries, captures = [], []
    seen_calls = set()
    for identity, manifest in manifests:
        request = manifest["request"]
        require(request.get("collection_binding") == binding and request["profile"].get("collection_binding") == binding, "collection receipt is not bound to the actual frozen corpus/provider")
        require(timestamp(request["evidence_cutoff"]) == cutoff and request["question"]["question_revision_id"] == row["question"]["question_revision_id"] and request["profile"]["artifact"]["sha256"] == row["profile_artifact"]["sha256"], "collection question/profile/cutoff mismatch")
        admitted = {call["id"]: call for call in request["tool_calls"]}
        require(0 < len(admitted) == len(request["tool_calls"]) <= request["max_queries"], "collection query admission missing/repeated/over budget")
        payloads = [node for node in nodes(manifest["tool_results"]) if node.get("schema_version") == "web_search_tool_result/v2"]
        require(len(payloads) == len(admitted), "collection results do not account for admitted queries")
        for payload in payloads:
            call_id = payload["call_id"]
            key = (request["operation"]["operation_key"], call_id)
            require(call_id in admitted and key not in seen_calls, "query receipt lacks unique admission")
            seen_calls.add(key)
            arguments = admitted[call_id]["arguments"]
            require(isinstance(arguments, dict) and arguments.get("query") == payload["query"] and payload["query"].strip(), "returned query text differs from admitted query")
            executed = timestamp(payload["search_executed_at_utc"])
            require(cutoff < executed <= now and timestamp(payload["source_cutoff_utc"]) == cutoff, "archive query operation is backdated or cutoff changed")
            queries.append({"manifest_sha256": identity, "call_id": call_id, "query": payload["query"], "search_executed_at_utc": payload["search_executed_at_utc"]})
            for result in payload["results"]:
                document = corpus_by_url.get(result["url"])
                require(document is not None and result["final_url"] == result["url"], "capture is outside the frozen archive corpus or redirected")
                require(result["point_in_time_evidence"] == binding["mode"] and result["artifact"]["sha256"] == result["archive_content_sha256"] == document["body_sha256"], "capture body/archive identity mismatch")
                snapshot = timestamp(result["archive_snapshot_at_utc"])
                require(snapshot == timestamp(document["archive_snapshot_at_utc"]) == timestamp(result["content_observed_at_utc"]) and snapshot <= cutoff, "capture snapshot/content observation is not the exact pre-cutoff archive")
                require(executed <= timestamp(result["fetched_at_utc"]) <= now, "capture fetch is backdated or predates its query")
                require(result.get("published_at_utc") == document.get("published_at_utc") or (result.get("published_at_utc") and document.get("published_at_utc") and timestamp(result["published_at_utc"]) == timestamp(document["published_at_utc"])), "capture publication precision differs from corpus proof")
                if result.get("published_at_utc"):
                    require(timestamp(result["published_at_utc"]) <= cutoff, "capture published after cutoff")
                body = store.raw(result["artifact"]["sha256"])
                require(body and len(body) == result["artifact"]["size_bytes"], "capture body is empty or size differs")
                require(base64.b32encode(hashlib.sha1(body).digest()).decode() == document["archive_payload_sha1_base32"], "capture bytes differ from the frozen CDX payload digest")
                require(result["excerpts"], "capture supplies no body-derived evidence")
                for excerpt in result["excerpts"]:
                    start, end = excerpt["start_byte"], excerpt["end_byte"]
                    require(type(start) is int and type(end) is int and 0 <= start < end <= len(body), "capture excerpt offsets are invalid")
                    selected = body[start:end]
                    require(excerpt["body_sha256"] == document["body_sha256"] and hashlib.sha256(selected).hexdigest() == excerpt["excerpt_sha256"] and selected.decode("utf-8") == excerpt["text"], "capture excerpt is not bound to retained body bytes")
                captures.append({"manifest_sha256": identity, "call_id": call_id, "query": payload["query"], "search_executed_at_utc": payload["search_executed_at_utc"], "result": result})
    limits = profile["run_profile"]["collection"]
    require(0 < len(queries) <= limits["max_queries"] and 0 < len(captures) <= limits["max_fetched_pages"], "positive bounded archive queries and captures are required")
    source_by_id = {source["source_id"]: source for source in context["sources"]}
    trace_queries = {query["query_id"]: query for query in context["collection_trace"]["queries"]}
    trace_results = {result["trace_entry_id"]: result for result in context["collection_trace"]["results"]}
    remote_documents = {}
    for document in context["documents"]:
        matching = [capture for capture in captures if capture["result"]["artifact"]["sha256"] == document["content_sha256"] and capture["result"]["final_url"] == document["canonical_url"]]
        if not matching:
            continue
        source = source_by_id.get(document["source_id"])
        require(source is not None, "remote graph document lacks its exact source")
        entry = trace_results.get(document["collection_trace_entry_id"], {})
        query = trace_queries.get(entry.get("query_id"), {})
        matching = [capture for capture in matching if query.get("query_text") == capture["query"] and timestamp(query["executed_at_utc"]) == timestamp(capture["search_executed_at_utc"]) and entry.get("result_rank") == capture["result"]["rank"]]
        require(matching and entry.get("included_document_id") == document["document_id"] and entry.get("canonical_url") == document["canonical_url"], "remote graph trace is not bound to the admitted collection receipt")
        result = matching[0]["result"]
        for record in (source, document):
            require(record["content_sha256"] == result["artifact"]["sha256"] and record["archive_content_sha256"] == result["archive_content_sha256"] and record["canonical_url"] == result["final_url"] and record["point_in_time_evidence"] == binding["mode"] and record["immutable_capture_reference"] == "urn:sha256:" + result["artifact"]["sha256"], "graph source/document content binding differs from remote capture")
            require(timestamp(record["archive_snapshot_at_utc"]) == timestamp(result["archive_snapshot_at_utc"]) and timestamp(record["content_observed_at_utc"]) == timestamp(result["content_observed_at_utc"]) and timestamp(record["retrieval_time_utc"]) == timestamp(result["fetched_at_utc"]), "graph source/document time binding differs from capture")
            require(timestamp(result["fetched_at_utc"]) <= timestamp(record["verification_time_utc"]) <= now, "graph source verification backdates actual retrieval")
            require(bool(record.get("publication_time_unknown")) == (result.get("published_at_utc") is None), "graph invented/lost publication precision")
            if result.get("published_at_utc"):
                require(timestamp(record["publication_time_utc"]) == timestamp(result["published_at_utc"]), "graph publication differs from capture")
        remote_documents[document["document_id"]] = document
    require(remote_documents, "final graph uses no verified remote archive document")
    used_claims, used_citations = [], []
    if arm == "direct":
        direct = provenance["estimator"]["direct"]
        require(direct["context_output"]["sha256"] == graph["context_output"]["sha256"], "direct inference context differs from final graph")
        response = store.json(direct["raw_response"])
        rationale = store.json(provenance["rationale"])
        require(response["rationale"] == rationale, "sealed direct rationale differs from actual model response")
        remote_sources = {(document["source_id"], document["content_sha256"]) for document in remote_documents.values()}
        used_citations = [citation for citation in rationale["citations"] if (citation["source_id"], citation["content_sha256"]) in remote_sources]
        require(used_citations, "direct forecast cites no verified remote content (seed-only result)")
    else:
        closure = store.json(provenance["analysis_closure"])
        require(closure.get("schema_version") == "ai_ach.analysis_closure/v1" and closure["context_graph_manifest"] == graph_ref and closure["context_graph_manifest_sha256"] == graph_ref["sha256"] and all(closure["context_output"][key] == graph["context_output"][key] for key in ("artifact_id", "sha256", "locator")), "analysis closure differs from final graph")
        selection = store.json(closure["claim_selection_receipt"])
        require(selection["context_graph_manifest_sha256"] == graph_ref["sha256"], "claim selection receipt belongs to another graph")
        selected = {decision["claim_id"] for decision in selection["decisions"] if decision["included"] is True}
        for claim in context["claims"]:
            if claim["claim_id"] in selected and any(reference["document_id"] in remote_documents and remote_documents[reference["document_id"]]["source_id"] == reference["source_id"] for reference in claim["supporting_references"] + claim["contradicting_references"]):
                used_claims.append(claim["claim_id"])
        require(used_claims, "no remote claim was selected for downstream analysis")
        assessed = {assessment["claim_id"] for assessment in closure["assessments"]}
        require(set(used_claims) <= assessed, "selected remote claims lack retained assessments")
        plan = store.json(closure["panel_plan"])
        require(plan["claim_selection_receipt"] == selection and set(plan["claim_ids"]) == selected and plan["context_graph_manifest_sha256"] == graph_ref["sha256"], "panel plan does not consume exact selected claims")
        for assessment in closure["assessments"]:
            assessed_document = store.json(assessment["artifact"])
            require(assessed_document["assessment_id"] == assessment["assessment_id"] and assessed_document["claim_id"] == assessment["claim_id"] and assessed_document["context_graph_manifest_sha256"] == graph_ref["sha256"] and assessed_document["question_revision_id"] == row["question"]["question_revision_id"] and assessed_document["panel_sha256"] == plan["panel_sha256"], "assessment identity/graph/panel differs from the sealed closure")
            if assessment["claim_id"] in used_claims:
                require(any(source["document_id"] in remote_documents and source["source_id"] == remote_documents[source["document_id"]]["source_id"] and source["source_artifact_sha256"] == remote_documents[source["document_id"]]["content_sha256"] for source in assessed_document["source_artifacts"]), "selected remote claim assessment omitted its exact remote source bytes")
        stage2 = store.json(provenance["stage2_result"])
        require(stage2["categorical_method"] == row["method_id"] and stage2["question_revision_id"] == row["question"]["question_revision_id"] and stage2["context_graph_manifest_sha256"] == graph_ref["sha256"] and stage2["context_graph_output_sha256"] == graph["context_output"]["sha256"] and stage2["panel_plan_sha256"] == closure["panel_plan"]["sha256"] and stage2["panel_sha256"] == plan["panel_sha256"], "Stage2 output did not consume the final remote-claim graph/panel closure")
        store.json(provenance["stage2_manifest"])
    return {"schema_version": "att_historical_research_trace/v1", "arm_id": arm, "question_id": row["question_id"], "workflow_id": row["workflow_id"], "run_id": row["run_id"], "semantic_run_id": row["semantic_run_id"], "method_id": row["method_id"], "verified_at_utc": now.isoformat(), "collection_binding": binding, "source_cutoff_utc": corpus["source_cutoff_utc"], "admitted_queries": queries, "verified_capture_count": len(captures), "remote_captures": [{"manifest_sha256": capture["manifest_sha256"], "call_id": capture["call_id"], "archive_url": capture["result"]["url"], "body_sha256": capture["result"]["artifact"]["sha256"], "archive_snapshot_at_utc": capture["result"]["archive_snapshot_at_utc"], "fetched_at_utc": capture["result"]["fetched_at_utc"]} for capture in captures], "context_graph_sha256": graph_ref["sha256"], "remote_document_ids": sorted(remote_documents), "selected_assessed_remote_claim_ids": sorted(used_claims), "remote_direct_citations": used_citations, "seal_artifact_sha256": seal_sha, "forecast_sha256": forecast_ref["sha256"], "verified_provenance_artifact_sha256": sorted(closure_hashes), "downstream_remote_evidence_use_verified": True, "quality_or_competitive_edge_proven": False}
