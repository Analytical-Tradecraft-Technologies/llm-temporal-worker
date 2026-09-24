"""Synthetic provenance contracts; these fixtures are not ATT forecasts."""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import historical_research_trace as trace


class FrozenClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 1, 2, tzinfo=timezone.utc)


class HistoricalTraceTest(unittest.TestCase):
    def build(self, root, arm, *, remote_use=True, late_snapshot=False):
        objects = root / "plumbing/objects"
        objects.mkdir(parents=True)
        index = []

        def keep(value, journal=False):
            body = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
            digest = hashlib.sha256(body).hexdigest()
            (objects / digest).write_bytes(body)
            index.append({"path": ("/activity-journal/" if journal else "/artifacts/") + digest, "status": 201, "sha256": digest})
            return {"artifact_id": digest, "sha256": digest, "locator": "s3://synthetic-trace/" + digest}

        cutoff = "2025-12-07T00:00:00Z"
        snapshot = "2025-12-08T00:00:00Z" if late_snapshot else "2025-11-01T00:00:00Z"
        executed, fetched, verified = "2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"
        body = b"An agency published the licensing procedure."
        body_ref = keep(body)
        archive_url = "https://web.archive.org/web/20251101000000id_/https://example.org/procedure"
        binding = {"mode": "immutable_archive_snapshot", "search_provider": "historical_corpus_v1", "search_config_sha256": "1" * 64, "fetch_config_sha256": "2" * 64, "corpus_sha256": "3" * 64}
        corpus = {"source_cutoff_utc": cutoff, "documents": [{"archive_url": archive_url, "body_sha256": body_ref["sha256"], "archive_snapshot_at_utc": snapshot, "published_at_utc": None, "archive_payload_sha1_base32": base64.b32encode(hashlib.sha1(body).digest()).decode()}]}
        profile = {"run_profile": {"collection": {"max_queries": 1, "max_fetched_pages": 1}}}
        profile_ref, question_ref = keep(profile), keep({"question": "Synthetic trace contract only"})
        excerpt = {"text": body.decode(), "start_byte": 0, "end_byte": len(body), "body_sha256": body_ref["sha256"], "excerpt_sha256": body_ref["sha256"]}
        result = {"url": archive_url, "final_url": archive_url, "point_in_time_evidence": binding["mode"], "artifact": {**body_ref, "size_bytes": len(body)}, "archive_content_sha256": body_ref["sha256"], "archive_snapshot_at_utc": snapshot, "content_observed_at_utc": snapshot, "fetched_at_utc": fetched, "published_at_utc": None, "rank": 1, "excerpts": [excerpt]}
        payload = {"schema_version": "web_search_tool_result/v2", "collection_binding": binding, "call_id": "call", "query": "licensing", "search_executed_at_utc": executed, "source_cutoff_utc": cutoff, "results": [result]}
        collection = keep({"schema_version": "ai_ach.evidence_manifest/v2", "request": {"operation": {"run_id": "semantic-run", "operation_key": "collect"}, "collection_binding": binding, "profile": {"collection_binding": binding, "artifact": profile_ref}, "evidence_cutoff": cutoff, "question": {"question_revision_id": "question-revision"}, "tool_calls": [{"id": "call", "arguments": {"query": "licensing"}}], "max_queries": 1}, "tool_results": [payload]})
        source = {"source_id": "source", "content_sha256": body_ref["sha256"], "archive_content_sha256": body_ref["sha256"], "canonical_url": archive_url, "point_in_time_evidence": binding["mode"], "immutable_capture_reference": "urn:sha256:" + body_ref["sha256"], "archive_snapshot_at_utc": snapshot, "content_observed_at_utc": snapshot, "retrieval_time_utc": fetched, "verification_time_utc": verified, "publication_time_unknown": True, "publication_time_utc": ""}
        document = {**source, "document_id": "document", "collection_trace_entry_id": "entry"}
        claim = {"claim_id": "claim", "supporting_references": [{"document_id": "document", "source_id": "source"}], "contradicting_references": []}
        context_ref = keep({"evidence_cutoff_utc": cutoff, "sources": [source], "documents": [document], "claims": [claim], "collection_trace": {"queries": [{"query_id": "query", "query_text": "licensing", "executed_at_utc": executed}], "results": [{"trace_entry_id": "entry", "query_id": "query", "result_rank": 1, "included_document_id": "document", "canonical_url": archive_url}]}})
        graph_ref = keep({"schema_version": "context_graph_manifest/v1", "validation": {"status": "validated"}, "question_revision_id": "question-revision", "evidence_cutoff_utc": cutoff, "question_cutoff_utc": cutoff, "context_output": context_ref})
        method = {"direct": "direct_categorical_v1", "fixed": "fixed_ordinal_summary_stage2_v1", "irt": "ordinal_irt_stage2_v1"}[arm]
        forecast = {"question_revision_id": "question-revision", "run_id": "semantic-run", "forecast_id": "forecast"}
        if arm == "direct":
            forecast.update({"option_ids": ["yes", "no"], "submitted_probabilities": [0.5, 0.5], "final_probabilities": [0.5, 0.5]})
        else:
            vector = [{"outcome_id": "yes", "probability": 0.5}, {"outcome_id": "no", "probability": 0.5}]
            forecast.update({"probability_vector": vector, "platform_submitted_vector": vector})
        forecast_ref = keep(forecast)
        sealed = {"forecast_id": "forecast", "sha256": forecast_ref["sha256"], "document": forecast_ref}
        provenance = {"forecast": forecast_ref, "profile": profile_ref, "question": question_ref, "context_graph": graph_ref, "evidence": collection, "estimator": {"method": method}}
        if arm == "direct":
            rationale = {"citations": [{"source_id": "source", "content_sha256": body_ref["sha256"]}] if remote_use else []}
            provenance["rationale"] = keep(rationale)
            provenance["estimator"]["direct"] = {"context_output": context_ref, "raw_response": keep({"rationale": rationale})}
        else:
            selection = {"context_graph_manifest_sha256": graph_ref["sha256"], "decisions": [{"claim_id": "claim", "included": remote_use}]}
            selection_ref = keep(selection)
            plan = {"claim_selection_receipt": selection, "claim_ids": ["claim"] if remote_use else [], "context_graph_manifest_sha256": graph_ref["sha256"], "panel_sha256": "4" * 64}
            plan_ref = keep(plan)
            assessment = {"assessment_id": "assessment", "claim_id": "claim", "context_graph_manifest_sha256": graph_ref["sha256"], "question_revision_id": "question-revision", "panel_sha256": plan["panel_sha256"], "source_artifacts": [{"document_id": "document", "source_id": "source", "source_artifact_sha256": body_ref["sha256"]}]}
            assessment_ref = keep(assessment)
            provenance["analysis_closure"] = keep({"schema_version": "ai_ach.analysis_closure/v1", "context_graph_manifest": graph_ref, "context_graph_manifest_sha256": graph_ref["sha256"], "context_output": context_ref, "claim_selection_receipt": selection_ref, "panel_plan": plan_ref, "assessments": [{"claim_id": "claim", "assessment_id": "assessment", "artifact": assessment_ref}]})
            provenance["stage2_result"] = keep({"categorical_method": method, "question_revision_id": "question-revision", "context_graph_manifest_sha256": graph_ref["sha256"], "context_graph_output_sha256": context_ref["sha256"], "panel_plan_sha256": plan_ref["sha256"], "panel_sha256": plan["panel_sha256"]})
            provenance["stage2_manifest"] = keep({"synthetic_test_only": True})
        keep({"schema_version": "ai_ach.temporal_execution/v1", "forecast": sealed, "provenance": provenance, "operation": {"run_id": "semantic-run", "kind": "forecast_seal"}}, journal=True)
        workflow_ref = keep({"forecast": sealed, "input_sha256": "5" * 64})
        row = {"collection_binding": binding, "execution_selection": "primary_only_v1", "workflow_result": workflow_ref, "workflow_input_sha256": "5" * 64, "forecast": sealed, "question": {"question_revision_id": "question-revision", "document": question_ref}, "semantic_run_id": "semantic-run", "profile_artifact": profile_ref, "method_id": method, "probability_yes": 0.5, "question_id": "synthetic-question", "workflow_id": "workflow", "run_id": "run"}
        (root / "plumbing/artifacts.jsonl").write_text("".join(json.dumps(item) + "\n" for item in index))
        (root / "plumbing/ledger.jsonl").write_text("")
        return row, profile, binding, corpus, objects / body_ref["sha256"]

    def test_sealed_remote_use_required_for_each_method(self):
        for arm in ("direct", "fixed", "irt"):
            for used in (True, False):
                with self.subTest(arm=arm, remote_use=used), tempfile.TemporaryDirectory() as directory, patch.object(trace, "datetime", FrozenClock):
                    row, profile, binding, corpus, _ = self.build(Path(directory), arm, remote_use=used)
                    if used:
                        result = trace.verify_research_trace(directory, arm, row, profile, binding, corpus)
                        self.assertTrue(result["downstream_remote_evidence_use_verified"])
                        self.assertFalse(result["quality_or_competitive_edge_proven"])
                    else:
                        with self.assertRaises(trace.TraceInvalid):
                            trace.verify_research_trace(directory, arm, row, profile, binding, corpus)

    def test_self_consistent_future_snapshot_still_rejected(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(trace, "datetime", FrozenClock):
            row, profile, binding, corpus, _ = self.build(Path(directory), "direct", late_snapshot=True)
            with self.assertRaises(trace.TraceInvalid):
                trace.verify_research_trace(directory, "direct", row, profile, binding, corpus)

    def test_retained_body_tampering_rejected(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(trace, "datetime", FrozenClock):
            row, profile, binding, corpus, body = self.build(Path(directory), "direct")
            body.write_bytes(b"An altered licensing procedure.")
            with self.assertRaises(trace.TraceInvalid):
                trace.verify_research_trace(directory, "direct", row, profile, binding, corpus)


if __name__ == "__main__":
    unittest.main()
