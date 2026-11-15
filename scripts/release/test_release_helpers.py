"""Offline checks for the shared helpers in the release evidence scripts."""
import argparse
import importlib.util
from pathlib import Path
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collect = load("collect", "collect.py")
live = load("live_contract_evidence", "live-contract-evidence.py")
slo = load("slo_evidence", "slo-evidence.py")


class AddCommandTests(unittest.TestCase):
    def test_registers_required_options_and_handler(self):
        for module in (collect, live):
            with self.subTest(module=module.__name__):
                commands = argparse.ArgumentParser().add_subparsers(dest="command", required=True)
                handler = object()
                command = module.add_command(commands, "demo", handler, "input", "go-mod")
                arguments = command.parse_args(["--input", "a", "--go-mod", "b"])
                self.assertEqual((arguments.input, arguments.go_mod, arguments.handler), ("a", "b", handler))
                with self.assertRaises(SystemExit):
                    command.parse_args(["--input", "a"])

    def test_collect_parser_keeps_optional_flags(self):
        diagnostics = collect.parser().parse_args(["gate-diagnostics", "--input", "x", "--detail"])
        self.assertEqual((diagnostics.input, diagnostics.detail), ("x", True))
        self.assertIs(diagnostics.handler, collect.command_gate_diagnostics)
        rendered = collect.parser().parse_args(["rendered-manifests", "--output", "o"])
        self.assertEqual((rendered.entry, rendered.output), ([], "o"))

    def test_live_record_shares_binding_with_verify(self):
        binding = ["--evidence", "e", "--log", "l", "--source-revision", "r"]
        record = live.build_parser().parse_args(["record", "--profile", "p", "--input", "i", *binding])
        self.assertEqual((record.profile, record.input, record.evidence, record.generated_at), ("p", "i", "e", None))
        verify = live.build_parser().parse_args(["verify", *binding])
        self.assertEqual((verify.log, verify.source_revision, verify.handler), ("l", "r", live.command_verify))


class RequireConsistentSpendTests(unittest.TestCase):
    def test_accepts_consistent_spend(self):
        method = sorted(live.REPORTED_COST_METHODS)[0]
        live.require_consistent_spend(True, 5, method)
        live.require_consistent_spend(False, 0, "not_reported")

    def test_rejects_inconsistent_spend(self):
        for known, micro_usd, method in ((True, 5, "not_reported"), (False, 1, "not_reported"), (False, 0, "usage")):
            with self.subTest(known=known, micro_usd=micro_usd, method=method):
                with self.assertRaises(live.ContractError):
                    live.require_consistent_spend(known, micro_usd, method)


class AddPrometheusSeriesTests(unittest.TestCase):
    def test_accumulates_unique_series(self):
        seen = set()
        total = slo.add_prometheus_series(seen, {"origin": "worker"}, "2", 0)
        total = slo.add_prometheus_series(seen, {"origin": "other"}, "3", total)
        self.assertEqual(total, 5)
        self.assertEqual(seen, {(("origin", "worker"),), (("origin", "other"),)})

    def test_rejects_duplicate_series_and_overflow(self):
        with self.assertRaises(slo.EvidenceError):
            slo.add_prometheus_series({(("origin", "worker"),)}, {"origin": "worker"}, "1", 0)
        with self.assertRaises(slo.EvidenceError):
            slo.add_prometheus_series(set(), {"origin": "worker"}, "1", slo.MAX_COUNT)


if __name__ == "__main__":
    unittest.main()
