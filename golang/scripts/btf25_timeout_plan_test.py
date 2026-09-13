import json
import tempfile
import shutil
import unittest
from pathlib import Path

import btf25_timeout_plan


class BTF25TimeoutPlanTest(unittest.TestCase):
    def profile(
        self,
        runtime: int = 3300,
        deadline_reserve: int = 60,
        *,
        forecast_capacity: int = 8,
        stage2_capacity: int = 24,
        ensemble_size: int = 3,
    ) -> Path:
        directory = Path(tempfile.mkdtemp())
        path = directory / "profile.json"
        path.write_text(json.dumps({
            "max_runtime_seconds": runtime,
            "deadline_reserve_seconds": deadline_reserve,
            "resource_capacity": {
                "forecast_event_max_inflight": forecast_capacity,
                "python_stage2_max_inflight": stage2_capacity,
            },
            "limits": {"ensemble_size": ensemble_size},
            "run_profile": {
                "limits": {"max_runtime_seconds": runtime},
                "submission_policy": {"deadline_reserve_seconds": deadline_reserve},
            },
        }, separators=(",", ":")), encoding="utf-8")
        self.addCleanup(shutil.rmtree, directory)
        return path

    def test_derives_two_batches_four_waves_and_outer_reserve(self) -> None:
        result = btf25_timeout_plan.plan(self.profile(), 8, None, None)
        self.assertEqual(result["waves_per_batch"], 4)
        self.assertEqual(result["overall_timeout_seconds"], 2 * 4 * 3300 + 300)
        self.assertEqual(result["command_timeout_seconds"], result["overall_timeout_seconds"] + 120)

    def test_signed_capacity_reduces_requested_concurrency(self) -> None:
        result = btf25_timeout_plan.plan(
            self.profile(forecast_capacity=2, stage2_capacity=6),
            8,
            None,
            None,
        )
        self.assertEqual(result["requested_concurrency"], 8)
        self.assertEqual(result["effective_concurrency"], 2)
        self.assertEqual(result["waves_per_batch"], 13)
        self.assertEqual(result["overall_timeout_seconds"], 2 * 13 * 3300 + 300)

    def test_rejects_old_two_hour_overall_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot cover"):
            btf25_timeout_plan.plan(self.profile(), 8, "2h", None)

    def test_rejects_outer_timeout_not_above_engine_timeout(self) -> None:
        required = 2 * 4 * 3300 + 300
        with self.assertRaisesRegex(ValueError, "bounded container reserve"):
            btf25_timeout_plan.plan(self.profile(), 8, f"{required}s", required)

    def test_rejects_plan_above_engine_cap(self) -> None:
        with self.assertRaisesRegex(ValueError, "above the engine 24h cap"):
            btf25_timeout_plan.plan(self.profile(), 1, None, None)

    def test_rejects_unfrozen_runtime_drift(self) -> None:
        path = self.profile()
        document = json.loads(path.read_text(encoding="utf-8"))
        document["run_profile"]["limits"]["max_runtime_seconds"] -= 1
        path.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "differs from its frozen"):
            btf25_timeout_plan.plan(path, 8, None, None)


if __name__ == "__main__":
    unittest.main()
