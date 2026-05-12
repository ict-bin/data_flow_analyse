import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import build_task_config
from app.models import ServiceConfig, normalize_max_rounds_exceeded_review_strategy


class MaxRoundsExceededReviewPolicyTests(unittest.TestCase):
    def test_normalize_strategy_defaults_to_treat_as_passed(self):
        self.assertEqual(
            normalize_max_rounds_exceeded_review_strategy(None),
            "treat_as_passed",
        )
        self.assertEqual(
            normalize_max_rounds_exceeded_review_strategy("invalid"),
            "treat_as_passed",
        )

    def test_build_task_config_carries_strategy(self):
        svc = ServiceConfig(
            max_rounds_exceeded_review_strategy="treat_as_failed",
            workers={"agents": [{"model": "worker-model"}]},
            judges={"agents": [{"model": "judge-model"}]},
        )
        cfg = build_task_config(svc, "分析 main.c 中 handle_request 的数据流", cwd="/tmp")
        self.assertEqual(cfg.max_rounds_exceeded_review_strategy, "treat_as_failed")


if __name__ == "__main__":
    unittest.main()
