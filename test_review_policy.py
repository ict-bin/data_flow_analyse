import sys
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import build_task_config
from app.service.config_service import get_config_service
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

    def test_config_service_defaults_strategy_to_treat_as_passed(self):
        mock_db = mock.MagicMock()
        mock_db.query.return_value.filter_by.return_value.first.return_value = None
        cfg = get_config_service().get_config(db=mock_db, project_id="p1")
        self.assertEqual(cfg["max_rounds_exceeded_review_strategy"], "treat_as_passed")

    def test_config_service_normalizes_invalid_strategy_on_save(self):
        mock_db = mock.MagicMock()
        mock_query = mock_db.query.return_value
        mock_filter = mock_query.filter_by.return_value
        mock_filter.first.return_value = None
        saved = get_config_service().save_config(
            db=mock_db,
            project_id="p1",
            config_data={
                "project_id": "p1",
                "max_rounds_exceeded_review_strategy": "invalid",
            },
        )
        self.assertEqual(saved["max_rounds_exceeded_review_strategy"], "treat_as_passed")


if __name__ == "__main__":
    unittest.main()
