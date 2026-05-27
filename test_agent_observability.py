import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import AppDfaTask, Base
from app.service.agent_observability import AgentObservabilityService


class AgentObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.service = AgentObservabilityService()

    def _session(self):
        return self.Session()

    def _insert_task(self, **kwargs):
        db = self._session()
        try:
            row = AppDfaTask(
                task_id=kwargs.get("task_id", "dfa_obs_1"),
                project_id=kwargs.get("project_id", "p1"),
                task_name=kwargs.get("task_name", "observability test"),
                input_path=kwargs.get("input_path", "/tmp/input"),
                output_path=kwargs.get("output_path", "/tmp/output"),
                prompt_content=kwargs.get("prompt_content", "analyse"),
                status=kwargs.get("status", "running"),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()

    def test_build_snapshot_handles_task_without_session_crash(self):
        self._insert_task()
        db = self._session()
        try:
            with patch("app.service.agent_observability._iter_agent_processes", return_value=[]):
                snapshot = self.service.build_snapshot(db, project_id="p1")
            self.assertIn("summary", snapshot)
            self.assertIn("processes", snapshot)
            self.assertIn("sessions", snapshot)
            self.assertIn("tasks", snapshot)
            self.assertEqual(0, len(snapshot["processes"]))
            self.assertEqual(0, len(snapshot["sessions"]))
            self.assertEqual(1, len(snapshot["tasks"]))
            self.assertEqual("dfa_obs_1", snapshot["tasks"][0]["task_id"])
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
