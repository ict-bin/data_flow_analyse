import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import AppDfaTask, Base
from app.api import tasks as tasks_api
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
            self.assertIn("tasks", snapshot)
            self.assertIn("pods", snapshot)
            self.assertEqual(0, len(snapshot["processes"]))
            self.assertEqual(0, len(snapshot["tasks"]))
            self.assertEqual(1, len(snapshot["pods"]))
        finally:
            db.close()

    def test_build_snapshot_exposes_pod_runtime_summary_fields(self):
        self._insert_task(task_id="dfa_obs_runtime", status="success")
        db = self._session()
        try:
            with patch("app.service.agent_observability._iter_agent_processes", return_value=[
                {
                    "pid": 101,
                    "ppid": 1,
                    "pgid": 101,
                    "command": "npx pi worker",
                    "cwd": "/tmp/unknown",
                    "rss_bytes": 2048,
                }
            ]):
                snapshot = self.service.build_snapshot(db, project_id="p1")
            self.assertIn("pods", snapshot)
            self.assertEqual(1, len(snapshot["pods"]))
            pod = snapshot["pods"][0]
            self.assertIn("worker_id", pod)
            self.assertIn("healthy", pod)
            self.assertIn("tracked_process_count", pod)
            self.assertIn("unknown_process_count", pod)
            self.assertIn("task_count", pod)
            self.assertIn("running_task_count", pod)
            self.assertIn("last_scanned_at", pod)
            self.assertEqual(1, pod["unknown_process_count"])
            self.assertTrue(snapshot["processes"][0]["kill_allowed"])
        finally:
            db.close()

    def test_resolve_worker_targets_prefers_pod_ip_then_pod_name(self):
        self.assertEqual(
            ["10.0.0.8", "dfa-worker-1"],
            tasks_api._resolve_worker_targets(pod_ip="10.0.0.8", pod_name="dfa-worker-1"),
        )
        self.assertEqual(["dfa-worker-1"], tasks_api._resolve_worker_targets(pod_ip=None, pod_name="dfa-worker-1"))

    def test_aggregate_base_urls_prefers_worker_http_port(self):
        worker = type("Worker", (), {"pod_ip": "10.0.0.8", "pod_name": "dfa-worker-1", "http_port": 8080})()
        self.assertEqual(
            [
                "http://10.0.0.8:8080/api/app/dataflow-analyse",
                "http://dfa-worker-1:8080/api/app/dataflow-analyse",
            ],
            tasks_api._aggregate_base_urls(worker),
        )

    def test_build_agent_runtime_aggregate_prefers_summary_pod_counts(self):
        snapshot = {
            "summary": {
                "total_pods": 6,
                "healthy_pods": 5,
                "aggregate_partial": True,
                "aggregate_sources": 2,
                "aggregate_failed_targets": ["dfa-worker-3"],
                "aggregate_all_sources_failed": False,
                "scanned_at": 123.0,
            },
            "pods": [{"pod_name": "pod-a", "healthy": True}],
            "processes": [],
            "tasks": [],
        }

        runtime = tasks_api._build_agent_runtime_aggregate(snapshot)
        self.assertEqual(6, runtime["summary"]["total_pods"])
        self.assertEqual(5, runtime["summary"]["healthy_pods"])

    def test_build_agent_runtime_aggregate_exposes_failed_target_details(self):
        snapshot = {
            "summary": {
                "aggregate_partial": True,
                "aggregate_sources": 2,
                "aggregate_fanout_errors": 1,
                "aggregate_failed_targets": ["dfa-worker-3"],
                "aggregate_failed_target_details": [
                    {
                        "pod_name": "dfa-worker-3",
                        "pod_ip": "10.0.0.33",
                        "http_port": 8080,
                        "attempted_urls": ["http://10.0.0.33:8080/api/app/dataflow-analyse"],
                        "error_kind": "connect_timeout",
                        "status_code": None,
                        "message": "connect timeout",
                    }
                ],
                "aggregate_all_sources_failed": False,
            },
            "pods": [],
            "processes": [],
            "tasks": [],
        }

        runtime = tasks_api._build_agent_runtime_aggregate(snapshot)
        self.assertEqual(
            "connect_timeout",
            runtime["summary"]["aggregate_failed_target_details"][0]["error_kind"],
        )

if __name__ == "__main__":
    unittest.main()
