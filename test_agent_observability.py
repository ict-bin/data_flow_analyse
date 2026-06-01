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
            self.assertIn("sessions", snapshot)
            self.assertIn("tasks", snapshot)
            self.assertEqual(0, len(snapshot["processes"]))
            self.assertEqual(0, len(snapshot["sessions"]))
            self.assertEqual(1, len(snapshot["tasks"]))
            self.assertEqual("dfa_obs_1", snapshot["tasks"][0]["task_id"])
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
            self.assertIn("suspected_orphan_process_count", pod)
            self.assertIn("task_count", pod)
            self.assertIn("active_task_count", pod)
            self.assertIn("last_scanned_at", pod)
            self.assertEqual(1, pod["suspected_orphan_process_count"])
            self.assertTrue(snapshot["processes"][0]["kill_allowed"])
        finally:
            db.close()

    def test_build_snapshot_keeps_unknown_non_killable_when_live_task_signals_exist(self):
        db = self._session()
        try:
            row = AppDfaTask(
                task_id="dfa_obs_live",
                project_id="p1",
                task_name="live task",
                input_path="/tmp/input",
                output_path="/tmp/output",
                prompt_content="analyse",
                status="running",
            )
            db.add(row)
            db.commit()
            with patch("app.service.agent_observability._iter_agent_processes", return_value=[]), \
                 patch("app.service.agent_observability.get_task_service") as mocked_service, \
                 patch("app.service.agent_observability.build_worker_cluster_snapshot") as mocked_cluster:
                mocked_service.return_value.get_task_session_index.return_value = {
                    "nodes": [
                        {
                            "relative_path": "session-1.jsonl",
                            "session_name": "session-1",
                            "display_name": "session-1",
                            "is_active": True,
                            "stage_key": "taint",
                            "role": "worker",
                            "session_header": {"id": "s1"},
                        }
                    ]
                }
                mocked_cluster.return_value.workers = []
                with patch("app.service.agent_observability._iter_agent_processes", return_value=[
                    {
                        "pid": 202,
                        "ppid": 1,
                        "pgid": 202,
                        "command": "npx pi worker",
                        "cwd": "/tmp/session-1.jsonl",
                        "rss_bytes": 1024,
                    }
                ]):
                    snapshot = self.service.build_snapshot(db, project_id="p1")
            self.assertEqual("unknown", snapshot["processes"][0]["owner_kind"])
            self.assertFalse(snapshot["processes"][0]["kill_allowed"])
        finally:
            db.close()

    def test_build_snapshot_matches_workspace_root_without_session_cwd_overlap(self):
        db = self._session()
        try:
            row = AppDfaTask(
                task_id="dfa_obs_workspace",
                project_id="p1",
                task_name="workspace task",
                input_path="/tmp/input",
                module_input_path="/tmp/workspace-worker-1/module",
                source_root_path="/tmp/workspace-worker-1/src",
                output_path="/tmp/workspace-worker-1/out",
                prompt_content="analyse",
                status="running",
            )
            db.add(row)
            db.commit()
            with patch("app.service.agent_observability.get_task_service") as mocked_service, \
                 patch("app.service.agent_observability.build_worker_cluster_snapshot") as mocked_cluster, \
                 patch("app.service.agent_observability._iter_agent_processes", return_value=[
                     {
                         "pid": 303,
                         "ppid": 1,
                         "pgid": 303,
                         "command": "codex --session /tmp/workspace-worker-1/out/sessions/r1/agent.jsonl",
                         "cwd": "/tmp/workspace-worker-1",
                         "exe": "/usr/bin/codex",
                         "rss_bytes": 8192,
                         "runtime_kind": "codex",
                         "session_arg_path": "/tmp/workspace-worker-1/out/sessions/r1/agent.jsonl",
                         "open_session_paths": [],
                     }
                 ]):
                mocked_service.return_value.get_task_session_index.return_value = {
                    "nodes": [
                        {
                            "relative_path": "sessions/r1/agent.jsonl",
                            "session_name": "agent",
                            "display_name": "agent",
                            "is_active": True,
                            "stage_key": "taint",
                            "role": "worker",
                            "session_header": {"id": "sess-1"},
                        }
                    ]
                }
                mocked_cluster.return_value.workers = []
                snapshot = self.service.build_snapshot(db, project_id="p1")
            self.assertEqual(1, len(snapshot["processes"]))
            process = snapshot["processes"][0]
            self.assertEqual("codex", process["runtime_kind"])
            self.assertEqual("session_path", process["match_source"])
            self.assertEqual("dfa_obs_workspace", process["task_id"])
        finally:
            db.close()

    def test_resolve_worker_targets_prefers_pod_ip_then_pod_name(self):
        self.assertEqual(
            ["10.0.0.8"],
            tasks_api._resolve_worker_targets(pod_ip="10.0.0.8", pod_name="dfa-worker-1"),
        )
        self.assertEqual(["dfa-worker-1"], tasks_api._resolve_worker_targets(pod_ip=None, pod_name="dfa-worker-1"))

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

if __name__ == "__main__":
    unittest.main()
