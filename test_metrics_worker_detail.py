import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import AppDfaTask, AppDfaWorkerSlot, Base
from app.metrics import render_aggregate_metrics
from app.time_utils import now_local


class MetricsWorkerDetailTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def _insert_task(self, **kwargs):
        db = self.Session()
        try:
            row = AppDfaTask(
                task_id=kwargs.get("task_id", "dfa_test_1"),
                project_id=kwargs.get("project_id", "p1"),
                task_name=kwargs.get("task_name", "test"),
                input_path=kwargs.get("input_path", "/data/files/p1/input"),
                output_path=kwargs.get("output_path", "/data/files/p1/output"),
                prompt_content=kwargs.get("prompt_content", "analyse"),
                status=kwargs.get("status", "pending"),
                execution_owner_id=kwargs.get("execution_owner_id"),
                execution_lease_until=kwargs.get("execution_lease_until"),
                execution_heartbeat_at=kwargs.get("execution_heartbeat_at"),
                dispatch_status=kwargs.get("dispatch_status"),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()

    def _insert_worker(self, **kwargs):
        db = self.Session()
        try:
            row = AppDfaWorkerSlot(
                worker_id=kwargs.get("worker_id", "pod-a"),
                pod_name=kwargs.get("pod_name", "pod-a"),
                pod_ip=kwargs.get("pod_ip"),
                max_concurrent_tasks=kwargs.get("max_concurrent_tasks", 2),
                last_seen_status="running",
                last_heartbeat_at=kwargs.get("last_heartbeat_at", now_local()),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()

    def test_aggregate_metrics_export_worker_detail_samples(self):
        now = now_local()
        self._insert_worker(worker_id="pod-a", pod_name="pod-a", max_concurrent_tasks=2, last_heartbeat_at=now)
        self._insert_task(
            task_id="dfa_running",
            status="running",
            execution_owner_id="pod-a",
            execution_lease_until=now,
            execution_heartbeat_at=now,
            dispatch_status="running",
        )
        self._insert_task(
            task_id="dfa_pending",
            status="pending",
            execution_owner_id="pod-a",
            execution_lease_until=now,
            execution_heartbeat_at=now,
            dispatch_status="leased",
        )

        session_factory = self.Session

        def fake_get_db():
            db = session_factory()
            try:
                yield db
            finally:
                db.close()

        with patch("app.db.get_db", fake_get_db):
            rendered = render_aggregate_metrics()

        self.assertIn('secflow_dfa_cluster_worker_runtime{worker_id="pod-a",host_name="pod-a",healthy="true",source="worker_registry",kind="capacity"} 2', rendered)
        self.assertIn('secflow_dfa_cluster_worker_runtime{worker_id="pod-a",host_name="pod-a",healthy="true",source="worker_registry",kind="running_jobs"} 1', rendered)
        self.assertIn('secflow_dfa_cluster_worker_slots{kind="capacity"} 2', rendered)
        self.assertIn('secflow_dfa_cluster_worker_slots{kind="busy"} 1', rendered)
        self.assertIn('secflow_dfa_cluster_worker_slots{kind="free"} 1', rendered)
        self.assertIn('secflow_dfa_cluster_worker_active_jobs{worker_id="pod-a",host_name="pod-a",status="pending"} 1', rendered)
        self.assertIn('secflow_dfa_cluster_worker_active_jobs{worker_id="pod-a",host_name="pod-a",status="running"} 1', rendered)

    def test_aggregate_metrics_export_orphan_running_samples(self):
        now = now_local()
        self._insert_task(
            task_id="dfa_orphan_running",
            status="running",
            execution_owner_id=None,
            execution_lease_until=None,
            execution_heartbeat_at=None,
            dispatch_status=None,
        )

        session_factory = self.Session

        def fake_get_db():
            db = session_factory()
            try:
                yield db
            finally:
                db.close()

        with patch("app.db.get_db", fake_get_db):
            rendered = render_aggregate_metrics()

        self.assertIn("secflow_dfa_cluster_orphan_running_tasks 1", rendered)
        self.assertIn("secflow_dfa_cluster_running_without_owner 1", rendered)


if __name__ == "__main__":
    unittest.main()
