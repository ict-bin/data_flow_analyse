import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import AppDfaTask, Base
from app.service.worker_snapshot import build_worker_cluster_snapshot
from app.time_utils import now_local


class WorkerSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def _session(self):
        return self.Session()

    def _insert_task(self, **kwargs):
        db = self._session()
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
                execution_epoch=kwargs.get("execution_epoch", 0),
                control_version=kwargs.get("control_version", 0),
                dispatch_status=kwargs.get("dispatch_status"),
                parent_task_id=kwargs.get("parent_task_id"),
                parent_task_type=kwargs.get("parent_task_type"),
                task_origin_type=kwargs.get("task_origin_type"),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()

    def test_single_worker_running_task(self):
        now = now_local()
        self._insert_task(
            status="running",
            execution_owner_id="pod-a:abcd1234",
            execution_lease_until=now,
            execution_heartbeat_at=now,
        )
        db = self._session()
        try:
            with patch("app.service.worker_snapshot.MAX_LOCAL_RUNNING_TASKS", 2):
                snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(1, snapshot.worker_count)
            self.assertEqual(2, snapshot.total_capacity)
            self.assertEqual(1, snapshot.running_jobs)
            self.assertEqual(1, snapshot.available_slots)
            worker = snapshot.workers[0]
            self.assertEqual("pod-a:abcd1234", worker.worker_id)
            self.assertEqual("pod-a", worker.host_name)
            self.assertTrue(worker.healthy)
            self.assertEqual(1, len(worker.active_jobs))
            self.assertEqual("dfa_test_1", worker.active_jobs[0].task_id)
        finally:
            db.close()

    def test_multiple_running_tasks_aggregate_to_same_worker(self):
        now = now_local()
        self._insert_task(task_id="dfa_a", status="running", execution_owner_id="pod-a:1", execution_lease_until=now, execution_heartbeat_at=now)
        self._insert_task(task_id="dfa_b", status="running", execution_owner_id="pod-a:1", execution_lease_until=now, execution_heartbeat_at=now)
        db = self._session()
        try:
            with patch("app.service.worker_snapshot.MAX_LOCAL_RUNNING_TASKS", 3):
                snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(1, snapshot.worker_count)
            self.assertEqual(2, snapshot.running_jobs)
            self.assertEqual(2, len(snapshot.workers[0].active_jobs))
            self.assertEqual(1, snapshot.available_slots)
        finally:
            db.close()

    def test_multiple_workers_parse_host_name(self):
        now = now_local()
        self._insert_task(task_id="dfa_a", status="running", execution_owner_id="worker-a:aaaa", execution_lease_until=now)
        self._insert_task(task_id="dfa_b", status="running", execution_owner_id="worker-b", execution_lease_until=now)
        db = self._session()
        try:
            snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(2, snapshot.worker_count)
            host_names = {worker.worker_id: worker.host_name for worker in snapshot.workers}
            self.assertEqual("worker-a", host_names["worker-a:aaaa"])
            self.assertEqual("worker-b", host_names["worker-b"])
        finally:
            db.close()

    def test_pending_queue_jobs_stay_at_cluster_level(self):
        self._insert_task(task_id="dfa_pending", status="pending", execution_owner_id=None)
        db = self._session()
        try:
            snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(1, snapshot.queued_jobs)
            self.assertEqual(0, snapshot.worker_count)
        finally:
            db.close()

    def test_stale_worker_marked_unhealthy(self):
        stale = now_local()
        self._insert_task(
            status="running",
            execution_owner_id="pod-stale:1234",
            execution_lease_until=stale - __import__("datetime").timedelta(seconds=300),
            execution_heartbeat_at=stale - __import__("datetime").timedelta(seconds=300),
        )
        db = self._session()
        try:
            with patch("app.service.worker_snapshot.HEARTBEAT_INTERVAL_SECONDS", 15):
                snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(1, snapshot.worker_count)
            worker = snapshot.workers[0]
            self.assertFalse(worker.healthy)
            self.assertEqual(0, worker.available_slots)
            self.assertIn("stale", worker.error or "")
        finally:
            db.close()

    def test_terminal_tasks_without_owner_do_not_create_workers(self):
        self._insert_task(task_id="dfa_done", status="passed", execution_owner_id=None)
        db = self._session()
        try:
            snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(0, snapshot.worker_count)
        finally:
            db.close()

    def test_empty_owner_is_ignored(self):
        self._insert_task(task_id="dfa_empty", status="running", execution_owner_id="   ")
        db = self._session()
        try:
            snapshot = build_worker_cluster_snapshot(db, project_id="p1")
            self.assertEqual(0, snapshot.worker_count)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
