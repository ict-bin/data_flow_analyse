import sys
import unittest
from datetime import timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.db.models import AppDfaTask, Base
from app.service.execution_coordinator import (
    begin_execution_if_owner,
    claim_one_runnable_task,
    commit_terminal_state_if_owner,
    recover_running_task_if_owner,
    reclaim_orphaned_running_tasks,
    renew_lease,
    still_owner,
)
from app.runtime_context import LEASE_REQUEUE_DELAY_SECONDS
from app.time_utils import now_local


class ExecutionCoordinatorTests(unittest.TestCase):
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
                execution_epoch=kwargs.get("execution_epoch", 0),
                control_version=kwargs.get("control_version", 0),
                dispatch_status=kwargs.get("dispatch_status"),
                lease_lost_count=kwargs.get("lease_lost_count", 0),
                last_lease_lost_at=kwargs.get("last_lease_lost_at"),
                lease_requeue_not_before=kwargs.get("lease_requeue_not_before"),
                last_lease_lost_epoch=kwargs.get("last_lease_lost_epoch"),
                last_lease_lost_control_version=kwargs.get("last_lease_lost_control_version"),
            )
            db.add(row)
            db.commit()
        finally:
            db.close()

    def test_claim_one_runnable_task_assigns_owner_and_epoch(self):
        self._insert_task()
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-a")
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.task_id, "dfa_test_1")
            self.assertEqual(claimed.epoch, 1)
            self.assertEqual(claimed.dispatch_status, "leased")
            row = db.query(AppDfaTask).filter_by(task_id="dfa_test_1").first()
            self.assertEqual(row.execution_owner_id, "pod-a")
            self.assertEqual(row.execution_epoch, 1)
            self.assertEqual(row.dispatch_status, "leased")
        finally:
            db.close()

    def test_second_claim_fails_while_lease_is_live(self):
        self._insert_task()
        db1 = self._session()
        db2 = self._session()
        try:
            claimed = claim_one_runnable_task(db1, "pod-a")
            self.assertIsNotNone(claimed)
            claimed2 = claim_one_runnable_task(db2, "pod-b")
            self.assertIsNone(claimed2)
        finally:
            db1.close()
            db2.close()

    def test_claim_reacquires_running_task_with_expired_lease(self):
        self._insert_task(
            status="running",
            execution_owner_id="pod-old",
            execution_lease_until=now_local(),
            execution_epoch=3,
            control_version=2,
            dispatch_status="running",
        )
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-new")
            self.assertIsNotNone(claimed)
            self.assertEqual(claimed.epoch, 4)
            self.assertEqual(claimed.dispatch_status, "leased")
            row = db.query(AppDfaTask).filter_by(task_id="dfa_test_1").first()
            self.assertEqual(row.execution_owner_id, "pod-new")
            self.assertEqual(row.status, "pending")
            self.assertEqual(row.dispatch_status, "leased")
            self.assertEqual(row.execution_epoch, 4)
        finally:
            db.close()

    def test_claim_does_not_reacquire_running_task_with_live_lease(self):
        future = now_local() + timedelta(hours=1)
        self._insert_task(
            status="running",
            execution_owner_id="pod-old",
            execution_lease_until=future,
            execution_epoch=3,
            control_version=2,
            dispatch_status="running",
        )
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-new")
            self.assertIsNone(claimed)
            row = db.query(AppDfaTask).filter_by(task_id="dfa_test_1").first()
            self.assertEqual(row.execution_owner_id, "pod-old")
            self.assertEqual(row.status, "running")
            self.assertEqual(row.execution_epoch, 3)
        finally:
            db.close()

    def test_begin_and_commit_terminal_state_require_current_owner(self):
        self._insert_task()
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-a")
            self.assertTrue(begin_execution_if_owner(db, claimed.task_id, "pod-a", claimed.epoch, claimed.control_version, started_at=now_local()))
            self.assertTrue(still_owner(db, claimed.task_id, "pod-a", claimed.epoch, claimed.control_version))
            self.assertTrue(
                commit_terminal_state_if_owner(
                    db,
                    claimed.task_id,
                    "pod-a",
                    claimed.epoch,
                    claimed.control_version,
                    status="passed",
                    finished_at=now_local(),
                    stages_json={"events": [], "final": True},
                    result_json={"status": "passed"},
                    error=None,
                )
            )
            row = db.query(AppDfaTask).filter_by(task_id=claimed.task_id).first()
            self.assertEqual(row.status, "passed")
            self.assertIsNone(row.execution_owner_id)
            self.assertEqual(row.result_json["status"], "passed")
        finally:
            db.close()

    def test_commit_terminal_state_rejects_stale_epoch(self):
        self._insert_task()
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-a")
            self.assertTrue(begin_execution_if_owner(db, claimed.task_id, "pod-a", claimed.epoch, claimed.control_version, started_at=now_local()))
            self.assertFalse(
                commit_terminal_state_if_owner(
                    db,
                    claimed.task_id,
                    "pod-a",
                    claimed.epoch + 1,
                    claimed.control_version,
                    status="passed",
                    finished_at=now_local(),
                    stages_json={"events": [], "final": True},
                    result_json={"status": "passed"},
                    error=None,
                )
            )
            row = db.query(AppDfaTask).filter_by(task_id=claimed.task_id).first()
            self.assertEqual(row.status, "running")
        finally:
            db.close()

    def test_renew_lease_requires_running_state(self):
        self._insert_task()
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-a")
            self.assertFalse(renew_lease(db, claimed.task_id, "pod-a", claimed.epoch))
            self.assertTrue(begin_execution_if_owner(db, claimed.task_id, "pod-a", claimed.epoch, claimed.control_version, started_at=now_local()))
            self.assertTrue(renew_lease(db, claimed.task_id, "pod-a", claimed.epoch))
        finally:
            db.close()

    def test_recover_running_task_if_owner_requeues_without_orphan_running(self):
        future = now_local() + timedelta(hours=1)
        self._insert_task(
            status="running",
            execution_owner_id="pod-a",
            execution_lease_until=future,
            execution_epoch=2,
            control_version=3,
            dispatch_status="running",
        )
        db = self._session()
        try:
            self.assertTrue(recover_running_task_if_owner(db, "dfa_test_1", "pod-a", 2, 3))
            row = db.query(AppDfaTask).filter_by(task_id="dfa_test_1").first()
            self.assertEqual(row.status, "pending")
            self.assertEqual(row.dispatch_status, "pending")
            self.assertIsNone(row.execution_owner_id)
            self.assertIsNone(row.execution_lease_until)
            self.assertIsNone(row.execution_heartbeat_at)
            self.assertEqual(1, int(row.lease_lost_count or 0))
            self.assertIsNotNone(row.last_lease_lost_at)
            self.assertIsNotNone(row.lease_requeue_not_before)
            self.assertEqual(2, int(row.last_lease_lost_epoch or 0))
            self.assertEqual(3, int(row.last_lease_lost_control_version or 0))
            self.assertEqual(3, int(row.execution_epoch or 0))
            self.assertGreaterEqual(
                (row.lease_requeue_not_before - row.last_lease_lost_at).total_seconds(),
                LEASE_REQUEUE_DELAY_SECONDS - 1,
            )
        finally:
            db.close()

    def test_claim_skips_task_during_lease_requeue_backoff(self):
        now = now_local()
        self._insert_task(
            execution_lease_until=None,
            lease_requeue_not_before=now + timedelta(seconds=30),
        )
        db = self._session()
        try:
            claimed = claim_one_runnable_task(db, "pod-a")
            self.assertIsNone(claimed)
        finally:
            db.close()

    def test_reclaim_orphaned_running_tasks_repairs_missing_owner_and_expired_lease(self):
        expired = now_local() - timedelta(minutes=5)
        future = now_local() + timedelta(hours=1)
        self._insert_task(
            task_id="dfa_missing_owner",
            status="running",
            execution_owner_id=None,
            execution_lease_until=future,
            execution_epoch=1,
            control_version=0,
            dispatch_status=None,
        )
        self._insert_task(
            task_id="dfa_expired_lease",
            status="running",
            execution_owner_id="pod-old",
            execution_lease_until=expired,
            execution_epoch=4,
            control_version=1,
            dispatch_status="running",
        )
        db = self._session()
        try:
            recovered = reclaim_orphaned_running_tasks(db)
            recovered_ids = {item.task_id: item.reason for item in recovered}
            self.assertEqual(recovered_ids["dfa_missing_owner"], "missing_owner")
            self.assertEqual(recovered_ids["dfa_expired_lease"], "expired_lease")
            rows = {row.task_id: row for row in db.query(AppDfaTask).all()}
            self.assertEqual(rows["dfa_missing_owner"].status, "pending")
            self.assertEqual(rows["dfa_missing_owner"].dispatch_status, "pending")
            self.assertIsNone(rows["dfa_missing_owner"].execution_owner_id)
            self.assertEqual(1, int(rows["dfa_missing_owner"].lease_lost_count or 0))
            self.assertIsNotNone(rows["dfa_missing_owner"].lease_requeue_not_before)
            self.assertEqual(rows["dfa_expired_lease"].status, "pending")
            self.assertEqual(rows["dfa_expired_lease"].dispatch_status, "pending")
            self.assertIsNone(rows["dfa_expired_lease"].execution_owner_id)
            self.assertEqual(1, int(rows["dfa_expired_lease"].lease_lost_count or 0))
            self.assertIsNotNone(rows["dfa_expired_lease"].lease_requeue_not_before)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
