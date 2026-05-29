import sys
import tempfile
import unittest
import os
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.api import router as api_router
from app.api import tasks as tasks_api
from app.db.models import AppDfaTask, AppDfaTaskEvent, Base
from app.service import task_service as task_service_module
from app.service.task_service import TaskService


class TaskTimelineTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.service = TaskService()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.previous_fileserver_root = os.environ.get("FILESERVER_ROOT")
        self.project_id = "p1"
        self.files_root = Path(self.tmpdir.name) / "files"
        os.environ["FILESERVER_ROOT"] = str(self.files_root)
        self.input_dir = self.files_root / "input"
        self.output_dir = self.files_root / "output"
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        if self.previous_fileserver_root is None:
            os.environ.pop("FILESERVER_ROOT", None)
        else:
            os.environ["FILESERVER_ROOT"] = self.previous_fileserver_root
        self.tmpdir.cleanup()

    def _session(self):
        return self.Session()

    def _build_client(self):
        app = FastAPI()
        app.include_router(api_router)

        def _override_get_db():
            db = self._session()
            try:
                yield db
            finally:
                db.close()

        app.dependency_overrides[tasks_api.get_db] = _override_get_db
        return TestClient(app)

    def _create_task(self, **kwargs):
        db = self._session()
        try:
            payload = self.service.create_task(
                db,
                project_id=self.project_id,
                task_name=kwargs.get("task_name", "dfa timeline test"),
                input_path=str(kwargs.get("input_path", self.input_dir)),
                module_input_path=str(kwargs.get("module_input_path", self.input_dir)),
                source_root_path=str(kwargs.get("source_root_path", self.input_dir)),
                output_path=str(kwargs.get("output_path", self.output_dir)),
                prompt_content=kwargs.get("prompt_content", "analyse"),
                task_origin_type=kwargs.get("task_origin_type", "manual"),
            )
            return payload["task_id"]
        finally:
            db.close()

    def test_create_task_records_task_created_timeline_event(self):
        task_id = self._create_task()
        db = self._session()
        try:
            events = db.query(AppDfaTaskEvent).filter_by(task_id=task_id).all()
            self.assertEqual(1, len(events))
            self.assertEqual("task_created", events[0].event_type)
            self.assertEqual("pending", events[0].status)
        finally:
            db.close()

    def test_get_timeline_returns_events_in_descending_order(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            extra = AppDfaTaskEvent(
                id="evt-extra",
                task_id=task_id,
                project_id=self.project_id,
                source="dfa",
                level="info",
                event_type="task_started",
                status="running",
                message="任务已开始执行",
                dedupe_key="dedupe-task-started",
            )
            db.add(extra)
            db.commit()

            timeline = self.service.get_task_timeline(db, task_id)
            self.assertEqual(task_id, timeline["task_id"])
            self.assertEqual("task_started", timeline["events"][0]["event_type"])
            self.assertEqual("task_created", timeline["events"][-1]["event_type"])
            self.assertEqual(row.project_id, timeline["events"][0]["project_id"])
        finally:
            db.close()

    def test_clear_and_delete_timeline_events(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            second = AppDfaTaskEvent(
                id="evt-second",
                task_id=task_id,
                project_id=row.project_id,
                source="dfa",
                level="warning",
                event_type="task_cancelled",
                status="cancelled",
                message="任务已取消",
                dedupe_key="dedupe-task-cancelled",
            )
            db.add(second)
            db.commit()

            deleted_one = self.service.delete_task_timeline_event(db, task_id, "evt-second")
            self.assertEqual(1, deleted_one)
            db.commit()
            remaining = db.query(AppDfaTaskEvent).filter_by(task_id=task_id).all()
            self.assertEqual(1, len(remaining))

            deleted_all = self.service.clear_task_timeline(db, task_id)
            self.assertEqual(1, deleted_all)
            db.commit()
            self.assertEqual(0, db.query(AppDfaTaskEvent).filter_by(task_id=task_id).count())
        finally:
            db.close()

    def test_restart_task_records_task_retried_event(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "failed"
            row.control_version = 2
            row.execution_epoch = 4
            row.execution_owner_id = "worker-a"
            row.dispatch_status = "leased"
            db.commit()

            payload = self.service.restart_task(db, task_id)

            self.assertEqual("pending", payload["status"])
            self.assertEqual(3, payload["control_version"])
            events = self.service.get_task_timeline(db, task_id)["events"]
            self.assertEqual("task_retried", events[0]["event_type"])
            self.assertEqual(3, events[0]["control_version"])
            self.assertEqual("pending", events[0]["dispatch_status"])
        finally:
            db.close()

    def test_resume_task_records_task_resumed_event(self):
        task_id = self._create_task()
        db = self._session()
        previous_loader = task_service_module._load_svc_config_from_db
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "failed"
            row.execution_epoch = 2
            row.control_version = 5
            db.commit()

            task_service_module._load_svc_config_from_db = lambda _db, _project_id: SimpleNamespace(
                output_dir=str(self.output_dir)
            )
            payload = self.service.resume_task(db, task_id)

            self.assertEqual("pending", payload["status"])
            self.assertEqual(6, payload["control_version"])
            timeline = self.service.get_task_timeline(db, task_id)
            self.assertEqual("task_resumed", timeline["events"][0]["event_type"])
            self.assertEqual(3, timeline["events"][0]["payload"]["start_stage"])
            self.assertIn(f"{task_id}/run/epochs/0002/workspace-worker-0", timeline["events"][0]["payload"]["resume_workspace"])
        finally:
            task_service_module._load_svc_config_from_db = previous_loader
            db.close()

    def test_cancel_task_records_task_cancelled_event(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "running"
            row.control_version = 1
            row.execution_epoch = 2
            row.execution_owner_id = "worker-x"
            row.dispatch_status = "running"
            db.commit()

            payload = self.service.cancel_task(db, task_id)

            self.assertEqual("cancelled", payload["status"])
            timeline = self.service.get_task_timeline(db, task_id)
            self.assertEqual("task_cancelled", timeline["events"][0]["event_type"])
            self.assertEqual("cancelled", timeline["events"][0]["status"])
            self.assertEqual(2, timeline["events"][0]["control_version"])
        finally:
            db.close()

    def test_cancel_task_aborts_local_orchestrator_and_runs_targeted_cleanup(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "running"
            row.control_version = 3
            row.execution_epoch = 4
            row.execution_owner_id = "worker-x"
            row.dispatch_status = "running"
            db.commit()

            cancel_calls: list[str] = []

            class _FakeLocalTask:
                def done(self):
                    return False

                def cancel(self):
                    cancel_calls.append("task")

            class _FakeOrchestrator:
                def abort(self):
                    cancel_calls.append("orch")

            task_service_module._running_tasks[task_id] = _FakeLocalTask()
            task_service_module._running_task_contexts[task_id] = task_service_module._RunningTaskContext(
                task=task_service_module._running_tasks[task_id],
                orch=_FakeOrchestrator(),
                task_root="/tmp/dfa-task",
                run_root="/tmp/dfa-task/run/epochs/0004",
                epoch=4,
                control_version=3,
            )
            with patch("app.service.task_service.cleanup_task_agent_processes", return_value=2) as cleanup:
                payload = self.service.cancel_task(db, task_id)

            self.assertEqual("cancelled", payload["status"])
            self.assertEqual(["orch", "task"], cancel_calls)
            cleanup.assert_called_once()
            timeline = self.service.get_task_timeline(db, task_id)
            self.assertTrue(bool(timeline["events"][0]["payload"]["orchestrator_abort_sent"]))
            self.assertEqual("/tmp/dfa-task", timeline["events"][0]["payload"]["cleanup_task_root"])
        finally:
            task_service_module._running_tasks.pop(task_id, None)
            task_service_module._running_task_contexts.pop(task_id, None)
            db.close()

    def test_delete_task_records_task_deleted_event_before_soft_delete(self):
        task_id = self._create_task()
        task_dir = self.output_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        (task_dir / "marker.txt").write_text("ok", encoding="utf-8")
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "failed"
            row.control_version = 7
            db.commit()

            self.service.delete_task(db, task_id, delete_files=True)

            deleted_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertTrue(bool(deleted_row.is_deleted))
            deleted_event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_deleted").first()
            self.assertIsNotNone(deleted_event)
            self.assertEqual("failed", deleted_event.status)
            self.assertTrue(bool(deleted_event.payload.get("delete_files")))
            self.assertTrue(bool(deleted_event.payload.get("files_deleted")))
            self.assertEqual(str(task_dir), deleted_event.payload.get("task_dir"))
            self.assertEqual("failed", deleted_event.payload.get("status_before_delete"))
        finally:
            db.close()

    def test_delete_task_rejected_records_warning_without_soft_delete(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "running"
            row.execution_owner_id = "worker-a"
            row.execution_lease_until = task_service_module.now_local()
            row.dispatch_status = "running"
            db.commit()

            with self.assertRaises(HTTPException) as ctx:
                self.service.delete_task(db, task_id, delete_files=False)

            self.assertEqual(409, ctx.exception.status_code)
            refreshed = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertFalse(bool(refreshed.is_deleted))
            rejected_event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_delete_rejected").first()
            self.assertIsNotNone(rejected_event)
            self.assertEqual("warning", rejected_event.level)
            self.assertFalse(bool(rejected_event.payload.get("delete_files")))
            self.assertIn("lease_live", rejected_event.payload)
            self.assertFalse(bool(rejected_event.payload.get("local_task_active")))
        finally:
            db.close()

    def test_delete_task_is_idempotent_after_soft_delete(self):
        task_id = self._create_task()
        task_dir = self.output_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "failed"
            db.commit()

            self.service.delete_task(db, task_id, delete_files=True)
            self.service.delete_task(db, task_id, delete_files=True)

            deleted_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertTrue(bool(deleted_row.is_deleted))
            deleted_events = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_deleted").all()
            self.assertEqual(1, len(deleted_events))
        finally:
            db.close()

    def test_delete_task_ignores_duplicate_task_deleted_event_conflict(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "failed"
            db.commit()

            original_flush = db.flush
            call_state = {"raised": False}

            def flaky_flush(*args, **kwargs):
                original_flush(*args, **kwargs)
                if call_state["raised"]:
                    return
                for obj in list(db.identity_map.values()) + list(db.new):
                    if isinstance(obj, AppDfaTaskEvent) and getattr(obj, "event_type", "") == "task_deleted":
                        call_state["raised"] = True
                        raise IntegrityError("INSERT", {}, Exception("duplicate"))

            with patch.object(db, "flush", side_effect=flaky_flush):
                self.service.delete_task(db, task_id, delete_files=False)

            deleted_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertTrue(bool(deleted_row.is_deleted))
        finally:
            db.close()

    def test_record_task_event_deduplicates_same_event(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            first = task_service_module._record_task_event(
                db,
                row=row,
                event_type="task_lease_lost",
                message="任务心跳续租失败，租约已丢失",
                level="warning",
                status="running",
                execution_epoch=1,
                control_version=2,
            )
            second = task_service_module._record_task_event(
                db,
                row=row,
                event_type="task_lease_lost",
                message="任务心跳续租失败，租约已丢失",
                level="warning",
                status="running",
                execution_epoch=1,
                control_version=2,
            )
            db.commit()

            self.assertEqual(first.id, second.id)
            lost_events = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_lease_lost").all()
            self.assertEqual(1, len(lost_events))
        finally:
            db.close()

    def test_timeline_api_get_returns_events(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            task_service_module._record_task_event(
                db,
                row=row,
                event_type="task_started",
                message="任务已开始执行",
                status="running",
            )
            db.commit()
        finally:
            db.close()

        with self._build_client() as client:
            response = client.get(f"/api/app/dataflow-analyse/tasks/{task_id}/timeline")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(task_id, payload["task_id"])
        self.assertEqual("task_started", payload["events"][0]["event_type"])
        self.assertEqual("task_created", payload["events"][-1]["event_type"])

    def test_timeline_api_delete_single_event(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            extra = AppDfaTaskEvent(
                id="evt-api-delete-one",
                task_id=task_id,
                project_id=row.project_id,
                source="dfa",
                level="info",
                event_type="task_started",
                status="running",
                message="任务已开始执行",
                dedupe_key="dedupe-api-delete-one",
            )
            db.add(extra)
            db.commit()
        finally:
            db.close()

        with self._build_client() as client:
            response = client.delete(f"/api/app/dataflow-analyse/tasks/{task_id}/timeline/evt-api-delete-one")

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, response.json()["deleted_event_count"])
        db = self._session()
        try:
            self.assertEqual(1, db.query(AppDfaTaskEvent).filter_by(task_id=task_id).count())
            self.assertEqual(0, db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="timeline_event_deleted").count())
        finally:
            db.close()

    def test_timeline_api_clear_all_events(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            db.add(
                AppDfaTaskEvent(
                    id="evt-api-clear-all",
                    task_id=task_id,
                    project_id=row.project_id,
                    source="dfa",
                    level="warning",
                    event_type="task_cancelled",
                    status="cancelled",
                    message="任务已取消",
                    dedupe_key="dedupe-api-clear-all",
                )
            )
            db.commit()
        finally:
            db.close()

        with self._build_client() as client:
            response = client.delete(f"/api/app/dataflow-analyse/tasks/{task_id}/timeline")

        self.assertEqual(200, response.status_code)
        self.assertEqual(2, response.json()["deleted_event_count"])
        db = self._session()
        try:
            self.assertEqual(0, db.query(AppDfaTaskEvent).filter_by(task_id=task_id).count())
            self.assertEqual(0, db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="timeline_cleared").count())
        finally:
            db.close()

    def test_delete_task_api_records_task_deleted_event(self):
        task_id = self._create_task()
        task_dir = self.output_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        (task_dir / "marker.txt").write_text("ok", encoding="utf-8")

        with self._build_client() as client:
            response = client.delete(f"/api/app/dataflow-analyse/tasks/{task_id}")

        self.assertEqual(204, response.status_code)
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertTrue(bool(row.is_deleted))
            deleted_event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_deleted").first()
            self.assertIsNotNone(deleted_event)
            self.assertTrue(bool(deleted_event.payload.get("delete_files")))
            self.assertEqual(str(task_dir), deleted_event.payload.get("task_dir"))
        finally:
            db.close()

    def test_delete_task_api_rejects_running_task_and_records_warning(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "running"
            row.execution_owner_id = "worker-a"
            row.dispatch_status = "running"
            db.commit()
        finally:
            db.close()

        with self._build_client() as client:
            response = client.delete(f"/api/app/dataflow-analyse/tasks/{task_id}")

        self.assertEqual(409, response.status_code)
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            self.assertFalse(bool(row.is_deleted))
            rejected_event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_delete_rejected").first()
            self.assertIsNotNone(rejected_event)
            self.assertEqual("warning", rejected_event.level)
        finally:
            db.close()

    def test_execute_task_pre_execution_rejections_record_timeline_events(self):
        task_id = self._create_task()
        db = self._session()
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            row.status = "pending"
            row.execution_owner_id = "worker-x"
            row.execution_epoch = 1
            row.control_version = 2
            row.dispatch_status = "leased"
            db.commit()
        finally:
            db.close()

        previous_get_db = sys.modules["app.db"].get_db
        previous_still_owner = task_service_module.still_owner
        previous_begin = task_service_module.begin_execution_if_owner
        previous_cleanup = task_service_module.cleanup_orphan_pi_processes
        previous_release = task_service_module.release_lease
        try:
            def _fake_get_db():
                db = self._session()
                try:
                    yield db
                finally:
                    db.close()

            sys.modules["app.db"].get_db = _fake_get_db
            task_service_module.cleanup_orphan_pi_processes = lambda *args, **kwargs: 0
            task_service_module.release_lease = lambda db, task_id, owner_id, epoch: False

            task_service_module.still_owner = lambda db, task_id, owner_id, epoch, control_version: False
            asyncio.run(self.service._execute_task(task_id, 1, 2))

            db = self._session()
            try:
                event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_not_owner_pre_execute").first()
                self.assertIsNotNone(event)
            finally:
                db.close()

            db = self._session()
            try:
                row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                row.execution_owner_id = None
                row.execution_epoch = 0
                row.control_version = 0
                row.dispatch_status = "pending"
                db.commit()
            finally:
                db.close()

            task_service_module.still_owner = lambda db, task_id, owner_id, epoch, control_version: True
            task_service_module.begin_execution_if_owner = lambda db, task_id, owner_id, epoch, control_version, started_at=None: False
            asyncio.run(self.service._execute_task(task_id, 1, 0))

            db = self._session()
            try:
                event = db.query(AppDfaTaskEvent).filter_by(task_id=task_id, event_type="task_begin_execution_rejected").first()
                self.assertIsNotNone(event)
            finally:
                db.close()
        finally:
            sys.modules["app.db"].get_db = previous_get_db
            task_service_module.still_owner = previous_still_owner
            task_service_module.begin_execution_if_owner = previous_begin
            task_service_module.cleanup_orphan_pi_processes = previous_cleanup
            task_service_module.release_lease = previous_release


if __name__ == "__main__":
    unittest.main()
