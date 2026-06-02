import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap


class RuntimeBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_db_init_until_success(self):
        bootstrap = RuntimeBootstrap()
        app = SimpleNamespace(include_router=lambda router: None)
        init_attempts = []
        dispatch_calls = []
        reconcile_calls = []

        def fake_init_db(*args, **kwargs):
            init_attempts.append(1)
            if len(init_attempts) == 1:
                raise RuntimeError("mysql not ready")

        async def fake_dispatch_until_full():
            dispatch_calls.append(1)
            await asyncio.sleep(0)
            return 0

        def fake_reconcile_orphaned_running_tasks(db, *, limit=100):
            reconcile_calls.append(limit)
            return 0

        with patch("app.service.runtime_bootstrap.get_service_yaml", return_value=SimpleNamespace(
            database=SimpleNamespace(url="mysql://", pool_size=1, max_overflow=1, host="db", port=3306, name="dfa"),
        )), patch("app.service.runtime_bootstrap.DB_INIT_RETRY_SECONDS", 0.01), patch(
            "app.service.runtime_bootstrap.PUBLIC_API_ENABLED",
            True,
        ), patch(
            "app.service.runtime_bootstrap.REGISTRY_ENABLED",
            True,
        ), patch(
            "app.service.runtime_bootstrap.DISPATCHER_ENABLED",
            True,
        ), patch(
            "app.service.runtime_bootstrap.get_task_service",
            return_value=SimpleNamespace(
                dispatch_until_full=fake_dispatch_until_full,
                local_running_task_count=lambda: 0,
                reconcile_orphaned_running_tasks=fake_reconcile_orphaned_running_tasks,
            ),
        ), patch(
            "app.db.init_db",
            side_effect=fake_init_db,
        ), patch(
            "app.service.registry_service.get_registry_service",
            return_value=SimpleNamespace(register=lambda: asyncio.sleep(0), start=lambda: None, stop=lambda: None),
        ):
            await bootstrap.start(app)
            for _ in range(80):
                if bootstrap.status()["ready"]:
                    break
                await asyncio.sleep(0.01)
            await bootstrap.stop()

        status = bootstrap.status()
        self.assertTrue(status["ready"])
        self.assertTrue(status["db_ready"])
        self.assertTrue(status["management_api_ready"])
        self.assertTrue(status["registry_ready"])
        self.assertTrue(status["dispatcher_ready"])
        self.assertEqual(2, status["attempts"])
        self.assertEqual(2, len(init_attempts))
        self.assertGreaterEqual(len(dispatch_calls), 0)
        self.assertGreaterEqual(len(reconcile_calls), 0)

    async def test_worker_slot_loop_reconciles_orphaned_running_tasks(self):
        bootstrap = RuntimeBootstrap()
        reconcile_calls = []
        heartbeat_calls = []
        orphan_sweep_calls = []

        def fake_get_db():
            class _DummyDb:
                pass

            db = _DummyDb()
            try:
                yield db
            finally:
                return

        def fake_reconcile(db, *, limit=100):
            reconcile_calls.append(limit)
            return 1

        def fake_heartbeat(**kwargs):
            heartbeat_calls.append(kwargs["worker_id"])

        with patch("app.service.runtime_bootstrap.get_task_service", return_value=SimpleNamespace(reconcile_orphaned_running_tasks=fake_reconcile)), patch(
            "app.service.worker_slot_service.get_worker_slot_service",
            return_value=SimpleNamespace(upsert_heartbeat=lambda db, **kwargs: fake_heartbeat(**kwargs)),
        ), patch("app.service.runtime_bootstrap.cleanup_orphan_pi_processes", side_effect=lambda *args, **kwargs: orphan_sweep_calls.append(kwargs.get("label")) or 0), patch(
            "app.db.get_db",
            fake_get_db,
        ), patch("app.runtime_context.WORKER_SLOT_HEARTBEAT_SECONDS", 1), patch(
            "app.runtime_context.WORKER_ID",
            "pod-a",
        ), patch("app.runtime_context.POD_NAME", "pod-a"), patch(
            "app.runtime_context.POD_IP",
            "127.0.0.1",
        ), patch("app.runtime_context.MAX_LOCAL_RUNNING_TASKS", 4), patch.dict(
            "os.environ",
            {"DFA_ORPHAN_PI_SWEEP_SECONDS": "999", "DFA_ORPHAN_RUNNING_RECONCILE_SECONDS": "1"},
            clear=False,
        ):
            bootstrap._stop_event = asyncio.Event()
            bootstrap._start_worker_slot_registry()
            await asyncio.sleep(0.05)
            bootstrap._stop_event.set()
            bootstrap._worker_slot_stop.set()
            await asyncio.sleep(0.05)

        self.assertGreaterEqual(len(heartbeat_calls), 1)
        self.assertGreaterEqual(len(reconcile_calls), 1)
        self.assertGreaterEqual(len(orphan_sweep_calls), 1)

    def test_install_internal_observability_router_is_idempotent(self):
        bootstrap = RuntimeBootstrap()
        app = FastAPI()

        bootstrap.install_internal_observability_router(app)
        bootstrap.install_internal_observability_router(app)

        paths = [route.path for route in app.router.routes]
        self.assertIn("/api/app/dataflow-analyse/agent-observability/snapshot", paths)
        self.assertEqual(1, paths.count("/api/app/dataflow-analyse/agent-observability/snapshot"))


if __name__ == "__main__":
    unittest.main()
