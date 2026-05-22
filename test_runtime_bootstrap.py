import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap


class RuntimeBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_db_init_until_success(self):
        bootstrap = RuntimeBootstrap()
        app = SimpleNamespace(include_router=lambda router: None)
        init_attempts = []
        dispatch_calls = []

        def fake_init_db(*args, **kwargs):
            init_attempts.append(1)
            if len(init_attempts) == 1:
                raise RuntimeError("mysql not ready")

        async def fake_dispatch_until_full():
            dispatch_calls.append(1)
            await asyncio.sleep(0)
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


if __name__ == "__main__":
    unittest.main()
