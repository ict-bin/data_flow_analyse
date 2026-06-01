"""Bootstrap DB-dependent runtime components with retry."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Optional

from fastapi import FastAPI

from app.agent_process import cleanup_orphan_pi_processes
from app.config import get_service_yaml
from app.runtime_context import (
    DISPATCHER_ENABLED,
    DISPATCH_POLL_INTERVAL_SECONDS,
    INSTANCE_ID,
    PUBLIC_API_ENABLED,
    REGISTRY_ENABLED,
    ROLE,
    WORKER_SLOT_REGISTRY_ENABLED,
)
from app.service.task_service import get_task_service
from app.logging_utils import log_event

logger = logging.getLogger("dfa.bootstrap")

DB_INIT_RETRY_SECONDS = int(os.environ.get("DFA_DB_INIT_RETRY_SECONDS", "5"))


@dataclass
class RuntimeBootstrapStatus:
    db_ready: bool = False
    management_api_ready: bool = False
    registry_ready: bool = False
    dispatcher_ready: bool = False
    ready: bool = False
    phase: str = "booting"
    error: str | None = None
    attempts: int = 0


class RuntimeBootstrap:
    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._status = RuntimeBootstrapStatus()
        self._router_installed = False
        self._dispatcher_task: asyncio.Task | None = None
        self._worker_slot_task: asyncio.Task | None = None

    async def start(self, app: FastAPI) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._status = RuntimeBootstrapStatus()
        self._task = asyncio.create_task(self._bootstrap_loop(app), name="dfa_runtime_bootstrap")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        if self._dispatcher_task and not self._dispatcher_task.done():
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
        self._dispatcher_task = None
        if self._worker_slot_task and not self._worker_slot_task.done():
            self._worker_slot_task.cancel()
            try:
                await self._worker_slot_task
            except asyncio.CancelledError:
                pass
        self._worker_slot_task = None
        try:
            from app.service.registry_service import get_registry_service

            get_registry_service().stop()
        except Exception:
            pass
        log_event(logger, logging.INFO, "dispatcher stopped", event="dispatcher_stopped", owner_id=INSTANCE_ID)

    def status(self) -> dict:
        return asdict(self._status)

    def dispatcher_running(self) -> bool:
        return bool(self._dispatcher_task and not self._dispatcher_task.done())

    async def _bootstrap_loop(self, app: FastAPI) -> None:
        svc_yaml = get_service_yaml()

        while not self._stop_event.is_set():
            made_progress = False

            if not self._status.db_ready:
                made_progress = await self._init_db(svc_yaml)

            if self._status.db_ready:
                if PUBLIC_API_ENABLED and not self._router_installed:
                    made_progress = self._attempt_component_start(
                        "router_init",
                        lambda: self._install_management_router(app),
                    ) or made_progress

                if REGISTRY_ENABLED and not self._status.registry_ready:
                    made_progress = await self._attempt_async_component_start(
                        "registry_register",
                        self._register_registry,
                    ) or made_progress

                if DISPATCHER_ENABLED and not self._status.dispatcher_ready:
                    made_progress = self._attempt_component_start(
                        "dispatcher_start",
                        self._start_dispatcher,
                    ) or made_progress
                if WORKER_SLOT_REGISTRY_ENABLED and self._worker_slot_task is None:
                    made_progress = self._attempt_component_start(
                        "worker_slot_start",
                        self._start_worker_slot_registry,
                    ) or made_progress

                if self._all_required_components_ready():
                    self._status.phase = "ready"
                    self._status.ready = True
                    self._status.error = None
                    log_event(
                        logger,
                        logging.INFO,
                        "startup ready",
                        event="startup_ready",
                        owner_id=INSTANCE_ID,
                        role=ROLE,
                        public_api_enabled=PUBLIC_API_ENABLED,
                        dispatcher_enabled=DISPATCHER_ENABLED,
                        executor_enabled=False,
                        registry_enabled=REGISTRY_ENABLED,
                    )
                    return

            if made_progress:
                continue

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=DB_INIT_RETRY_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def _init_db(self, svc_yaml) -> bool:
        self._status.phase = "db_init"
        self._status.attempts += 1
        log_event(logger, logging.INFO, "startup phase begin", event="startup_phase_begin", phase=self._status.phase)
        try:
            from app.db import init_db

            await asyncio.wait_for(
                asyncio.to_thread(
                    init_db,
                    svc_yaml.database.url,
                    svc_yaml.database.pool_size,
                    svc_yaml.database.max_overflow,
                ),
                timeout=120,
            )
            self._status.db_ready = True
            self._status.error = None
            logger.info("DB initialized: %s:%s/%s", svc_yaml.database.host, svc_yaml.database.port, svc_yaml.database.name)
            return True
        except Exception as exc:
            self._status.error = f"db_init: {exc}"
            logger.warning(
                "startup DB init failed on %s (attempt %s, retry in %ss): %s",
                INSTANCE_ID,
                self._status.attempts,
                DB_INIT_RETRY_SECONDS,
                exc,
            )
            log_event(
                logger,
                logging.ERROR,
                "startup bootstrap failed",
                event="startup_bootstrap_failed",
                phase=self._status.phase,
                error=str(exc),
            )
            return False

    def _attempt_component_start(self, phase: str, starter) -> bool:
        self._status.phase = phase
        log_event(logger, logging.INFO, "startup phase begin", event="startup_phase_begin", phase=phase)
        try:
            starter()
            self._status.error = None
            return True
        except Exception as exc:
            self._status.error = f"{phase}: {exc}"
            logger.warning("%s failed on %s (retry in %ss): %s", phase, INSTANCE_ID, DB_INIT_RETRY_SECONDS, exc, exc_info=True)
            return False

    async def _attempt_async_component_start(self, phase: str, starter) -> bool:
        self._status.phase = phase
        log_event(logger, logging.INFO, "startup phase begin", event="startup_phase_begin", phase=phase)
        try:
            await starter()
            self._status.error = None
            return True
        except Exception as exc:
            self._status.error = f"{phase}: {exc}"
            logger.warning("%s failed on %s (retry in %ss): %s", phase, INSTANCE_ID, DB_INIT_RETRY_SECONDS, exc, exc_info=True)
            return False

    def _install_management_router(self, app: FastAPI) -> None:
        from app.api import router as mgmt_router

        app.include_router(mgmt_router)
        self._router_installed = True
        self._status.management_api_ready = True

    def install_internal_observability_router(self, app: FastAPI) -> None:
        if getattr(app.state, "dfa_internal_observability_router_installed", False):
            return
        from app.api.tasks import internal_observability_router

        app.include_router(internal_observability_router)
        app.state.dfa_internal_observability_router_installed = True

    async def _register_registry(self) -> None:
        from app.service.registry_service import get_registry_service

        registry = get_registry_service()
        await asyncio.wait_for(registry.register(), timeout=15)
        registry.start()
        self._status.registry_ready = True

    def _start_dispatcher(self) -> None:
        async def _dispatcher_loop() -> None:
            svc = get_task_service()
            while not self._stop_event.is_set():
                try:
                    claimed = await svc.dispatch_until_full()
                    if claimed:
                        log_event(
                            logger,
                            logging.INFO,
                            "dispatcher claimed tasks",
                            event="dispatcher_claim_batch",
                            owner_id=INSTANCE_ID,
                            claimed_count=claimed,
                            current_running=svc.local_running_task_count(),
                        )
                except Exception as exc:
                    logger.warning("dispatcher loop failed on %s: %s", INSTANCE_ID, exc, exc_info=True)
                await asyncio.sleep(DISPATCH_POLL_INTERVAL_SECONDS)

        self._dispatcher_task = asyncio.create_task(_dispatcher_loop(), name="dfa_dispatcher")
        self._status.dispatcher_ready = True
        log_event(logger, logging.INFO, "dispatcher started", event="dispatcher_started", owner_id=INSTANCE_ID)

    def _start_worker_slot_registry(self) -> None:
        async def _worker_slot_loop() -> None:
            from app.db import get_db
            from app.runtime_context import MAX_LOCAL_RUNNING_TASKS, POD_IP, POD_NAME, WORKER_ID, WORKER_SLOT_HEARTBEAT_SECONDS
            from app.service.worker_slot_service import get_worker_slot_service
            orphan_sweep_seconds = max(10, int(os.environ.get("DFA_ORPHAN_PI_SWEEP_SECONDS", "30")))
            running_reconcile_seconds = max(10, int(os.environ.get("DFA_ORPHAN_RUNNING_RECONCILE_SECONDS", str(max(10, WORKER_SLOT_HEARTBEAT_SECONDS)))))
            last_orphan_sweep = 0.0
            last_running_reconcile = 0.0

            while not self._stop_event.is_set():
                db_gen = get_db()
                db = next(db_gen)
                try:
                    now_ts = time.time()
                    if now_ts - last_orphan_sweep >= orphan_sweep_seconds:
                        cleanup_orphan_pi_processes(logger.warning, label="dfa_worker_registry")
                        last_orphan_sweep = now_ts
                    get_worker_slot_service().upsert_heartbeat(
                        db,
                        worker_id=WORKER_ID,
                        pod_name=POD_NAME,
                        pod_ip=POD_IP or None,
                        http_port=int(os.environ.get("PORT") or 8080),
                        max_concurrent_tasks=MAX_LOCAL_RUNNING_TASKS,
                        status="running",
                    )
                    if now_ts - last_running_reconcile >= running_reconcile_seconds:
                        recovered = get_task_service().reconcile_orphaned_running_tasks(db)
                        last_running_reconcile = now_ts
                        if recovered:
                            log_event(
                                logger,
                                logging.WARNING,
                                "recovered orphaned running tasks",
                                event="task_running_reconcile_batch",
                                owner_id=INSTANCE_ID,
                                recovered_count=recovered,
                            )
                except Exception as exc:
                    logger.warning("worker slot heartbeat failed on %s: %s", INSTANCE_ID, exc, exc_info=True)
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
                await asyncio.sleep(max(5, int(WORKER_SLOT_HEARTBEAT_SECONDS)))

        self._worker_slot_task = asyncio.create_task(_worker_slot_loop(), name="dfa_worker_slot_registry")

    def _all_required_components_ready(self) -> bool:
        if not self._status.db_ready:
            return False
        if PUBLIC_API_ENABLED and not self._status.management_api_ready:
            return False
        if REGISTRY_ENABLED and not self._status.registry_ready:
            return False
        if DISPATCHER_ENABLED and not self._status.dispatcher_ready:
            return False
        return True


_runtime_bootstrap: RuntimeBootstrap | None = None


def get_runtime_bootstrap() -> RuntimeBootstrap:
    global _runtime_bootstrap
    if _runtime_bootstrap is None:
        _runtime_bootstrap = RuntimeBootstrap()
    return _runtime_bootstrap
