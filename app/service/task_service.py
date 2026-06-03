"""Task management service for secflow-app-dataflow-analyse.

Bridges the FastAPI management layer with the Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import tempfile
import threading
import time as _time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.attributes import flag_modified

from app.config import build_task_config, load_service_config
from app.db.models import AppDfaTask, AppDfaTaskEvent
from app.logging_utils import log_event
from app.models import SwarmEvent, TaskStatus
from app.orchestrator import Orchestrator
from app.runtime_context import (
    HEARTBEAT_INTERVAL_SECONDS,
    LEASE_TTL_SECONDS,
    LEASE_REQUEUE_DELAY_SECONDS,
    MAX_LOCAL_RUNNING_TASKS,
    WORKER_ID,
)
from app.service.execution_coordinator import (
    begin_execution_if_owner,
    claim_one_runnable_task,
    commit_terminal_state_if_owner,
    list_recoverable_orphaned_running_tasks,
    load_execution_snapshot,
    recover_running_task_if_owner,
    release_lease,
    renew_lease,
    still_owner,
)
from app.service.session_index import build_session_catalog
from app.time_utils import UTC_PLUS_8, isoformat_local, now_local
from app.agent_process import cleanup_orphan_pi_processes, cleanup_task_agent_processes

logger = logging.getLogger("dfa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")
ENTRY_CONTEXT_MAX_CHARS = 32000
ENTRY_CONTEXT_MAX_TAINTS = 64
ENTRY_CONTEXT_MAX_DESC_CHARS = 2240
TASK_EVENT_SOURCE_DFA = "dfa"
TASK_EVENT_RENEW_INTERVAL_SECONDS = max(60, HEARTBEAT_INTERVAL_SECONDS * 6)
EXECUTION_SUPERVISOR_INTERVAL_SECONDS = float(os.environ.get("DFA_EXECUTION_SUPERVISOR_INTERVAL_SECONDS", "5"))
EXECUTION_NO_PROGRESS_SECONDS = float(os.environ.get("DFA_EXECUTION_NO_PROGRESS_SECONDS", "120"))
RUNNING_RECONCILE_LOCAL_GRACE_SECONDS = float(
    os.environ.get(
        "DFA_RUNNING_RECONCILE_LOCAL_GRACE_SECONDS",
        str(max(LEASE_TTL_SECONDS, HEARTBEAT_INTERVAL_SECONDS * 3)),
    )
)
AUTO_REQUEUE_REASON_LEASE_LOST = "lease_lost"
LEASE_REQUEUE_RETRY_DELAYS_SECONDS = (1.0, 2.0, 5.0)
LEASE_RECOVERY_STATE_IDLE = "idle"
LEASE_RECOVERY_STATE_REQUESTED = "requested"
LEASE_RECOVERY_STATE_REQUEUE_COMMITTED = "requeue_committed"
LEASE_RECOVERY_STATE_FAILED = "failed"
DB_RETRY_MAX_ATTEMPTS = int(os.environ.get("DFA_DB_RETRY_MAX_ATTEMPTS", "5"))
DB_RETRY_BASE_DELAY_SECONDS = float(os.environ.get("DFA_DB_RETRY_BASE_DELAY_SECONDS", "1"))
DB_FAILURE_REASON_CODE = "database_failure"
DB_FAILURE_REASON_TITLE = "数据库操作失败"

_RUNNING_TASK_LOCK = threading.RLock()
_running_tasks: dict[str, "_RunningTaskContext"] = {}


@dataclass
class _RunningTaskContext:
    execution_thread: threading.Thread | None = None
    lease_thread: threading.Thread | None = None
    orch: Orchestrator | None = None
    task_root: str | None = None
    run_root: str | None = None
    epoch: int | None = None
    control_version: int | None = None
    started_at: float = field(default_factory=_time.time)
    last_progress_at: float = field(default_factory=_time.time)
    last_lease_heartbeat_at: float = 0.0
    last_state_sync_at: float = 0.0
    last_lease_error: str | None = None
    lease_renew_failure_count: int = 0
    last_lease_renew_attempt_at: float = 0.0
    last_lease_renew_success_at: float = 0.0
    lease_recovery_state: str = LEASE_RECOVERY_STATE_IDLE
    lease_recovery_error: str | None = None
    lease_recovery_updated_at: float = 0.0
    termination_reason: str | None = None
    root_task_id: str | None = None
    parent_task_id: str | None = None
    session_kind: str | None = None
    cancel_requested: threading.Event = field(default_factory=threading.Event)
    lease_stop_requested: threading.Event = field(default_factory=threading.Event)

    def execution_alive(self) -> bool:
        return bool(self.execution_thread and self.execution_thread.is_alive())

    def lease_alive(self) -> bool:
        return bool(self.lease_thread and self.lease_thread.is_alive())


_running_task_contexts: dict[str, _RunningTaskContext] = {}
_child_execution_contexts: dict[str, _RunningTaskContext] = {}

_TASK_LIST_SORT_COLUMNS = {
    "created_at": AppDfaTask.created_at,
    "updated_at": AppDfaTask.updated_at,
    "started_at": AppDfaTask.started_at,
    "finished_at": AppDfaTask.finished_at,
    "status": AppDfaTask.status,
    "task_name": AppDfaTask.task_name,
}


def _fit_event_message(raw: object, *, limit: int = 400) -> str:
    text = str(raw or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def _event_payload_preview_value(value: object, *, limit: int = 240) -> object:
    if isinstance(value, str):
        return value if len(value) <= limit else f"{value[: limit - 1]}…"
    if isinstance(value, list):
        return [_event_payload_preview_value(item, limit=80) for item in value[:10]]
    if isinstance(value, dict):
        compact: dict[str, object] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 12:
                break
            compact[str(key)] = _event_payload_preview_value(item, limit=120)
        return compact
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return _fit_event_message(value, limit=limit)


def _compact_event_payload(payload: dict[str, object] | None) -> dict[str, object]:
    compact: dict[str, object] = {}
    for key, value in (payload or {}).items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[key] = _event_payload_preview_value(value)
        elif isinstance(value, list):
            compact[f"{key}_count"] = len(value)
            compact[key] = _event_payload_preview_value(value)
        elif isinstance(value, dict):
            compact[key] = _event_payload_preview_value(value)
        else:
            compact[key] = _fit_event_message(value)
    return compact


def _is_db_connection_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return _is_db_connection_error_text(message)


def _is_db_connection_error_text(text: object) -> bool:
    message = str(text or "").lower()
    return any(token in message for token in [
        "lost connection to mysql server",
        "server has gone away",
        "connection reset",
        "connection refused",
        "connection aborted",
        "broken pipe",
        "operationalerror",
    ])


def _mark_database_failure(row: AppDfaTask, exc: Exception, *, status: str = "error") -> None:
    failure_message = str(exc)
    if DB_FAILURE_REASON_TITLE not in failure_message:
        failure_message = _database_failure_message(exc, attempts=DB_RETRY_MAX_ATTEMPTS)
    _persist_terminal_failure(row, failure_message, status=status)
    reason = {
        "code": DB_FAILURE_REASON_CODE,
        "title": DB_FAILURE_REASON_TITLE,
        "status": status,
        "message": failure_message,
        "service": "dataflow-analysis",
        "category": "database",
        "terminal": True,
        "is_abnormal": True,
    }
    row.latest_abnormal_reason_json = reason
    flag_modified(row, "latest_abnormal_reason_json")


def _db_retry_delay_seconds(attempt: int) -> float:
    index = max(0, int(attempt) - 1)
    return float(DB_RETRY_BASE_DELAY_SECONDS) * (2 ** index)


def _database_failure_message(exc: Exception, *, attempts: int) -> str:
    return f"{DB_FAILURE_REASON_TITLE}: 重试 {attempts} 次后仍失败: {exc}"


def _run_db_retry(operation_name: str, fn, *, max_attempts: int | None = None):
    attempts = max(1, int(max_attempts or DB_RETRY_MAX_ATTEMPTS))
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(attempt)
        except Exception as exc:
            last_error = exc
            if not _is_db_connection_error(exc) or attempt >= attempts:
                break
            delay = _db_retry_delay_seconds(attempt)
            logger.warning(
                "db operation retry scheduled: op=%s attempt=%s/%s delay=%.2fs error=%s",
                operation_name,
                attempt,
                attempts,
                delay,
                exc,
            )
            _time.sleep(delay)
    assert last_error is not None
    raise RuntimeError(_database_failure_message(last_error, attempts=attempts)) from last_error


def _consume_db_generator(db_gen) -> None:
    try:
        next(db_gen)
    except StopIteration:
        pass


def _task_event_dedupe_key(
    task_id: str,
    event_type: str,
    status: str | None,
    message: str,
    *,
    epoch: int | None = None,
    function_name: str | None = None,
    dispatch_status: str | None = None,
    line_hint: str | None = None,
) -> str:
    parts = [
        task_id,
        event_type,
        str(status or ""),
        str(epoch or ""),
        str(function_name or ""),
        str(dispatch_status or ""),
        str(line_hint or ""),
        hashlib.sha1(message.encode("utf-8")).hexdigest()[:12],
    ]
    return "::".join(parts)[:255]


def _latest_execution_started_at(db: Session, task_id: str) -> str | None:
    row = (
        db.query(AppDfaTaskEvent.created_at)
        .filter(
            AppDfaTaskEvent.task_id == task_id,
            AppDfaTaskEvent.event_type.in_(("task_started", "task_execution_started")),
        )
        .order_by(AppDfaTaskEvent.created_at.desc())
        .first()
    )
    if not row:
        return None
    created_at = row[0] if isinstance(row, tuple) else getattr(row, "created_at", None)
    return isoformat_local(created_at) if created_at else None


def _build_task_event_response(event: AppDfaTaskEvent) -> dict[str, object]:
    return {
        "id": event.id,
        "task_id": event.task_id,
        "project_id": event.project_id,
        "source": event.source,
        "level": event.level,
        "event_type": event.event_type,
        "status": event.status,
        "worker_id": event.worker_id,
        "execution_owner_id": event.execution_owner_id,
        "execution_epoch": event.execution_epoch,
        "control_version": event.control_version,
        "dispatch_status": event.dispatch_status,
        "function_name": event.function_name,
        "source_file": event.source_file,
        "line_hint": event.line_hint,
        "parent_task_id": event.parent_task_id,
        "parent_stage_item_id": event.parent_stage_item_id,
        "message": event.message,
        "payload": event.payload,
        "created_at": isoformat_local(event.created_at),
    }


def _record_task_event(
    db: Session,
    *,
    row: AppDfaTask,
    event_type: str,
    message: str,
    source: str = TASK_EVENT_SOURCE_DFA,
    level: str = "info",
    status: str | None = None,
    payload: dict[str, object] | None = None,
    worker_id: str | None = None,
    execution_owner_id: str | None = None,
    execution_epoch: int | None = None,
    control_version: int | None = None,
    dispatch_status: str | None = None,
    function_name: str | None = None,
    source_file: str | None = None,
    line_hint: str | None = None,
    dedupe_key: str | None = None,
) -> AppDfaTaskEvent | None:
    normalized_message = _fit_event_message(message, limit=1000)
    normalized_status = str(status or row.status or "").strip() or None
    normalized_worker_id = str(worker_id or row.execution_owner_id or "").strip() or None
    normalized_owner_id = str(execution_owner_id or row.execution_owner_id or "").strip() or None
    normalized_dispatch_status = str(dispatch_status or row.dispatch_status or "").strip() or None
    normalized_function_name = str(function_name or (row.task_config_json or {}).get("function_name") or "").strip() or None
    normalized_source_file = str(source_file or (row.task_config_json or {}).get("source_file") or "").strip() or None
    normalized_line_hint = str(line_hint or (row.task_config_json or {}).get("line_hint") or "").strip() or None
    event_dedupe_key = dedupe_key or _task_event_dedupe_key(
        row.task_id,
        event_type,
        normalized_status,
        normalized_message,
        epoch=execution_epoch or int(row.execution_epoch or 0),
        function_name=normalized_function_name,
        dispatch_status=normalized_dispatch_status,
        line_hint=normalized_line_hint,
    )
    existing = db.query(AppDfaTaskEvent).filter(AppDfaTaskEvent.dedupe_key == event_dedupe_key).first()
    if existing is not None:
        return existing
    event = AppDfaTaskEvent(
        id=uuid.uuid4().hex[:32],
        task_id=row.task_id,
        project_id=row.project_id,
        source=source,
        level=level,
        event_type=event_type,
        status=normalized_status,
        worker_id=normalized_worker_id,
        execution_owner_id=normalized_owner_id,
        execution_epoch=int(execution_epoch) if execution_epoch is not None else int(row.execution_epoch or 0),
        control_version=int(control_version) if control_version is not None else int(row.control_version or 0),
        dispatch_status=normalized_dispatch_status,
        function_name=normalized_function_name,
        source_file=normalized_source_file,
        line_hint=normalized_line_hint,
        parent_task_id=row.parent_task_id,
        parent_stage_item_id=row.parent_stage_item_id,
        message=normalized_message,
        dedupe_key=event_dedupe_key,
    )
    event.payload = _compact_event_payload(payload or {})
    db.add(event)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        logger.warning(
            "dfa task event dedupe conflict ignored: task_id=%s event_type=%s dedupe_key=%s",
            row.task_id,
            event_type,
            event_dedupe_key,
        )
        return db.query(AppDfaTaskEvent).filter(AppDfaTaskEvent.dedupe_key == event_dedupe_key).first()
    return event


def _active_local_task(task_id: str) -> _RunningTaskContext | None:
    with _RUNNING_TASK_LOCK:
        ctx = _running_tasks.get(task_id)
        if ctx is None:
            return None
        if not ctx.execution_alive():
            return None
        return ctx


def _context_latest_activity_at(ctx: _RunningTaskContext | None) -> float:
    if ctx is None:
        return 0.0
    return max(
        float(ctx.last_progress_at or 0.0),
        float(ctx.last_lease_heartbeat_at or 0.0),
        float(ctx.last_state_sync_at or 0.0),
        float(ctx.started_at or 0.0),
    )


def _build_runtime_snapshot_index(runtime_snapshots: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    index: dict[str, list[dict[str, object]]] = {}
    for item in runtime_snapshots:
        if not isinstance(item, dict):
            continue
        keys = {
            str(item.get("task_id") or "").strip(),
            str(item.get("root_task_id") or "").strip(),
            str(item.get("parent_task_id") or "").strip(),
        }
        for key in keys:
            if not key:
                continue
            index.setdefault(key, []).append(item)
    return index


def _runtime_snapshot_latest_activity(snapshot: dict[str, object]) -> float:
    values = [
        snapshot.get("last_progress_at"),
        snapshot.get("last_lease_heartbeat_at"),
        snapshot.get("last_state_sync_at"),
    ]
    latest = 0.0
    for value in values:
        try:
            latest = max(latest, float(value or 0.0))
        except (TypeError, ValueError):
            continue
    return latest


def _is_runtime_snapshot_active(snapshot: dict[str, object], *, now_ts: float) -> bool:
    if bool(snapshot.get("active_context")) and (bool(snapshot.get("execution_alive")) or bool(snapshot.get("lease_alive"))):
        return True
    latest_activity = _runtime_snapshot_latest_activity(snapshot)
    if latest_activity <= 0.0:
        return False
    return (now_ts - latest_activity) <= RUNNING_RECONCILE_LOCAL_GRACE_SECONDS


def _register_running_task_context(
    task_id: str,
    *,
    execution_thread: threading.Thread | None = None,
    lease_thread: threading.Thread | None = None,
    orch: Orchestrator | None = None,
    task_root: str | None = None,
    run_root: str | None = None,
    epoch: int | None = None,
    control_version: int | None = None,
    root_task_id: str | None = None,
    parent_task_id: str | None = None,
    session_kind: str | None = None,
) -> _RunningTaskContext:
    with _RUNNING_TASK_LOCK:
        ctx = _running_task_contexts.get(task_id) or _RunningTaskContext()
    if execution_thread is not None:
        ctx.execution_thread = execution_thread
    if lease_thread is not None:
        ctx.lease_thread = lease_thread
    if orch is not None:
        ctx.orch = orch
    if task_root is not None:
        ctx.task_root = task_root
    if run_root is not None:
        ctx.run_root = run_root
    if epoch is not None:
        ctx.epoch = epoch
    if control_version is not None:
        ctx.control_version = control_version
    if root_task_id is not None:
        ctx.root_task_id = root_task_id
    if parent_task_id is not None:
        ctx.parent_task_id = parent_task_id
    if session_kind is not None:
        ctx.session_kind = session_kind
    ctx.last_state_sync_at = _time.time()
    with _RUNNING_TASK_LOCK:
        _running_task_contexts[task_id] = ctx
        _running_tasks[task_id] = ctx
    return ctx


def _get_running_task_context(task_id: str) -> _RunningTaskContext | None:
    with _RUNNING_TASK_LOCK:
        return _running_task_contexts.get(task_id)


def _unregister_running_task_context(task_id: str) -> None:
    with _RUNNING_TASK_LOCK:
        _running_task_contexts.pop(task_id, None)
        _running_tasks.pop(task_id, None)
        _child_execution_contexts.pop(task_id, None)


def _register_child_execution_context(
    task_id: str,
    *,
    root_task_id: str,
    parent_task_id: str | None,
    task_root: str | None,
    run_root: str | None,
    epoch: int | None,
    session_kind: str,
) -> _RunningTaskContext:
    with _RUNNING_TASK_LOCK:
        ctx = _child_execution_contexts.get(task_id) or _RunningTaskContext()
        ctx.root_task_id = root_task_id
        ctx.parent_task_id = parent_task_id
        ctx.task_root = task_root
        ctx.run_root = run_root
        ctx.epoch = epoch
        ctx.session_kind = session_kind
        now = _time.time()
        ctx.last_progress_at = now
        ctx.last_state_sync_at = now
        _child_execution_contexts[task_id] = ctx
        return ctx


def _unregister_child_execution_context(task_id: str) -> None:
    with _RUNNING_TASK_LOCK:
        _child_execution_contexts.pop(task_id, None)


def _mark_child_execution_progress(task_id: str) -> None:
    with _RUNNING_TASK_LOCK:
        ctx = _child_execution_contexts.get(task_id)
        if ctx is not None:
            now = _time.time()
            ctx.last_progress_at = now
            ctx.last_state_sync_at = now


def _mark_task_progress(task_id: str) -> None:
    with _RUNNING_TASK_LOCK:
        ctx = _running_task_contexts.get(task_id)
        if ctx is not None:
            now = _time.time()
            ctx.last_progress_at = now
            ctx.last_state_sync_at = now


def _epoch_run_root_from_task_root(task_root: str | None, epoch: int | None) -> str | None:
    normalized_root = str(task_root or "").strip()
    if not normalized_root or epoch is None:
        return None
    return str(Path(normalized_root) / f"{int(epoch):04d}")


def _start_task_lease_heartbeat(
    task_id: str,
    *,
    epoch: int,
    control_version: int,
    on_lease_lost,
) -> threading.Thread:
    def _worker() -> None:
        from app.db import get_db
        from app.metrics import observe_local_event

        last_timeline_renew_at = 0.0
        last_renew_failure_event_at = 0.0
        while True:
            ctx = _get_running_task_context(task_id)
            if ctx is None or ctx.lease_stop_requested.is_set():
                return
            if ctx.cancel_requested.wait(timeout=HEARTBEAT_INTERVAL_SECONDS):
                return
            _hb_gen = get_db()
            _hb_db: Session = next(_hb_gen)
            try:
                current_ts = _time.time()
                if ctx is not None:
                    ctx.last_lease_renew_attempt_at = current_ts
                snapshot = load_execution_snapshot(_hb_db, task_id)
                lease_until = snapshot.execution_lease_until if snapshot is not None else None
                lease_expired = lease_until is None or lease_until <= now_local()
                ok = renew_lease(_hb_db, task_id, WORKER_ID, epoch)
                if ok:
                    owner_ok = still_owner(_hb_db, task_id, WORKER_ID, epoch, control_version)
                    if owner_ok:
                        observe_local_event("lease_renew", "success")
                        if ctx is not None:
                            ctx.last_lease_heartbeat_at = current_ts
                            ctx.last_state_sync_at = current_ts
                            ctx.last_lease_error = None
                            ctx.lease_renew_failure_count = 0
                            ctx.last_lease_renew_success_at = current_ts
                        if current_ts - last_timeline_renew_at >= TASK_EVENT_RENEW_INTERVAL_SECONDS:
                            lease_row = _hb_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                            if lease_row is not None:
                                _record_task_event(
                                    _hb_db,
                                    row=lease_row,
                                    event_type="task_lease_renewed",
                                    message="任务租约续约成功",
                                    status=lease_row.status,
                                    worker_id=WORKER_ID,
                                    execution_owner_id=WORKER_ID,
                                    execution_epoch=epoch,
                                    control_version=control_version,
                                    payload={
                                        "owner_id": WORKER_ID,
                                        "epoch": epoch,
                                        "control_version": control_version,
                                        "lease_until": isoformat_local(lease_row.execution_lease_until),
                                    },
                                )
                                _hb_db.commit()
                                last_timeline_renew_at = current_ts
                        continue

                observe_local_event("lease_renew", "failed")
                if ctx is not None:
                    ctx.lease_renew_failure_count += 1
                    ctx.last_lease_error = "lease_renew_failed" if not lease_expired else "lease_lost"
                failure_payload = {
                    "owner_id": WORKER_ID,
                    "epoch": epoch,
                    "control_version": control_version,
                    "failure_count": int(ctx.lease_renew_failure_count if ctx is not None else 1),
                    "lease_until": isoformat_local(lease_until),
                    "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
                    "lease_ttl_seconds": LEASE_TTL_SECONDS,
                    "error": (ctx.last_lease_error if ctx is not None else "lease_renew_failed"),
                }
                if not lease_expired:
                    if current_ts - last_renew_failure_event_at >= TASK_EVENT_RENEW_INTERVAL_SECONDS:
                        renew_failed_row = _hb_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                        if renew_failed_row is not None:
                            _record_task_event(
                                _hb_db,
                                row=renew_failed_row,
                                event_type="task_lease_renew_failed",
                                message="任务租约续租失败，但租约尚未过期，将继续重试",
                                level="warning",
                                status=renew_failed_row.status,
                                worker_id=WORKER_ID,
                                execution_owner_id=WORKER_ID,
                                execution_epoch=epoch,
                                control_version=control_version,
                                payload=failure_payload,
                            )
                            _hb_db.commit()
                            last_renew_failure_event_at = current_ts
                    continue

                if ctx is not None:
                    ctx.termination_reason = "lease_lost"
                log_event(
                    logger,
                    logging.WARNING,
                    "lease expired during threaded heartbeat, aborting task",
                    event="task_lease_lost",
                    task_id=task_id,
                    owner_id=WORKER_ID,
                    epoch=epoch,
                    control_version=control_version,
                )
                lost_row = _hb_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if lost_row is not None:
                    _record_task_event(
                        _hb_db,
                        row=lost_row,
                        event_type="task_lease_lost",
                        message="任务租约已超过超期窗口，判定为 lease_lost",
                        level="warning",
                        status=lost_row.status,
                        worker_id=WORKER_ID,
                        execution_owner_id=WORKER_ID,
                        execution_epoch=epoch,
                        control_version=control_version,
                        payload=failure_payload,
                    )
                    _hb_db.commit()
                on_lease_lost(_hb_db)
                return
            except Exception as exc:
                observe_local_event("lease_renew", "thread_error")
                if ctx is not None:
                    ctx.lease_renew_failure_count += 1
                    ctx.last_lease_error = str(exc)
            finally:
                try:
                    next(_hb_gen)
                except StopIteration:
                    pass

    thread = threading.Thread(target=_worker, name=f"dfa_lease_{task_id}", daemon=True)
    thread.start()
    return thread


def _run_execute_task_in_thread(service: "TaskService", task_id: str, epoch: int, control_version: int) -> None:
    asyncio.run(service._execute_task(task_id, epoch, control_version))


def _recover_running_task_for_cleanup(
    db: Session,
    *,
    task_id: str,
    owner_id: str,
    epoch: int,
    control_version: int,
    reason: str,
) -> bool:
    recovered = recover_running_task_if_owner(
        db,
        task_id,
        owner_id,
        epoch,
        control_version,
        reason=reason,
    )
    if not recovered:
        return False
    row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
    if row is not None:
        _record_task_event(
            db,
            row=row,
            event_type="task_running_recovered",
            message="任务在 worker 非正常收尾后已回退为 pending 并等待重排",
            level="warning",
            status=row.status,
            worker_id=owner_id,
            execution_owner_id=owner_id,
            execution_epoch=epoch,
            control_version=control_version,
            dispatch_status=row.dispatch_status,
            payload={
                "owner_id": owner_id,
                "epoch": epoch,
                "control_version": control_version,
                "recovery_reason": reason,
            },
        )
        _record_task_event(
            db,
            row=row,
            event_type="task_requeued_after_orphaned_running",
            message="任务已重新进入调度队列",
            level="warning",
            status=row.status,
            worker_id=owner_id,
            execution_owner_id=owner_id,
            execution_epoch=epoch,
            control_version=control_version,
            dispatch_status=row.dispatch_status,
            payload={
                "owner_id": owner_id,
                "epoch": epoch,
                "control_version": control_version,
                "recovery_reason": reason,
            },
        )
        db.commit()
    return True


def _abnormal_evidence(key: str, label: str, value: object) -> dict | None:
    text = str(value or "").strip()
    if not text:
        return None
    return {"key": key, "label": label, "value": text}


def _task_abnormal_reason(row: AppDfaTask) -> dict | None:
    status = str(row.status or "")
    if status not in {"failed", "error", "cancelled"}:
        return None
    if isinstance(row.latest_abnormal_reason_json, dict):
        return dict(row.latest_abnormal_reason_json)
    result_json = _load_task_result_json(row) or {}
    events = (row.stages_json or {}).get("events") if isinstance(row.stages_json, dict) else []
    latest_event = next((event for event in reversed(events or []) if isinstance(event, dict) and (event.get("error") or event.get("event"))), None)
    message = str(
        row.error
        or result_json.get("error")
        or result_json.get("completion_reason")
        or (latest_event or {}).get("error")
        or (latest_event or {}).get("message")
        or ""
    ).strip()
    if status == "cancelled":
        code, category, title = "user_cancelled", "cancel", "任务已取消"
    elif "lease" in message.lower() or "租约" in message:
        code, category, title = "lease_lost", "runtime", "任务租约丢失"
    elif "dispatch" in message.lower() or "调度" in message:
        code, category, title = "dispatch_failed", "runtime", "调度失败"
    elif "timeout" in message.lower() or "dependency" in message.lower():
        code, category, title = "dependency_unavailable", "runtime", "依赖不可用"
    else:
        code, category, title = ("unknown_abnormal" if status == "error" else "orchestration_failed"), "orchestration", "任务异常结束"
    return {
        "is_abnormal": True,
        "category": category,
        "code": code,
        "title": title,
        "message": message or "任务以非正常状态结束。",
        "terminal": True,
        "source_layer": "task",
        "status": status,
        "service": "dataflow-analysis",
        "stage_name": str((latest_event or {}).get("stage") or (latest_event or {}).get("stage_name") or "").strip() or None,
        "item_key": None,
        "downstream_task_id": None,
        "downstream_service": None,
        "first_seen_at": isoformat_local(row.started_at),
        "last_seen_at": isoformat_local(row.finished_at or row.updated_at),
        "evidence": [
            item for item in [
                _abnormal_evidence("status", "状态", row.status),
                _abnormal_evidence("dispatch_status", "调度状态", row.dispatch_status),
                _abnormal_evidence("error", "原始错误", row.error),
            ] if item is not None
        ],
        "recommended_action": "查看 result_json、stages_json 和执行租约状态，确认是调度问题还是运行阶段中断。",
        "related_event_ids": [],
    }


def _abnormal_reason_event(reason: dict, *, event_id: str | None = None) -> dict:
    timestamp = str(reason.get("last_seen_at") or isoformat_local(now_local()) or "")
    return {
        "ts": _time.time(),
        "timestamp": timestamp,
        "event": "abnormal_reason_recorded",
        "type": "abnormal_reason_recorded",
        "event_id": event_id or f"abn-{uuid.uuid4().hex[:12]}",
        "message": str(reason.get("title") or "任务异常结束"),
        "level": "warning" if str(reason.get("status") or "") == "cancelled" else "error",
        "data": {"reason": dict(reason)},
    }


def _abnormal_reason_history(row: AppDfaTask) -> list[dict]:
    stages_json = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = stages_json.get("events") if isinstance(stages_json.get("events"), list) else []
    history: list[dict] = []
    for event in reversed(events):
        if not isinstance(event, dict) or event.get("event") != "abnormal_reason_recorded":
            continue
        payload = event.get("data") if isinstance(event.get("data"), dict) else {}
        reason = payload.get("reason") if isinstance(payload.get("reason"), dict) else None
        if not isinstance(reason, dict):
            continue
        history.append(
            {
                "event_id": event.get("event_id"),
                "created_at": event.get("timestamp") or event.get("ts"),
                "reason": reason,
            }
        )
        if len(history) >= 10:
            break
    return history


def _sync_task_abnormal_reason(row: AppDfaTask) -> tuple[dict | None, bool]:
    reason = _task_abnormal_reason(row)
    next_payload = dict(reason) if isinstance(reason, dict) else None
    changed = row.latest_abnormal_reason_json != next_payload
    if changed:
        row.latest_abnormal_reason_json = next_payload
        flag_modified(row, "latest_abnormal_reason_json")
    return next_payload, changed


def _auto_requeue_pending(row: AppDfaTask) -> bool:
    return bool(
        str(row.status or "") == "pending"
        and row.lease_requeue_not_before is not None
        and row.lease_requeue_not_before > now_local()
    )


def _record_abnormal_reason(row: AppDfaTask, reason: dict | None, *, changed: bool) -> None:
    if not changed or not isinstance(reason, dict):
        return
    payload = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = list(payload.get("events") or [])
    events.append(_abnormal_reason_event(reason))
    row.stages_json = {**payload, "events": events, "final": bool(payload.get("final", False))}
    flag_modified(row, "stages_json")


def _record_abnormal_reason_timeline(db: Session, row: AppDfaTask, reason: dict | None, *, changed: bool) -> None:
    if not changed or not isinstance(reason, dict):
        return
    _record_task_event(
        db,
        row=row,
        event_type="abnormal_reason_recorded",
        message=str(reason.get("title") or "任务异常结束"),
        level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
        status=str(reason.get("status") or row.status or ""),
        payload={"reason": reason},
        dedupe_key=_task_event_dedupe_key(
            row.task_id,
            "abnormal_reason_recorded",
            str(reason.get("status") or row.status or ""),
            str(reason.get("message") or reason.get("title") or ""),
            epoch=int(row.execution_epoch or 0),
        ),
    )


def _task_root(row: AppDfaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _task_run_root(row: AppDfaTask) -> Path | None:
    root = _task_root(row)
    return root / "run" if root else None


def _task_epoch_run_root(row: AppDfaTask, epoch: int) -> Path | None:
    root = _task_run_root(row)
    if root is None:
        return None
    return root / "epochs" / f"{int(epoch):04d}"


def _task_result_path(row: AppDfaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _latest_epoch_run_root(row: AppDfaTask) -> Path | None:
    run_root = _task_run_root(row)
    if run_root is None:
        return None
    epochs_root = run_root / "epochs"
    if not epochs_root.is_dir():
        return run_root
    candidates = sorted([path for path in epochs_root.iterdir() if path.is_dir()], key=lambda path: path.name)
    return candidates[-1] if candidates else run_root


def _epoch_label_from_path(path: Path | None) -> str | None:
    if path is None:
        return None
    parts = path.parts
    if "epochs" in parts:
        idx = parts.index("epochs")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
        Path(tmp_name).replace(path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _safe_session_file(root: Path, relative_path: str) -> Path:
    rel = Path(relative_path)
    if rel.is_absolute() or ".." in rel.parts:
        from fastapi import HTTPException
        raise HTTPException(400, "非法会话路径")
    run_root = (root / "run").resolve()
    target = (run_root / rel).resolve()
    try:
        target.relative_to(run_root)
    except ValueError:
        from fastapi import HTTPException
        raise HTTPException(400, "非法会话路径")
    if target.suffix != ".jsonl":
        from fastapi import HTTPException
        raise HTTPException(400, "仅支持 jsonl 会话文件")
    return target


def _parse_session_file(path: Path) -> dict[str, object]:
    events: list[dict[str, object]] = []
    warnings: list[str] = []
    session_meta: dict[str, object] | None = None
    if not path.exists() or not path.is_file():
        from fastapi import HTTPException
        raise HTTPException(404, "会话文件不存在")
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for index, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            warnings.append(f"第 {index} 行 JSON 解析失败")
            events.append({"type": "raw", "event_index": index, "line": index, "raw_line": line[:500], "summary": line[:200]})
            continue
        if isinstance(obj, dict) and obj.get("type") == "session":
            session_meta = obj
            continue
        if not isinstance(obj, dict):
            warnings.append(f"第 {index} 行不是 JSON 对象")
            continue
        obj.setdefault("event_index", index)
        obj.setdefault("line", index)
        obj.setdefault("raw_line", line)
        events.append(obj)
    return {
        "events": events,
        "warnings": warnings,
        "session_meta": session_meta,
        "line_count": len(lines),
    }


def _build_task_session_catalog(row: AppDfaTask) -> dict[str, object]:
    root = _task_root(row)
    run_root = root / "run" if str(root) else Path()
    if not run_root.exists():
        return {
            "task_id": row.task_id,
            "status": row.status,
            "sessions_root": str(run_root / "sessions"),
            "index_path": str(run_root / "sessions" / "index.json"),
            "generated_at": None,
            "items": [],
            "index": {
                "version": 1,
                "generated_at": None,
                "task_id": row.task_id,
                "task_status": row.status,
                "sessions_root": str(run_root / "sessions"),
                "summary": {},
                "nodes": [],
                "edges": [],
                "groups": [],
                "warnings": [],
            },
        }
    result_json = _load_task_result_json(row)
    return build_session_catalog(
        task_id=row.task_id,
        row_status=row.status,
        run_root=run_root,
        result_json=result_json,
        write_json_atomic=_write_json_atomic,
    )


def _load_task_result_json(row: AppDfaTask) -> dict | None:
    path = _task_result_path(row)
    if path and path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return loaded
        except Exception as exc:
            logger.warning("failed to load task result file %s: %s", path, exc)
    return row.result_json if isinstance(row.result_json, dict) else None


def _write_task_result_json(row: AppDfaTask, payload: dict) -> str | None:
    path = _task_result_path(row)
    if not path:
        return None
    _write_json_atomic(path, payload)
    return str(path)


def _build_entry_analysis_context(task_config_json: dict | None) -> str:
    cfg = task_config_json if isinstance(task_config_json, dict) else {}
    function_description = str(cfg.get("function_description") or "").strip()
    entry_reason = str(cfg.get("entry_reason") or "").strip()
    taint_details = cfg.get("taint_details") if isinstance(cfg.get("taint_details"), list) else []
    taint_params = [
        str(value).strip()
        for value in (cfg.get("taint_params") or [])
        if str(value).strip()
    ]
    if not function_description and not entry_reason and not taint_details:
        return ""

    lines = ["# 上游入口分析提供的上下文"]
    if function_description:
        fn_source = str(cfg.get("function_description_source") or "").strip()
        fn_suffix = f" [source={fn_source}]" if fn_source else ""
        lines.append(f"- 函数说明{fn_suffix}: {function_description[:ENTRY_CONTEXT_MAX_DESC_CHARS]}")
    if entry_reason:
        reason_source = str(cfg.get("entry_reason_source") or "").strip()
        reason_suffix = f" [source={reason_source}]" if reason_source else ""
        lines.append(f"- 入口判定原因{reason_suffix}: {entry_reason[:ENTRY_CONTEXT_MAX_DESC_CHARS]}")
    if taint_params:
        lines.append(f"- 上游标记的污点参数: {', '.join(taint_params)}")
    if taint_details:
        lines.append("- 污点参数说明:")
        omitted = max(0, len(taint_details) - ENTRY_CONTEXT_MAX_TAINTS)
        for item in taint_details[:ENTRY_CONTEXT_MAX_TAINTS]:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            description = (str(item.get("description") or "").strip() or "上游未提供额外说明。")[:ENTRY_CONTEXT_MAX_DESC_CHARS]
            source_kind = str(item.get("source_kind") or "").strip()
            description_source = str(item.get("description_source") or "").strip()
            suffix_parts = []
            if source_kind:
                suffix_parts.append(f"source_kind={source_kind}")
            if description_source:
                suffix_parts.append(f"source={description_source}")
            suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
            lines.append(f"  - {name}: {description}{suffix}")
        if omitted > 0:
            lines.append(f"  - ... 另有 {omitted} 个 taint 说明被折叠，避免上下文过长。")
    lines.append("以上信息来自上游入口分析，仅作为辅助上下文；若与源码不一致，以源码为准。")
    context = "\n".join(lines)
    if len(context) > ENTRY_CONTEXT_MAX_CHARS:
        context = context[:ENTRY_CONTEXT_MAX_CHARS].rstrip() + "\n...（上游入口分析上下文已截断）"
    return context


def _persist_terminal_failure(row: AppDfaTask, error: str, *, status: str = "error") -> dict:
    payload = {
        "task_id": row.task_id,
        "status": status,
        "analysis_status": status,
        "completion_reason": error,
        "task": row.prompt_content or row.task_name or "",
        "error": error,
        "rounds": [],
        "total_duration_ms": 0,
        "total_tokens": _token_usage_dict(None),
    }
    result_file = _write_task_result_json(row, payload)
    row.result_json = _lightweight_result_json(row, payload, result_file)
    row.error = error
    return payload


def _input_manifest_path(row: AppDfaTask) -> Path | None:
    root = _task_root(row)
    return root / "input" / "input_manifest.json" if root else None


def _path_metadata(path_value: str | None) -> dict:
    if not path_value:
        return {"path": None, "exists": False}
    path = Path(path_value)
    try:
        stat = path.stat()
        kind = "directory" if path.is_dir() else "file" if path.is_file() else "other"
        return {
            "path": str(path),
            "real_path": str(path.resolve()),
            "exists": True,
            "kind": kind,
            "size_bytes": stat.st_size if path.is_file() else None,
            "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }
    except OSError:
        return {
            "path": str(path),
            "real_path": os.path.realpath(os.path.abspath(str(path))),
            "exists": False,
        }


def _validate_fileserver_path(path_value: str, *, fs_root: str, field_name: str, must_exist: bool = True, must_be_dir: bool = False) -> str:
    from fastapi import HTTPException as _HTTPException

    raw = str(path_value or "").strip()
    if not raw:
        raise _HTTPException(400, f"{field_name} 不能为空")
    normalized = os.path.realpath(os.path.abspath(raw))
    normalized_fs = os.path.realpath(os.path.abspath(fs_root))
    if not normalized.startswith(normalized_fs + os.sep) and normalized != normalized_fs:
        raise _HTTPException(400, f"{field_name} 必须位于 {fs_root} 下")
    if must_exist and not os.path.exists(normalized):
        raise _HTTPException(400, f"{field_name} 不存在: {raw}")
    if must_be_dir and os.path.exists(normalized) and not os.path.isdir(normalized):
        raise _HTTPException(400, f"{field_name} 必须是目录: {raw}")
    return normalized


def _normalize_source_file_for_root(source_root_path: str, source_file: str) -> str:
    from fastapi import HTTPException as _HTTPException

    raw = str(source_file or "").strip().replace("\\", "/")
    if not raw:
        return ""
    root = Path(os.path.realpath(os.path.abspath(source_root_path)))
    marker = "/data/files/"
    embedded_absolute = raw[raw.index(marker):] if marker in raw else None
    candidate = Path(embedded_absolute or raw)
    resolved = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
    try:
        relative = resolved.relative_to(root)
    except Exception as exc:
        raise _HTTPException(400, f"source_file 超出 source_root_path 范围: {source_file}") from exc
    normalized = relative.as_posix().strip()
    if not normalized or normalized == "." or any(part == ".." for part in relative.parts):
        raise _HTTPException(400, f"source_file 非法: {source_file}")
    if not resolved.is_file():
        raise _HTTPException(400, f"source_file 对应文件不存在: {source_file}")
    return normalized


def _write_input_manifest(row: AppDfaTask) -> str | None:
    """Write task input metadata only; never copy original input contents."""
    path = _input_manifest_path(row)
    if not path:
        return None
    prompt = row.prompt_content or ""
    payload = {
        "schema_version": 1,
        "generated_at": isoformat_local(now_local()),
        "task": {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "task_name": row.task_name,
            "task_description": row.task_description,
            "created_by": row.created_by,
            "created_at": isoformat_local(row.created_at),
            "started_at": isoformat_local(row.started_at),
        },
        "input": _path_metadata(row.input_path),
        "paths": {
            "module_input_path": row.module_input_path or row.input_path,
            "source_root_path": row.source_root_path or row.input_path,
        },
        "prompt": {
            "template_id": row.prompt_template_id,
            "content_length": len(prompt),
            "content_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest() if prompt else None,
        },
        "origin": _origin_payload(row),
        "config": {
            "has_task_overrides": bool(row.task_config_json),
            "override_keys": sorted((row.task_config_json or {}).keys()),
            "source_file": str((row.task_config_json or {}).get("source_file") or ""),
            "definition_kind": str((row.task_config_json or {}).get("definition_kind") or ""),
        },
    }
    _write_json_atomic(path, payload)
    return str(path)


def _lightweight_result_json(row: AppDfaTask, payload: dict | None, result_file: str | None = None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("result_externalized"):
        return {
            **payload,
            "result_file": payload.get("result_file") or result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
            "result_externalized": True,
        }
    total_tokens = payload.get("total_tokens") if isinstance(payload.get("total_tokens"), dict) else None
    rounds = payload.get("rounds") if isinstance(payload.get("rounds"), list) else []
    return {
        "result_file": result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
        "result_externalized": True,
        "status": payload.get("status") or row.status,
        "analysis_status": payload.get("analysis_status") or payload.get("status") or row.status,
        "completion_reason": payload.get("completion_reason"),
        "error": payload.get("error"),
        "round_count": len(rounds),
        "total_duration_ms": payload.get("total_duration_ms"),
        "total_tokens": total_tokens,
    }


def _token_usage_dict(value: dict | None) -> dict[str, float | int]:
    usage = value if isinstance(value, dict) else {}
    return {
        "input": int(usage.get("input", 0) or 0),
        "output": int(usage.get("output", 0) or 0),
        "cache_read": int(usage.get("cache_read", 0) or 0),
        "cache_write": int(usage.get("cache_write", 0) or 0),
        "cost": float(usage.get("cost", 0.0) or 0.0),
    }


def _merge_usage(items: list[dict | None]) -> dict[str, float | int]:
    total = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0}
    for item in items:
        usage = _token_usage_dict(item)
        total["input"] += int(usage["input"])
        total["output"] += int(usage["output"])
        total["cache_read"] += int(usage["cache_read"])
        total["cache_write"] += int(usage["cache_write"])
        total["cost"] += float(usage["cost"])
    return total


def _token_total(usage: dict[str, float | int]) -> int:
    return int(usage.get("input", 0)) + int(usage.get("output", 0)) + int(usage.get("cache_read", 0)) + int(usage.get("cache_write", 0))


def _safe_eval_key(value: str | None, fallback: str) -> str:
    raw = (value or "").strip() or fallback
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw)
    return safe.strip("._") or fallback


def _build_evaluation_payload(task_id: str, task_status: str, result_payload: dict) -> tuple[dict | None, list[dict]]:
    rounds_payload = result_payload.get("rounds")
    rounds_payload = rounds_payload if isinstance(rounds_payload, list) else []
    records: list[dict] = []
    function_names: set[str] = set()
    passed_rounds = 0
    total_duration_ms = 0.0
    total_usage = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0}
    stage_summary: dict[str, dict[str, float | int]] = {}

    for index, item in enumerate(rounds_payload, start=1):
        if not isinstance(item, dict):
            continue
        function_name = str(item.get("function_name") or item.get("function") or item.get("func") or item.get("entry") or "unknown")
        source_path = str(item.get("source_path") or "")
        status = str(item.get("status") or ("passed" if item.get("passed") else "failed"))
        worker_results = item.get("worker_results") if isinstance(item.get("worker_results"), list) else []
        judge_results = item.get("judge_results") if isinstance(item.get("judge_results"), list) else []
        pass_count = int(item.get("pass_count") or 0)
        judge_count = int(item.get("total_judges") or len(judge_results) or 0)
        scores: list[float] = []
        normalized_judges: list[dict] = []
        judge_usages: list[dict] = []
        for judge_index, judge in enumerate(judge_results, start=1):
            if not isinstance(judge, dict):
                continue
            evaluations = judge.get("evaluations") if isinstance(judge.get("evaluations"), list) else []
            score = None
            feedback_excerpt = ""
            passed_flag = False
            if evaluations and isinstance(evaluations[0], dict):
                score = evaluations[0].get("score")
                feedback_excerpt = str(evaluations[0].get("feedback") or "")
                passed_flag = bool(evaluations[0].get("passed"))
            try:
                if score is not None:
                    scores.append(float(score))
            except (TypeError, ValueError):
                pass
            usage = _token_usage_dict(judge.get("token_usage"))
            judge_usages.append(usage)
            normalized_judges.append({
                "judge_id": judge.get("judge_id") or f"judge-{judge_index}",
                "model": judge.get("model") or "",
                "session_file": judge.get("session_file") or "",
                "score": score,
                "passed": passed_flag,
                "feedback_excerpt": feedback_excerpt[:1000],
                "token_usage": usage,
            })

        worker = worker_results[0] if worker_results and isinstance(worker_results[0], dict) else {}
        worker_usage = _token_usage_dict(worker.get("token_usage") if isinstance(worker, dict) else {})
        merged_usage = _merge_usage([worker_usage, *judge_usages])
        review_pass_rate = (pass_count / judge_count) if judge_count else None
        avg_score = (sum(scores) / len(scores)) if scores else None
        passed_by_vote = bool(item.get("passed"))
        record = {
            "task_id": task_id,
            "module_name": function_name,
            "stage": str(item.get("stage") or "analyse"),
            "round": int(item.get("round") or index),
            "stage_round": int(item.get("stage_round") or item.get("round") or index),
            "status": status,
            "started_at": item.get("started_at"),
            "ended_at": item.get("ended_at"),
            "duration_ms": float(item.get("duration_ms") or 0.0),
            "worker": {
                "model": worker.get("model") if isinstance(worker, dict) else "",
                "session_file": worker.get("session_file") if isinstance(worker, dict) else "",
                "token_usage": worker_usage,
                "error": worker.get("error") if isinstance(worker, dict) else None,
                "artifact_paths": [worker.get("dataflow_file")] if isinstance(worker, dict) and worker.get("dataflow_file") else [],
            },
            "judges": normalized_judges,
            "metrics": {
                "review_pass_rate": review_pass_rate,
                "avg_judge_score": avg_score,
                "token_usage": merged_usage,
                "token_total": _token_total(merged_usage),
                "cost": float(merged_usage["cost"]),
                "passed_by_vote": passed_by_vote,
                "pass_count": pass_count,
                "total_judges": judge_count,
            },
            "module_completed": bool(item.get("module_completed") or passed_by_vote),
            "completion_reason": item.get("completion_reason") or ("passed" if passed_by_vote else status),
            "extra": {
                "function_name": function_name,
                "source_path": source_path,
                "feedback_to_workers": item.get("feedback_to_workers"),
                "best_worker_id": item.get("best_worker_id"),
                "worker_count": len(worker_results),
            },
        }
        function_names.add(function_name)
        if passed_by_vote:
            passed_rounds += 1
        total_duration_ms += float(record["duration_ms"] or 0.0)
        total_usage = _merge_usage([total_usage, merged_usage])
        stage = str(record["stage"])
        stage_item = stage_summary.setdefault(stage, {
            "round_count": 0,
            "passed_round_count": 0,
            "review_pass_rate_total": 0.0,
            "review_pass_rate_count": 0,
        })
        stage_item["round_count"] += 1
        stage_item["passed_round_count"] += 1 if passed_by_vote else 0
        if review_pass_rate is not None:
            stage_item["review_pass_rate_total"] += float(review_pass_rate)
            stage_item["review_pass_rate_count"] += 1
        records.append(record)

    if not records:
        return None, []

    latest_by_function: dict[str, dict] = {}
    for record in records:
        function_name = str(record.get("module_name") or "")
        current = latest_by_function.get(function_name)
        if current is None or int(record.get("stage_round") or 0) >= int(current.get("stage_round") or 0):
            latest_by_function[function_name] = record
    completed_function_count = sum(1 for record in latest_by_function.values() if record.get("metrics", {}).get("passed_by_vote"))
    failed_function_count = max(0, len(latest_by_function) - completed_function_count)

    summary = {
        "task_id": task_id,
        "task_status": result_payload.get("status") or task_status,
        "module_count": len(function_names),
        "completed_module_count": completed_function_count,
        "failed_module_count": failed_function_count,
        "round_count": len(records),
        "passed_round_count": passed_rounds,
        "function_count": len(function_names),
        "total_duration_ms": total_duration_ms,
        "avg_duration_ms": (total_duration_ms / len(records)) if records else 0.0,
        "total_token_usage": total_usage,
        "total_tokens": _token_total(total_usage),
        "total_cost": float(total_usage["cost"]),
        "generated_at": isoformat_local(now_local()),
        "stage_summary": {
            stage: {
                "round_count": int(item["round_count"]),
                "passed_round_count": int(item["passed_round_count"]),
                "avg_review_pass_rate": (
                    float(item["review_pass_rate_total"]) / int(item["review_pass_rate_count"])
                ) if int(item["review_pass_rate_count"]) > 0 else None,
            }
            for stage, item in stage_summary.items()
        },
        "effectiveness": {
            "final_round_pass_rate": (completed_function_count / len(latest_by_function)) if latest_by_function else 0.0,
        },
    }
    return summary, records


def _write_task_evaluation_files(row: AppDfaTask, result_payload: dict) -> None:
    run_root = _task_run_root(row)
    if not run_root:
        return
    summary, rounds = _build_evaluation_payload(row.task_id, row.status, result_payload)
    if summary is None:
        return
    for round_dir in run_root.glob("round_*"):
        if round_dir.is_dir():
            for path in round_dir.glob("*.json"):
                if path.name.endswith(".tmp"):
                    continue
                try:
                    path.unlink()
                except OSError:
                    pass
    for record in rounds:
        round_no = int(record.get("round") or 0)
        round_dir = run_root / f"round_{round_no:03d}"
        module_key = _safe_eval_key(str(record.get("module_name") or ""), "function")
        stage_key = _safe_eval_key(str(record.get("stage") or ""), "stage")
        _write_json_atomic(round_dir / f"{module_key}.{stage_key}.json", record)
    _write_json_atomic(run_root / "evaluation_summary.json", summary)


def _origin_payload(row: AppDfaTask) -> dict:
    task_origin_type = str(row.task_origin_type or "").strip() or "manual"
    parent_task_type = str(row.parent_task_type or "").strip() or None
    origin_label = (
        "二进制安全-源码扫描"
        if task_origin_type == "binary_security" and parent_task_type == "source"
        else "二进制安全-二进制类扫描"
        if task_origin_type == "binary_security"
        else "手动任务"
    )
    return {
        "task_origin_type": task_origin_type,
        "parent_project_id": row.parent_project_id,
        "parent_task_id": row.parent_task_id,
        "parent_task_type": parent_task_type,
        "parent_stage_name": row.parent_stage_name,
        "parent_stage_item_id": row.parent_stage_item_id,
        "parent_stage_item_key": row.parent_stage_item_key,
        "origin_label": origin_label,
        "parent_task_display": row.parent_task_id,
    }


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/data_flow_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def _load_svc_config_from_db(db: Session, project_id: str) -> "object":
    """从数据库读取分析配置，构造 ServiceConfig；失败时回退到文件读取。"""
    try:
        from app.service.config_service import get_config_service
        from app.models import ServiceConfig as _ServiceConfig
        cfg_dict = get_config_service().get_config(db, project_id)
        for _k in ("updated_at", "project_id"):
            cfg_dict.pop(_k, None)
        svc = _ServiceConfig(**cfg_dict)
        if not svc.workers.agents or not svc.judges.agents:
            logger.warning(
                "project config has empty agents (%s), falling back to file defaults: workers=%s judges=%s",
                project_id,
                len(svc.workers.agents),
                len(svc.judges.agents),
            )
            fallback = _load_svc_config()
            if not svc.workers.agents:
                svc.workers = fallback.workers
            if not svc.judges.agents:
                svc.judges = fallback.judges
        return svc
    except Exception as _exc:
        logger.warning("_load_svc_config_from_db failed (%s), falling back to file: %s", project_id, _exc)
        return _load_svc_config()


def _write_models_json_from_db(db: Session) -> None:
    """从配置中心拉取 LLM Provider 并写入 pi 的 models.json。"""
    try:
        from app.config import get_service_yaml
        from app.service.llm_provider_sync import sync_providers_to_pi
        svc_yaml = get_service_yaml()
        sync_providers_to_pi(
            base_url=svc_yaml.configcenter.base_url,
            token=svc_yaml.auth_service.service_machine_token,
            timeout=svc_yaml.configcenter.timeout,
        )
    except Exception as _exc:
        logger.warning("_write_models_json_from_db failed: %s", _exc, exc_info=True)


def generate_prompt_from_path(input_path: str) -> str:
    """Generate a default Chinese data flow analysis prompt from the input path."""
    path_lower = input_path.lower()
    if any(kw in path_lower for kw in (".c", ".cpp", ".cc", "source", "src")):
        subject = "C/C++ 源代码文件"
        action = "重点识别外部输入的污点传播路径、危险函数调用链及潜在注入点"
    elif any(kw in path_lower for kw in (".py", "python", "script")):
        subject = "Python 脚本文件"
        action = "追踪用户输入的数据流向，识别不安全的反序列化、命令注入及SQL注入风险"
    elif any(kw in path_lower for kw in ("firmware", "binary", "elf", "bin")):
        subject = "二进制/固件文件"
        action = "分析数据流传播路径，识别缓冲区溢出、格式字符串漏洞及权限提升路径"
    elif any(kw in path_lower for kw in ("java", ".jar", ".class")):
        subject = "Java 代码文件"
        action = "追踪输入数据流，识别反序列化漏洞、SSRF及XXE等安全风险"
    else:
        subject = "目标文件"
        action = "完成全面的数据流安全分析，识别污点传播路径与潜在漏洞"

    return (
        f"对路径 `{input_path}` 下的{subject}进行数据流安全分析，"
        f"{action}，并输出详细的数据流分析报告。"
    )


def _flush_stages(task_id: str, events: list[dict], owner_id: str | None = None, epoch: int | None = None, control_version: int | None = None) -> None:
    """将实时事件缓冲写入 DB，供前端轮询展示进度。"""
    try:
        from sqlalchemy.orm.attributes import flag_modified
        from app.db import get_db as _get_db
        _gen = _get_db()
        _db = next(_gen)
        try:
            _r = _db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if _r:
                if owner_id is not None and epoch is not None and control_version is not None:
                    if not (
                        _r.execution_owner_id == owner_id
                        and int(_r.execution_epoch or 0) == int(epoch)
                        and int(_r.control_version or 0) == int(control_version)
                    ):
                        return
                _r.stages_json = {"events": [dict(e) for e in events]}
                flag_modified(_r, "stages_json")
                _db.commit()
        finally:
            try:
                next(_gen)
            except StopIteration:
                pass
    except Exception as _exc:
        logger.warning("_flush_stages failed: %s", _exc, exc_info=True)


class TaskService:
    def __init__(self) -> None:
        self._supervisor_thread: threading.Thread | None = None
        self._supervisor_stop = threading.Event()
        self._last_supervisor_run_at = 0.0
        self._last_supervisor_error: str | None = None

    def local_running_task_count(self) -> int:
        with _RUNNING_TASK_LOCK:
            return sum(1 for ctx in _running_tasks.values() if ctx.execution_alive())

    def running_task_snapshot(self) -> list[dict[str, object]]:
        with _RUNNING_TASK_LOCK:
            rows = []
            for task_id, ctx in _running_tasks.items():
                rows.append(
                    {
                        "task_id": task_id,
                        "execution_alive": ctx.execution_alive(),
                        "lease_alive": ctx.lease_alive(),
                        "last_progress_at": ctx.last_progress_at,
                        "last_lease_heartbeat_at": ctx.last_lease_heartbeat_at,
                        "last_state_sync_at": ctx.last_state_sync_at,
                        "termination_reason": ctx.termination_reason,
                        "cancel_requested": ctx.cancel_requested.is_set(),
                        "epoch": ctx.epoch,
                        "execution_epoch": ctx.epoch,
                        "control_version": ctx.control_version,
                        "owner_id": WORKER_ID,
                        "status": "running" if ctx.execution_alive() else "pending",
                        "task_root": ctx.task_root,
                        "run_root": ctx.run_root or _epoch_run_root_from_task_root(ctx.task_root, ctx.epoch),
                        "active_context": ctx.execution_alive() or ctx.lease_alive(),
                        "root_task_id": ctx.root_task_id or task_id,
                        "parent_task_id": ctx.parent_task_id,
                        "session_kind": ctx.session_kind or "root",
                    }
                )
            for task_id, ctx in _child_execution_contexts.items():
                rows.append(
                    {
                        "task_id": task_id,
                        "execution_alive": ctx.execution_alive(),
                        "lease_alive": ctx.lease_alive(),
                        "last_progress_at": ctx.last_progress_at,
                        "last_lease_heartbeat_at": ctx.last_lease_heartbeat_at,
                        "last_state_sync_at": ctx.last_state_sync_at,
                        "termination_reason": ctx.termination_reason,
                        "cancel_requested": ctx.cancel_requested.is_set(),
                        "epoch": ctx.epoch,
                        "execution_epoch": ctx.epoch,
                        "control_version": ctx.control_version,
                        "owner_id": WORKER_ID,
                        "status": "running",
                        "task_root": ctx.task_root,
                        "run_root": ctx.run_root or _epoch_run_root_from_task_root(ctx.task_root, ctx.epoch),
                        "active_context": True,
                        "root_task_id": ctx.root_task_id or task_id,
                        "parent_task_id": ctx.parent_task_id,
                        "session_kind": ctx.session_kind or "subtask",
                    }
                )
            return rows

    def request_cancel(self, task_id: str, *, reason: str) -> bool:
        ctx = _get_running_task_context(task_id)
        if ctx is None:
            return False
        ctx.termination_reason = reason
        ctx.cancel_requested.set()
        if ctx.orch is not None:
            ctx.orch.abort()
        return True

    def request_lease_requeue(
        self,
        db: Session,
        task_id: str,
        *,
        owner_id: str,
        epoch: int,
        control_version: int,
        recovery_reason: str = AUTO_REQUEUE_REASON_LEASE_LOST,
    ) -> bool:
        ctx = _get_running_task_context(task_id)
        if ctx is not None:
            ctx.termination_reason = recovery_reason
            ctx.cancel_requested.set()
            ctx.lease_recovery_state = LEASE_RECOVERY_STATE_REQUESTED
            ctx.lease_recovery_error = None
            ctx.lease_recovery_updated_at = _time.time()
            if ctx.orch is not None:
                ctx.orch.abort()
        return self._request_lease_requeue_with_retry(
            task_id,
            owner_id=owner_id,
            epoch=epoch,
            control_version=control_version,
            recovery_reason=recovery_reason,
        )

    def _request_lease_requeue_with_retry(
        self,
        task_id: str,
        *,
        owner_id: str,
        epoch: int,
        control_version: int,
        recovery_reason: str,
    ) -> bool:
        from app.db import get_db

        def _attempt(attempt: int) -> bool:
            db_gen = get_db()
            lease_db: Session = next(db_gen)
            try:
                recovered = self._commit_lease_requeue(
                    lease_db,
                    task_id,
                    owner_id=owner_id,
                    epoch=epoch,
                    control_version=control_version,
                    recovery_reason=recovery_reason,
                    attempt=attempt,
                )
                if recovered:
                    ctx = _get_running_task_context(task_id)
                    if ctx is not None:
                        ctx.lease_recovery_state = LEASE_RECOVERY_STATE_REQUEUE_COMMITTED
                        ctx.lease_recovery_error = None
                        ctx.lease_recovery_updated_at = _time.time()
                return recovered
            except Exception as exc:
                ctx = _get_running_task_context(task_id)
                if ctx is not None:
                    ctx.lease_recovery_error = str(exc)
                    ctx.lease_recovery_updated_at = _time.time()
                try:
                    lease_db.rollback()
                except Exception:
                    pass
                recover_row = lease_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if recover_row is not None:
                    recover_row.lease_recovery_state = LEASE_RECOVERY_STATE_FAILED
                    recover_row.lease_recovery_error = str(exc)
                    recover_row.lease_recovery_updated_at = now_local()
                    _record_task_event(
                        lease_db,
                        row=recover_row,
                        event_type="task_lease_requeue_retry" if attempt < DB_RETRY_MAX_ATTEMPTS else "task_lease_requeue_failed",
                        message="任务 lease_lost 自动回队失败，正在重试" if attempt < DB_RETRY_MAX_ATTEMPTS else "任务 lease_lost 自动回队失败，原因为数据库失败",
                        level="warning",
                        status=recover_row.status,
                        dispatch_status=recover_row.dispatch_status,
                        payload={
                            "attempt": attempt,
                            "error": str(exc),
                            "owner_id": owner_id,
                            "epoch": epoch,
                            "control_version": control_version,
                            "failure_code": DB_FAILURE_REASON_CODE if _is_db_connection_error(exc) else None,
                        },
                    )
                    lease_db.commit()
                raise
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass

        try:
            return bool(_run_db_retry("request_lease_requeue", _attempt, max_attempts=DB_RETRY_MAX_ATTEMPTS))
        except Exception as exc:
            ctx = _get_running_task_context(task_id)
            if ctx is not None:
                ctx.lease_recovery_state = LEASE_RECOVERY_STATE_FAILED
                ctx.lease_recovery_error = str(exc)
                ctx.lease_recovery_updated_at = _time.time()
            return False

    def _commit_lease_requeue(
        self,
        db: Session,
        task_id: str,
        *,
        owner_id: str,
        epoch: int,
        control_version: int,
        recovery_reason: str,
        attempt: int,
    ) -> bool:
        row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
        if row is None:
            return False
        row.lease_recovery_state = LEASE_RECOVERY_STATE_REQUESTED
        row.lease_recovery_error = None
        row.lease_recovery_updated_at = now_local()
        recovered = recover_running_task_if_owner(
            db,
            task_id,
            owner_id,
            epoch,
            control_version,
            reason=recovery_reason,
        )
        if not recovered:
            refreshed = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            return bool(refreshed and str(refreshed.status or "") == "pending")
        refreshed = db.query(AppDfaTask).filter_by(task_id=task_id).first()
        if refreshed is None:
            return False
        refreshed.lease_recovery_state = LEASE_RECOVERY_STATE_REQUEUE_COMMITTED
        refreshed.lease_recovery_error = None
        refreshed.lease_recovery_updated_at = now_local()
        payload = {
            "owner_id": owner_id,
            "epoch": epoch,
            "control_version": control_version,
            "lease_lost_count": int(refreshed.lease_lost_count or 0),
            "lease_requeue_not_before": isoformat_local(refreshed.lease_requeue_not_before),
            "recovery_reason": recovery_reason,
            "attempt": attempt,
        }
        _record_task_event(
            db,
            row=refreshed,
            event_type="task_lease_lost",
            message="任务心跳续租失败，租约已丢失",
            level="warning",
            status=refreshed.status,
            dispatch_status=refreshed.dispatch_status,
            worker_id=owner_id,
            execution_owner_id=owner_id,
            execution_epoch=epoch,
            control_version=control_version,
            payload=payload,
        )
        _record_task_event(
            db,
            row=refreshed,
            event_type="task_auto_requeued_after_lease_lost",
            message="任务因租约丢失已自动回退为 pending",
            level="warning",
            status=refreshed.status,
            dispatch_status=refreshed.dispatch_status,
            worker_id=owner_id,
            execution_owner_id=owner_id,
            execution_epoch=epoch,
            control_version=control_version,
            payload=payload,
        )
        _record_task_event(
            db,
            row=refreshed,
            event_type="task_lease_requeue_waiting",
            message=f"任务等待自动重排队，固定退避 {LEASE_REQUEUE_DELAY_SECONDS}s",
            level="info",
            status=refreshed.status,
            dispatch_status=refreshed.dispatch_status,
            payload=payload,
        )
        db.commit()
        return True

    def supervisor_status(self) -> dict[str, object]:
        return {
            "thread_alive": bool(self._supervisor_thread and self._supervisor_thread.is_alive()),
            "last_run_at": self._last_supervisor_run_at,
            "last_error": self._last_supervisor_error,
        }

    def start_supervisor(self) -> None:
        if self._supervisor_thread and self._supervisor_thread.is_alive():
            return
        self._supervisor_stop = threading.Event()

        def _worker() -> None:
            from app.db import get_db
            while not self._supervisor_stop.wait(EXECUTION_SUPERVISOR_INTERVAL_SECONDS):
                self._last_supervisor_run_at = _time.time()
                try:
                    with _RUNNING_TASK_LOCK:
                        contexts = list(_running_task_contexts.items())
                    for task_id, ctx in contexts:
                        if ctx.execution_alive():
                            if ctx.cancel_requested.is_set() and ctx.orch is not None:
                                ctx.orch.abort()
                            if EXECUTION_NO_PROGRESS_SECONDS > 0 and (_time.time() - max(ctx.last_progress_at, ctx.started_at)) > EXECUTION_NO_PROGRESS_SECONDS:
                                ctx.termination_reason = "no_progress"
                                ctx.cancel_requested.set()
                                if ctx.orch is not None:
                                    ctx.orch.abort()
                            continue
                        db_gen = get_db()
                        db: Session = next(db_gen)
                        try:
                            snapshot = load_execution_snapshot(db, task_id)
                            if (
                                snapshot is not None
                                and snapshot.status == "running"
                                and snapshot.execution_owner_id == WORKER_ID
                                and int(snapshot.execution_epoch or 0) == int(ctx.epoch or 0)
                                and int(snapshot.control_version or 0) == int(ctx.control_version or 0)
                            ):
                                _recover_running_task_for_cleanup(
                                    db,
                                    task_id=task_id,
                                    owner_id=WORKER_ID,
                                    epoch=int(ctx.epoch or 0),
                                    control_version=int(ctx.control_version or 0),
                                    reason=ctx.termination_reason or "executor_thread_dead",
                                )
                        finally:
                            try:
                                next(db_gen)
                            except StopIteration:
                                pass
                        _unregister_running_task_context(task_id)
                    db_gen = get_db()
                    db: Session = next(db_gen)
                    try:
                        repaired = self.reconcile_failed_lease_recoveries(db, limit=20)
                        if repaired:
                            log_event(
                                logger,
                                logging.WARNING,
                                "lease recovery repair worker repaired failed samples",
                                event="task_lease_recovery_repaired",
                                repaired_count=repaired,
                            )
                    finally:
                        try:
                            next(db_gen)
                        except StopIteration:
                            pass
                    self._last_supervisor_error = None
                except Exception as exc:
                    self._last_supervisor_error = str(exc)
                    logger.warning("execution supervisor loop failed: %s", exc, exc_info=True)

        self._supervisor_thread = threading.Thread(target=_worker, name="dfa_execution_supervisor", daemon=True)
        self._supervisor_thread.start()

    def stop_supervisor(self) -> None:
        self._supervisor_stop.set()

    def reconcile_orphaned_running_tasks(self, db: Session, *, limit: int = 100) -> int:
        from app.metrics import observe_local_event

        runtime_snapshots = self.running_task_snapshot()
        runtime_index = _build_runtime_snapshot_index(runtime_snapshots)
        now_ts = _time.time()
        candidates = list_recoverable_orphaned_running_tasks(db, limit=limit)
        recovered_rows: list[tuple[AppDfaTask, str, str | None, str | None, str | None]] = []
        events_written = False
        for candidate in candidates:
            matching = runtime_index.get(candidate.task_id, [])
            latest_snapshot = None
            for snapshot in matching:
                if _is_runtime_snapshot_active(snapshot, now_ts=now_ts):
                    latest_snapshot = snapshot
                    break
            if latest_snapshot is not None:
                row = db.query(AppDfaTask).filter_by(task_id=candidate.task_id).first()
                if row is not None:
                    latest_runtime_activity_at = _runtime_snapshot_latest_activity(latest_snapshot)
                    event_type = (
                        "task_running_reconcile_skipped_local_active"
                        if bool(latest_snapshot.get("execution_alive")) or bool(latest_snapshot.get("lease_alive"))
                        else "task_running_reconcile_skipped_recent_activity"
                    )
                    skip_reason = (
                        "local_active_runtime"
                        if event_type == "task_running_reconcile_skipped_local_active"
                        else "recent_runtime_activity"
                    )
                    _record_task_event(
                        db,
                        row=row,
                        event_type=event_type,
                        message="后台巡检检测到本机仍有活跃运行上下文，跳过 orphan-running 回收",
                        level="warning",
                        status=row.status,
                        dispatch_status=row.dispatch_status,
                        worker_id=WORKER_ID,
                        execution_owner_id=row.execution_owner_id,
                        execution_epoch=int(row.execution_epoch or 0),
                        control_version=int(row.control_version or 0),
                        payload={
                            "task_id": row.task_id,
                            "db_owner_id": row.execution_owner_id,
                            "db_execution_epoch": int(row.execution_epoch or 0),
                            "db_lease_until": isoformat_local(row.execution_lease_until),
                            "db_execution_heartbeat_at": isoformat_local(row.execution_heartbeat_at),
                            "runtime_owner_id": latest_snapshot.get("owner_id"),
                            "runtime_execution_epoch": latest_snapshot.get("execution_epoch"),
                            "runtime_active_context": bool(latest_snapshot.get("active_context")),
                            "runtime_execution_alive": bool(latest_snapshot.get("execution_alive")),
                            "runtime_lease_alive": bool(latest_snapshot.get("lease_alive")),
                            "latest_runtime_activity_at": datetime.fromtimestamp(latest_runtime_activity_at, tz=UTC_PLUS_8).isoformat() if latest_runtime_activity_at > 0 else None,
                            "skip_reason": skip_reason,
                            "recovery_reason": candidate.reason,
                        },
                    )
                    events_written = True
                    observe_local_event(
                        "task_running_reconcile",
                        "skipped_local_active" if event_type == "task_running_reconcile_skipped_local_active" else "skipped_recent_activity",
                    )
                continue
            row = db.query(AppDfaTask).filter_by(task_id=candidate.task_id).first()
            if row is not None:
                _record_task_event(
                    db,
                    row=row,
                    event_type="task_running_reconcile_candidate",
                    message="后台巡检确认任务满足 orphan-running 回收条件",
                    level="warning",
                    status=row.status,
                    dispatch_status=row.dispatch_status,
                    worker_id=WORKER_ID,
                    execution_owner_id=row.execution_owner_id,
                    execution_epoch=int(row.execution_epoch or 0),
                    control_version=int(row.control_version or 0),
                    payload={
                        "task_id": row.task_id,
                        "db_owner_id": row.execution_owner_id,
                        "db_execution_epoch": int(row.execution_epoch or 0),
                        "db_lease_until": isoformat_local(row.execution_lease_until),
                        "db_execution_heartbeat_at": isoformat_local(row.execution_heartbeat_at),
                        "runtime_owner_id": None,
                        "runtime_execution_epoch": None,
                        "runtime_active_context": False,
                        "runtime_execution_alive": False,
                        "runtime_lease_alive": False,
                        "latest_runtime_activity_at": None,
                        "recovery_reason": candidate.reason,
                    },
                )
                events_written = True
            if row is None:
                continue
            previous_owner_id = row.execution_owner_id
            previous_dispatch_status = row.dispatch_status
            previous_lease_until = isoformat_local(row.execution_lease_until)
            recovered_ok = False
            if candidate.execution_owner_id:
                recovered_ok = recover_running_task_if_owner(
                    db,
                    candidate.task_id,
                    candidate.execution_owner_id,
                    candidate.execution_epoch,
                    candidate.control_version,
                    reason=candidate.reason,
                )
            else:
                now_local_ts = now_local()
                updated = (
                    db.query(AppDfaTask)
                    .filter(
                        AppDfaTask.task_id == candidate.task_id,
                        AppDfaTask.is_deleted.is_(False),
                        AppDfaTask.status == "running",
                        AppDfaTask.execution_owner_id.is_(None),
                    )
                    .update(
                        {
                            AppDfaTask.status: "pending",
                            AppDfaTask.execution_owner_id: None,
                            AppDfaTask.execution_lease_until: None,
                            AppDfaTask.execution_heartbeat_at: None,
                            AppDfaTask.dispatch_status: "pending",
                            AppDfaTask.finished_at: None,
                            AppDfaTask.error: None,
                            AppDfaTask.latest_abnormal_reason_json: None,
                            AppDfaTask.lease_lost_count: AppDfaTask.lease_lost_count + 1,
                            AppDfaTask.last_lease_lost_at: now_local_ts,
                            AppDfaTask.lease_requeue_not_before: now_local_ts + timedelta(seconds=LEASE_REQUEUE_DELAY_SECONDS),
                            AppDfaTask.last_lease_lost_epoch: AppDfaTask.execution_epoch,
                            AppDfaTask.last_lease_lost_control_version: AppDfaTask.control_version,
                            AppDfaTask.execution_epoch: AppDfaTask.execution_epoch + 1,
                        },
                        synchronize_session=False,
                    )
                )
                if updated:
                    db.commit()
                    recovered_ok = True
            if recovered_ok:
                refreshed = db.query(AppDfaTask).filter_by(task_id=candidate.task_id).first()
                if refreshed is not None:
                    recovered_rows.append((refreshed, candidate.reason, previous_owner_id, previous_dispatch_status, previous_lease_until))
        for row, recovery_reason, previous_owner_id, previous_dispatch_status, previous_lease_until in recovered_rows:
            if row is None:
                continue
            payload = {
                "previous_owner_id": previous_owner_id,
                "previous_dispatch_status": previous_dispatch_status,
                "previous_lease_until": previous_lease_until,
                "recovery_reason": recovery_reason,
                "lease_lost_count": int(row.lease_lost_count or 0),
                "lease_requeue_not_before": isoformat_local(row.lease_requeue_not_before),
                "db_owner_id": row.execution_owner_id,
                "db_execution_epoch": int(row.execution_epoch or 0),
                "db_lease_until": isoformat_local(row.execution_lease_until),
                "runtime_owner_id": None,
                "runtime_execution_epoch": None,
                "runtime_active_context": False,
                "runtime_execution_alive": False,
                "runtime_lease_alive": False,
                "latest_runtime_activity_at": None,
            }
            _record_task_event(
                db,
                row=row,
                event_type="task_running_reconcile_recovered",
                message="后台巡检确认任务已失去有效执行权，已自动回收并重新排队",
                level="warning",
                status=row.status,
                dispatch_status=row.dispatch_status,
                payload=payload,
            )
            _record_task_event(
                db,
                row=row,
                event_type="task_auto_requeued_after_lease_lost",
                message="孤儿 running 任务已自动回退为 pending",
                level="warning",
                status=row.status,
                dispatch_status=row.dispatch_status,
                payload=payload,
            )
            _record_task_event(
                db,
                row=row,
                event_type="task_lease_requeue_waiting",
                message=f"任务等待自动重排队，固定退避 {LEASE_REQUEUE_DELAY_SECONDS}s",
                level="warning",
                status=row.status,
                dispatch_status=row.dispatch_status,
                payload=payload,
            )
            observe_local_event("task_running_reconcile", "recovered")
            events_written = True
        if events_written:
            db.commit()
        return len(recovered_rows)

    def reconcile_failed_lease_recoveries(self, db: Session, *, limit: int = 50) -> int:
        candidate_rows = (
            db.query(AppDfaTask)
            .filter(
                AppDfaTask.is_deleted.is_(False),
                AppDfaTask.status == "error",
                AppDfaTask.execution_owner_id.is_(None),
                (
                    (AppDfaTask.lease_recovery_state == LEASE_RECOVERY_STATE_FAILED)
                    | (AppDfaTask.error.like("%Lost connection to MySQL server during query%"))
                    | (AppDfaTask.error.like("%MySQL server has gone away%"))
                    | (AppDfaTask.error.like("%Broken pipe%"))
                ),
            )
            .order_by(AppDfaTask.updated_at.desc(), AppDfaTask.id.desc())
            .limit(max(1, int(limit or 50)) * 4)
            .all()
        )
        rows: list[AppDfaTask] = []
        for row in candidate_rows:
            abnormal_reason = row.latest_abnormal_reason_json if isinstance(row.latest_abnormal_reason_json, dict) else {}
            abnormal_reason_code = str(abnormal_reason.get("code") or "").strip()
            mysql_disconnect = _is_db_connection_error_text(row.error)
            expected_requeue = bool(
                mysql_disconnect
                and (
                    abnormal_reason_code == "lease_lost"
                    or int(row.lease_lost_count or 0) >= 1
                    or str(row.lease_recovery_state or "") == LEASE_RECOVERY_STATE_FAILED
                )
            )
            if str(row.lease_recovery_state or "") == LEASE_RECOVERY_STATE_FAILED or expected_requeue:
                rows.append(row)
            if len(rows) >= max(1, int(limit or 50)):
                break
        repaired = 0
        for row in rows:
            row.status = "pending"
            row.finished_at = None
            row.error = None
            row.latest_abnormal_reason_json = None
            row.dispatch_status = "pending"
            row.lease_lost_count = max(1, int(row.lease_lost_count or 0) + 1)
            row.last_lease_lost_at = row.last_lease_lost_at or now_local()
            row.lease_requeue_not_before = now_local()
            row.lease_recovery_state = LEASE_RECOVERY_STATE_REQUEUE_COMMITTED
            row.lease_recovery_error = None
            row.lease_recovery_updated_at = now_local()
            flag_modified(row, "latest_abnormal_reason_json")
            _record_task_event(
                db,
                row=row,
                event_type="task_lease_recovery_repaired",
                message="后台修复 worker 已补救 lease_lost 回队失败样本",
                level="warning",
                status=row.status,
                dispatch_status=row.dispatch_status,
                payload={"repair_reason": "lease_recovery_failed_sample"},
            )
            repaired += 1
        if repaired:
            db.commit()
        return repaired

    async def dispatch_once(self) -> str | None:
        if self.local_running_task_count() >= MAX_LOCAL_RUNNING_TASKS:
            from app.metrics import observe_local_event

            observe_local_event("dispatch_capacity_blocked", "skip")
            return None
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            claimed = claim_one_runnable_task(db, WORKER_ID)
            if claimed is None:
                from app.metrics import observe_local_event

                observe_local_event("dispatch_claim", "empty")
                return None
            if _active_local_task(claimed.task_id) is not None:
                release_lease(db, claimed.task_id, WORKER_ID, claimed.epoch)
                from app.metrics import observe_local_event

                observe_local_event("dispatch_claim", "duplicate_local")
                log_event(
                    logger,
                    logging.WARNING,
                    "claimed task already running locally, released duplicate lease",
                    event="task_lease_released_duplicate_local",
                    task_id=claimed.task_id,
                    owner_id=WORKER_ID,
                    epoch=claimed.epoch,
                    control_version=claimed.control_version,
                )
                return None
            execution_thread = threading.Thread(
                target=_run_execute_task_in_thread,
                args=(self, claimed.task_id, claimed.epoch, claimed.control_version),
                name=f"dfa_task_{claimed.task_id}",
                daemon=True,
            )
            _register_running_task_context(
                claimed.task_id,
                execution_thread=execution_thread,
                epoch=claimed.epoch,
                control_version=claimed.control_version,
            )
            execution_thread.start()
            from app.metrics import observe_local_event

            observe_local_event("dispatch_claim", "success")
            log_event(
                logger,
                logging.INFO,
                "task leased by dispatcher",
                event="task_leased",
                task_id=claimed.task_id,
                owner_id=WORKER_ID,
                epoch=claimed.epoch,
                control_version=claimed.control_version,
            )
            claimed_row = db.query(AppDfaTask).filter_by(task_id=claimed.task_id).first()
            if claimed_row is not None:
                _record_task_event(
                    db,
                    row=claimed_row,
                    event_type="task_leased",
                    message="任务已被 worker 领取租约",
                    status=claimed_row.status,
                    execution_epoch=claimed.epoch,
                    control_version=claimed.control_version,
                    dispatch_status=claimed.dispatch_status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    payload={
                        "owner_id": WORKER_ID,
                        "epoch": claimed.epoch,
                        "control_version": claimed.control_version,
                        "dispatch_status": claimed.dispatch_status,
                    },
                )
                db.commit()
            return claimed.task_id
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def dispatch_until_full(self) -> int:
        claimed = 0
        while self.local_running_task_count() < MAX_LOCAL_RUNNING_TASKS:
            task_id = await self.dispatch_once()
            if not task_id:
                break
            claimed += 1
        return claimed

    def list_task_sessions(self, db: Session, task_id: str) -> list[dict[str, object]]:
        row = self._get_or_404(db, task_id)
        return list(_build_task_session_catalog(row).get("items", []))

    def get_task_session_index(self, db: Session, task_id: str) -> dict[str, object]:
        row = self._get_or_404(db, task_id)
        catalog = _build_task_session_catalog(row)
        return {
            "task_id": catalog.get("task_id") or row.task_id,
            "status": catalog.get("status") or row.status,
            "sessions_root": catalog.get("sessions_root"),
            "index_path": catalog.get("index_path"),
            "generated_at": catalog.get("generated_at"),
            **(catalog.get("index") or {}),
        }

    def get_task_session_file(self, db: Session, task_id: str, relative_path: str) -> dict[str, object]:
        row = self._get_or_404(db, task_id)
        root = _task_root(row)
        if root is None:
            from fastapi import HTTPException
            raise HTTPException(404, "会话目录不存在")
        target = _safe_session_file(root, relative_path)
        parsed = _parse_session_file(target)
        return {
            "path": str(relative_path),
            "session_meta": parsed["session_meta"],
            "events": parsed["events"],
            "warnings": parsed["warnings"],
            "line_count": parsed["line_count"],
        }

    def get_task_evaluation(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        run_root = _latest_epoch_run_root(row)
        warnings: list[str] = []
        if not run_root or not run_root.is_dir():
            return {
                "task_id": row.task_id,
                "status": row.status,
                "current_epoch": None,
                "run_root": None,
                "available": False,
                "summary": None,
                "rounds": [],
                "warnings": warnings,
            }

        summary: dict | None = None
        summary_path = run_root / "evaluation_summary.json"
        if summary_path.exists():
            try:
                loaded = json.loads(summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    summary = loaded
                else:
                    warnings.append("evaluation_summary.json 格式不是对象")
            except Exception as exc:
                warnings.append(f"evaluation_summary.json 读取失败: {exc}")

        rounds: list[dict] = []
        for round_dir in sorted(run_root.glob("round_*")):
            if not round_dir.is_dir():
                continue
            for path in sorted(round_dir.glob("*.json")):
                if path.name.endswith(".tmp"):
                    continue
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception as exc:
                    warnings.append(f"{path.relative_to(run_root)} 读取失败: {exc}")
                    continue
                if not isinstance(payload, dict):
                    warnings.append(f"{path.relative_to(run_root)} 格式不是对象")
                    continue
                payload.setdefault("source_path", str(path))
                rounds.append(payload)

        if summary is None and not rounds:
            result_json = _load_task_result_json(row)
            if result_json:
                summary, rounds = _build_evaluation_payload(row.task_id, row.status, result_json)

        rounds.sort(key=lambda item: (
            int(item.get("round") or 0),
            str(item.get("module_name") or ""),
            str(item.get("stage") or ""),
        ))
        return {
            "task_id": row.task_id,
            "status": row.status,
            "current_epoch": _epoch_label_from_path(run_root),
            "run_root": str(run_root),
            "available": bool(summary or rounds),
            "summary": summary,
            "rounds": rounds,
            "warnings": warnings,
        }

    def list_tasks(self, db: Session, *, project_id: str, page: int = 1,
                   per_page: int = 100, status: Optional[str] = None,
                   mode: Optional[str] = None,
                   parent_task_id: Optional[str] = None,
                   parent_stage_item_id: Optional[str] = None,
                   sort_by: str = "created_at",
                   sort_order: str = "desc") -> dict:
        query = db.query(AppDfaTask).filter(
            AppDfaTask.project_id == project_id,
            AppDfaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppDfaTask.status == status)
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode == "manual":
            query = query.filter(
                (AppDfaTask.task_origin_type.is_(None)) | (AppDfaTask.task_origin_type != "binary_security")
            )
        elif normalized_mode == "binary":
            query = query.filter(
                AppDfaTask.task_origin_type == "binary_security",
                (AppDfaTask.parent_task_type.is_(None)) | (AppDfaTask.parent_task_type != "source"),
            )
        elif normalized_mode == "source":
            query = query.filter(
                AppDfaTask.task_origin_type == "binary_security",
                AppDfaTask.parent_task_type == "source",
            )
        normalized_parent_task_id = str(parent_task_id or "").strip()
        if normalized_parent_task_id:
            query = query.filter(AppDfaTask.parent_task_id == normalized_parent_task_id)
        normalized_parent_stage_item_id = str(parent_stage_item_id or "").strip()
        if normalized_parent_stage_item_id:
            query = query.filter(AppDfaTask.parent_stage_item_id == normalized_parent_stage_item_id)
        sort_column = _TASK_LIST_SORT_COLUMNS.get(str(sort_by or "").strip(), AppDfaTask.created_at)
        order_expr = sort_column.asc() if str(sort_order or "").lower() == "asc" else sort_column.desc()
        total = query.count()
        rows = (query.options(*self._list_load_options())
                .order_by(order_expr, AppDfaTask.id.desc())
                .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_dict(r, include_heavy=False, db=db) for r in rows],
                "total": total, "page": page, "per_page": per_page}

    def get_task_stats(
        self,
        db: Session,
        *,
        project_id: str,
        status: Optional[str] = None,
        mode: Optional[str] = None,
        parent_task_id: Optional[str] = None,
        parent_stage_item_id: Optional[str] = None,
    ) -> dict:
        query = db.query(AppDfaTask.status, func.count(AppDfaTask.id)).filter(
            AppDfaTask.project_id == project_id,
            AppDfaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppDfaTask.status == status)
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode == "manual":
            query = query.filter(
                (AppDfaTask.task_origin_type.is_(None)) | (AppDfaTask.task_origin_type != "binary_security")
            )
        elif normalized_mode == "binary":
            query = query.filter(
                AppDfaTask.task_origin_type == "binary_security",
                (AppDfaTask.parent_task_type.is_(None)) | (AppDfaTask.parent_task_type != "source"),
            )
        elif normalized_mode == "source":
            query = query.filter(
                AppDfaTask.task_origin_type == "binary_security",
                AppDfaTask.parent_task_type == "source",
            )
        normalized_parent_task_id = str(parent_task_id or "").strip()
        if normalized_parent_task_id:
            query = query.filter(AppDfaTask.parent_task_id == normalized_parent_task_id)
        normalized_parent_stage_item_id = str(parent_stage_item_id or "").strip()
        if normalized_parent_stage_item_id:
            query = query.filter(AppDfaTask.parent_stage_item_id == normalized_parent_stage_item_id)
        rows = query.group_by(AppDfaTask.status).all()
        counts = {str(task_status or ""): int(count or 0) for task_status, count in rows}
        return {
            "total": sum(counts.values()),
            "pending": counts.get("pending", 0),
            "running": counts.get("running", 0),
            "passed": counts.get("passed", 0),
            "failed": counts.get("failed", 0),
            "error": counts.get("error", 0),
            "cancelled": counts.get("cancelled", 0),
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        return self._row_to_dict(self._get_or_404(db, task_id))

    def get_task_execution(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        snapshot = load_execution_snapshot(db, task_id)
        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "status": row.status,
            "execution": None if snapshot is None else {
                "owner_id": snapshot.execution_owner_id,
                "epoch": snapshot.execution_epoch,
                "control_version": snapshot.control_version,
                "dispatch_status": snapshot.dispatch_status,
                "lease_until": isoformat_local(snapshot.execution_lease_until),
                "heartbeat_at": isoformat_local(snapshot.execution_heartbeat_at),
            },
        }

    def get_task_timeline(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        events = (
            db.query(AppDfaTaskEvent)
            .filter(AppDfaTaskEvent.task_id == row.task_id)
            .order_by(AppDfaTaskEvent.created_at.desc())
            .all()
        )
        return {
            "task_id": row.task_id,
            "events": [_build_task_event_response(event) for event in events],
        }

    def clear_task_timeline(self, db: Session, task_id: str) -> int:
        row = self._get_or_404(db, task_id)
        deleted = (
            db.query(AppDfaTaskEvent)
            .filter(AppDfaTaskEvent.task_id == row.task_id)
            .delete(synchronize_session=False)
        )
        return int(deleted or 0)

    def delete_task_timeline_event(self, db: Session, task_id: str, event_id: str) -> int:
        row = self._get_or_404(db, task_id)
        deleted = (
            db.query(AppDfaTaskEvent)
            .filter(
                AppDfaTaskEvent.task_id == row.task_id,
                AppDfaTaskEvent.id == event_id,
            )
            .delete(synchronize_session=False)
        )
        return int(deleted or 0)

    def create_task(self, db: Session, *, project_id: str, task_name: str,
                    input_path: str, module_input_path: Optional[str] = None,
                    source_root_path: Optional[str] = None, output_path: Optional[str] = None,
                    task_description: Optional[str] = None,
                    prompt_template_id: Optional[str] = None,
                    prompt_content: str, created_by: Optional[str] = None,
                    task_config_json: Optional[dict] = None,
                    task_origin_type: Optional[str] = None,
                    parent_project_id: Optional[str] = None,
                    parent_task_id: Optional[str] = None,
                    parent_task_type: Optional[str] = None,
                    parent_stage_name: Optional[str] = None,
                    parent_stage_item_id: Optional[str] = None,
                    parent_stage_item_key: Optional[str] = None) -> dict:
        task_id = f"dfa_{uuid.uuid4().hex[:16]}"
        _fs_base = os.environ.get("FILESERVER_ROOT", "/data/files")
        effective_module_input_path = module_input_path or input_path
        normalized_module_input_path = _validate_fileserver_path(
            effective_module_input_path,
            fs_root=_fs_base,
            field_name="module_input_path",
            must_exist=True,
            must_be_dir=True,
        )
        normalized_source_root_path = _validate_fileserver_path(
            source_root_path or effective_module_input_path,
            fs_root=_fs_base,
            field_name="source_root_path",
            must_exist=True,
            must_be_dir=True,
        )
        normalized_task_config = dict(task_config_json or {})
        if normalized_task_config.get("source_file"):
            normalized_task_config["source_file"] = _normalize_source_file_for_root(
                normalized_source_root_path,
                str(normalized_task_config.get("source_file") or ""),
            )
        raw_definition_kind = str(normalized_task_config.get("definition_kind") or "").strip().lower()
        if raw_definition_kind:
            if raw_definition_kind not in {"definition", "declaration", "unknown"}:
                from fastapi import HTTPException as _HTTPException

                raise _HTTPException(400, f"definition_kind 非法: {raw_definition_kind}")
            normalized_task_config["definition_kind"] = raw_definition_kind
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-dataflow-analyse"
        normalized_output = _validate_fileserver_path(
            effective_output,
            fs_root=_fs_base,
            field_name="output_path",
            must_exist=False,
            must_be_dir=False,
        )
        row = AppDfaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=normalized_module_input_path,
            module_input_path=normalized_module_input_path,
            source_root_path=normalized_source_root_path,
            output_path=normalized_output, prompt_template_id=prompt_template_id,
            prompt_content=prompt_content, status="pending", created_by=created_by,
            task_config_json=normalized_task_config or None,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
            execution_owner_id=None,
            execution_lease_until=None,
            execution_heartbeat_at=None,
            execution_epoch=0,
            control_version=0,
            dispatch_status="pending",
            lease_lost_count=0,
            last_lease_lost_at=None,
            lease_requeue_not_before=None,
            last_lease_lost_epoch=None,
            last_lease_lost_control_version=None,
        )
        db.add(row); db.commit(); db.refresh(row)
        _record_task_event(
            db,
            row=row,
            event_type="task_created",
            message="数据流分析任务已创建",
            status=row.status,
            dispatch_status=row.dispatch_status,
            payload={
                "task_name": row.task_name,
                "input_path": row.input_path,
                "module_input_path": row.module_input_path,
                "source_root_path": row.source_root_path,
                "task_origin_type": row.task_origin_type,
                "parent_task_id": row.parent_task_id,
                "parent_stage_item_id": row.parent_stage_item_id,
            },
        )
        db.commit(); db.refresh(row)
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        """在原任务ID上重置并重新执行（SA 模式：in-place restart）。"""
        row = self._get_or_404(db, task_id)
        from sqlalchemy.orm.attributes import flag_modified
        clean_config = {k: v for k, v in (row.task_config_json or {}).items()
                        if k not in ("start_stage", "resume_workspace", "resume")} or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.stages_json = None
        row.result_json = None
        row.error = None
        row.latest_abnormal_reason_json = None
        row.execution_owner_id = None
        row.execution_lease_until = None
        row.execution_heartbeat_at = None
        row.control_version = int(row.control_version or 0) + 1
        row.dispatch_status = "pending"
        row.lease_lost_count = 0
        row.last_lease_lost_at = None
        row.lease_requeue_not_before = None
        row.last_lease_lost_epoch = None
        row.last_lease_lost_control_version = None
        flag_modified(row, "task_config_json")
        flag_modified(row, "latest_abnormal_reason_json")
        db.commit(); db.refresh(row)
        _record_task_event(
            db,
            row=row,
            event_type="task_retried",
            message="任务已原地重置并重新执行",
            status=row.status,
            control_version=int(row.control_version or 0),
            dispatch_status=row.dispatch_status,
            payload={"control_version": int(row.control_version or 0)},
        )
        db.commit(); db.refresh(row)
        self.request_cancel(task_id, reason="restart_requested")
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id, control_version=row.control_version)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """从断点续跑：保留同一任务 ID，跳过已完成阶段从断点继续。"""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再续跑")
        from sqlalchemy.orm.attributes import flag_modified
        svc = _load_svc_config_from_db(db, row.project_id)
        effective_output = row.output_path or svc.output_dir
        prior_epoch = max(1, int(row.execution_epoch or 0))
        resume_workspace = os.path.join(effective_output, task_id, "run", "epochs", f"{prior_epoch:04d}", "workspace-worker-0")
        tcfg = dict(row.task_config_json or {})
        tcfg["start_stage"] = 3
        tcfg["resume_workspace"] = resume_workspace
        tcfg["resume"] = True
        row.task_config_json = tcfg
        row.status = "pending"
        row.finished_at = None
        row.result_json = None
        row.error = None
        row.latest_abnormal_reason_json = None
        row.execution_owner_id = None
        row.execution_lease_until = None
        row.execution_heartbeat_at = None
        row.control_version = int(row.control_version or 0) + 1
        row.dispatch_status = "pending"
        row.lease_lost_count = 0
        row.last_lease_lost_at = None
        row.lease_requeue_not_before = None
        row.last_lease_lost_epoch = None
        row.last_lease_lost_control_version = None
        flag_modified(row, "task_config_json")
        flag_modified(row, "latest_abnormal_reason_json")
        db.commit(); db.refresh(row)
        _record_task_event(
            db,
            row=row,
            event_type="task_resumed",
            message="任务已从断点恢复执行",
            status=row.status,
            control_version=int(row.control_version or 0),
            dispatch_status=row.dispatch_status,
            payload={
                "control_version": int(row.control_version or 0),
                "resume_workspace": resume_workspace,
                "start_stage": 3,
            },
        )
        db.commit(); db.refresh(row)
        self.request_cancel(task_id, reason="resume_requested")
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id, control_version=row.control_version, status="pending")
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        ctx = _get_running_task_context(task_id)
        local_task = _active_local_task(task_id)
        if local_task is not None:
            if self.request_cancel(task_id, reason="control_plane_cancel"):
                log_event(
                    logger,
                    logging.INFO,
                    "task cancel signal sent to orchestrator",
                    event="task_cancel_signal_sent",
                    task_id=task_id,
                    project_id=row.project_id,
                    owner_id=WORKER_ID,
                    epoch=ctx.epoch if ctx is not None else None,
                    control_version=ctx.control_version if ctx is not None else None,
                )
        if ctx is not None and (ctx.task_root or ctx.run_root):
            cleaned = cleanup_task_agent_processes(
                logger.warning,
                label=f"task_cancel:{task_id}",
                task_id=task_id,
                task_root=ctx.task_root,
                run_root=ctx.run_root,
                worker_id=WORKER_ID,
            )
            log_event(
                logger,
                logging.INFO,
                "task agent cleanup finished after cancel",
                event="task_agent_cleanup_finished",
                task_id=task_id,
                project_id=row.project_id,
                owner_id=WORKER_ID,
                cleaned_groups=cleaned,
            )
        row.status = "cancelled"
        row.finished_at = now_local()
        row.control_version = int(row.control_version or 0) + 1
        row.execution_owner_id = None
        row.execution_epoch = int(row.execution_epoch or 0) + 1
        row.execution_lease_until = None
        row.execution_heartbeat_at = None
        row.dispatch_status = None
        reason, changed = _sync_task_abnormal_reason(row)
        _record_abnormal_reason(row, reason, changed=changed)
        _record_abnormal_reason_timeline(db, row, reason, changed=changed)
        _record_task_event(
            db,
            row=row,
            event_type="task_cancelled",
            message="任务已被控制面取消",
            level="warning",
            status=row.status,
            control_version=int(row.control_version or 0),
            payload={
                "orchestrator_abort_sent": bool(ctx and ctx.orch is not None),
                "local_task_active": local_task is not None,
                "cleanup_task_root": ctx.task_root if ctx is not None else None,
                "cleanup_run_root": ctx.run_root if ctx is not None else None,
                "control_version": int(row.control_version or 0),
            },
        )
        db.commit(); db.refresh(row)
        _unregister_running_task_context(task_id)
        log_event(logger, logging.INFO, "task cancelled by control plane", event="task_cancel_requested",
                  task_id=task_id, project_id=row.project_id, control_version=row.control_version, status=row.status,
                  local_task_active=local_task is not None)
        return self._row_to_dict(row)

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> None:
        """软删除任务记录，并可选删除输出目录下的任务文件。运行中任务不允许删除。"""
        from fastapi import HTTPException
        row = self._get_or_404(db, task_id, include_deleted=True)
        lease_live = bool(row.execution_owner_id and row.execution_lease_until and row.execution_lease_until >= now_local())
        local_task = _active_local_task(task_id)
        if bool(row.is_deleted):
            if delete_files and row.output_path:
                task_dir = os.path.join(row.output_path, task_id)
                if os.path.isdir(task_dir):
                    try:
                        shutil.rmtree(task_dir)
                        logger.info("delete_task: removed task dir %s", task_dir)
                    except FileNotFoundError:
                        logger.info("delete_task: task dir already absent %s", task_dir)
                    except Exception as _e:
                        logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
            return
        if row.status == "running" or lease_live or local_task is not None:
            detail = "任务仍在本地执行清理中，请稍后再删除" if local_task is not None else "任务正在运行，请先取消后再删除"
            _record_task_event(
                db,
                row=row,
                event_type="task_delete_rejected",
                message=detail,
                level="warning",
                status=row.status,
                control_version=int(row.control_version or 0),
                dispatch_status=row.dispatch_status,
                payload={
                    "delete_files": bool(delete_files),
                    "lease_live": lease_live,
                    "local_task_active": local_task is not None,
                },
            )
            db.commit()
            raise HTTPException(status_code=409, detail=detail)
        task_dir = None
        files_deleted = False
        files_delete_error: str | None = None
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    shutil.rmtree(task_dir)
                    files_deleted = True
                    logger.info("delete_task: removed task dir %s", task_dir)
                except FileNotFoundError:
                    files_deleted = True
                    logger.info("delete_task: task dir already absent %s", task_dir)
                except Exception as _e:
                    files_delete_error = str(_e)
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        _record_task_event(
            db,
            row=row,
            event_type="task_deleted",
            message="任务已被软删除",
            level="warning",
            status=row.status,
            control_version=int(row.control_version or 0),
            dispatch_status=row.dispatch_status,
            payload={
                "delete_files": bool(delete_files),
                "files_deleted": files_deleted,
                "files_delete_error": files_delete_error,
                "task_dir": task_dir,
                "control_version": int(row.control_version or 0),
                "status_before_delete": row.status,
            },
        )
        row.is_deleted = True
        db.commit()

    async def _execute_task(self, task_id: str, epoch: int, control_version: int) -> None:
        """Run the Orchestrator engine and persist results."""
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        event_buffer: list[dict] = []
        guard_counter = 0
        orch_holder: dict[str, Orchestrator] = {}
        task_root_path: str | None = None
        epoch_run_root_path: str | None = None
        ctx: _RunningTaskContext | None = None

        def _close_current_db() -> None:
            nonlocal db, db_gen
            if db is None:
                return
            _consume_db_generator(db_gen)
            db = None
            db_gen = None

        def _ensure_db() -> Session:
            nonlocal db, db_gen
            if db is None:
                db_gen = get_db()
                db = next(db_gen)
            return db

        # Snapshot any previously-saved events BEFORE execution begins.
        # On resume, row.stages_json already has correct historical events
        # (e.g. root trace_callees from the prior run). Without this baseline,
        # the very first _flush_stages call would overwrite the DB with just
        # [first_new_event], wiping the history and causing the frontend tree
        # to briefly (or permanently, if root is cached) show wrong callees.
        _prev_row_for_baseline = db.query(AppDfaTask).filter_by(task_id=task_id).first()
        _baseline_events: list[dict] = []
        if _prev_row_for_baseline and isinstance(_prev_row_for_baseline.stages_json, dict):
            _baseline_events = list(_prev_row_for_baseline.stages_json.get("events") or [])

        def on_event(event: SwarmEvent) -> None:
            nonlocal guard_counter
            _mark_task_progress(task_id)
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            try:
                event_db_gen = get_db()
                event_db: Session = next(event_db_gen)
                try:
                    event_row = event_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                    if event_row is not None:
                        event_data = dict(event.data or {})
                        event_type = str(event.type or "").strip() or "runtime_event"
                        mapped_event_type = {
                            "task_start": "root_analysis_started",
                            "trace_callees": "callee_discovered",
                            "round_start": "round_started",
                            "round_end": "round_finished",
                            "judge_done": "judge_completed",
                            "task_end": "result_materialized",
                            "error": "task_runtime_error",
                        }.get(event_type, event_type)
                        if event_type == "trace_start":
                            depth = int(event_data.get("depth") or 0)
                            max_depth = int((event_row.task_config_json or {}).get("max_trace_depth") or 0)
                            if max_depth > 0 and depth >= max_depth:
                                mapped_event_type = "depth_limit_reached"
                            else:
                                mapped_event_type = "trace_started"
                        message = {
                            "root_analysis_started": "开始执行根函数分析",
                            "trace_started": "开始追踪函数",
                            "callee_discovered": "发现新的调用函数",
                            "round_started": "新一轮分析开始",
                            "round_finished": "分析轮次结束",
                            "judge_completed": "Judge 评估完成",
                            "depth_limit_reached": "追踪达到最大深度限制",
                            "result_materialized": "任务结果已产出",
                            "task_runtime_error": str(event_data.get("error") or "分析过程中出现错误"),
                        }.get(mapped_event_type, f"运行事件: {mapped_event_type}")
                        _record_task_event(
                            event_db,
                            row=event_row,
                            event_type=mapped_event_type,
                            message=message,
                            level="error" if mapped_event_type == "task_runtime_error" else "info",
                            status=event_row.status,
                            worker_id=WORKER_ID,
                            execution_owner_id=WORKER_ID,
                            execution_epoch=epoch,
                            control_version=control_version,
                            function_name=str(event_data.get("function") or event_data.get("task") or ""),
                            source_file=str(event_data.get("source_path") or event_data.get("source_file") or ""),
                            line_hint=str(event_data.get("line") or event_data.get("line_hint") or ""),
                            payload=event_data,
                        )
                        event_db.commit()
                finally:
                    try:
                        next(event_db_gen)
                    except StopIteration:
                        pass
            except Exception:
                logger.debug("failed to persist DFA task runtime event", exc_info=True)
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, _baseline_events + event_buffer, WORKER_ID, epoch, control_version)
            guard_counter += 1
            if guard_counter % 10 == 0:
                try:
                    from app.db import get_db as _get_db
                    _guard_gen = _get_db()
                    _guard_db: Session = next(_guard_gen)
                    try:
                        if not still_owner(_guard_db, task_id, WORKER_ID, epoch, control_version):
                            guard_row = _guard_db.query(AppDfaTask).filter_by(task_id=task_id).first()
                            ctx = _get_running_task_context(task_id)
                            latest_ctx_activity = _context_latest_activity_at(ctx)
                            local_reconcile_exempt = bool(
                                ctx is not None
                                and (
                                    ctx.execution_alive()
                                    or ctx.lease_alive()
                                    or (
                                        latest_ctx_activity > 0
                                        and (_time.time() - latest_ctx_activity) <= RUNNING_RECONCILE_LOCAL_GRACE_SECONDS
                                    )
                                )
                            )
                            log_event(
                                logger,
                                logging.WARNING,
                                "control-plane ownership changed during event streaming",
                                event="task_control_guard_abort",
                                task_id=task_id,
                                owner_id=WORKER_ID,
                                epoch=epoch,
                                control_version=control_version,
                                background_reconcile_reclaimed=(
                                    guard_row is not None
                                    and str(guard_row.status or "") == "pending"
                                    and str(guard_row.dispatch_status or "") == "pending"
                                ),
                                lease_requeue_committed=(
                                    guard_row is not None
                                    and str(guard_row.lease_recovery_state or "") == LEASE_RECOVERY_STATE_REQUEUE_COMMITTED
                                ),
                                external_owner_changed=(
                                    guard_row is not None
                                    and str(guard_row.execution_owner_id or "").strip() not in {"", WORKER_ID}
                                ),
                                manual_control_change=(
                                    guard_row is not None
                                    and int(guard_row.control_version or 0) != int(control_version or 0)
                                ),
                            )
                            if guard_row is not None:
                                _record_task_event(
                                    _guard_db,
                                    row=guard_row,
                                    event_type="task_control_guard_abort",
                                    message="控制面检测到执行权漂移，终止当前任务",
                                    level="warning",
                                    status=guard_row.status,
                                    worker_id=WORKER_ID,
                                    execution_owner_id=WORKER_ID,
                                    execution_epoch=epoch,
                                    control_version=control_version,
                                    payload={
                                        "owner_id": WORKER_ID,
                                        "epoch": epoch,
                                        "control_version": control_version,
                                        "db_execution_owner_id": guard_row.execution_owner_id,
                                        "db_execution_epoch": int(guard_row.execution_epoch or 0),
                                        "db_control_version": int(guard_row.control_version or 0),
                                        "db_status": guard_row.status,
                                        "ctx_epoch": int(ctx.epoch or 0) if ctx is not None and ctx.epoch is not None else None,
                                        "ctx_control_version": int(ctx.control_version or 0) if ctx is not None and ctx.control_version is not None else None,
                                        "ctx_latest_activity_at": (
                                            datetime.fromtimestamp(latest_ctx_activity, tz=UTC_PLUS_8).isoformat()
                                            if latest_ctx_activity > 0
                                            else None
                                        ),
                                        "local_reconcile_exempt": local_reconcile_exempt,
                                        "background_reconcile_reclaimed": (
                                            str(guard_row.status or "") == "pending"
                                            and str(guard_row.dispatch_status or "") == "pending"
                                        ),
                                        "lease_requeue_committed": (
                                            str(guard_row.lease_recovery_state or "") == LEASE_RECOVERY_STATE_REQUEUE_COMMITTED
                                        ),
                                        "external_owner_changed": str(guard_row.execution_owner_id or "").strip() not in {"", WORKER_ID},
                                        "manual_control_change": int(guard_row.control_version or 0) != int(control_version or 0),
                                    },
                                )
                                _guard_db.commit()
                            if ctx is not None:
                                ctx.cancel_requested.set()
                                ctx.termination_reason = "control_guard_abort"
                            orch = orch_holder.get("orch")
                            if orch and orch._cancel_event is not None:
                                orch._cancel_event.set()
                    finally:
                        try:
                            next(_guard_gen)
                        except StopIteration:
                            pass
                except Exception as exc:
                    logger.warning("control guard check failed for %s: %s", task_id, exc, exc_info=True)

        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                log_event(logger, logging.INFO, "task skipped before execution", event="task_skip_pre_execute",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, status=row.status if row else "missing")
                if row is not None:
                    _record_task_event(
                        db,
                        row=row,
                        event_type="task_skip_pre_execute",
                        message="任务在执行前被跳过",
                        level="warning",
                        status=row.status,
                        worker_id=WORKER_ID,
                        execution_owner_id=WORKER_ID,
                        execution_epoch=epoch,
                        control_version=control_version,
                        dispatch_status=row.dispatch_status,
                        payload={"reason": "cancelled_before_execute"},
                    )
                    db.commit()
                return
            if not still_owner(db, task_id, WORKER_ID, epoch, control_version):
                log_event(logger, logging.INFO, "task lost ownership before execution", event="task_not_owner_pre_execute",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version)
                _record_task_event(
                    db,
                    row=row,
                    event_type="task_not_owner_pre_execute",
                    message="任务在执行前已失去执行权",
                    level="warning",
                    status=row.status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    execution_epoch=epoch,
                    control_version=control_version,
                    dispatch_status=row.dispatch_status,
                    payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                )
                db.commit()
                return

            started_at = row.started_at or now_local()
            if not begin_execution_if_owner(db, task_id, WORKER_ID, epoch, control_version, started_at=started_at):
                from app.metrics import observe_local_event

                observe_local_event("task_started", "rejected")
                log_event(logger, logging.INFO, "failed to enter running state as owner", event="task_begin_execution_rejected",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version)
                rejected_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if rejected_row is not None:
                    _record_task_event(
                        db,
                        row=rejected_row,
                        event_type="task_begin_execution_rejected",
                        message="任务获取执行态失败，未能进入 running",
                        level="warning",
                        status=rejected_row.status,
                        worker_id=WORKER_ID,
                        execution_owner_id=WORKER_ID,
                        execution_epoch=epoch,
                        control_version=control_version,
                        dispatch_status=rejected_row.dispatch_status,
                        payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                    )
                    db.commit()
                return
            from app.metrics import observe_local_event
            started_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if started_row is not None:
                _record_task_event(
                    db,
                    row=started_row,
                    event_type="task_started",
                    message="任务已开始执行",
                    status=started_row.status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    execution_epoch=epoch,
                    control_version=control_version,
                    dispatch_status=started_row.dispatch_status,
                    payload={
                        "owner_id": WORKER_ID,
                        "epoch": epoch,
                        "control_version": control_version,
                        "started_at": isoformat_local(started_at),
                    },
                )
                db.commit()

            observe_local_event("task_started", "success")
            log_event(logger, logging.INFO, "task execution started", event="task_execution_started",
                      task_id=task_id, project_id=row.project_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, status="running")
            db.expire(row)
            db.refresh(row)
            if row.status == "cancelled" or not still_owner(db, task_id, WORKER_ID, epoch, control_version):
                log_event(logger, logging.INFO, "task lost ownership before llm sync", event="task_not_owner_pre_llm_sync",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, status=row.status)
                _record_task_event(
                    db,
                    row=row,
                    event_type="task_not_owner_pre_llm_sync",
                    message="任务在模型执行前失去执行权",
                    level="warning",
                    status=row.status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    execution_epoch=epoch,
                    control_version=control_version,
                    dispatch_status=row.dispatch_status,
                    payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                )
                db.commit()
                return
            _write_input_manifest(row)

            _write_models_json_from_db(db)
            svc = _load_svc_config_from_db(db, row.project_id)

            # Apply per-task config overrides
            tcfg = row.task_config_json or {}
            if tcfg.get("start_stage"):
                svc.start_stage = tcfg["start_stage"]
            if tcfg.get("resume_workspace"):
                svc.resume_workspace = tcfg["resume_workspace"]
            # Legacy resume flag support
            if tcfg.get("resume") and not tcfg.get("start_stage"):
                svc.resume = True

            # Use row.output_path as the working root
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path

            epoch_run_root = _task_epoch_run_root(row, epoch)
            root_output_dir = (_task_root(row) / "output") if _task_root(row) else None
            task_root_path = str(_task_root(row)) if _task_root(row) else None
            epoch_run_root_path = str(epoch_run_root) if epoch_run_root is not None else None
            if epoch_run_root is not None and not bool(tcfg.get("resume", False)):
                if epoch_run_root.exists():
                    try:
                        shutil.rmtree(epoch_run_root)
                    except OSError as exc:
                        logger.warning("failed to clean epoch run root %s: %s", epoch_run_root, exc)
                epoch_run_root.mkdir(parents=True, exist_ok=True)
            elif epoch_run_root is not None:
                epoch_run_root.mkdir(parents=True, exist_ok=True)

            cfg = build_task_config(svc, row.prompt_content, cwd=row.source_root_path or row.input_path)
            if tcfg.get("source_file"):
                cfg.source_file = str(tcfg["source_file"])
            if tcfg.get("function_name"):
                cfg.function_name = str(tcfg["function_name"])
            if tcfg.get("line_hint"):
                cfg.line_hint = str(tcfg["line_hint"])
            if isinstance(tcfg.get("taint_params"), list):
                cfg.taint_params = [str(value).strip() for value in tcfg["taint_params"] if str(value).strip()]
            if tcfg.get("function_description"):
                cfg.function_description = str(tcfg["function_description"]).strip()
            if tcfg.get("function_description_source"):
                cfg.function_description_source = str(tcfg["function_description_source"]).strip()
            if tcfg.get("entry_reason"):
                cfg.entry_reason = str(tcfg["entry_reason"]).strip()
            if tcfg.get("entry_reason_source"):
                cfg.entry_reason_source = str(tcfg["entry_reason_source"]).strip()
            if isinstance(tcfg.get("taint_details"), list):
                cfg.taint_details = [
                    {
                        "name": str(item.get("name") or "").strip(),
                        "description": str(item.get("description") or "").strip(),
                        **({"description_source": str(item.get("description_source")).strip()} if str(item.get("description_source") or "").strip() else {}),
                        **({"source_kind": str(item.get("source_kind")).strip()} if str(item.get("source_kind") or "").strip() else {}),
                    }
                    for item in tcfg["taint_details"]
                    if isinstance(item, dict) and str(item.get("name") or "").strip()
                ]
            entry_context = _build_entry_analysis_context(tcfg)
            if entry_context:
                cfg.context = ((cfg.context or "").rstrip() + "\n\n" + entry_context).strip()
            orch = Orchestrator(config=cfg, on_event=on_event)
            orch_holder["orch"] = orch
            ctx = _register_running_task_context(
                task_id,
                orch=orch,
                task_root=task_root_path,
                run_root=epoch_run_root_path,
                epoch=epoch,
                control_version=control_version,
            )
            if not ctx.lease_alive():
                ctx.lease_stop_requested.clear()
                ctx.lease_thread = _start_task_lease_heartbeat(
                    task_id,
                    epoch=epoch,
                    control_version=control_version,
                    on_lease_lost=lambda lease_db: self.request_lease_requeue(
                        lease_db,
                        task_id,
                        owner_id=WORKER_ID,
                        epoch=epoch,
                        control_version=control_version,
                        recovery_reason=AUTO_REQUEUE_REASON_LEASE_LOST,
                    ),
                )
            _close_current_db()
            result = await orch.execute_recursive(
                task_id,
                _root_out_dir=epoch_run_root,
                _root_output_dir=root_output_dir,
                resume=bool(tcfg.get("resume", False)),
            )
            if ctx is not None:
                ctx.lease_stop_requested.set()

            _flush_stages(task_id, _baseline_events + event_buffer, WORKER_ID, epoch, control_version)
            _ensure_db()
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if row is None:
                return
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                log_event(logger, logging.INFO, "task stopped after control-plane cancel", event="task_cancelled_during_execution",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, status=row.status)
                return
            if not still_owner(db, task_id, WORKER_ID, epoch, control_version):
                log_event(logger, logging.INFO, "task lost ownership before terminal commit", event="task_not_owner_pre_commit",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version)
                _record_task_event(
                    db,
                    row=row,
                    event_type="task_not_owner_pre_commit",
                    message="任务在写入终态前失去执行权",
                    level="warning",
                    status=row.status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    execution_epoch=epoch,
                    control_version=control_version,
                    dispatch_status=row.dispatch_status,
                    payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                )
                db.commit()
                return

            finished_at = now_local()
            stages_json = {"events": _baseline_events + event_buffer, "final": True}
            lightweight_result = None
            terminal_error = None
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = _write_task_result_json(row, result_payload)
                _write_task_evaluation_files(row, result_payload)
                lightweight_result = _lightweight_result_json(row, result_payload, result_file)
                if result.error:
                    terminal_error = result.error
            if not commit_terminal_state_if_owner(
                db,
                task_id,
                WORKER_ID,
                epoch,
                control_version,
                status=result.status.value if result else "error",
                finished_at=finished_at,
                stages_json=stages_json,
                result_json=lightweight_result,
                error=terminal_error,
            ):
                from app.metrics import observe_local_event

                observe_local_event("task_finished", "commit_rejected")
                log_event(logger, logging.WARNING, "terminal commit rejected for stale owner", event="task_terminal_commit_rejected",
                          task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version)
                rejected_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if rejected_row is not None:
                    _record_task_event(
                        db,
                        row=rejected_row,
                        event_type="task_terminal_commit_rejected",
                        message="任务终态提交被拒绝，执行权已过期",
                        level="warning",
                        status=rejected_row.status,
                        worker_id=WORKER_ID,
                        execution_owner_id=WORKER_ID,
                        execution_epoch=epoch,
                        control_version=control_version,
                        dispatch_status=rejected_row.dispatch_status,
                        payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                    )
                    db.commit()
                return
            refreshed = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if refreshed is not None:
                reason, changed = _sync_task_abnormal_reason(refreshed)
                _record_abnormal_reason(refreshed, reason, changed=changed)
                _record_abnormal_reason_timeline(db, refreshed, reason, changed=changed)
                terminal_status = result.status.value if result else "error"
                _record_task_event(
                    db,
                    row=refreshed,
                    event_type={
                        "passed": "task_passed",
                        "failed": "task_failed",
                        "error": "task_error",
                        "completed_limited": "task_completed_limited",
                        "cancelled": "task_cancelled",
                    }.get(terminal_status, "task_finished"),
                    message=f"任务执行结束，状态={terminal_status}",
                    level="error" if terminal_status in {"failed", "error"} else "info",
                    status=terminal_status,
                    worker_id=WORKER_ID,
                    execution_owner_id=WORKER_ID,
                    execution_epoch=epoch,
                    control_version=control_version,
                    payload={
                        "completion_reason": result.completion_reason if result else None,
                        "round_count": len(result.rounds) if result else 0,
                        "error": result.error if result else None,
                    },
                )
                db.commit()
            from app.metrics import observe_local_event

            terminal_status = result.status.value if result else "error"
            observe_local_event("task_finished", terminal_status)
            log_event(logger, logging.INFO, "terminal state committed", event="task_terminal_committed",
                      task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version,
                      status=terminal_status)

        except asyncio.CancelledError:
            from app.metrics import observe_local_event

            observe_local_event("task_finished", "cancelled")
            orch = orch_holder.get("orch")
            if orch is not None:
                orch.abort()
            cleaned = cleanup_task_agent_processes(
                logger.warning,
                label=f"task_cancelled:{task_id}",
                task_id=task_id,
                task_root=task_root_path,
                run_root=epoch_run_root_path,
                worker_id=WORKER_ID,
            )
            log_event(logger, logging.INFO, "task execution cancelled", event="task_execution_cancelled",
                      task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version,
                      cleaned_groups=cleaned)
            try:
                _ensure_db()
                cancelled_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if cancelled_row is not None:
                    _record_task_event(
                        db,
                        row=cancelled_row,
                        event_type="task_execution_cancelled",
                        message="任务执行已取消",
                        level="warning",
                        status=cancelled_row.status,
                        worker_id=WORKER_ID,
                        execution_owner_id=WORKER_ID,
                        execution_epoch=epoch,
                        control_version=control_version,
                        dispatch_status=cancelled_row.dispatch_status,
                        payload={
                            "owner_id": WORKER_ID,
                            "epoch": epoch,
                            "control_version": control_version,
                            "cleanup_groups": cleaned,
                        },
                    )
                    db.commit()
            except Exception:
                db.rollback()
            pass
        except Exception as exc:
            from app.metrics import observe_local_event

            observe_local_event("task_finished", "exception")
            log_event(logger, logging.ERROR, "task execution failed",
                      event="task_error", task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, error=str(exc))
            try:
                _ensure_db()
                db.rollback()
                r = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                lease_recovery_in_progress = bool(
                    ctx is not None
                    and ctx.termination_reason == AUTO_REQUEUE_REASON_LEASE_LOST
                )
                if r and str(r.lease_recovery_state or "") in {
                    LEASE_RECOVERY_STATE_REQUESTED,
                    LEASE_RECOVERY_STATE_REQUEUE_COMMITTED,
                    LEASE_RECOVERY_STATE_FAILED,
                }:
                    lease_recovery_in_progress = True
                if lease_recovery_in_progress and _is_db_connection_error(exc):
                    log_event(
                        logger,
                        logging.WARNING,
                        "task exception suppressed because lease recovery has priority",
                        event="task_lease_recovery_exception_suppressed",
                        task_id=task_id,
                        owner_id=WORKER_ID,
                        epoch=epoch,
                        control_version=control_version,
                        error=str(exc),
                    )
                    return
                if r and r.status == "running" and still_owner(db, task_id, WORKER_ID, epoch, control_version):
                    if _is_db_connection_error(exc):
                        _mark_database_failure(r, exc, status="error")
                    else:
                        _persist_terminal_failure(r, str(exc), status="error")

                    def _commit_terminal(attempt: int) -> bool:
                        return bool(
                            commit_terminal_state_if_owner(
                                db,
                                task_id,
                                WORKER_ID,
                                epoch,
                                control_version,
                                status="error",
                                finished_at=now_local(),
                                stages_json={"events": _baseline_events + event_buffer, "final": True},
                                result_json=r.result_json,
                                error=(
                                    _database_failure_message(exc, attempts=DB_RETRY_MAX_ATTEMPTS)
                                    if _is_db_connection_error(exc)
                                    else str(exc)
                                ),
                            )
                        )

                    _run_db_retry("commit_terminal_state_if_owner", _commit_terminal, max_attempts=DB_RETRY_MAX_ATTEMPTS)
                    refreshed = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                    if refreshed is not None:
                        reason, changed = _sync_task_abnormal_reason(refreshed)
                        _record_abnormal_reason(refreshed, reason, changed=changed)
                        _record_abnormal_reason_timeline(db, refreshed, reason, changed=changed)
                        _record_task_event(
                            db,
                            row=refreshed,
                            event_type="task_error",
                            message="任务因异常退出并已写入错误终态",
                            level="error",
                            status="error",
                            worker_id=WORKER_ID,
                            execution_owner_id=WORKER_ID,
                            execution_epoch=epoch,
                            control_version=control_version,
                            payload={"error": str(exc)},
                        )
                        db.commit()
                    log_event(logger, logging.ERROR, "error terminal state committed", event="task_error_committed",
                              task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version, status="error")
            except Exception:
                pass
        finally:
            ctx = _get_running_task_context(task_id)
            if ctx is not None:
                ctx.lease_stop_requested.set()
                ctx.cancel_requested.set()
            orch = orch_holder.get("orch")
            if orch is not None:
                orch.abort()
            targeted_cleaned = 0
            if task_root_path or epoch_run_root_path:
                log_event(
                    logger,
                    logging.INFO,
                    "task agent cleanup started",
                    event="task_agent_cleanup_started",
                    task_id=task_id,
                    owner_id=WORKER_ID,
                    epoch=epoch,
                    control_version=control_version,
                )
                targeted_cleaned = cleanup_task_agent_processes(
                    logger.warning,
                    label=f"task_finally:{task_id}",
                    task_id=task_id,
                    task_root=task_root_path,
                    run_root=epoch_run_root_path,
                    worker_id=WORKER_ID,
                )
            try:
                log_event(
                    logger,
                    logging.INFO,
                    "task agent cleanup finished",
                    event="task_agent_cleanup_finished",
                    task_id=task_id,
                    owner_id=WORKER_ID,
                    epoch=epoch,
                    control_version=control_version,
                    cleaned_groups=targeted_cleaned,
                    orphan_cleaned_groups=0,
                )
            except Exception:
                logger.warning("failed to cleanup orphan pi processes for %s", task_id, exc_info=True)
            try:
                snapshot = load_execution_snapshot(db, task_id)
                if (
                    snapshot is not None
                    and snapshot.status == "running"
                    and snapshot.execution_owner_id == WORKER_ID
                    and int(snapshot.execution_epoch or 0) == int(epoch)
                    and int(snapshot.control_version or 0) == int(control_version)
                ):
                    recovered = _recover_running_task_for_cleanup(
                        db,
                        task_id=task_id,
                        owner_id=WORKER_ID,
                        epoch=epoch,
                        control_version=control_version,
                        reason="worker_finally_without_terminal_state",
                    )
                    if recovered:
                        from app.metrics import observe_local_event

                        observe_local_event("task_running_recovered", "cleanup")
                        log_event(
                            logger,
                            logging.WARNING,
                            "running task recovered to pending during worker cleanup",
                            event="task_running_recovered",
                            task_id=task_id,
                            owner_id=WORKER_ID,
                            epoch=epoch,
                            control_version=control_version,
                        )
                released = release_lease(db, task_id, WORKER_ID, epoch)
                if released:
                    from app.metrics import observe_local_event

                    observe_local_event("lease_release", "success")
                    log_event(logger, logging.INFO, "lease released", event="task_lease_released",
                              task_id=task_id, owner_id=WORKER_ID, epoch=epoch, control_version=control_version)
                    released_row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                    if released_row is not None:
                        _record_task_event(
                            db,
                            row=released_row,
                            event_type="task_lease_released",
                            message="任务租约已释放",
                            status=released_row.status,
                            worker_id=WORKER_ID,
                            execution_owner_id=WORKER_ID,
                            execution_epoch=epoch,
                            control_version=control_version,
                            payload={"owner_id": WORKER_ID, "epoch": epoch, "control_version": control_version},
                        )
                        db.commit()
                else:
                    from app.metrics import observe_local_event

                    observe_local_event("lease_release", "noop")
            except Exception:
                from app.metrics import observe_local_event

                observe_local_event("lease_release", "failed")
                if db is not None:
                    db.rollback()
            _unregister_running_task_context(task_id)
            _close_current_db()

    def _get_or_404(self, db: Session, task_id: str, *, include_deleted: bool = False) -> AppDfaTask:
        query = db.query(AppDfaTask).filter(AppDfaTask.task_id == task_id)
        if not include_deleted:
            query = query.filter(AppDfaTask.is_deleted.is_(False))
        row = query.first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return row

    @staticmethod
    def _list_load_options():
        return (
            load_only(
                AppDfaTask.id,
                AppDfaTask.task_id,
                AppDfaTask.project_id,
                AppDfaTask.task_origin_type,
                AppDfaTask.parent_project_id,
                AppDfaTask.parent_task_id,
                AppDfaTask.parent_task_type,
                AppDfaTask.parent_stage_name,
                AppDfaTask.parent_stage_item_id,
                AppDfaTask.parent_stage_item_key,
                AppDfaTask.task_name,
                AppDfaTask.task_description,
                AppDfaTask.input_path,
                AppDfaTask.module_input_path,
                AppDfaTask.source_root_path,
                AppDfaTask.output_path,
                AppDfaTask.prompt_template_id,
                AppDfaTask.status,
                AppDfaTask.error,
                AppDfaTask.created_by,
                AppDfaTask.created_at,
                AppDfaTask.updated_at,
                AppDfaTask.started_at,
                AppDfaTask.finished_at,
                AppDfaTask.execution_owner_id,
                AppDfaTask.execution_lease_until,
                AppDfaTask.execution_heartbeat_at,
                AppDfaTask.execution_epoch,
                AppDfaTask.control_version,
                AppDfaTask.dispatch_status,
                AppDfaTask.lease_lost_count,
                AppDfaTask.last_lease_lost_at,
                AppDfaTask.lease_requeue_not_before,
                AppDfaTask.last_lease_lost_epoch,
                AppDfaTask.last_lease_lost_control_version,
                AppDfaTask.lease_recovery_state,
                AppDfaTask.lease_recovery_error,
                AppDfaTask.lease_recovery_updated_at,
            ),
        )

    @staticmethod
    def _row_to_dict(row: AppDfaTask, *, include_heavy: bool = True, db: Session | None = None) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        def latest_started_at_value() -> str | None:
            latest = _latest_execution_started_at(db, row.task_id) if db is not None else None
            if latest:
                return latest
            if str(row.status or "").strip().lower() in {"running", "passed", "failed", "error", "cancelled"}:
                return fmt(row.started_at)
            return None
        def duration_ms() -> float | None:
            if isinstance(result_json, dict) and result_json.get("total_duration_ms") is not None:
                try:
                    return float(result_json.get("total_duration_ms"))
                except (TypeError, ValueError):
                    return None
            normalized_status = str(row.status or "").strip().lower()
            if normalized_status not in {"running", "passed", "failed", "error", "cancelled"}:
                return None
            if row.started_at is None:
                return None
            finished_at = row.finished_at or datetime.now(timezone.utc)
            started_at = row.started_at
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=timezone.utc)
            duration = (finished_at - started_at).total_seconds() * 1000.0
            return max(duration, 0.0)
        abnormal_reason = _task_abnormal_reason(row)
        auto_requeue_pending = _auto_requeue_pending(row)
        task_root = str(Path(row.output_path) / row.task_id) if row.output_path else None
        run_root = str(Path(task_root) / "run") if task_root else None
        workspace_root = str(Path(run_root) / "epochs") if run_root else None
        ctx = _get_running_task_context(row.task_id)
        result_json = _lightweight_result_json(row, row.result_json) if include_heavy else None
        latest_started_at = latest_started_at_value()
        return {
            **_origin_payload(row),
            "task_id": row.task_id, "project_id": row.project_id,
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path,
            "module_input_path": row.module_input_path or row.input_path,
            "source_root_path": row.source_root_path or row.input_path,
            "output_path": row.output_path,
            "task_root": task_root,
            "run_root": run_root,
            "workspace_root": workspace_root,
            "input_summary": {
                "module_input_path": row.module_input_path or row.input_path,
                "source_root_path": row.source_root_path or row.input_path,
            } if include_heavy else None,
            "output_summary": {
                "latest_workspace_root": workspace_root,
                "result_path": str(Path(run_root) / "result.json") if run_root else None,
                "dataflow_output_path": str(Path(task_root) / "output" / "dataflow") if task_root else None,
            } if include_heavy else None,
            "definition_kind": str((row.task_config_json or {}).get("definition_kind") or ""),
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "error": row.error,
            "result_json": result_json,
            "stages_json": row.stages_json if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
            "latest_started_at": latest_started_at,
            "execution_duration_ms": duration_ms(),
            "execution_owner_id": row.execution_owner_id,
            "execution_lease_until": fmt(row.execution_lease_until),
            "execution_heartbeat_at": fmt(row.execution_heartbeat_at),
            "execution_epoch": int(row.execution_epoch or 0),
            "control_version": int(row.control_version or 0),
            "rerun_count": max(0, int(row.control_version or 0)),
            "dispatch_status": row.dispatch_status,
            "lease_lost_count": int(row.lease_lost_count or 0),
            "last_lease_lost_at": fmt(row.last_lease_lost_at),
            "lease_requeue_not_before": fmt(row.lease_requeue_not_before),
            "last_lease_lost_epoch": row.last_lease_lost_epoch,
            "last_lease_lost_control_version": row.last_lease_lost_control_version,
            "lease_recovery_state": row.lease_recovery_state,
            "lease_recovery_error": row.lease_recovery_error,
            "lease_recovery_failed": str(row.lease_recovery_state or "") == LEASE_RECOVERY_STATE_FAILED,
            "lease_recovery_expected_requeue": bool(
                str(row.status or "") in {"error", "cancelled"}
                and _is_db_connection_error_text(row.error)
            ),
            "lease_renew_failure_count": int(ctx.lease_renew_failure_count if ctx is not None else 0),
            "last_lease_renew_success_at": (
                isoformat_local(datetime.fromtimestamp(ctx.last_lease_renew_success_at, tz=UTC_PLUS_8).replace(tzinfo=None))
                if ctx is not None and ctx.last_lease_renew_success_at > 0
                else None
            ),
            "last_lease_renew_error": ctx.last_lease_error if ctx is not None else None,
            "auto_requeue_pending": auto_requeue_pending,
            "auto_requeue_reason": AUTO_REQUEUE_REASON_LEASE_LOST if auto_requeue_pending else None,
            "abnormal_reason": None if (auto_requeue_pending or str(row.status or "") in {"running", "pending"}) else abnormal_reason,
            "abnormal_reason_history": _abnormal_reason_history(row) if include_heavy else [],
            "abnormal_reason_title": None if (auto_requeue_pending or str(row.status or "") in {"running", "pending"}) else (abnormal_reason or {}).get("title"),
            "abnormal_reason_code": None if (auto_requeue_pending or str(row.status or "") in {"running", "pending"}) else (abnormal_reason or {}).get("code"),
            "abnormal_reason_category": None if (auto_requeue_pending or str(row.status or "") in {"running", "pending"}) else (abnormal_reason or {}).get("category"),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
