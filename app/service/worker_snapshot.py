from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.db.models import AppDfaTask
from app.runtime_context import HEARTBEAT_INTERVAL_SECONDS, MAX_LOCAL_RUNNING_TASKS
from app.time_utils import now_local

_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled", "invalid_input", "completed_limited"}
_ACTIVE_STATUSES = {"pending", "running"}


@dataclass(frozen=True)
class DfaWorkerActiveJobSnapshot:
    task_id: str
    task_name: str
    status: str
    parent_task_id: str | None
    parent_task_type: str | None
    task_origin_type: str | None
    input_path: str
    started_at: datetime | None
    updated_at: datetime | None
    dispatch_status: str | None
    execution_owner_id: str | None
    execution_lease_until: datetime | None
    execution_heartbeat_at: datetime | None
    mapped: bool = True
    mapping_reason: str = "matched_execution_owner"


@dataclass(frozen=True)
class DfaWorkerSnapshot:
    worker_id: str
    host_name: str
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int
    available_slots: int
    source: str
    last_heartbeat_at: datetime | None
    active_jobs: list[DfaWorkerActiveJobSnapshot] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class DfaClusterCapacitySnapshot:
    worker_count: int
    total_capacity: int
    running_jobs: int
    queued_jobs: int
    available_slots: int
    updated_at: datetime | None
    workers: list[DfaWorkerSnapshot] = field(default_factory=list)


def _normalize_owner(owner_id: str | None) -> str:
    return str(owner_id or "").strip()


def _parse_host_name(owner_id: str) -> str:
    separator = owner_id.find(":")
    return owner_id[:separator] if separator >= 0 else owner_id


def _heartbeat_is_live(heartbeat_at: datetime | None, now: datetime) -> bool:
    if heartbeat_at is None:
        return False
    return heartbeat_at >= now - timedelta(seconds=max(1, HEARTBEAT_INTERVAL_SECONDS * 2))


def _lease_is_live(lease_until: datetime | None, now: datetime) -> bool:
    return bool(lease_until and lease_until >= now)


def _active_job_sort_key(job: DfaWorkerActiveJobSnapshot) -> tuple[int, float, str]:
    updated_ts = job.updated_at.timestamp() if job.updated_at else 0.0
    return (0 if job.status == "running" else 1, -updated_ts, job.task_id)


def build_worker_cluster_snapshot(db: Session, *, project_id: str | None = None) -> DfaClusterCapacitySnapshot:
    query = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False))
    if project_id:
        query = query.filter(AppDfaTask.project_id == project_id)
    rows = query.all()
    now = now_local()
    queued_jobs = sum(
        1
        for row in rows
        if str(row.status or "").strip() == "pending" and not _normalize_owner(row.execution_owner_id)
    )
    grouped_rows: dict[str, list[AppDfaTask]] = defaultdict(list)
    for row in rows:
        owner_id = _normalize_owner(row.execution_owner_id)
        if owner_id:
            grouped_rows[owner_id].append(row)

    worker_snapshots: list[DfaWorkerSnapshot] = []
    for owner_id, owner_rows in grouped_rows.items():
        latest_heartbeat = max(
            (row.execution_heartbeat_at for row in owner_rows if row.execution_heartbeat_at is not None),
            default=None,
        )
        heartbeat_live = _heartbeat_is_live(latest_heartbeat, now)
        active_rows = [row for row in owner_rows if str(row.status or "").strip() not in _TERMINAL_STATUSES]
        if not active_rows and not heartbeat_live:
            continue

        running_rows = [row for row in active_rows if str(row.status or "").strip() == "running"]
        lease_live = any(_lease_is_live(row.execution_lease_until, now) for row in active_rows)
        healthy = lease_live or heartbeat_live
        active_jobs = [
            DfaWorkerActiveJobSnapshot(
                task_id=row.task_id,
                task_name=row.task_name,
                status=str(row.status or ""),
                parent_task_id=row.parent_task_id,
                parent_task_type=row.parent_task_type,
                task_origin_type=row.task_origin_type,
                input_path=row.input_path,
                started_at=row.started_at,
                updated_at=row.updated_at,
                dispatch_status=row.dispatch_status,
                execution_owner_id=row.execution_owner_id,
                execution_lease_until=row.execution_lease_until,
                execution_heartbeat_at=row.execution_heartbeat_at,
            )
            for row in active_rows
        ]
        active_jobs.sort(key=_active_job_sort_key)

        error: str | None = None
        if not healthy:
            if latest_heartbeat is None:
                error = "stale lease and no fresh heartbeat"
            else:
                error = "stale lease and stale heartbeat"

        running_jobs = len(running_rows)
        max_concurrent_jobs = max(0, int(MAX_LOCAL_RUNNING_TASKS))
        worker_snapshots.append(
            DfaWorkerSnapshot(
                worker_id=owner_id,
                host_name=_parse_host_name(owner_id),
                healthy=healthy,
                max_concurrent_jobs=max_concurrent_jobs,
                running_jobs=running_jobs,
                available_slots=max(0, max_concurrent_jobs - running_jobs) if healthy else 0,
                source="lease_registry",
                last_heartbeat_at=latest_heartbeat,
                active_jobs=active_jobs,
                error=error,
            )
        )

    worker_snapshots.sort(key=lambda item: (0 if item.healthy else 1, -item.running_jobs, item.worker_id))
    return DfaClusterCapacitySnapshot(
        worker_count=len(worker_snapshots),
        total_capacity=sum(worker.max_concurrent_jobs for worker in worker_snapshots),
        running_jobs=sum(worker.running_jobs for worker in worker_snapshots),
        queued_jobs=queued_jobs,
        available_slots=sum(worker.available_slots for worker in worker_snapshots),
        updated_at=now,
        workers=worker_snapshots,
    )
