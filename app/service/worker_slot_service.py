from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppDfaTask, AppDfaWorkerSlot
from app.runtime_context import (
    MAX_LOCAL_RUNNING_TASKS,
    WORKER_SLOT_HEARTBEAT_SECONDS,
    WORKER_SLOT_RETENTION_SECONDS,
    WORKER_SLOT_STALE_AFTER_SECONDS,
)
from app.time_utils import add_seconds_local, now_local

_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled", "invalid_input", "completed_limited"}


@dataclass(frozen=True)
class DfaWorkerActiveJobSnapshot:
    task_id: str
    task_name: str
    status: str
    parent_task_id: str | None
    parent_task_type: str | None
    task_origin_type: str | None
    input_path: str
    started_at: Any
    updated_at: Any
    dispatch_status: str | None
    execution_owner_id: str | None
    execution_lease_until: Any
    execution_heartbeat_at: Any
    mapped: bool = True
    mapping_reason: str = "matched_execution_owner"


@dataclass(frozen=True)
class DfaWorkerSnapshot:
    worker_id: str
    host_name: str
    pod_name: str
    pod_ip: str | None
    http_port: int | None
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int
    available_slots: int
    source: str
    last_heartbeat_at: Any
    active_jobs: list[DfaWorkerActiveJobSnapshot] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class DfaClusterCapacitySnapshot:
    worker_count: int
    healthy_workers: int
    stale_workers: int
    total_capacity: int
    running_jobs: int
    queued_jobs: int
    available_slots: int
    updated_at: Any
    workers: list[DfaWorkerSnapshot] = field(default_factory=list)


def _normalize_owner(owner_id: str | None) -> str:
    return str(owner_id or "").strip()


def _parse_host_name(owner_id: str) -> str:
    separator = owner_id.find(":")
    return owner_id[:separator] if separator >= 0 else owner_id


def _active_job_sort_key(job: DfaWorkerActiveJobSnapshot) -> tuple[int, float, str]:
    updated_ts = job.updated_at.timestamp() if getattr(job.updated_at, "timestamp", None) and job.updated_at else 0.0
    return (0 if job.status == "running" else 1, -updated_ts, job.task_id)


class WorkerSlotService:
    def upsert_heartbeat(
        self,
        db: Session,
        *,
        worker_id: str,
        instance_id: str | None,
        pod_name: str,
        pod_ip: str | None,
        http_port: int = 8080,
        max_concurrent_tasks: int,
        status: str = "running",
    ) -> None:
        row = db.query(AppDfaWorkerSlot).filter(AppDfaWorkerSlot.worker_id == worker_id).first()
        now = now_local()
        capacity = max(0, int(max_concurrent_tasks or MAX_LOCAL_RUNNING_TASKS))
        if row is None:
            row = AppDfaWorkerSlot(
                worker_id=worker_id,
                instance_id=instance_id,
                pod_name=pod_name,
                pod_ip=pod_ip,
                http_port=max(1, int(http_port or 8080)),
                max_concurrent_tasks=capacity,
                last_seen_status=status,
                last_heartbeat_at=now,
            )
            db.add(row)
        else:
            row.instance_id = instance_id
            row.pod_name = pod_name
            row.pod_ip = pod_ip
            row.http_port = max(1, int(http_port or 8080))
            row.max_concurrent_tasks = capacity
            row.last_seen_status = status
            row.last_heartbeat_at = now
        db.commit()

    def cleanup_retired_workers(self, db: Session) -> int:
        cutoff = add_seconds_local(now_local(), -max(WORKER_SLOT_RETENTION_SECONDS, WORKER_SLOT_HEARTBEAT_SECONDS))
        rows = db.query(AppDfaWorkerSlot).filter(AppDfaWorkerSlot.last_heartbeat_at < cutoff).all()
        for row in rows:
            db.delete(row)
        if rows:
            db.commit()
        return len(rows)

    def get_cluster_snapshot(self, db: Session, *, project_id: str | None = None) -> DfaClusterCapacitySnapshot:
        self.cleanup_retired_workers(db)
        now = now_local()
        stale_cutoff = add_seconds_local(now, -max(WORKER_SLOT_STALE_AFTER_SECONDS, WORKER_SLOT_HEARTBEAT_SECONDS))
        worker_rows = db.query(AppDfaWorkerSlot).order_by(AppDfaWorkerSlot.pod_name.asc(), AppDfaWorkerSlot.id.asc()).all()
        query = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppDfaTask.project_id == project_id)
        rows = query.all()

        queued_jobs = sum(
            1
            for row in rows
            if str(row.status or "").strip() == "pending" and not _normalize_owner(row.execution_owner_id)
        )

        active_by_owner: dict[str, list[DfaWorkerActiveJobSnapshot]] = {}
        for row in rows:
            owner_id = _normalize_owner(row.execution_owner_id)
            if not owner_id:
                continue
            status = str(row.status or "").strip()
            if status in _TERMINAL_STATUSES:
                continue
            active_by_owner.setdefault(owner_id, []).append(
                DfaWorkerActiveJobSnapshot(
                    task_id=row.task_id,
                    task_name=row.task_name,
                    status=status,
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
            )

        workers: list[DfaWorkerSnapshot] = []
        for row in worker_rows:
            active_jobs = sorted(active_by_owner.pop(row.worker_id, []), key=_active_job_sort_key)
            running_jobs = sum(1 for job in active_jobs if job.status == "running")
            healthy = row.last_heartbeat_at >= stale_cutoff
            workers.append(
                DfaWorkerSnapshot(
                    worker_id=row.worker_id,
                    host_name=row.pod_name or _parse_host_name(row.worker_id),
                    pod_name=row.pod_name,
                    pod_ip=row.pod_ip,
                    http_port=int(getattr(row, "http_port", 0) or 8080),
                    healthy=healthy,
                    max_concurrent_jobs=max(0, int(row.max_concurrent_tasks or 0)),
                    running_jobs=running_jobs,
                    available_slots=max(0, int(row.max_concurrent_tasks or 0) - running_jobs) if healthy else 0,
                    source="worker_registry" if healthy else "stale_worker_registry",
                    last_heartbeat_at=row.last_heartbeat_at,
                    active_jobs=active_jobs,
                    error=None if healthy else "worker heartbeat stale",
                )
            )

        for owner_id, active_jobs in sorted(active_by_owner.items()):
            running_jobs = sum(1 for job in active_jobs if job.status == "running")
            workers.append(
                DfaWorkerSnapshot(
                    worker_id=f"stale-owner::{owner_id}",
                    host_name=_parse_host_name(owner_id),
                    pod_name=owner_id,
                    pod_ip=None,
                    http_port=8080,
                    healthy=False,
                    max_concurrent_jobs=max(running_jobs, len(active_jobs)),
                    running_jobs=running_jobs,
                    available_slots=0,
                    source="stale_owner",
                    last_heartbeat_at=None,
                    active_jobs=sorted(active_jobs, key=_active_job_sort_key),
                    error="owner pod has running tasks but no live worker heartbeat",
                )
            )

        workers.sort(key=lambda item: (0 if item.healthy else 1, -item.running_jobs, item.worker_id))
        healthy_workers = sum(1 for worker in workers if worker.healthy)
        total_capacity = sum(worker.max_concurrent_jobs for worker in workers)
        running_jobs = sum(worker.running_jobs for worker in workers)
        available_slots = sum(worker.available_slots for worker in workers)
        return DfaClusterCapacitySnapshot(
            worker_count=len(workers),
            healthy_workers=healthy_workers,
            stale_workers=max(0, len(workers) - healthy_workers),
            total_capacity=total_capacity,
            running_jobs=running_jobs,
            queued_jobs=queued_jobs,
            available_slots=available_slots,
            updated_at=now,
            workers=workers,
        )


_worker_slot_service: WorkerSlotService | None = None


def get_worker_slot_service() -> WorkerSlotService:
    global _worker_slot_service
    if _worker_slot_service is None:
        _worker_slot_service = WorkerSlotService()
    return _worker_slot_service
