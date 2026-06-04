from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from app.db.models import AppDfaTask
from app.runtime_context import INSTANCE_ID
from app.runtime_context import LEASE_REQUEUE_DELAY_SECONDS, LEASE_TTL_SECONDS
from app.time_utils import now_local


@dataclass
class ClaimedTask:
    task_id: str
    epoch: int
    control_version: int
    dispatch_status: str | None = None


@dataclass
class ExecutionSnapshot:
    task_id: str
    status: str
    execution_owner_id: str | None
    execution_owner_instance_id: str | None
    execution_epoch: int
    control_version: int
    dispatch_status: str | None
    execution_lease_until: object | None
    execution_heartbeat_at: object | None


@dataclass(frozen=True)
class RecoveredRunningTask:
    task_id: str
    previous_owner_id: str | None
    previous_dispatch_status: str | None
    previous_lease_until: object | None
    reason: str


@dataclass(frozen=True)
class OrphanedRunningCandidate:
    task_id: str
    execution_owner_id: str | None
    execution_owner_instance_id: str | None
    execution_epoch: int
    control_version: int
    dispatch_status: str | None
    execution_lease_until: object | None
    execution_heartbeat_at: object | None
    reason: str


def _lease_requeue_deadline(base_time=None):
    return (base_time or now_local()) + timedelta(seconds=LEASE_REQUEUE_DELAY_SECONDS)


def _lease_deadline():
    return now_local() + timedelta(seconds=LEASE_TTL_SECONDS)


def _running_reclaim_cutoff(base_time=None):
    return (base_time or now_local()) - timedelta(seconds=max(LEASE_TTL_SECONDS, LEASE_REQUEUE_DELAY_SECONDS) * 2)


def claim_one_runnable_task(db: Session, owner_id: str) -> ClaimedTask | None:
    now = now_local()
    candidate = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status.in_(["pending", "running"]),
            (
                (AppDfaTask.lease_requeue_not_before.is_(None))
                | (AppDfaTask.lease_requeue_not_before <= now)
            ),
            ((AppDfaTask.execution_lease_until.is_(None)) | (AppDfaTask.execution_lease_until < now)),
        )
        .order_by(AppDfaTask.status.asc(), AppDfaTask.created_at.asc(), AppDfaTask.id.asc())
        .first()
    )
    if candidate is None:
        return None

    expected_status = str(candidate.status or "pending")
    update_fields = {
        AppDfaTask.execution_owner_id: owner_id,
        AppDfaTask.execution_owner_instance_id: INSTANCE_ID,
        AppDfaTask.execution_lease_until: _lease_deadline(),
        AppDfaTask.execution_heartbeat_at: now,
        AppDfaTask.execution_epoch: int(candidate.execution_epoch or 0) + 1,
        AppDfaTask.dispatch_status: "leased",
        AppDfaTask.lease_requeue_not_before: None,
    }
    if expected_status == "running":
        # Reclaimed tasks lost their worker lease during rollout/crash; make them
        # re-enter the normal dispatch path instead of staying in stale running.
        update_fields[AppDfaTask.status] = "pending"

    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.id == candidate.id,
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status == expected_status,
            (
                (AppDfaTask.lease_requeue_not_before.is_(None))
                | (AppDfaTask.lease_requeue_not_before <= now)
            ),
            ((AppDfaTask.execution_lease_until.is_(None)) | (AppDfaTask.execution_lease_until < now)),
        )
        .update(
            update_fields,
            synchronize_session=False,
        )
    )
    db.commit()
    if not updated:
        return None
    refreshed = db.query(AppDfaTask).filter(AppDfaTask.id == candidate.id).first()
    if refreshed is None:
        return None
    return ClaimedTask(
        task_id=refreshed.task_id,
        epoch=int(refreshed.execution_epoch or 0),
        control_version=int(refreshed.control_version or 0),
        dispatch_status=refreshed.dispatch_status,
    )


def renew_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
    now = now_local()
    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.execution_owner_id == owner_id,
            AppDfaTask.execution_owner_instance_id == INSTANCE_ID,
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status == "running",
        )
        .update(
            {
                AppDfaTask.execution_lease_until: _lease_deadline(),
                AppDfaTask.execution_heartbeat_at: now,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def release_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.execution_owner_id == owner_id,
            AppDfaTask.execution_epoch == epoch,
            AppDfaTask.status != "running",
        )
        .update(
            {
                AppDfaTask.execution_owner_id: None,
                AppDfaTask.execution_owner_instance_id: None,
                AppDfaTask.execution_lease_until: None,
                AppDfaTask.dispatch_status: None,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def recover_running_task_if_owner(
    db: Session,
    task_id: str,
    owner_id: str,
    epoch: int,
    control_version: int,
    *,
    reason: str = "owner_cleanup",
) -> bool:
    now = now_local()
    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.execution_owner_id == owner_id,
            AppDfaTask.execution_epoch == epoch,
            AppDfaTask.control_version == control_version,
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status == "running",
        )
        .update(
            {
                AppDfaTask.status: "pending",
                AppDfaTask.execution_owner_id: None,
                AppDfaTask.execution_owner_instance_id: None,
                AppDfaTask.execution_lease_until: None,
                AppDfaTask.execution_heartbeat_at: None,
                AppDfaTask.dispatch_status: "pending",
                AppDfaTask.finished_at: None,
                AppDfaTask.error: None,
                AppDfaTask.latest_abnormal_reason_json: None,
                AppDfaTask.lease_lost_count: AppDfaTask.lease_lost_count + 1,
                AppDfaTask.last_lease_lost_at: now,
                AppDfaTask.lease_requeue_not_before: _lease_requeue_deadline(now),
                AppDfaTask.last_lease_lost_epoch: epoch,
                AppDfaTask.last_lease_lost_control_version: control_version,
                AppDfaTask.execution_epoch: int(epoch) + 1,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def list_recoverable_orphaned_running_tasks(db: Session, *, limit: int = 100) -> list[OrphanedRunningCandidate]:
    now = now_local()
    reclaim_cutoff = _running_reclaim_cutoff(now)
    rows = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status == "running",
            (
                (AppDfaTask.execution_owner_id.is_(None))
                | (
                    (AppDfaTask.execution_lease_until.is_(None) | (AppDfaTask.execution_lease_until < reclaim_cutoff))
                    & (
                        AppDfaTask.execution_heartbeat_at.is_(None)
                        | (AppDfaTask.execution_heartbeat_at < reclaim_cutoff)
                    )
                )
            ),
        )
        .order_by(AppDfaTask.updated_at.asc(), AppDfaTask.id.asc())
        .limit(max(1, int(limit or 100)))
        .all()
    )
    candidates: list[OrphanedRunningCandidate] = []
    for row in rows:
        if row.execution_owner_id is None:
            reason = "missing_owner"
        elif row.execution_lease_until is None and row.execution_heartbeat_at is None:
            reason = "missing_lease_and_heartbeat"
        elif row.execution_lease_until is None:
            reason = "missing_lease_stale_heartbeat"
        elif row.execution_heartbeat_at is None:
            reason = "expired_lease_missing_heartbeat"
        else:
            reason = "expired_lease_stale_heartbeat"
        candidates.append(
            OrphanedRunningCandidate(
                task_id=row.task_id,
                execution_owner_id=row.execution_owner_id,
                execution_owner_instance_id=row.execution_owner_instance_id,
                execution_epoch=int(row.execution_epoch or 0),
                control_version=int(row.control_version or 0),
                dispatch_status=row.dispatch_status,
                execution_lease_until=row.execution_lease_until,
                execution_heartbeat_at=row.execution_heartbeat_at,
                reason=reason,
            )
        )
    return candidates


def reclaim_orphaned_running_tasks(db: Session, *, limit: int = 100) -> list[RecoveredRunningTask]:
    now = now_local()
    recovered: list[RecoveredRunningTask] = []
    for candidate in list_recoverable_orphaned_running_tasks(db, limit=limit):
        row = db.query(AppDfaTask).filter(AppDfaTask.task_id == candidate.task_id).first()
        if row is None:
            continue
        updated = (
            db.query(AppDfaTask)
            .filter(
                AppDfaTask.id == row.id,
                AppDfaTask.is_deleted.is_(False),
                AppDfaTask.status == "running",
            )
            .update(
                {
                    AppDfaTask.status: "pending",
                    AppDfaTask.execution_owner_id: None,
                    AppDfaTask.execution_owner_instance_id: None,
                    AppDfaTask.execution_lease_until: None,
                    AppDfaTask.execution_heartbeat_at: None,
                    AppDfaTask.dispatch_status: "pending",
                    AppDfaTask.finished_at: None,
                    AppDfaTask.error: None,
                    AppDfaTask.latest_abnormal_reason_json: None,
                    AppDfaTask.lease_lost_count: AppDfaTask.lease_lost_count + 1,
                    AppDfaTask.last_lease_lost_at: now,
                    AppDfaTask.lease_requeue_not_before: _lease_requeue_deadline(now),
                    AppDfaTask.last_lease_lost_epoch: AppDfaTask.execution_epoch,
                    AppDfaTask.last_lease_lost_control_version: AppDfaTask.control_version,
                    AppDfaTask.execution_epoch: AppDfaTask.execution_epoch + 1,
                },
                synchronize_session=False,
            )
        )
        if not updated:
            db.rollback()
            continue
        recovered.append(
            RecoveredRunningTask(
                task_id=row.task_id,
                previous_owner_id=row.execution_owner_id,
                previous_dispatch_status=row.dispatch_status,
                previous_lease_until=row.execution_lease_until,
                reason=candidate.reason,
            )
        )
    db.commit()
    return recovered


def still_owner(db: Session, task_id: str, owner_id: str, epoch: int, control_version: int) -> bool:
    row = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.is_deleted.is_(False),
        )
        .first()
    )
    if row is None:
        return False
    return (
        row.execution_owner_id == owner_id
        and str(row.execution_owner_instance_id or "") == INSTANCE_ID
        and int(row.execution_epoch or 0) == int(epoch)
        and int(row.control_version or 0) == int(control_version)
        and row.status in {"pending", "running"}
    )


def load_execution_snapshot(db: Session, task_id: str) -> ExecutionSnapshot | None:
    row = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.is_deleted.is_(False),
        )
        .first()
    )
    if row is None:
        return None
    return ExecutionSnapshot(
        task_id=row.task_id,
        status=str(row.status or ""),
        execution_owner_id=row.execution_owner_id,
        execution_owner_instance_id=row.execution_owner_instance_id,
        execution_epoch=int(row.execution_epoch or 0),
        control_version=int(row.control_version or 0),
        dispatch_status=row.dispatch_status,
        execution_lease_until=row.execution_lease_until,
        execution_heartbeat_at=row.execution_heartbeat_at,
    )


def begin_execution_if_owner(db: Session, task_id: str, owner_id: str, epoch: int, control_version: int, *, started_at) -> bool:
    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.execution_owner_id == owner_id,
            AppDfaTask.execution_epoch == epoch,
            AppDfaTask.control_version == control_version,
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status.in_(["pending", "running"]),
        )
        .update(
            {
                AppDfaTask.status: "running",
                AppDfaTask.dispatch_status: "running",
                AppDfaTask.started_at: started_at,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def commit_terminal_state_if_owner(
    db: Session,
    task_id: str,
    owner_id: str,
    epoch: int,
    control_version: int,
    *,
    status: str,
    finished_at,
    stages_json: dict,
    result_json: dict | None,
    error: str | None,
) -> bool:
    updated = (
        db.query(AppDfaTask)
        .filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.execution_owner_id == owner_id,
            AppDfaTask.execution_epoch == epoch,
            AppDfaTask.control_version == control_version,
            AppDfaTask.is_deleted.is_(False),
            AppDfaTask.status == "running",
        )
        .update(
            {
                AppDfaTask.status: status,
                AppDfaTask.finished_at: finished_at,
                AppDfaTask.stages_json: stages_json,
                AppDfaTask.result_json: result_json,
                AppDfaTask.error: error,
                AppDfaTask.execution_owner_id: None,
                AppDfaTask.execution_lease_until: None,
                AppDfaTask.execution_heartbeat_at: None,
                AppDfaTask.dispatch_status: None,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)
