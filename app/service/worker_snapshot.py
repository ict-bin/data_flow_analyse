from __future__ import annotations

from app.service.worker_slot_service import (
    DfaClusterCapacitySnapshot,
    DfaWorkerActiveJobSnapshot,
    DfaWorkerSnapshot,
    get_worker_slot_service,
)


def build_worker_cluster_snapshot(db, *, project_id: str | None = None):
    return get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id)

__all__ = [
    "DfaClusterCapacitySnapshot",
    "DfaWorkerActiveJobSnapshot",
    "DfaWorkerSnapshot",
    "build_worker_cluster_snapshot",
]
