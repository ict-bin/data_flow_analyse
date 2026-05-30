"""Task management API routes for dataflow-analyse."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import httpx
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppDfaTask
from app.time_utils import isoformat_local
from app.service.worker_snapshot import build_worker_cluster_snapshot
from app.service.session_index import build_session_catalog
from app.service.task_service import generate_prompt_from_path, get_task_service
from .deps import ensure_admin_user, ensure_project_access, get_current_user

from . import router

logger = logging.getLogger(__name__)
internal_observability_router = APIRouter(prefix="/api/app/dataflow-analyse")
AGGREGATE_HTTP_TIMEOUT_SECONDS = float(os.environ.get("DFA_AGENT_AGGREGATE_TIMEOUT_SECONDS", "3"))
AGGREGATE_HTTP_PORT = int(os.environ.get("DFA_AGENT_AGGREGATE_PORT", os.environ.get("PORT", "3000")))
AGGREGATE_CACHE_TTL_SECONDS = max(0.0, float(os.environ.get("DFA_AGENT_AGGREGATE_CACHE_TTL_SECONDS", "2.5")))
_LAST_AGENT_AGGREGATE_META: dict[str, Any] = {
    "partial": False,
    "sources": 0,
    "fanout_errors": 0,
    "duration_seconds": 0.0,
    "cache_hit": False,
    "cache_age_seconds": 0.0,
    "failed_targets": [],
    "cache_hits": 0,
    "cache_misses": 0,
}
_AGENT_AGGREGATE_CACHE: dict[str, dict[str, Any]] = {}


TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled", "invalid_input", "completed_limited"}


class TaskCreateRequest(BaseModel):
    project_id: str
    task_name: str
    input_path: str
    module_input_path: Optional[str] = None
    source_root_path: Optional[str] = None
    output_path: Optional[str] = None
    task_description: Optional[str] = None
    prompt_template_id: Optional[str] = None
    prompt_content: Optional[str] = None  # If omitted, auto-generated from input_path
    source_file: Optional[str] = None
    function_name: Optional[str] = None
    line_hint: Optional[str] = None
    definition_kind: Optional[str] = None
    taint_params: list[str] = []
    function_description: Optional[str] = None
    function_description_source: Optional[str] = None
    entry_reason: Optional[str] = None
    entry_reason_source: Optional[str] = None
    taint_details: list[Dict[str, Any]] = []
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


class TaskSessionIndexNodeResponse(BaseModel):
    node_id: str
    relative_path: str
    session_name: str
    display_name: str
    role: str
    role_label: str
    status: str
    is_active: bool = False
    stage_key: str
    stage_label: str
    stage_order: int = 0
    stage_group: str
    module_name: Optional[str] = None
    attempt: Optional[int] = None
    judge_index: Optional[int] = None
    batch_index: Optional[int] = None
    parent_relative_path: Optional[str] = None
    parallel_group: Optional[str] = None
    family_key: Optional[str] = None
    flow_kind: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    started_ts: Optional[float] = None
    last_event_at: Optional[str] = None
    last_event_ts: Optional[float] = None
    mtime: float = 0
    size: int = 0
    event_count: int = 0
    line_count: int = 0
    warnings: List[str] = []
    session_header: Dict[str, Any] = {}
    cwd: Optional[str] = None
    model: Optional[str] = None
    latest_round_ref: Optional[Dict[str, Any]] = None
    round_refs: List[Dict[str, Any]] = []
    attempts_seen: List[int] = []


class TaskSessionIndexEdgeResponse(BaseModel):
    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str
    label: str


class TaskSessionIndexGroupResponse(BaseModel):
    group_id: str
    kind: str
    label: str
    stage_key: Optional[str] = None
    module_name: Optional[str] = None
    node_ids: List[str] = []


class TaskSessionIndexResponse(BaseModel):
    version: int = 1
    generated_at: Optional[str] = None
    task_id: str
    task_status: str
    status: Optional[str] = None
    sessions_root: Optional[str] = None
    index_path: Optional[str] = None
    summary: Dict[str, Any] = {}
    nodes: List[TaskSessionIndexNodeResponse] = []
    edges: List[TaskSessionIndexEdgeResponse] = []
    groups: List[TaskSessionIndexGroupResponse] = []
    warnings: List[str] = []


class WorkerActiveJobResponse(BaseModel):
    task_id: str
    task_name: str
    status: str
    parent_task_id: str | None = None
    parent_task_type: str | None = None
    task_origin_type: str | None = None
    input_path: str
    started_at: str | None = None
    updated_at: str | None = None
    dispatch_status: str | None = None
    execution_owner_id: str | None = None
    execution_lease_until: str | None = None
    execution_heartbeat_at: str | None = None
    mapped: bool = True
    mapping_reason: str = "matched_execution_owner"


class WorkerCapacityResponse(BaseModel):
    worker_id: str
    host_name: str
    pod_name: str | None = None
    pod_ip: str | None = None
    healthy: bool
    max_concurrent_jobs: int
    running_jobs: int = 0
    available_slots: int = 0
    source: str = "lease_registry"
    last_heartbeat_at: str | None = None
    active_jobs: list[WorkerActiveJobResponse] = Field(default_factory=list)
    error: str | None = None


class WorkerClusterCapacityResponse(BaseModel):
    worker_count: int = 0
    healthy_workers: int = 0
    stale_workers: int = 0
    total_capacity: int = 0
    running_jobs: int = 0
    queued_jobs: int = 0
    available_slots: int = 0
    updated_at: str | None = None
    workers: list[WorkerCapacityResponse] = Field(default_factory=list)


class AgentProcessSnapshotResponse(BaseModel):
    pod_name: str
    pid: int
    pgid: Optional[int] = None
    ppid: Optional[int] = None
    command: str
    cwd: Optional[str] = None
    exe: Optional[str] = None
    rss_bytes: Optional[int] = None
    runtime_kind: Optional[str] = None
    match_source: Optional[str] = None
    match_confidence: Optional[str] = None
    workspace_root: Optional[str] = None
    session_arg_path: Optional[str] = None
    open_session_paths: list[str] = Field(default_factory=list)
    session_file: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    task_status: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    owner_kind: str
    owner_reason: str
    kill_allowed: bool = False
    kill_block_reason: Optional[str] = None
    termination_state: str


class AgentSessionSnapshotResponse(BaseModel):
    pod_name: str
    session_file: str
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    display_name: str
    line_count: int = 0
    last_event_at: Optional[str] = None
    live: bool = False
    has_process: bool = False
    process_pid: Optional[int] = None
    orphan_session: bool = False
    parse_warnings: list[str] = Field(default_factory=list)


class AgentTaskOwnershipSnapshotResponse(BaseModel):
    task_id: str
    task_name: str
    task_status: str
    stage_key: Optional[str] = None
    pod_name: str
    process_count: int = 0
    session_count: int = 0
    agent_roles: list[str] = Field(default_factory=list)
    process_pids: list[int] = Field(default_factory=list)
    session_ids: list[str] = Field(default_factory=list)
    ownership_status: str


class AgentPodSnapshotResponse(BaseModel):
    pod_name: str
    worker_id: Optional[str] = None
    healthy: bool = True
    process_count: int = 0
    tracked_process_count: int = 0
    orphan_process_count: int = 0
    suspected_orphan_process_count: int = 0
    session_count: int = 0
    orphan_session_count: int = 0
    task_count: int = 0
    active_task_count: int = 0
    last_scanned_at: Optional[float] = None
    scan_errors: int = 0
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)
    sessions: list[AgentSessionSnapshotResponse] = Field(default_factory=list)


class AgentObservabilitySummaryResponse(BaseModel):
    pod_name: str
    active_processes: int = 0
    orphan_processes: int = 0
    unknown_processes: int = 0
    killable_orphan_processes: int = 0
    killable_suspected_orphan_processes: int = 0
    orphan_sessions: int = 0
    scanned_at: Optional[float] = None
    scan_errors: int = 0
    aggregate_mode: Optional[str] = None
    aggregate_partial: Optional[bool] = None
    aggregate_sources: Optional[int] = None
    aggregate_fanout_errors: Optional[int] = None
    aggregate_duration_seconds: Optional[float] = None
    aggregate_cache_hit: Optional[bool] = None
    aggregate_cache_age_seconds: Optional[float] = None
    aggregate_failed_targets: list[str] = Field(default_factory=list)


class AgentProcessKillItemResponse(BaseModel):
    pid: int
    pgid: Optional[int] = None
    status: str
    reason: Optional[str] = None


class AgentProcessKillResponse(BaseModel):
    requested: int
    matched: int
    succeeded: int
    failed: int
    skipped: int
    items: list[AgentProcessKillItemResponse] = Field(default_factory=list)


class AgentRuntimeAggregateSummaryResponse(BaseModel):
    total_pods: int = 0
    healthy_pods: int = 0
    total_processes: int = 0
    tracked_processes: int = 0
    orphan_processes: int = 0
    suspected_orphan_processes: int = 0
    killable_orphan_processes: int = 0
    killable_suspected_orphan_processes: int = 0
    orphan_sessions: int = 0
    aggregate_partial: bool = False
    aggregate_sources: int = 0
    aggregate_fanout_errors: int = 0
    aggregate_failed_targets: list[str] = Field(default_factory=list)
    aggregate_all_sources_failed: bool = False
    scanned_at: Optional[float] = None


class AgentRuntimeAggregateResponse(BaseModel):
    summary: AgentRuntimeAggregateSummaryResponse
    pods: list[AgentPodSnapshotResponse] = Field(default_factory=list)
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)


def _auth_headers_from_token(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _agent_cache_key() -> str:
    return "__global__"


def _snapshot_query_params() -> dict[str, Any]:
    return {}


def _resolve_worker_targets(*, pod_ip: str | None, pod_name: str | None) -> list[str]:
    targets: list[str] = []
    normalized_ip = str(pod_ip or "").strip()
    if normalized_ip:
        targets.append(normalized_ip)
    return targets


def _aggregate_base_urls(worker) -> list[str]:
    targets: list[str] = []
    pod_ip = str(getattr(worker, "pod_ip", "") or "").strip()
    pod_name = str(getattr(worker, "pod_name", "") or "").strip()
    for host in _resolve_worker_targets(pod_ip=pod_ip, pod_name=pod_name):
        if not host:
            continue
        targets.append(f"http://{host}:{AGGREGATE_HTTP_PORT}/api/app/dataflow-analyse")
    return targets


async def _fanout_get_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url
                logger.warning("dfa agent fanout non-200 status=%s url=%s", response.status_code, url)
            except Exception:
                logger.warning("dfa agent fanout request failed url=%s", url, exc_info=True)
                continue
    return None, None


async def _get_agent_observability_snapshot_impl(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, _token = user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)


@router.get("/agent-observability/snapshot")
async def get_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


@internal_observability_router.get("/agent-observability/snapshot", response_model=dict[str, Any], include_in_schema=False)
async def get_internal_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


async def _build_agent_aggregate_snapshot(token: str, db: Session) -> dict[str, Any]:
    now_ts = __import__("time").time()
    cache_key = _agent_cache_key()
    cached = _AGENT_AGGREGATE_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("created_at") or 0.0)) <= AGGREGATE_CACHE_TTL_SECONDS:
        cache_age = now_ts - float(cached.get("created_at") or 0.0)
        meta = cached.get("meta") or {}
        _LAST_AGENT_AGGREGATE_META.update({
            "partial": bool(meta.get("partial")),
            "sources": int(meta.get("sources") or 0),
            "fanout_errors": int(meta.get("fanout_errors") or 0),
            "duration_seconds": float(meta.get("duration_seconds") or 0.0),
            "cache_hit": True,
            "cache_age_seconds": cache_age,
            "failed_targets": list(meta.get("failed_targets") or []),
            "cache_hits": int(_LAST_AGENT_AGGREGATE_META.get("cache_hits") or 0) + 1,
        })
        return cached["snapshot"]

    started = __import__("time").perf_counter()
    local_snapshot = get_task_service()  # ensure task service initialized
    del local_snapshot
    from app.service.agent_observability import get_agent_observability_service

    local = get_agent_observability_service().build_snapshot(db, project_id=None)
    cluster_snapshot = build_worker_cluster_snapshot(db, project_id=None)
    workers = [worker for worker in cluster_snapshot.workers if worker.healthy and _resolve_worker_targets(pod_ip=worker.pod_ip, pod_name=worker.pod_name)]

    merged_processes: list[dict[str, Any]] = []
    merged_sessions: list[dict[str, Any]] = []
    merged_tasks: list[dict[str, Any]] = []
    pod_rows: list[dict[str, Any]] = []
    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    seen_pods: set[str] = set()
    seen_process_keys: set[tuple[str, int]] = set()
    seen_session_keys: set[tuple[str, str]] = set()
    seen_task_keys: set[tuple[str, str]] = set()

    for worker in workers:
        urls = _aggregate_base_urls(worker)
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(str(worker.pod_name or worker.worker_id or "unknown"))
            continue
        worker_snapshot, process_source = await _fanout_get_json(urls, path="/agent-observability/snapshot", token=token, params=_snapshot_query_params())
        if worker_snapshot is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(str(worker.pod_name or worker.worker_id or "unknown"))
            continue
        sources += 1
        if process_source:
            logger.info("dfa agent aggregate source=%s", process_source)
        for item in worker_snapshot.get("processes") or []:
            key = (str(item.get("pod_name") or ""), int(item.get("pid") or 0))
            if key in seen_process_keys:
                continue
            seen_process_keys.add(key)
            merged_processes.append(item)
            seen_pods.add(str(item.get("pod_name") or ""))
        for item in worker_snapshot.get("sessions") or []:
            key = (str(item.get("pod_name") or ""), str(item.get("session_file") or ""))
            if key in seen_session_keys:
                continue
            seen_session_keys.add(key)
            merged_sessions.append(item)
        for item in worker_snapshot.get("tasks") or []:
            key = (str(item.get("pod_name") or ""), str(item.get("task_id") or ""))
            if key in seen_task_keys:
                continue
            seen_task_keys.add(key)
            merged_tasks.append(item)
        for item in worker_snapshot.get("pods") or []:
            pod_name = str(item.get("pod_name") or "")
            if pod_name in seen_pods:
                pod_rows = [row for row in pod_rows if str(row.get("pod_name") or "") != pod_name]
            pod_rows.append(item)
            seen_pods.add(pod_name)

    all_sources_failed = bool(workers) and sources == 0 and fanout_errors > 0
    if not workers:
        merged_processes = list(local.get("processes") or [])
        merged_sessions = list(local.get("sessions") or [])
        merged_tasks = list(local.get("tasks") or [])
        pod_rows = list(local.get("pods") or [])
        sources = 1
        partial = False
        all_sources_failed = False

    summary = {
        "pod_name": "dfa-aggregate",
        "active_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "tracked"]),
        "orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "orphan"]),
        "unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown"]),
        "killable_orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "orphan" and bool(item.get("kill_allowed"))]),
        "killable_suspected_orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
        "orphan_sessions": len([item for item in merged_sessions if bool(item.get("orphan_session"))]),
        "scanned_at": __import__("time").time(),
        "scan_errors": 0,
        "aggregate_mode": "fanout",
        "aggregate_partial": partial,
        "aggregate_sources": sources,
        "aggregate_fanout_errors": fanout_errors,
        "aggregate_duration_seconds": __import__("time").perf_counter() - started,
        "aggregate_cache_hit": False,
        "aggregate_cache_age_seconds": 0.0,
        "aggregate_failed_targets": failed_targets,
        "aggregate_all_sources_failed": all_sources_failed,
    }
    _LAST_AGENT_AGGREGATE_META.update({
        "partial": partial,
        "sources": sources,
        "fanout_errors": fanout_errors,
        "duration_seconds": summary["aggregate_duration_seconds"],
        "cache_hit": False,
        "cache_age_seconds": 0.0,
        "failed_targets": failed_targets,
        "cache_misses": int(_LAST_AGENT_AGGREGATE_META.get("cache_misses") or 0) + 1,
    })
    snapshot = {
        "summary": summary,
        "processes": merged_processes,
        "sessions": merged_sessions,
        "tasks": merged_tasks,
        "pods": pod_rows,
    }
    _AGENT_AGGREGATE_CACHE[cache_key] = {
        "created_at": now_ts,
        "snapshot": snapshot,
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return snapshot


def _build_agent_runtime_aggregate(snapshot: dict[str, Any]) -> dict[str, Any]:
    pods = list(snapshot.get("pods") or [])
    processes = list(snapshot.get("processes") or [])
    tasks = list(snapshot.get("tasks") or [])
    summary = dict(snapshot.get("summary") or {})
    return {
        "summary": {
            "total_pods": len(pods),
            "healthy_pods": len([item for item in pods if bool(item.get("healthy", True))]),
            "total_processes": len(processes),
            "tracked_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "tracked"]),
            "orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "orphan"]),
            "suspected_orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown"]),
            "killable_orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "orphan" and bool(item.get("kill_allowed"))]),
            "killable_suspected_orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
            "orphan_sessions": len([item for item in snapshot.get("sessions") or [] if bool(item.get("orphan_session"))]),
            "aggregate_partial": bool(summary.get("aggregate_partial")),
            "aggregate_sources": int(summary.get("aggregate_sources") or 0),
            "aggregate_fanout_errors": int(summary.get("aggregate_fanout_errors") or 0),
            "aggregate_failed_targets": list(summary.get("aggregate_failed_targets") or []),
            "aggregate_all_sources_failed": bool(summary.get("aggregate_all_sources_failed")),
            "scanned_at": summary.get("scanned_at"),
        },
        "pods": pods,
        "processes": processes,
        "tasks": tasks,
    }


def _invalidate_agent_aggregate_cache() -> None:
    _AGENT_AGGREGATE_CACHE.clear()


def _audit_agent_kill_event(
    db: Session,
    *,
    project_id: str,
    operator: str,
    event_type: str,
    message: str,
    payload: dict[str, object],
    task_id: str | None = None,
) -> None:
    if not task_id:
        return
    row = db.query(AppDfaTask).filter(AppDfaTask.task_id == task_id, AppDfaTask.is_deleted.is_(False)).first()
    if row is None:
        return
    from app.service.task_service import _record_task_event

    _record_task_event(
        db,
        row=row,
        event_type=event_type,
        message=message,
        source="agent_observability",
        level="warning",
        status=row.status,
        payload={
            "operator": operator,
            **payload,
        },
        worker_id=str(payload.get("pod_name") or ""),
        execution_owner_id=str(payload.get("pod_name") or ""),
    )


class TaskTimelineEventResponse(BaseModel):
    id: str
    task_id: str
    project_id: str
    source: str
    level: str
    event_type: str
    status: str | None = None
    worker_id: str | None = None
    execution_owner_id: str | None = None
    execution_epoch: int | None = None
    control_version: int | None = None
    dispatch_status: str | None = None
    function_name: str | None = None
    source_file: str | None = None
    line_hint: str | None = None
    parent_task_id: str | None = None
    parent_stage_item_id: str | None = None
    message: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class TaskTimelineResponse(BaseModel):
    task_id: str
    events: list[TaskTimelineEventResponse] = Field(default_factory=list)


class ActionResponse(BaseModel):
    status: str = "ok"
    task_id: str
    message: str
    deleted_event_count: int = 0


def _get_task_row(db: Session, task_id: str):
    from app.db.models import AppDfaTask

    row = db.query(AppDfaTask).filter(
        AppDfaTask.task_id == task_id,
        AppDfaTask.is_deleted.is_(False),
    ).first()
    if not row:
        raise HTTPException(404, f"任务不存在: {task_id}")
    return row


def _task_root(row) -> Path:
    output_path = row.output_path or ""
    if not output_path:
        return Path()
    return Path(output_path).expanduser().resolve() / row.task_id


def _latest_epoch_run_root(root: Path) -> Path:
    run_root = root / "run"
    epochs_root = run_root / "epochs"
    if not epochs_root.exists():
        return run_root
    candidates = [path for path in epochs_root.iterdir() if path.is_dir()]
    if not candidates:
        return run_root
    return sorted(candidates, key=lambda path: path.name)[-1]


def _epoch_label(path: Path) -> str | None:
    if not path:
        return None
    parts = path.parts
    if "epochs" in parts:
        idx = parts.index("epochs")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def _read_text(path: Path, warnings: List[str], label: str, limit: int = 2_000_000) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        data = path.read_text(encoding="utf-8", errors="replace")
        if len(data) > limit:
            warnings.append(f"{label} 内容过大，仅返回前 {limit} 字符")
            return data[:limit]
        return data
    except Exception as exc:  # pragma: no cover - best effort read endpoint
        warnings.append(f"读取 {label} 失败: {exc}")
        return ""


def _load_result_json(row, root: Path, warnings: List[str]) -> Dict[str, Any]:
    result_path = root / "run" / "result.json"
    if result_path.exists():
        try:
            return json.loads(result_path.read_text(encoding="utf-8", errors="replace") or "{}")
        except Exception as exc:
            warnings.append(f"解析 run/result.json 失败: {exc}")
    return row.result_json or {}


def _collect_rounds(result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    rounds = result_json.get("rounds")
    if isinstance(rounds, list):
        return [item for item in rounds if isinstance(item, dict)]
    task_result = result_json.get("task_result")
    if isinstance(task_result, dict) and isinstance(task_result.get("rounds"), list):
        return [item for item in task_result["rounds"] if isinstance(item, dict)]
    return []


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _summarize_rounds(rounds: List[Dict[str, Any]], result_json: Dict[str, Any]) -> Dict[str, Any]:
    token_total = 0.0
    cost_total = 0.0
    passed_count = 0
    functions = set()
    for item in rounds:
        if item.get("passed") is True or item.get("status") in {"passed", "success"}:
            passed_count += 1
        func = item.get("function") or item.get("func") or item.get("entry")
        if func:
            functions.add(str(func))
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        usage = item.get("token_usage") if isinstance(item.get("token_usage"), dict) else {}
        token_total += _number(metrics.get("token_total") or usage.get("total_tokens") or item.get("total_tokens"))
        cost_total += _number(metrics.get("cost") or usage.get("cost") or item.get("cost"))
    root_usage = result_json.get("token_usage") if isinstance(result_json.get("token_usage"), dict) else {}
    token_total = token_total or _number(root_usage.get("total_tokens"))
    cost_total = cost_total or _number(root_usage.get("cost"))
    return {
        "round_count": len(rounds),
        "passed_round_count": passed_count,
        "function_count": len(functions),
        "total_tokens": int(token_total),
        "total_cost": cost_total,
        "effectiveness": {
            "final_round_pass_rate": (passed_count / len(rounds)) if rounds else 0,
        },
    }


def _safe_session_file(root: Path, relative_path: str) -> Path:
    rel = Path(relative_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise HTTPException(400, "非法会话路径")
    run_root = (root / "run").resolve()
    target = (run_root / rel).resolve()
    try:
        target.relative_to(run_root)
    except ValueError:
        raise HTTPException(400, "非法会话路径")
    if target.suffix != ".jsonl":
        raise HTTPException(400, "仅支持 jsonl 会话文件")
    return target


def _parse_session_file(path: Path) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = []
    warnings: List[str] = []
    session_meta: Optional[Dict[str, Any]] = None
    if not path.exists() or not path.is_file():
        raise HTTPException(404, "会话文件不存在")
    for index, raw in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
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
    return {"events": events, "warnings": warnings, "session_meta": session_meta, "line_count": len(path.read_text(encoding="utf-8", errors="replace").splitlines())}


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
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


def _build_task_session_catalog(row) -> Dict[str, Any]:
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
    result_json = _load_result_json(row, root, [])
    return build_session_catalog(
        task_id=row.task_id,
        row_status=row.status,
        run_root=run_root,
        result_json=result_json,
        write_json_atomic=_write_json_atomic,
    )


@router.post("/tasks", status_code=201)
def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
    prompt = body.prompt_content
    if not prompt or not prompt.strip():
        prompt = generate_prompt_from_path(body.input_path)

    task_config_json: Dict[str, Any] = {}
    if body.source_file:
        task_config_json["source_file"] = body.source_file
    if body.function_name:
        task_config_json["function_name"] = body.function_name
    if body.line_hint:
        task_config_json["line_hint"] = body.line_hint
    if body.definition_kind:
        task_config_json["definition_kind"] = str(body.definition_kind).strip()
    if body.taint_params:
        task_config_json["taint_params"] = [str(value).strip() for value in body.taint_params if str(value).strip()]
    if body.function_description:
        task_config_json["function_description"] = str(body.function_description).strip()
    if body.entry_reason:
        task_config_json["entry_reason"] = str(body.entry_reason).strip()
    if body.function_description or body.function_description_source:
        task_config_json["function_description_source"] = str(body.function_description_source or "agent").strip() or "agent"
    if body.entry_reason or body.entry_reason_source:
        task_config_json["entry_reason_source"] = str(body.entry_reason_source or "agent").strip() or "agent"
    if body.taint_details:
        task_config_json["taint_details"] = [
            {
                "name": str(item.get("name") or item.get("taint") or item.get("param") or "").strip(),
                "description": str(item.get("description") or item.get("summary") or "").strip(),
                "description_source": "agent" if str(item.get("description") or item.get("summary") or "").strip() else "default",
                **({"source_kind": str(item.get("source_kind")).strip()} if str(item.get("source_kind") or "").strip() else {}),
            }
            for item in body.taint_details
            if isinstance(item, dict) and str(item.get("name") or item.get("taint") or item.get("param") or "").strip()
        ]

    svc = get_task_service()
    return svc.create_task(
        db,
        project_id=body.project_id,
        task_name=body.task_name,
        input_path=body.input_path,
        module_input_path=body.module_input_path,
        source_root_path=body.source_root_path,
        output_path=body.output_path,
        task_description=body.task_description,
        prompt_template_id=body.prompt_template_id,
        prompt_content=prompt,
        task_config_json=task_config_json or None,
        task_origin_type=body.task_origin_type,
        parent_project_id=body.parent_project_id,
        parent_task_id=body.parent_task_id,
        parent_task_type=body.parent_task_type,
        parent_stage_name=body.parent_stage_name,
        parent_stage_item_id=body.parent_stage_item_id,
        parent_stage_item_key=body.parent_stage_item_key,
    )


@router.get("/tasks")
def list_tasks(
    project_id: str = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
    parent_stage_item_id: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
):
    return get_task_service().list_tasks(
        db,
        project_id=project_id,
        page=page,
        per_page=per_page,
        status=status,
        mode=mode,
        parent_task_id=parent_task_id,
        parent_stage_item_id=parent_stage_item_id,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/workers/cluster-capacity", response_model=WorkerClusterCapacityResponse)
def get_worker_cluster_capacity(
    project_id: str | None = Query(None),
    db: Session = Depends(get_db),
):
    snapshot = build_worker_cluster_snapshot(db, project_id=project_id)
    return WorkerClusterCapacityResponse(
        worker_count=snapshot.worker_count,
        healthy_workers=snapshot.healthy_workers,
        stale_workers=snapshot.stale_workers,
        total_capacity=snapshot.total_capacity,
        running_jobs=snapshot.running_jobs,
        queued_jobs=snapshot.queued_jobs,
        available_slots=snapshot.available_slots,
        updated_at=isoformat_local(snapshot.updated_at),
        workers=[
            WorkerCapacityResponse(
                worker_id=worker.worker_id,
                host_name=worker.host_name,
                pod_name=worker.pod_name,
                pod_ip=worker.pod_ip,
                healthy=worker.healthy,
                max_concurrent_jobs=worker.max_concurrent_jobs,
                running_jobs=worker.running_jobs,
                available_slots=worker.available_slots,
                source=worker.source,
                last_heartbeat_at=isoformat_local(worker.last_heartbeat_at),
                error=worker.error,
                active_jobs=[
                    WorkerActiveJobResponse(
                        task_id=job.task_id,
                        task_name=job.task_name,
                        status=job.status,
                        parent_task_id=job.parent_task_id,
                        parent_task_type=job.parent_task_type,
                        task_origin_type=job.task_origin_type,
                        input_path=job.input_path,
                        started_at=isoformat_local(job.started_at),
                        updated_at=isoformat_local(job.updated_at),
                        dispatch_status=job.dispatch_status,
                        execution_owner_id=job.execution_owner_id,
                        execution_lease_until=isoformat_local(job.execution_lease_until),
                        execution_heartbeat_at=isoformat_local(job.execution_heartbeat_at),
                        mapped=job.mapped,
                        mapping_reason=job.mapping_reason,
                    )
                    for job in worker.active_jobs
                ],
            )
            for worker in snapshot.workers
        ],
    )


@router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["summary"]


@router.get("/agent-observability/aggregate/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_aggregate_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    return snapshot["summary"]


@router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_processes(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    owner_kind: Optional[str] = Query(None),
    kill_allowed: Optional[bool] = Query(None),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=None)["processes"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if owner_kind:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == owner_kind]
    if kill_allowed is not None:
        rows = [row for row in rows if bool(row.get("kill_allowed")) is bool(kill_allowed)]
    if orphan_only:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "orphan"]
    return rows


@router.get("/agent-observability/aggregate/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_aggregate_processes(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    owner_kind: Optional[str] = Query(None),
    kill_allowed: Optional[bool] = Query(None),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["processes"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if owner_kind:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == owner_kind]
    if kill_allowed is not None:
        rows = [row for row in rows if bool(row.get("kill_allowed")) is bool(kill_allowed)]
    if orphan_only:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "orphan"]
    return rows


@router.get("/agent-observability/sessions", response_model=list[AgentSessionSnapshotResponse])
async def list_agent_sessions(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    live_only: bool = Query(False),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=None)["sessions"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if live_only:
        rows = [row for row in rows if bool(row.get("live"))]
    if orphan_only:
        rows = [row for row in rows if bool(row.get("orphan_session"))]
    return rows


@router.get("/agent-observability/aggregate/sessions", response_model=list[AgentSessionSnapshotResponse])
async def list_agent_aggregate_sessions(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    live_only: bool = Query(False),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["sessions"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if live_only:
        rows = [row for row in rows if bool(row.get("live"))]
    if orphan_only:
        rows = [row for row in rows if bool(row.get("orphan_session"))]
    return rows


@router.get("/agent-observability/sessions/content")
async def get_agent_session_content(
    task_id: str = Query(...),
    session_file: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    return get_task_service().get_task_session_file(db, task_id, session_file)


@router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_tasks(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["tasks"]


@router.get("/agent-observability/aggregate/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_aggregate_tasks(
    pod: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["tasks"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    return rows


@router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["pods"]


@router.get("/agent-observability/aggregate/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_aggregate_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    return (await _build_agent_aggregate_snapshot(token, db))["pods"]


@router.get("/agent-observability/aggregate/runtime", response_model=AgentRuntimeAggregateResponse)
async def get_agent_aggregate_runtime(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    return _build_agent_runtime_aggregate(snapshot)


async def _fanout_post_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.post(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url
                logger.warning("dfa agent fanout post non-200 status=%s url=%s", response.status_code, url)
            except Exception:
                logger.warning("dfa agent fanout post failed url=%s", url, exc_info=True)
                continue
    return None, None


async def _kill_agent_process_impl(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    matched = [row for row in snapshot["processes"] if int(row.get("pid") or -1) == pid]
    if not matched:
        return AgentProcessKillResponse(requested=1, matched=0, succeeded=0, failed=0, skipped=1, items=[])
    row = matched[0]
    if not row.get("kill_allowed"):
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=0,
            skipped=1,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="skipped", reason=row.get("kill_block_reason"))],
        )
    logger.warning(
        "dataflow-agent-manual-kill operator=%s project_id=%s pid=%s pgid=%s task_id=%s session_file=%s owner_reason=%s",
        user.get("username") or user.get("name") or "unknown",
        row.get("project_id"),
        pid,
        row.get("pgid"),
        row.get("task_id"),
        row.get("session_file"),
        row.get("owner_reason"),
    )
    _audit_agent_kill_event(
        db,
        project_id=str(row.get("project_id") or ""),
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员手工终止孤儿智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "session_file": row.get("session_file"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "local",
        },
        task_id=row.get("task_id"),
    )
    result = get_agent_observability_service().kill_process(pid)
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=1,
        matched=1,
        succeeded=1 if result.get("status") in {"killed", "gone"} else 0,
        failed=1 if result.get("status") == "failed" else 0,
        skipped=0,
        items=[AgentProcessKillItemResponse(**result)],
    )


@router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _kill_agent_process_impl(pid=pid, db=db, user_and_token=user_and_token)


@internal_observability_router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse, include_in_schema=False)
async def kill_internal_agent_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _kill_agent_process_impl(pid=pid, db=db, user_and_token=user_and_token)


@router.post("/agent-observability/aggregate/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_aggregate_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    matched = [row for row in snapshot["processes"] if int(row.get("pid") or -1) == pid]
    if not matched:
        return AgentProcessKillResponse(requested=1, matched=0, succeeded=0, failed=0, skipped=1, items=[])
    row = matched[0]
    if not row.get("kill_allowed"):
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=0,
            skipped=1,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="skipped", reason=row.get("kill_block_reason"))],
        )

    cluster_snapshot = build_worker_cluster_snapshot(db, project_id=None)
    target_worker = next((worker for worker in cluster_snapshot.workers if str(worker.pod_name or "") == str(row.get("pod_name") or "")), None)
    if target_worker is None:
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=1,
            skipped=0,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="failed", reason="target pod not found in cluster snapshot")],
        )

    logger.warning(
        "dataflow-agent-aggregate-manual-kill operator=%s project_id=%s pid=%s target_pod=%s task_id=%s",
        user.get("username") or user.get("name") or "unknown",
        row.get("project_id"),
        pid,
        row.get("pod_name"),
        row.get("task_id"),
    )
    _audit_agent_kill_event(
        db,
        project_id=str(row.get("project_id") or ""),
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员跨 Pod 手工终止孤儿智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "session_file": row.get("session_file"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "aggregate",
        },
        task_id=row.get("task_id"),
    )
    result, _ = await _fanout_post_json(
        _aggregate_base_urls(target_worker),
        path=f"/agent-observability/processes/{pid}/kill",
        token=token,
        params={},
    )
    if result is None:
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=1,
            skipped=0,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="failed", reason="fanout kill request failed")],
        )
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(**result)


@router.post("/agent-observability/processes/kill-all-orphans", response_model=AgentProcessKillResponse)
async def kill_all_orphan_processes(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "orphan" and row.get("kill_allowed")]
    logger.warning(
        "dataflow-agent-bulk-kill operator=%s project_id=%s count=%s pids=%s",
        user.get("username") or user.get("name") or "unknown",
        None,
        len(killable),
        [row.get("pid") for row in killable],
    )
    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=str(row.get("project_id") or ""),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员批量终止孤儿智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "session_file": row.get("session_file"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "local_bulk",
            },
            task_id=row.get("task_id"),
        )
    items = [get_agent_observability_service().kill_process(int(row["pid"])) for row in killable]
    _invalidate_agent_aggregate_cache()
    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=0,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.post("/agent-observability/aggregate/processes/kill-all-orphans", response_model=AgentProcessKillResponse)
async def kill_all_agent_aggregate_orphans(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "orphan" and row.get("kill_allowed")]
    cluster_snapshot = build_worker_cluster_snapshot(db, project_id=None)
    worker_by_pod = {str(worker.pod_name or ""): worker for worker in cluster_snapshot.workers}
    items: list[dict[str, Any]] = []

    logger.warning(
        "dataflow-agent-aggregate-bulk-kill operator=%s project_id=%s count=%s",
        user.get("username") or user.get("name") or "unknown",
        None,
        len(killable),
    )

    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=str(row.get("project_id") or ""),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员跨 Pod 批量终止孤儿智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "session_file": row.get("session_file"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "aggregate_bulk",
            },
            task_id=row.get("task_id"),
        )
        target_worker = worker_by_pod.get(str(row.get("pod_name") or ""))
        if target_worker is None:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "target pod not found in cluster snapshot"})
            continue
        result, _ = await _fanout_post_json(
            _aggregate_base_urls(target_worker),
            path=f"/agent-observability/processes/{int(row.get('pid') or 0)}/kill",
            token=token,
            params={},
        )
        if not result:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "fanout kill request failed"})
            continue
        for item in result.get("items") or []:
            items.append(item)

    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    skipped = sum(1 for item in items if item.get("status") == "skipped")
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.post("/agent-observability/aggregate/processes/kill-all-suspected-orphans", response_model=AgentProcessKillResponse)
async def kill_all_agent_aggregate_suspected_orphans(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "unknown" and row.get("kill_allowed")]
    cluster_snapshot = build_worker_cluster_snapshot(db, project_id=None)
    worker_by_pod = {str(worker.pod_name or ""): worker for worker in cluster_snapshot.workers}
    items: list[dict[str, Any]] = []

    logger.warning(
        "dataflow-agent-aggregate-bulk-kill-suspected operator=%s project_id=%s count=%s",
        user.get("username") or user.get("name") or "unknown",
        None,
        len(killable),
    )

    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=str(row.get("project_id") or ""),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员跨 Pod 批量终止疑似孤儿智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "session_file": row.get("session_file"),
                "owner_reason": row.get("owner_reason"),
                "owner_kind": row.get("owner_kind"),
                "kill_mode": "aggregate_bulk_suspected",
            },
            task_id=row.get("task_id"),
        )
        target_worker = worker_by_pod.get(str(row.get("pod_name") or ""))
        if target_worker is None:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "target pod not found in cluster snapshot"})
            continue
        result, _ = await _fanout_post_json(
            _aggregate_base_urls(target_worker),
            path=f"/agent-observability/processes/{int(row.get('pid') or 0)}/kill",
            token=token,
            params={},
        )
        if not result:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "fanout kill request failed"})
            continue
        for item in result.get("items") or []:
            items.append(item)

    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    skipped = sum(1 for item in items if item.get("status") == "skipped")
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.get("/tasks/{task_id}")
def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.get("/tasks/{task_id}/execution")
def get_task_execution(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_execution(db, task_id)


@router.get("/tasks/{task_id}/result")
def get_task_result(task_id: str, db: Session = Depends(get_db)):
    row = _get_task_row(db, task_id)
    root = _task_root(row)
    warnings: List[str] = []
    output_root = root / "output" if str(root) else Path()
    run_root = root / "run" if str(root) else Path()
    result_json = _load_result_json(row, root, warnings) if str(root) else (row.result_json or {})
    rounds = _collect_rounds(result_json)
    latest_run_root = _latest_epoch_run_root(root) if str(root) else Path()
    current_epoch = _epoch_label(latest_run_root)

    output_files: List[Dict[str, Any]] = []
    dataflow_files: List[Dict[str, Any]] = []
    result_markdown = ""
    if output_root.exists():
        for path in sorted(output_root.glob("*.md")):
            markdown = _read_text(path, warnings, path.name)
            item = {
                "name": path.name,
                "relative_path": str(path.relative_to(root)),
                "markdown": markdown,
                "size": path.stat().st_size,
                "mtime": path.stat().st_mtime,
            }
            output_files.append(item)
            if not result_markdown:
                result_markdown = markdown
        dataflow_dir = output_root / "dataflow"
        if dataflow_dir.exists():
            for path in sorted(dataflow_dir.glob("*.md")):
                dataflow_files.append({
                    "name": path.name,
                    "relative_path": str(path.relative_to(root)),
                    "markdown": _read_text(path, warnings, path.name),
                    "size": path.stat().st_size,
                    "mtime": path.stat().st_mtime,
                })

    run_report = _read_text(latest_run_root / "report.md", warnings, "run/report.md") if latest_run_root.exists() else ""
    available = bool(result_markdown or run_report or dataflow_files or result_json)
    if row.status not in TERMINAL_STATUSES and not available:
        available = False
    return {
        "task_id": task_id,
        "available": available,
        "status": row.status,
        "output_root": str(output_root) if str(root) else "",
        "latest_run_root": str(latest_run_root) if latest_run_root.exists() else "",
        "current_epoch": current_epoch,
        "warnings": warnings,
        "result_markdown": result_markdown,
        "run_report_markdown": run_report,
        "result_json": result_json,
        "output_files": output_files,
        "dataflow_files": dataflow_files,
        "summary": _summarize_rounds(rounds, result_json),
    }


@router.get("/tasks/{task_id}/sessions")
def list_task_sessions(task_id: str, db: Session = Depends(get_db)):
    row = _get_task_row(db, task_id)
    catalog = _build_task_session_catalog(row)
    return {"task_id": task_id, "items": catalog.get("items", []), "current_epoch": None}

@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
def get_task_session_index(task_id: str, db: Session = Depends(get_db)):
    row = _get_task_row(db, task_id)
    catalog = _build_task_session_catalog(row)
    return {
        "task_id": catalog.get("task_id") or row.task_id,
        "status": catalog.get("status") or row.status,
        "sessions_root": catalog.get("sessions_root"),
        "index_path": catalog.get("index_path"),
        "generated_at": catalog.get("generated_at"),
        **(catalog.get("index") or {}),
    }


@router.get("/tasks/{task_id}/sessions/file")
def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db)):
    row = _get_task_row(db, task_id)
    root = _task_root(row)
    target = _safe_session_file(root, path)
    parsed = _parse_session_file(target)
    stat = target.stat()
    return {
        "task_id": task_id,
        "path": path,
        "line_count": parsed["line_count"],
        "events": parsed["events"],
        "warnings": parsed["warnings"],
        "session_meta": parsed["session_meta"],
        "meta": {
            "session_id": path,
            "session_name": target.stem,
            "relative_path": path,
            "stage_group": "root",
            "role_name": target.stem,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "event_count": parsed["line_count"],
            "is_active": row.status == "running",
            "display_name": target.stem,
        },
    }


@router.get("/tasks/{task_id}/evaluation")
def get_task_evaluation(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_evaluation(db, task_id)


@router.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().cancel_task(db, task_id)

@router.post("/tasks/{task_id}/restart", status_code=201)
def restart_task(task_id: str, db: Session = Depends(get_db)):
    """Clone an existing task and start it immediately."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
def resume_task(task_id: str, db: Session = Depends(get_db)):
    """从断点续跑：跳过已完成的函数，继续分析未完成部分。"""
    return get_task_service().resume_task(db, task_id)


@router.delete("/tasks/{task_id}", status_code=204)
def delete_task(
    task_id: str,
    delete_files: bool = True,
    db: Session = Depends(get_db),
):
    """软删除任务记录，可选删除输出目录文件。"""
    get_task_service().delete_task(db, task_id, delete_files=delete_files)


@router.get("/tasks/{task_id}/timeline", response_model=TaskTimelineResponse)
def get_task_timeline(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_timeline(db, task_id)


@router.delete("/tasks/{task_id}/timeline", response_model=ActionResponse)
def clear_task_timeline(task_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().clear_task_timeline(db, task_id)
    db.commit()
    return ActionResponse(status="ok", task_id=task_id, message="任务时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=ActionResponse)
def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().delete_task_timeline_event(db, task_id, event_id)
    db.commit()
    return ActionResponse(status="ok", task_id=task_id, message="事件已删除", deleted_event_count=deleted_event_count)


@router.get("/tasks/{task_id}/logs")
def get_task_logs(task_id: str, db: Session = Depends(get_db)):
    """获取任务的实时阶段事件（stages_json）。"""
    from app.db.models import AppDfaTask
    row = db.query(AppDfaTask).filter(
        AppDfaTask.task_id == task_id,
        AppDfaTask.is_deleted.is_(False),
    ).first()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(404, f"任务不存在: {task_id}")
    return {"task_id": task_id, "status": row.status,
            "stages_json": row.stages_json or {"events": []}}


@router.post("/generate-prompt")
def generate_prompt(body: GeneratePromptRequest):
    """Auto-generate a data flow analysis prompt from an input path."""
    return {"prompt": generate_prompt_from_path(body.input_path)}
