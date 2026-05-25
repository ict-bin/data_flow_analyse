"""Task management API routes for dataflow-analyse."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.time_utils import isoformat_local
from app.service.worker_snapshot import build_worker_cluster_snapshot
from app.service.session_index import build_session_catalog
from app.service.task_service import generate_prompt_from_path, get_task_service

from . import router


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
async def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
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
async def list_tasks(
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
async def get_worker_cluster_capacity(
    project_id: str = Query(...),
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


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


@router.get("/tasks/{task_id}/execution")
async def get_task_execution(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_execution(db, task_id)


@router.get("/tasks/{task_id}/result")
async def get_task_result(task_id: str, db: Session = Depends(get_db)):
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
async def list_task_sessions(task_id: str, db: Session = Depends(get_db)):
    row = _get_task_row(db, task_id)
    catalog = _build_task_session_catalog(row)
    return {"task_id": task_id, "items": catalog.get("items", []), "current_epoch": None}

@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
async def get_task_session_index(task_id: str, db: Session = Depends(get_db)):
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
async def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db)):
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
async def get_task_evaluation(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_evaluation(db, task_id)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().cancel_task(db, task_id)

@router.post("/tasks/{task_id}/restart", status_code=201)
async def restart_task(task_id: str, db: Session = Depends(get_db)):
    """Clone an existing task and start it immediately."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
async def resume_task(task_id: str, db: Session = Depends(get_db)):
    """从断点续跑：跳过已完成的函数，继续分析未完成部分。"""
    return get_task_service().resume_task(db, task_id)


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: str,
    delete_files: bool = True,
    db: Session = Depends(get_db),
):
    """软删除任务记录，可选删除输出目录文件。"""
    get_task_service().delete_task(db, task_id, delete_files=delete_files)


@router.get("/tasks/{task_id}/timeline", response_model=TaskTimelineResponse)
async def get_task_timeline(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task_timeline(db, task_id)


@router.delete("/tasks/{task_id}/timeline", response_model=ActionResponse)
async def clear_task_timeline(task_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().clear_task_timeline(db, task_id)
    db.commit()
    return ActionResponse(status="ok", task_id=task_id, message="任务时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=ActionResponse)
async def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db)):
    deleted_event_count = get_task_service().delete_task_timeline_event(db, task_id, event_id)
    db.commit()
    return ActionResponse(status="ok", task_id=task_id, message="事件已删除", deleted_event_count=deleted_event_count)


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: str, db: Session = Depends(get_db)):
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
async def generate_prompt(body: GeneratePromptRequest):
    """Auto-generate a data flow analysis prompt from an input path."""
    return {"prompt": generate_prompt_from_path(body.input_path)}
