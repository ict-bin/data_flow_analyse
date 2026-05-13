"""Task management API routes for dataflow-analyse."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import generate_prompt_from_path, get_task_service

from . import router


TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled", "invalid_input", "completed_limited"}


class TaskCreateRequest(BaseModel):
    project_id: str
    task_name: str
    input_path: str
    output_path: Optional[str] = None
    task_description: Optional[str] = None
    prompt_template_id: Optional[str] = None
    prompt_content: Optional[str] = None  # If omitted, auto-generated from input_path
    source_file: Optional[str] = None
    function_name: Optional[str] = None
    line_hint: Optional[str] = None
    taint_params: list[str] = []
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


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
    if body.taint_params:
        task_config_json["taint_params"] = [str(value).strip() for value in body.taint_params if str(value).strip()]

    svc = get_task_service()
    return svc.create_task(
        db,
        project_id=body.project_id,
        task_name=body.task_name,
        input_path=body.input_path,
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
    db: Session = Depends(get_db),
):
    return get_task_service().list_tasks(db, project_id=project_id, page=page, per_page=per_page, status=status)


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
    root = _task_root(row)
    run_root = root / "run" if str(root) else Path()
    if not run_root.exists():
        return {"task_id": task_id, "items": []}

    candidates = list((run_root / "sessions").glob("**/*.jsonl")) if (run_root / "sessions").exists() else []
    if (run_root / "epochs").exists():
        candidates.extend((run_root / "epochs").glob("**/sessions/**/*.jsonl"))
    candidates.extend(run_root.glob("subtasks/**/sessions/**/*.jsonl"))
    seen = set()
    items: List[Dict[str, Any]] = []
    now_ts = __import__("time").time()
    latest_run_root = _latest_epoch_run_root(root)
    for path in sorted(candidates):
        resolved = path.resolve()
        if resolved in seen or not path.is_file():
            continue
        seen.add(resolved)
        rel = path.relative_to(run_root)
        parts = rel.parts
        stage_group = "root"
        if len(parts) > 1:
            stage_group = parts[0] if parts[0] != "sessions" else "root"
            if parts[0] == "subtasks" and len(parts) > 2:
                stage_group = "/".join(parts[:3])
        stat = path.stat()
        try:
            event_count = sum(1 for line in path.open("r", encoding="utf-8", errors="replace") if line.strip())
        except Exception:
            event_count = 0
        session_name = path.stem
        is_latest_epoch = False
        try:
            path.relative_to(latest_run_root)
            is_latest_epoch = True
        except ValueError:
            is_latest_epoch = False
        items.append({
            "session_id": str(rel),
            "session_name": session_name,
            "relative_path": str(rel),
            "stage_group": stage_group,
            "role_name": session_name,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "event_count": event_count,
            "message_count": event_count,
            "is_active": row.status == "running" and (now_ts - stat.st_mtime) < 120,
            "display_name": session_name,
            "epoch": _epoch_label(path),
            "is_latest_epoch": is_latest_epoch,
        })
    items.sort(key=lambda item: (item["stage_group"], -float(item["mtime"]), item["relative_path"]))
    return {"task_id": task_id, "items": items, "current_epoch": _epoch_label(latest_run_root)}


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


@router.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: str, db: Session = Depends(get_db)):
    get_task_service().delete_task(db, task_id)


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
