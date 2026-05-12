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
import time as _time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session, load_only

from app.config import build_task_config, load_service_config
from app.db.models import AppDfaTask
from app.logging_utils import log_event
from app.models import SwarmEvent, TaskStatus
from app.orchestrator import Orchestrator
from app.time_utils import isoformat_local, now_local

logger = logging.getLogger("dfa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

# Running asyncio tasks keyed by task_id so we can cancel them
_running_tasks: dict[str, asyncio.Task] = {}


def _task_root(row: AppDfaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _task_run_root(row: AppDfaTask) -> Path | None:
    root = _task_root(row)
    return root / "run" if root else None


def _task_result_path(row: AppDfaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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
        "prompt": {
            "template_id": row.prompt_template_id,
            "content_length": len(prompt),
            "content_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest() if prompt else None,
        },
        "origin": _origin_payload(row),
        "config": {
            "has_task_overrides": bool(row.task_config_json),
            "override_keys": sorted((row.task_config_json or {}).keys()),
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
        return _ServiceConfig(**cfg_dict)
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


def _flush_stages(task_id: str, events: list[dict]) -> None:
    """将实时事件缓冲写入 DB，供前端轮询展示进度。"""
    try:
        from sqlalchemy.orm.attributes import flag_modified
        from app.db import get_db as _get_db
        _gen = _get_db()
        _db = next(_gen)
        try:
            _r = _db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if _r:
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

    def get_task_evaluation(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        run_root = _task_run_root(row)
        warnings: list[str] = []
        if not run_root or not run_root.is_dir():
            return {
                "task_id": row.task_id,
                "status": row.status,
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
            "available": bool(summary or rounds),
            "summary": summary,
            "rounds": rounds,
            "warnings": warnings,
        }

    def list_tasks(self, db: Session, *, project_id: str, page: int = 1,
                   per_page: int = 100, status: Optional[str] = None) -> dict:
        query = db.query(AppDfaTask).filter(
            AppDfaTask.project_id == project_id,
            AppDfaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppDfaTask.status == status)
        total = query.count()
        rows = (query.options(*self._list_load_options())
                .order_by(AppDfaTask.created_at.desc())
                .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_dict(r, include_heavy=False) for r in rows],
                "total": total, "page": page, "per_page": per_page}

    def get_task(self, db: Session, task_id: str) -> dict:
        return self._row_to_dict(self._get_or_404(db, task_id))

    def create_task(self, db: Session, *, project_id: str, task_name: str,
                    input_path: str, output_path: Optional[str] = None,
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
        # Validate paths are under FILESERVER_ROOT to prevent path traversal
        from fastapi import HTTPException as _HTTPException
        _abs_input = os.path.realpath(os.path.abspath(input_path))
        _abs_fs = os.path.realpath(os.path.abspath(_fs_base))
        if not _abs_input.startswith(_abs_fs + os.sep) and _abs_input != _abs_fs:
            raise _HTTPException(400, f"input_path 必须位于 {_fs_base} 下")
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-dataflow-analyse"
        _abs_output = os.path.realpath(os.path.abspath(effective_output))
        if not _abs_output.startswith(_abs_fs + os.sep) and _abs_output != _abs_fs:
            raise _HTTPException(400, f"output_path 必须位于 {_fs_base} 下")
        row = AppDfaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=input_path,
            output_path=effective_output, prompt_template_id=prompt_template_id,
            prompt_content=prompt_content, status="pending", created_by=created_by,
            task_config_json=task_config_json,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
        )
        db.add(row); db.commit(); db.refresh(row)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"dfa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        """在原任务ID上重置并重新执行（SA 模式：in-place restart）。"""
        row = self._get_or_404(db, task_id)
        # 自动取消正在运行/等待的任务，而不是报错
        if row.status in ("pending", "running"):
            at = _running_tasks.get(task_id)
            if at and not at.done():
                at.cancel()
            _running_tasks.pop(task_id, None)
        from sqlalchemy.orm.attributes import flag_modified
        # 清除上次续跑的 start_stage / resume_workspace，保留其他覆盖项
        clean_config = {k: v for k, v in (row.task_config_json or {}).items()
                        if k not in ("start_stage", "resume_workspace", "resume")} or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.stages_json = None
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        # 删除上次执行目录，保证从零开始
        # 路径逻辑与 _execute_task 一致：优先用 row.output_path，否则从 svc config 取默认值
        import shutil as _shutil
        _svc_cleanup = _load_svc_config_from_db(db, row.project_id)
        _effective_output = row.output_path or _svc_cleanup.output_dir
        task_root = os.path.join(_effective_output, task_id)
        if os.path.isdir(task_root):
            try:
                _shutil.rmtree(task_root)
            except Exception as _e:
                logger.warning("Failed to clean task dir %s: %s", task_root, _e)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"dfa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
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
        resume_workspace = os.path.join(effective_output, task_id, "run", "workspace-worker-0")
        tcfg = dict(row.task_config_json or {})
        tcfg["start_stage"] = 3
        tcfg["resume_workspace"] = resume_workspace
        # 保持旧的 resume 标志兼容性
        tcfg["resume"] = True
        row.task_config_json = tcfg
        row.status = "pending"
        # 保留 started_at 和 stages_json，续跑后仍能看到前序阶段记录
        row.finished_at = None
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"dfa_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id, resume_workspace=resume_workspace)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row.status = "cancelled"
        row.finished_at = now_local()
        db.commit(); db.refresh(row)
        return self._row_to_dict(row)

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> None:
        """软删除任务记录，并可选删除输出目录下的任务文件。运行中任务不允许删除。"""
        import shutil as _shutil
        from fastapi import HTTPException
        row = self._get_or_404(db, task_id)
        if row.status == "running":
            raise HTTPException(status_code=409, detail="任务正在运行，请先取消后再删除")
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    logger.info("delete_task: removed task dir %s", task_dir)
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        row.is_deleted = True
        db.commit()

    async def _execute_task(self, task_id: str) -> None:
        """Run the Orchestrator engine and persist results."""
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        event_buffer: list[dict] = []

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
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, _baseline_events + event_buffer)

        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return

            row.status = "running"
            # 续跑时保留原始 started_at，首次运行才设置
            if row.started_at is None:
                row.started_at = now_local()
            db.commit()
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

            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path)
            if tcfg.get("source_file"):
                cfg.source_file = str(tcfg["source_file"])
            if tcfg.get("function_name"):
                cfg.function_name = str(tcfg["function_name"])
            if tcfg.get("line_hint"):
                cfg.line_hint = str(tcfg["line_hint"])
            if isinstance(tcfg.get("taint_params"), list):
                cfg.taint_params = [str(value).strip() for value in tcfg["taint_params"] if str(value).strip()]
            orch = Orchestrator(config=cfg, on_event=on_event)
            result = await orch.execute_recursive(task_id, resume=bool(tcfg.get("resume", False)))

            _flush_stages(task_id, _baseline_events + event_buffer)
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                return

            row.status = result.status.value if result else "error"
            row.finished_at = now_local()
            # _baseline_events already contains all prior history; just append
            # the new event_buffer.  Do NOT re-read row.stages_json here because
            # _flush_stages already wrote (_baseline + buffer) to DB, so re-reading
            # would produce duplicates.
            row.stages_json = {"events": _baseline_events + event_buffer, "final": True}
            if result:
                result_payload = result.model_dump(mode="json")
                result_file = _write_task_result_json(row, result_payload)
                _write_task_evaluation_files(row, result_payload)
                row.result_json = _lightweight_result_json(row, result_payload, result_file)
                if result.error:
                    row.error = result.error
            db.commit()

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed",
                      event="task_error", task_id=task_id, error=str(exc))
            try:
                db.rollback()
                r = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.finished_at = now_local()
                    r.stages_json = {"events": _baseline_events + event_buffer, "final": True}
                    _persist_terminal_failure(r, str(exc), status="error")
                    db.commit()
            except Exception:
                pass
        finally:
            _running_tasks.pop(task_id, None)
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _get_or_404(self, db: Session, task_id: str) -> AppDfaTask:
        row = db.query(AppDfaTask).filter(
            AppDfaTask.task_id == task_id,
            AppDfaTask.is_deleted.is_(False),
        ).first()
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
                AppDfaTask.output_path,
                AppDfaTask.prompt_template_id,
                AppDfaTask.status,
                AppDfaTask.error,
                AppDfaTask.created_by,
                AppDfaTask.created_at,
                AppDfaTask.updated_at,
                AppDfaTask.started_at,
                AppDfaTask.finished_at,
            ),
        )

    @staticmethod
    def _row_to_dict(row: AppDfaTask, *, include_heavy: bool = True) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        return {
            **_origin_payload(row),
            "task_id": row.task_id, "project_id": row.project_id,
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "error": row.error,
            "result_json": _lightweight_result_json(row, row.result_json) if include_heavy else None,
            "stages_json": row.stages_json if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
