"""Task management service for secflow-app-dataflow-analyse.

Bridges the FastAPI management layer with the Orchestrator engine.
Each task is persisted in MySQL and executed asynchronously.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time as _time
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

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

    def list_tasks(self, db: Session, *, project_id: str, page: int = 1,
                   per_page: int = 20, status: Optional[str] = None) -> dict:
        query = db.query(AppDfaTask).filter(
            AppDfaTask.project_id == project_id,
            AppDfaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppDfaTask.status == status)
        total = query.count()
        rows = (query.order_by(AppDfaTask.created_at.desc())
                .offset((page - 1) * per_page).limit(per_page).all())
        return {"items": [self._row_to_dict(r) for r in rows],
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
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")
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
        resume_workspace = os.path.join(effective_output, task_id, "run", "workspace")
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

        def on_event(event: SwarmEvent) -> None:
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, event_buffer)

        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return

            row.status = "running"
            # 续跑时保留原始 started_at，首次运行才设置
            if row.started_at is None:
                row.started_at = now_local()
            db.commit()

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
            orch = Orchestrator(config=cfg, on_event=on_event)
            result = await orch.execute_recursive(task_id, resume=bool(tcfg.get("resume", False)))

            _flush_stages(task_id, event_buffer)
            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                return

            row.status = result.status.value if result else "error"
            row.finished_at = now_local()
            # 合并历史事件（续跑场景保留前序阶段记录）
            _prev = row.stages_json
            _prev_events = (_prev["events"] if isinstance(_prev, dict)
                            and isinstance(_prev.get("events"), list) else [])
            row.stages_json = {"events": _prev_events + event_buffer, "final": True}
            if result:
                row.result_json = result.model_dump(mode="json")
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
                    r.error = str(exc)
                    r.finished_at = now_local()
                    _prev2 = r.stages_json
                    _prev_events2 = (_prev2["events"] if isinstance(_prev2, dict)
                                     and isinstance(_prev2.get("events"), list) else [])
                    r.stages_json = {"events": _prev_events2 + event_buffer, "final": True}
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
    def _row_to_dict(row: AppDfaTask) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        return {
            **_origin_payload(row),
            "task_id": row.task_id, "project_id": row.project_id,
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content, "status": row.status,
            "error": row.error, "result_json": row.result_json,
            "stages_json": row.stages_json,
            "task_config_json": row.task_config_json,
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
