"""Task management service for secflow-app-dataflow-analyse."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.config import build_task_config, load_service_config
from app.db.models import AppDfaTask
from app.models import TaskStatus
from app.orchestrator import Orchestrator

logger = logging.getLogger("dfa.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

# Running asyncio tasks keyed by task_id so we can cancel them
_running_tasks: dict[str, asyncio.Task] = {}


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/data_flow_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


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


class TaskService:
    def list_tasks(
        self,
        db: Session,
        *,
        project_id: str,
        page: int = 1,
        per_page: int = 20,
        status: Optional[str] = None,
    ) -> dict:
        query = db.query(AppDfaTask).filter(
            AppDfaTask.project_id == project_id,
            AppDfaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppDfaTask.status == status)
        total = query.count()
        rows = (
            query.order_by(AppDfaTask.created_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        return {
            "items": [self._row_to_dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        return self._row_to_dict(row)

    def create_task(
        self,
        db: Session,
        *,
        project_id: str,
        task_name: str,
        input_path: str,
        output_path: Optional[str] = None,
        task_description: Optional[str] = None,
        prompt_content: str,
        created_by: Optional[str] = None,
    ) -> dict:
        task_id = f"dfa_{uuid.uuid4().hex[:16]}"
        effective_output = output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        row = AppDfaTask(
            task_id=task_id,
            project_id=project_id,
            task_name=task_name,
            task_description=task_description,
            input_path=input_path,
            output_path=effective_output,
            prompt_content=prompt_content,
            status="pending",
            created_by=created_by,
        )
        db.add(row)
        db.commit()
        db.refresh(row)

        asyncio_task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"dfa_task_{task_id}",
        )
        _running_tasks[task_id] = asyncio_task

        logger.info("task created: %s project=%s", task_id, project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")

        new_task_id = f"dfa_{uuid.uuid4().hex[:16]}"
        effective_output = row.output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        new_row = AppDfaTask(
            task_id=new_task_id,
            project_id=row.project_id,
            task_name=row.task_name,
            task_description=row.task_description,
            input_path=row.input_path,
            output_path=effective_output,
            prompt_content=row.prompt_content,
            status="pending",
            created_by=row.created_by,
        )
        db.add(new_row)
        db.commit()
        db.refresh(new_row)

        asyncio_task = asyncio.create_task(
            self._execute_task(new_task_id),
            name=f"dfa_task_{new_task_id}",
        )
        _running_tasks[new_task_id] = asyncio_task

        logger.info("task restarted: %s <- %s", new_task_id, task_id)
        return self._row_to_dict(new_row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """从断点续跑：保留同一任务 ID，跳过已完成函数直接从断点继续分析。"""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再续跑")

        from sqlalchemy.orm.attributes import flag_modified
        tcfg = dict(row.task_config_json or {})
        tcfg["resume"] = True
        row.task_config_json = tcfg
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit()
        db.refresh(row)

        asyncio_task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"dfa_task_{task_id}",
        )
        _running_tasks[task_id] = asyncio_task

        logger.info("task resumed: %s", task_id)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)

        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()

        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def delete_task(self, db: Session, task_id: str) -> None:
        row = self._get_or_404(db, task_id)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        row.is_deleted = True
        output_path = row.output_path
        db.commit()
        if output_path and os.path.exists(output_path):
            shutil.rmtree(output_path, ignore_errors=True)

    async def _execute_task(self, task_id: str) -> None:
        """Run the Orchestrator engine and persist results."""
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = db.query(AppDfaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return

            row.status = "running"
            row.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            db.commit()

            svc = _load_svc_config()
            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path)

            tcfg = row.task_config_json or {}
            resume = bool(tcfg.get("resume", False))

            orch = Orchestrator(config=cfg)
            result = await orch.execute_recursive(task_id, resume=resume)

            db.expire(row)
            db.refresh(row)
            if row.status == "cancelled":
                return

            row.status = result.status.value if result else "error"
            row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if result:
                row.result_json = result.model_dump(mode="json")
                if result.error:
                    row.error = result.error
            db.commit()

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("task execution failed: %s error=%s", task_id, exc)
            try:
                db.rollback()
                r = db.query(AppDfaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            return dt.isoformat() if dt else None

        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "task_name": row.task_name,
            "task_description": row.task_description,
            "input_path": row.input_path,
            "output_path": row.output_path,
            "prompt_content": row.prompt_content,
            "status": row.status,
            "error": row.error,
            "result_json": row.result_json,
            "task_config_json": row.task_config_json,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at),
            "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at),
            "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
