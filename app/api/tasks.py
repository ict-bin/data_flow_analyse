"""Task management API routes for dataflow-analyse."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import generate_prompt_from_path, get_task_service

from . import router


class TaskCreateRequest(BaseModel):
    project_id: str
    task_name: str
    input_path: str
    output_path: Optional[str] = None
    task_description: Optional[str] = None
    prompt_template_id: Optional[str] = None
    prompt_content: Optional[str] = None  # If omitted, auto-generated from input_path
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


@router.post("/tasks", status_code=201)
async def create_task(body: TaskCreateRequest, db: Session = Depends(get_db)):
    prompt = body.prompt_content
    if not prompt or not prompt.strip():
        prompt = generate_prompt_from_path(body.input_path)

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
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    return get_task_service().list_tasks(db, project_id=project_id, page=page, per_page=per_page, status=status)


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db)):
    return get_task_service().get_task(db, task_id)


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
