"""Prompt template CRUD API endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.prompt_service import get_prompt_service

from . import router


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class PromptCreateRequest(BaseModel):
    name: str
    category: str = "general"
    description: Optional[str] = None
    content: str
    variables_json: Optional[list] = None
    is_default: bool = False
    is_enabled: bool = True
    created_by: Optional[str] = None


class PromptUpdateRequest(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None
    variables_json: Optional[list] = None
    is_default: Optional[bool] = None
    is_enabled: Optional[bool] = None
    updated_by: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/prompts")
async def list_prompts(
    category: Optional[str] = None,
    is_default: Optional[bool] = None,
    is_enabled: Optional[bool] = None,
    page: int = 1,
    per_page: int = 20,
    db: Session = Depends(get_db),
):
    return get_prompt_service().list_prompts(
        db, category=category, is_default=is_default, is_enabled=is_enabled,
        page=page, per_page=per_page,
    )


@router.post("/prompts", status_code=201)
async def create_prompt(body: PromptCreateRequest, db: Session = Depends(get_db)):
    return get_prompt_service().create_prompt(db, **body.model_dump())


@router.get("/prompts/{prompt_id}")
async def get_prompt(prompt_id: str, db: Session = Depends(get_db)):
    return get_prompt_service().get_prompt(db, prompt_id)


@router.put("/prompts/{prompt_id}")
async def update_prompt(
    prompt_id: str, body: PromptUpdateRequest, db: Session = Depends(get_db)
):
    return get_prompt_service().update_prompt(
        db, prompt_id, **{k: v for k, v in body.model_dump().items() if v is not None}
    )


@router.delete("/prompts/{prompt_id}", status_code=204)
async def delete_prompt(prompt_id: str, db: Session = Depends(get_db)):
    get_prompt_service().delete_prompt(db, prompt_id)


@router.post("/prompts/{prompt_id}/clone", status_code=201)
async def clone_prompt(
    prompt_id: str,
    new_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    return get_prompt_service().clone_prompt(db, prompt_id, new_name=new_name)
