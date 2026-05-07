"""Prompt template CRUD service."""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy.orm import Session

from app.db.models import AppDfaPromptTemplate


class PromptService:
    def list_prompts(
        self,
        db: Session,
        *,
        page: int = 1,
        per_page: int = 20,
        category: Optional[str] = None,
        keyword: Optional[str] = None,
        is_enabled: Optional[bool] = None,
    ) -> dict:
        query = db.query(AppDfaPromptTemplate).filter(AppDfaPromptTemplate.is_deleted.is_(False))
        if category:
            query = query.filter(AppDfaPromptTemplate.category == category)
        if keyword:
            like = f"%{keyword}%"
            query = query.filter(
                (AppDfaPromptTemplate.name.like(like)) | (AppDfaPromptTemplate.description.like(like))
            )
        if is_enabled is not None:
            query = query.filter(AppDfaPromptTemplate.is_enabled.is_(is_enabled))
        total = query.count()
        rows = (
            query.order_by(AppDfaPromptTemplate.updated_at.desc())
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

    def get_prompt(self, db: Session, prompt_id: str) -> dict:
        return self._row_to_dict(self._get_or_404(db, prompt_id))

    def create_prompt(self, db: Session, payload: dict, username: str) -> dict:
        if payload.get("is_default"):
            self._unset_default(db)
        row = AppDfaPromptTemplate(
            prompt_id=f"tpl_{uuid.uuid4().hex[:12]}",
            name=payload["name"],
            category=payload.get("category", "general"),
            description=payload.get("description"),
            content=payload["content"],
            variables_json=payload.get("variables_json") or [],
            version=1,
            is_default=bool(payload.get("is_default", False)),
            is_enabled=bool(payload.get("is_enabled", True)),
            created_by=username,
            updated_by=username,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def update_prompt(self, db: Session, prompt_id: str, payload: dict, username: str) -> dict:
        row = self._get_or_404(db, prompt_id)
        if payload.get("is_default") is True:
            self._unset_default(db)
        for key, value in payload.items():
            if hasattr(row, key):
                setattr(row, key, value)
        row.version = int(row.version or 1) + 1
        row.updated_by = username
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def delete_prompt(self, db: Session, prompt_id: str) -> None:
        row = self._get_or_404(db, prompt_id)
        row.is_deleted = True
        db.commit()

    def clone_prompt(self, db: Session, prompt_id: str, new_name: str, username: str) -> dict:
        src = self._get_or_404(db, prompt_id)
        row = AppDfaPromptTemplate(
            prompt_id=f"tpl_{uuid.uuid4().hex[:12]}",
            name=new_name,
            category=src.category,
            description=src.description,
            content=src.content,
            variables_json=src.variables_json,
            version=1,
            is_default=False,
            is_enabled=bool(src.is_enabled),
            created_by=username,
            updated_by=username,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _get_or_404(self, db: Session, prompt_id: str) -> AppDfaPromptTemplate:
        row = (
            db.query(AppDfaPromptTemplate)
            .filter(
                AppDfaPromptTemplate.prompt_id == prompt_id,
                AppDfaPromptTemplate.is_deleted.is_(False),
            )
            .first()
        )
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"Prompt 模板不存在: {prompt_id}")
        return row

    def _unset_default(self, db: Session) -> None:
        db.query(AppDfaPromptTemplate).filter(
            AppDfaPromptTemplate.is_default.is_(True),
            AppDfaPromptTemplate.is_deleted.is_(False),
        ).update({"is_default": False})

    @staticmethod
    def _row_to_dict(row: AppDfaPromptTemplate) -> dict:
        return {
            "prompt_id": row.prompt_id,
            "name": row.name,
            "category": row.category,
            "description": row.description,
            "content": row.content,
            "variables_json": row.variables_json or [],
            "version": row.version,
            "is_default": bool(row.is_default),
            "is_enabled": bool(row.is_enabled),
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }


_prompt_service: PromptService | None = None


def get_prompt_service() -> PromptService:
    global _prompt_service
    if _prompt_service is None:
        _prompt_service = PromptService()
    return _prompt_service
