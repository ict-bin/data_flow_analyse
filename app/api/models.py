"""Models config API routes for dataflow-analyse."""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppDfaModelsConfig

from . import router

logger = logging.getLogger("dfa.api.models")

_DEFAULT_MODELS_CONFIG: Dict[str, Any] = {
    "providers": {
        "icsl_vllm_1": {
            "baseUrl": "http://172.31.29.10:8000/v1/",
            "api": "openai-completions",
            "apiKey": "1234",
            "models": [{"id": "zai-org/GLM-5", "reasoning": True}],
        }
    }
}


class ModelsConfigSaveRequest(BaseModel):
    config: Dict[str, Any]


@router.get("/models")
async def get_models_config(db: Session = Depends(get_db)):
    row = db.query(AppDfaModelsConfig).filter_by(config_key="global").first()
    if not row or not row.config_json:
        return {**_DEFAULT_MODELS_CONFIG, "updated_at": None}
    return {**row.config_json, "updated_at": row.updated_at.isoformat() if row.updated_at else None}


@router.put("/models")
async def save_models_config(body: ModelsConfigSaveRequest, db: Session = Depends(get_db)):
    row = db.query(AppDfaModelsConfig).filter_by(config_key="global").first()
    if row:
        row.config_json = body.config
    else:
        row = AppDfaModelsConfig(config_key="global", config_json=body.config)
        db.add(row)
    db.commit()
    db.refresh(row)
    return {**row.config_json, "updated_at": row.updated_at.isoformat() if row.updated_at else None}
