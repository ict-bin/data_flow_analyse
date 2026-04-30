"""Project config API routes for dataflow-analyse."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppDfaProjectConfig

from . import router

logger = logging.getLogger("dfa.api.config")

_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": 1,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "pi_max_retries": -1,
    "pi_retry_delay": 10,
    "max_trace_depth": 5,
    "callee_concurrency": -1,
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "default_thinking_level": "off",
        "agents": [],
        "stage_models": {},
    },
    "judges": {
        "default_tools": ["read", "bash", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "default_thinking_level": "off",
        "agents": [],
        "stage_models": {},
    },
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output",
}


class ConfigSaveRequest(BaseModel):
    project_id: str
    config: Dict[str, Any]


@router.get("/config")
async def get_config(project_id: str = Query(...), db: Session = Depends(get_db)):
    row = db.query(AppDfaProjectConfig).filter_by(project_id=project_id).first()
    base = dict(_DEFAULT_CONFIG)
    if row and row.config_json:
        merged = {**base, **row.config_json, "project_id": project_id}
    else:
        merged = {**base, "project_id": project_id}
    merged.setdefault("updated_at", row.updated_at.isoformat() if row and row.updated_at else None)
    return merged


@router.put("/config")
async def save_config(body: ConfigSaveRequest, db: Session = Depends(get_db)):
    row = db.query(AppDfaProjectConfig).filter_by(project_id=body.project_id).first()
    if row:
        row.config_json = body.config
    else:
        row = AppDfaProjectConfig(project_id=body.project_id, config_json=body.config)
        db.add(row)
    db.commit()
    db.refresh(row)
    return {
        **(_DEFAULT_CONFIG),
        **(row.config_json or {}),
        "project_id": body.project_id,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
