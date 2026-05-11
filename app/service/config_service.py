"""Per-project analysis config service for secflow-app-dataflow-analyse."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy.orm import Session

from app.db.models import AppDfaProjectConfig

logger = logging.getLogger("dfa.config_service")

# Fields in workers/judges that must NOT be stored in DB — always use fixed defaults
_ROLE_READONLY_FIELDS = {"system_prompt_dir"}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge *override* into *base*, returning a new dict."""
    result = dict(base)
    for key, val in override.items():
        base_val = result.get(key)
        if isinstance(base_val, dict) and not isinstance(val, dict):
            continue
        if isinstance(base_val, dict) and isinstance(val, dict):
            result[key] = _deep_merge(base_val, val)
        else:
            result[key] = val
    return result


_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": "majority",
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "agent_run_timeout_seconds": 3600,
    "agent_timeout_retry_enabled": True,
    "agent_timeout_max_retries": 3,
    "pi_max_retries": -1,
    "pi_retry_delay": 10,
    "max_trace_depth": 5,
    "callee_concurrency": 4,
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
    "output_dir": "/data/app/secflow-app-dataflow-analyse",
    "archive_dir": "/data/app/secflow-app-dataflow-analyse",
    "result_dir": "/data/app/secflow-app-dataflow-analyse",
}


class ConfigService:
    def get_config(self, db: Session, project_id: str) -> dict:
        row = db.query(AppDfaProjectConfig).filter_by(project_id=project_id).first()
        if row and row.config_json:
            data = _deep_merge(_DEFAULT_CONFIG, row.config_json)
        else:
            data = dict(_DEFAULT_CONFIG)
        data["project_id"] = project_id
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_config(self, db: Session, project_id: str, config_data: dict) -> dict:
        blob = {k: v for k, v in config_data.items() if k not in ("project_id", "updated_at")}
        for role_key in ("workers", "judges"):
            if isinstance(blob.get(role_key), dict):
                blob[role_key] = {k: v for k, v in blob[role_key].items() if k not in _ROLE_READONLY_FIELDS}
        row = db.query(AppDfaProjectConfig).filter_by(project_id=project_id).first()
        if row:
            row.config_json = blob
        else:
            row = AppDfaProjectConfig(project_id=project_id, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
        result = _deep_merge(_DEFAULT_CONFIG, blob)
        result["project_id"] = project_id
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_config_service: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service


