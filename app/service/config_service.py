"""Per-project analysis config service for secflow-app-dataflow-analyse."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

from sqlalchemy.orm import Session

from app.config import load_service_config
from app.db.models import AppDfaProjectConfig
from app.models import normalize_max_rounds_exceeded_review_strategy, normalize_pass_threshold

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


_FALLBACK_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds": 3,
    "max_rounds_exceeded_review_strategy": "treat_as_passed",
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
        "agents": [{"model": "gaiasec/auto"}],
        "stage_models": {},
    },
    "judges": {
        "default_tools": ["read", "bash", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "default_thinking_level": "off",
        "agents": [{"model": "gaiasec/auto"}],
        "stage_models": {},
    },
    "output_dir": "/data/app/secflow-app-dataflow-analyse",
    "archive_dir": "/data/app/secflow-app-dataflow-analyse",
    "result_dir": "/data/app/secflow-app-dataflow-analyse",
}
_runtime_default_config: Dict[str, Any] | None = None


def _service_config_paths() -> list[str]:
    configured = os.environ.get("SERVICE_CONFIG")
    paths = []
    if configured:
        paths.append(configured)
    paths.append("/opt/data_flow_analyse/config.example.json")
    return paths


def _load_runtime_default_config() -> Dict[str, Any]:
    global _runtime_default_config
    if _runtime_default_config is not None:
        return _runtime_default_config
    for path in _service_config_paths():
        if not os.path.isfile(path):
            continue
        try:
            _runtime_default_config = load_service_config(path).model_dump(mode="json")
            return _runtime_default_config
        except Exception as exc:
            logger.warning("failed to load runtime default config from %s: %s", path, exc)
    _runtime_default_config = dict(_FALLBACK_DEFAULT_CONFIG)
    return _runtime_default_config


def _normalize_config_blob(data: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(data)
    normalized["max_rounds_exceeded_review_strategy"] = normalize_max_rounds_exceeded_review_strategy(
        normalized.get("max_rounds_exceeded_review_strategy")
    )
    normalized["pass_threshold"] = normalize_pass_threshold(normalized.get("pass_threshold"))
    return normalized


class ConfigService:
    def get_config(self, db: Session, project_id: str) -> dict:
        base_config = _load_runtime_default_config()
        row = db.query(AppDfaProjectConfig).filter_by(project_id=project_id).first()
        if row and row.config_json:
            data = _deep_merge(base_config, row.config_json)
        else:
            data = dict(base_config)
        data = _normalize_config_blob(data)
        data["project_id"] = project_id
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_config(self, db: Session, project_id: str, config_data: dict) -> dict:
        base_config = _load_runtime_default_config()
        blob = {k: v for k, v in config_data.items() if k not in ("project_id", "updated_at")}
        blob = _normalize_config_blob(blob)
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
        result = _normalize_config_blob(_deep_merge(base_config, blob))
        result["project_id"] = project_id
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_config_service: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service
