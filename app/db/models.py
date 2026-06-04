"""SQLAlchemy ORM models for secflow-app-dataflow-analyse."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import json

from sqlalchemy import Boolean, DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.time_utils import now_local


class Base(DeclarativeBase):
    pass


class AppDfaTask(Base):
    """Data flow analysis task, scoped to a project."""
    __tablename__ = "secflow_app_dfa_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    task_origin_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    parent_project_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    parent_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    parent_task_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    parent_stage_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    input_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    module_input_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    source_root_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    output_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    prompt_template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    prompt_content: Mapped[str] = mapped_column(Text, nullable=False)

    # Status: pending | running | passed | failed | error | cancelled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    stages_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    latest_abnormal_reason_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Per-task overrides / resume flags (e.g. {"resume": true})
    task_config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    execution_owner_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    execution_owner_instance_id: Mapped[Optional[str]] = mapped_column(String(160), nullable=True, index=True)
    execution_lease_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    execution_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    control_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dispatch_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    lease_lost_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_lease_lost_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    lease_requeue_not_before: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    last_lease_lost_epoch: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_lease_lost_control_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    lease_recovery_state: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    lease_recovery_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lease_recovery_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppDfaTaskEvent(Base):
    """Database-backed task timeline event for DFA task execution tracing."""

    __tablename__ = "secflow_app_dfa_task_events"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_secflow_app_dfa_task_events_dedupe_key"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="dfa", index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info", index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    worker_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    execution_owner_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    execution_epoch: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    control_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    dispatch_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    event_category: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    event_visibility: Mapped[Optional[str]] = mapped_column(String(16), nullable=True, index=True)
    attempt_no: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    supersedes_event_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    function_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_file: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    line_hint: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    parent_stage_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)

    @property
    def payload(self) -> dict[str, Any]:
        if not self.payload_json:
            return {}
        try:
            loaded = json.loads(self.payload_json)
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}

    @payload.setter
    def payload(self, value: dict[str, Any] | None) -> None:
        self.payload_json = json.dumps(value or {}, ensure_ascii=False)


class AppDfaPromptTemplate(Base):
    """Reusable prompt templates for secflow-app-dataflow-analyse."""
    __tablename__ = "secflow_app_dfa_prompt_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prompt_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False, default="general")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    variables_json: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppDfaProjectConfig(Base):
    """Per-project dataflow analysis configuration blob."""
    __tablename__ = "secflow_app_dfa_project_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppDfaWorkerSlot(Base):
    """Worker slot registry for live DFA worker pods."""
    __tablename__ = "secflow_app_dfa_worker_slots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    instance_id: Mapped[Optional[str]] = mapped_column(String(160), nullable=True, index=True)
    pod_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    pod_ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    http_port: Mapped[int] = mapped_column(Integer, nullable=False, default=8080)
    max_concurrent_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_seen_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
