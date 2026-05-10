"""Database engine and session management."""

from __future__ import annotations

import logging
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

logger = logging.getLogger("dfa.db")

_engine = None
_SessionLocal = None

_MIGRATIONS = [
    # Add task_config_json for per-task overrides / resume flags (added 2026-05)
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN task_config_json JSON NULL",
    # Add stages_json for real-time stage event tracking (added 2026-05)
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN stages_json JSON NULL",
    # Add prompt_template_id for template linkage (added 2026-05)
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN prompt_template_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN task_origin_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_project_id VARCHAR(100) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_task_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_task_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_name VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_item_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_item_key VARCHAR(255) NULL",
    "CREATE INDEX ix_dfa_tasks_project_deleted_created_id ON secflow_app_dfa_tasks (project_id, is_deleted, created_at, id)",
    "CREATE INDEX ix_dfa_tasks_project_created_id ON secflow_app_dfa_tasks (project_id, created_at, id)",
    "CREATE INDEX ix_dfa_tasks_project_deleted_status_created_id ON secflow_app_dfa_tasks (project_id, is_deleted, status, created_at, id)",
]


def _run_migrations(engine) -> None:
    """Apply additive schema migrations; silently skips already-applied ones."""
    with engine.connect() as conn:
        for stmt in _MIGRATIONS:
            try:
                conn.execute(text(stmt))
                conn.commit()
                logger.info("Migration applied: %s", stmt[:60])
            except Exception:
                conn.rollback()


def init_db(db_url: str, pool_size: int = 5, max_overflow: int = 10) -> None:
    """Initialize the database engine and create tables."""
    global _engine, _SessionLocal
    _engine = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    Base.metadata.create_all(bind=_engine)
    _run_migrations(_engine)
    logger.info("Database initialized")


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that provides a DB session."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    db: Session = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
