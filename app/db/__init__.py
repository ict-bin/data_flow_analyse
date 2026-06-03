"""Database engine and session management."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Generator, Literal

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

logger = logging.getLogger("dfa.db")

_engine = None
_SessionLocal = None

@dataclass(frozen=True)
class Migration:
    kind: Literal["column", "index", "table"]
    table_name: str
    name: str
    statement: str


_MIGRATIONS = [
    Migration(
        kind="table",
        table_name="secflow_app_dfa_task_events",
        name="secflow_app_dfa_task_events",
        statement="""
        CREATE TABLE secflow_app_dfa_task_events (
            id VARCHAR(32) NOT NULL PRIMARY KEY,
            task_id VARCHAR(64) NOT NULL,
            project_id VARCHAR(100) NOT NULL,
            source VARCHAR(32) NOT NULL DEFAULT 'dfa',
            level VARCHAR(16) NOT NULL DEFAULT 'info',
            event_type VARCHAR(64) NOT NULL,
            status VARCHAR(32) NULL,
            worker_id VARCHAR(128) NULL,
            execution_owner_id VARCHAR(128) NULL,
            execution_epoch INT NULL,
            control_version INT NULL,
            dispatch_status VARCHAR(32) NULL,
            function_name VARCHAR(255) NULL,
            source_file VARCHAR(1024) NULL,
            line_hint VARCHAR(64) NULL,
            parent_task_id VARCHAR(64) NULL,
            parent_stage_item_id VARCHAR(64) NULL,
            message TEXT NOT NULL,
            payload_json TEXT NULL,
            dedupe_key VARCHAR(255) NOT NULL,
            created_at DATETIME NOT NULL,
            UNIQUE KEY uq_secflow_app_dfa_task_events_dedupe_key (dedupe_key),
            KEY ix_dfa_task_events_task_id (task_id),
            KEY ix_dfa_task_events_project_id (project_id),
            KEY ix_dfa_task_events_event_type (event_type),
            KEY ix_dfa_task_events_created_at (created_at),
            KEY ix_dfa_task_events_worker_id (worker_id),
            KEY ix_dfa_task_events_execution_owner_id (execution_owner_id),
            KEY ix_dfa_task_events_parent_task_id (parent_task_id),
            KEY ix_dfa_task_events_parent_stage_item_id (parent_stage_item_id)
        )
        """,
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_worker_slots",
        name="ix_dfa_worker_slots_heartbeat",
        statement="CREATE INDEX ix_dfa_worker_slots_heartbeat ON secflow_app_dfa_worker_slots (last_heartbeat_at)",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_worker_slots",
        name="http_port",
        statement="ALTER TABLE secflow_app_dfa_worker_slots ADD COLUMN http_port INTEGER NOT NULL DEFAULT 8080",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="task_config_json",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN task_config_json JSON NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="stages_json",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN stages_json JSON NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="latest_abnormal_reason_json",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN latest_abnormal_reason_json JSON NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="prompt_template_id",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN prompt_template_id VARCHAR(64) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="task_origin_type",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN task_origin_type VARCHAR(32) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_project_id",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_project_id VARCHAR(100) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_task_id",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_task_id VARCHAR(64) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_task_type",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_task_type VARCHAR(32) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_stage_name",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_name VARCHAR(64) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_stage_item_id",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_item_id VARCHAR(64) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="parent_stage_item_key",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN parent_stage_item_key VARCHAR(255) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="module_input_path",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN module_input_path VARCHAR(1024) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="source_root_path",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN source_root_path VARCHAR(1024) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="execution_owner_id",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN execution_owner_id VARCHAR(128) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="execution_lease_until",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN execution_lease_until DATETIME NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="execution_heartbeat_at",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN execution_heartbeat_at DATETIME NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="execution_epoch",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN execution_epoch INT NOT NULL DEFAULT 0",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="control_version",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN control_version INT NOT NULL DEFAULT 0",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="dispatch_status",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN dispatch_status VARCHAR(32) NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="lease_lost_count",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN lease_lost_count INT NOT NULL DEFAULT 0",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="last_lease_lost_at",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN last_lease_lost_at DATETIME NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="lease_requeue_not_before",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN lease_requeue_not_before DATETIME NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="last_lease_lost_epoch",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN last_lease_lost_epoch INT NULL",
    ),
    Migration(
        kind="column",
        table_name="secflow_app_dfa_tasks",
        name="last_lease_lost_control_version",
        statement="ALTER TABLE secflow_app_dfa_tasks ADD COLUMN last_lease_lost_control_version INT NULL",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_project_deleted_created_id",
        statement="CREATE INDEX ix_dfa_tasks_project_deleted_created_id ON secflow_app_dfa_tasks (project_id, is_deleted, created_at, id)",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_project_created_id",
        statement="CREATE INDEX ix_dfa_tasks_project_created_id ON secflow_app_dfa_tasks (project_id, created_at, id)",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_project_deleted_status_created_id",
        statement="CREATE INDEX ix_dfa_tasks_project_deleted_status_created_id ON secflow_app_dfa_tasks (project_id, is_deleted, status, created_at, id)",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_sched",
        statement="CREATE INDEX ix_dfa_tasks_sched ON secflow_app_dfa_tasks (is_deleted, status, execution_lease_until, created_at, id)",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_owner",
        statement="CREATE INDEX ix_dfa_tasks_owner ON secflow_app_dfa_tasks (execution_owner_id, status)",
    ),
    Migration(
        kind="index",
        table_name="secflow_app_dfa_tasks",
        name="ix_dfa_tasks_lease_requeue_not_before",
        statement="CREATE INDEX ix_dfa_tasks_lease_requeue_not_before ON secflow_app_dfa_tasks (lease_requeue_not_before)",
    ),
]


def _migration_exists(engine, migration: Migration) -> bool:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if migration.kind == "table":
        return migration.table_name in table_names
    if migration.table_name == "secflow_app_dfa_worker_slots" and "secflow_app_dfa_worker_slots" not in table_names:
        return False
    if migration.table_name == "secflow_app_dfa_task_events" and "secflow_app_dfa_task_events" not in table_names:
        return False
    if migration.kind == "column":
        return migration.name in {col["name"] for col in inspector.get_columns(migration.table_name)}
    return migration.name in {idx["name"] for idx in inspector.get_indexes(migration.table_name)}


def _run_migrations(engine) -> None:
    """Apply additive schema migrations; silently skips already-applied ones."""
    with engine.connect() as conn:
        try:
            conn.execute(text("SET SESSION lock_wait_timeout = 5"))
        except Exception:
            conn.rollback()
        try:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 5"))
        except Exception:
            conn.rollback()
        for migration in _MIGRATIONS:
            stmt = migration.statement
            if _migration_exists(engine, migration):
                logger.info("Migration already satisfied: %s %s.%s", migration.kind, migration.table_name, migration.name)
                continue
            try:
                logger.info("Migration begin: %s", stmt[:120])
                conn.execute(text(stmt))
                conn.commit()
                logger.info("Migration applied: %s", stmt[:120])
            except Exception as exc:
                conn.rollback()
                logger.info("Migration skipped: %s (%s)", stmt[:120], exc)


def init_db(db_url: str, pool_size: int = 5, max_overflow: int = 10) -> None:
    """Initialize the database engine and create tables."""
    global _engine, _SessionLocal
    logger.info("Database engine init begin")
    _engine = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    logger.info("Database metadata create_all begin")
    Base.metadata.create_all(bind=_engine)
    logger.info("Database metadata create_all done")
    logger.info("Database migrations begin")
    _run_migrations(_engine)
    logger.info("Database migrations done")
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
