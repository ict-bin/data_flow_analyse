from __future__ import annotations

import contextlib
from datetime import datetime
import os
import pathlib
import shlex
import signal
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppDfaTask
from app.service.worker_snapshot import build_worker_cluster_snapshot

POD_NAME = (
    os.environ.get("DFA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "dataflow-analyse-pod"
)
_RUNNING_TASK_STATUSES = {"running", "pending", "queued", "dispatching"}

_SESSION_ARG_KEYS = {
    "--session",
    "--session-file",
    "--session_path",
    "--session-path",
    "--resume",
}
_AGENT_TOKENS: tuple[tuple[str, str], ...] = (
    ("claude-code", "claude-code"),
    ("claude", "claude"),
    ("opencode", "opencode"),
    ("codex", "codex"),
    ("npx pi", "pi"),
    (" pi ", "pi"),
    ("/pi", "pi"),
)
_WRAPPER_NAMES = {"node", "npm", "npx", "pnpm", "yarn", "python", "python3", "uv"}


@dataclass
class AgentProcessSnapshot:
    pod_name: str
    pid: int
    pgid: int | None
    ppid: int | None
    command: str
    cwd: str | None
    exe: str | None
    rss_bytes: int | None
    runtime_kind: str | None
    match_source: str | None
    match_confidence: str | None
    workspace_root: str | None
    task_id: str | None
    task_name: str | None
    task_status: str | None
    stage_key: str | None
    role_kind: str | None
    owner_kind: str
    owner_reason: str
    kill_allowed: bool
    kill_block_reason: str | None
    termination_state: str


def _read_text(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return ""


def _normalize_path(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return str(pathlib.Path(value).resolve(strict=False))
    except Exception:
        return value


def _infer_runtime_kind(command: str, exe: str | None) -> str | None:
    normalized = f" {command.lower()} "
    for token, runtime_kind in _AGENT_TOKENS:
        if token in normalized:
            return runtime_kind
    exe_name = pathlib.Path(exe or "").name.lower()
    if exe_name in {"pi", "claude", "claude-code", "codex", "opencode"}:
        return exe_name
    if exe_name in _WRAPPER_NAMES:
        for runtime_name in ("claude-code", "claude", "codex", "opencode", "pi"):
            if runtime_name in normalized:
                return runtime_name
    return None


def _extract_role_kind(command: str) -> str | None:
    lowered = f" {command.lower()} "
    for token in ("judge", "review", "reviewer", "worker", "coder", "analysis", "planner", "critic", "round"):
        if f" {token} " in lowered or f"/{token}" in lowered or f"--{token}" in lowered:
            return token
    return None


def _extract_session_arg_path(command: str) -> str | None:
    with contextlib.suppress(Exception):
        tokens = shlex.split(command)
        for index, token in enumerate(tokens):
            if token in _SESSION_ARG_KEYS and index + 1 < len(tokens):
                return _normalize_path(tokens[index + 1])
            for key in _SESSION_ARG_KEYS:
                prefix = f"{key}="
                if token.startswith(prefix):
                    return _normalize_path(token[len(prefix):])
    return None


def _collect_open_paths(proc_dir: pathlib.Path) -> list[str]:
    rows: list[str] = []
    fd_dir = proc_dir / "fd"
    if not fd_dir.exists():
        return rows
    with contextlib.suppress(Exception):
        for fd_entry in fd_dir.iterdir():
            with contextlib.suppress(Exception):
                normalized = _normalize_path(os.readlink(fd_entry))
                if normalized:
                    rows.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in rows:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _iter_agent_processes() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for proc_dir in pathlib.Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            command = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        except Exception:
            continue
        exe = None
        with contextlib.suppress(Exception):
            exe = os.readlink(proc_dir / "exe")
        runtime_kind = _infer_runtime_kind(command, exe)
        if runtime_kind is None:
            continue
        stat_raw = _read_text(proc_dir / "stat").split()
        ppid = int(stat_raw[3]) if len(stat_raw) > 4 else None
        pgid = int(stat_raw[4]) if len(stat_raw) > 5 else None
        cwd = None
        with contextlib.suppress(Exception):
            cwd = os.readlink(proc_dir / "cwd")
        rss_bytes = None
        for line in _read_text(proc_dir / "status").splitlines():
            if line.startswith("VmRSS:"):
                parts = line.split()
                if len(parts) >= 2:
                    rss_bytes = int(parts[1]) * 1024
                break
        rows.append({
            "pid": pid,
            "ppid": ppid,
            "pgid": pgid,
            "command": command,
            "cwd": cwd,
            "exe": exe,
            "rss_bytes": rss_bytes,
            "runtime_kind": runtime_kind,
            "session_arg_path": _extract_session_arg_path(command),
            "open_paths": _collect_open_paths(proc_dir),
        })
    return rows


def _task_roots(row: AppDfaTask) -> list[str]:
    output_root = _normalize_path(getattr(row, "output_path", None))
    task_id = str(getattr(row, "task_id", "") or "").strip()
    candidates = []
    if output_root and task_id:
        candidates.extend(
            [
                os.path.join(output_root, task_id),
                os.path.join(output_root, task_id, "run"),
                os.path.join(output_root, task_id, "output"),
            ]
        )
    candidates.extend(
        [
            getattr(row, "input_path", None),
            getattr(row, "module_input_path", None),
            getattr(row, "source_root_path", None),
            output_root,
        ]
    )
    roots: list[str] = []
    for item in candidates:
        normalized = _normalize_path(item)
        if normalized:
            roots.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in roots:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _path_belongs_to_root(path_value: str | None, root: str | None) -> bool:
    if not path_value or not root:
        return False
    try:
        pathlib.Path(path_value).relative_to(pathlib.Path(root))
        return True
    except Exception:
        return False


def _belongs_to_any_root(proc: dict[str, Any], root: str) -> bool:
    candidates = [
        _normalize_path(proc.get("cwd")),
        _normalize_path(proc.get("exe")),
        _normalize_path(proc.get("session_arg_path")),
    ]
    candidates.extend(_normalize_path(item) for item in (proc.get("open_paths") or []))
    command = str(proc.get("command") or "")
    for candidate in candidates:
        if _path_belongs_to_root(candidate, root):
            return True
    return bool(root and root in command)


def _task_sort_key(row: AppDfaTask) -> tuple[int, float]:
    status = str(getattr(row, "status", "") or "").strip().lower()
    status_rank = 2 if status == "running" else 1 if status else 0
    updated_at = getattr(row, "updated_at", None)
    updated_ts = updated_at.timestamp() if isinstance(updated_at, datetime) else 0.0
    return status_rank, updated_ts


def _match_task(proc: dict[str, Any], task_rows: list[AppDfaTask], task_roots_by_id: dict[str, list[str]]) -> tuple[str | None, str | None, str | None]:
    matches: list[tuple[tuple[int, int, int, float], str, str, str]] = []

    def _record_matches(path_value: str | None, source: str, source_rank: int) -> None:
        normalized_path = _normalize_path(path_value)
        if not normalized_path:
            return
        for row in task_rows:
            task_id = str(row.task_id or "")
            status_rank, updated_ts = _task_sort_key(row)
            for root in task_roots_by_id.get(task_id, []):
                if _path_belongs_to_root(normalized_path, root):
                    matches.append(((source_rank, len(root), status_rank, updated_ts), task_id, source, root))

    session_arg_path = _normalize_path(proc.get("session_arg_path"))
    _record_matches(session_arg_path, "session_arg_path", 3)
    cwd = _normalize_path(proc.get("cwd"))
    _record_matches(cwd, "cwd", 2)
    for row in task_rows:
        task_id = str(row.task_id or "")
        status_rank, updated_ts = _task_sort_key(row)
        for root in task_roots_by_id.get(task_id, []):
            if _belongs_to_any_root(proc, root):
                matches.append(((1, len(root), status_rank, updated_ts), task_id, "task_root", root))
    if matches:
        _, task_id, match_source, workspace_root = max(matches, key=lambda item: item[0])
        return task_id, match_source, workspace_root
    return None, None, None


class AgentObservabilityService:
    def build_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        query = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppDfaTask.project_id == project_id)
        task_rows = query.all()
        task_by_id = {row.task_id: row for row in task_rows}
        task_roots_by_id = {row.task_id: _task_roots(row) for row in task_rows}
        cluster_snapshot = build_worker_cluster_snapshot(db, project_id=project_id)

        processes: list[dict[str, Any]] = []
        for proc in _iter_agent_processes():
            task_id, match_source, workspace_root = _match_task(proc, task_rows, task_roots_by_id)
            task_row = task_by_id.get(task_id or "")
            task_name = task_row.task_name if task_row is not None else None
            task_status = str(task_row.status or "") if task_row is not None else None
            stage_key = str(task_row.current_stage or "") if task_row is not None else None
            role_kind = _extract_role_kind(str(proc.get("command") or ""))
            if task_row is not None and str(task_status or "").strip() in _RUNNING_TASK_STATUSES:
                owner_kind = "tracked"
                owner_reason = "active_task_matched"
                kill_allowed = False
                kill_block_reason = "进程归属于活动任务"
            elif task_row is not None:
                owner_kind = "residual"
                owner_reason = "non_running_task_residual"
                kill_allowed = True
                kill_block_reason = None
            else:
                owner_kind = "unknown"
                owner_reason = "unmatched_process"
                kill_allowed = True
                kill_block_reason = None
            processes.append(
                AgentProcessSnapshot(
                    pod_name=POD_NAME,
                    pid=int(proc["pid"]),
                    pgid=proc.get("pgid"),
                    ppid=proc.get("ppid"),
                    command=str(proc.get("command") or ""),
                    cwd=proc.get("cwd"),
                    exe=proc.get("exe"),
                    rss_bytes=proc.get("rss_bytes"),
                    runtime_kind=proc.get("runtime_kind"),
                    match_source=match_source,
                    match_confidence="high" if match_source in {"session_arg_path", "cwd"} else ("medium" if match_source == "task_root" else None),
                    workspace_root=workspace_root,
                    task_id=task_id,
                    task_name=task_name,
                    task_status=task_status,
                    stage_key=stage_key,
                    role_kind=role_kind,
                    owner_kind=owner_kind,
                    owner_reason=owner_reason,
                    kill_allowed=kill_allowed,
                    kill_block_reason=kill_block_reason,
                    termination_state="running",
                ).__dict__
            )

        tasks: list[dict[str, Any]] = []
        for row in task_rows:
            linked_processes = [item for item in processes if item.get("task_id") == row.task_id]
            if not linked_processes:
                continue
            tasks.append({
                "task_id": row.task_id,
                "task_name": row.task_name,
                "task_status": row.status,
                "stage_key": str(row.current_stage or ""),
                "pod_name": POD_NAME,
                "process_count": len(linked_processes),
                "agent_roles": sorted({str(item.get("role_kind") or "") for item in linked_processes if item.get("role_kind")}),
                "process_pids": [int(item["pid"]) for item in linked_processes],
                "ownership_status": "tracked" if str(row.status or "").strip() in _RUNNING_TASK_STATUSES else "residual",
            })

        tracked_process_count = len([item for item in processes if item.get("owner_kind") == "tracked"])
        residual_process_count = len([item for item in processes if item.get("owner_kind") == "residual"])
        unknown_process_count = len([item for item in processes if item.get("owner_kind") == "unknown"])
        scanned_at = time.time()
        return {
            "summary": {
                "pod_name": POD_NAME,
                "active_processes": tracked_process_count,
                "residual_processes": residual_process_count,
                "unknown_processes": unknown_process_count,
                "killable_residual_processes": len([item for item in processes if item.get("owner_kind") == "residual" and item.get("kill_allowed")]),
                "killable_unknown_processes": len([item for item in processes if item.get("owner_kind") == "unknown" and item.get("kill_allowed")]),
                "scanned_at": scanned_at,
                "scan_errors": 0,
            },
            "processes": processes,
            "tasks": tasks,
            "pods": [{
                "pod_name": POD_NAME,
                "worker_id": POD_NAME,
                "healthy": True,
                "process_count": len(processes),
                "tracked_process_count": tracked_process_count,
                "residual_process_count": residual_process_count,
                "unknown_process_count": unknown_process_count,
                "task_count": len(tasks),
                "running_task_count": len([item for item in tasks if str(item.get("ownership_status") or "") == "tracked"]),
                "residual_task_count": len([item for item in tasks if str(item.get("ownership_status") or "") == "residual"]),
                "last_scanned_at": scanned_at,
                "scan_errors": 0,
                "processes": processes,
                "tasks": tasks,
            }],
        }

    def kill_process(self, pid: int) -> dict[str, Any]:
        proc_dir = pathlib.Path("/proc") / str(pid)
        stat = _read_text(proc_dir / "stat").split()
        pgid = int(stat[4]) if len(stat) > 4 else None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                os.kill(pid, signal.SIGTERM)
            time.sleep(0.2)
            with contextlib.suppress(ProcessLookupError):
                if pgid is not None:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    os.kill(pid, signal.SIGKILL)
            return {"pid": pid, "pgid": pgid, "status": "killed"}
        except ProcessLookupError:
            return {"pid": pid, "pgid": pgid, "status": "gone"}
        except Exception as exc:
            return {"pid": pid, "pgid": pgid, "status": "failed", "reason": str(exc)}


_service: AgentObservabilityService | None = None


def get_agent_observability_service() -> AgentObservabilityService:
    global _service
    if _service is None:
        _service = AgentObservabilityService()
    return _service
