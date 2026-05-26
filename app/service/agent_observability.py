from __future__ import annotations

import contextlib
import os
import pathlib
import signal
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppDfaTask
from app.service.task_service import get_task_service
from app.service.worker_snapshot import build_worker_cluster_snapshot

POD_NAME = (
    os.environ.get("DFA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "dataflow-analyse-pod"
)


@dataclass
class AgentProcessSnapshot:
    pod_name: str
    pid: int
    pgid: int | None
    ppid: int | None
    command: str
    cwd: str | None
    rss_bytes: int | None
    session_file: str | None
    session_id: str | None
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
        if " pi " not in f" {command} " and "/pi" not in command and " npx pi" not in command and "node" not in command:
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
        rows.append({"pid": pid, "ppid": ppid, "pgid": pgid, "command": command, "cwd": cwd, "rss_bytes": rss_bytes})
    return rows


class AgentObservabilityService:
    def build_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        query = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False))
        if project_id:
            query = query.filter(AppDfaTask.project_id == project_id)
        task_rows = query.all()
        task_by_id = {row.task_id: row for row in task_rows}
        cluster_snapshot = build_worker_cluster_snapshot(db, project_id=project_id)
        active_owner_ids = {str(worker.worker_id or "") for worker in cluster_snapshot.workers if worker.healthy}

        session_nodes: list[dict[str, Any]] = []
        for row in task_rows:
            index = get_task_service().get_task_session_index(db, row.task_id)
            for node in index.get("nodes") or []:
                item = dict(node)
                item["task_id"] = row.task_id
                item["task_name"] = row.task_name
                item["task_status"] = row.status
                session_nodes.append(item)

        processes: list[dict[str, Any]] = []
        for proc in _iter_agent_processes():
            matched = next((node for node in session_nodes if str(node.get("relative_path") or "") in str(proc.get("cwd") or "")), None)
            if matched:
                owner_kind = "unknown"
                kill_allowed = False
                owner_reason = "未完成归属判定"
                kill_block_reason = "仅明确孤儿进程可手工终止"
                task_row = task_by_id.get(str(matched.get("task_id") or ""))
                task_status = str(matched.get("task_status") or "")
                if task_status in {"running", "pending"}:
                    owner_id = str(getattr(task_row, "execution_owner_id", "") or "")
                    lease_expires_at = getattr(task_row, "execution_lease_until", None) if task_row is not None else None
                    heartbeat_at = getattr(task_row, "execution_heartbeat_at", None) if task_row is not None else None
                    lease_live = bool(lease_expires_at and lease_expires_at.timestamp() >= time.time())
                    heartbeat_live = bool(heartbeat_at and (time.time() - heartbeat_at.timestamp()) <= 120)
                    if owner_id and owner_id in active_owner_ids:
                        owner_kind = "tracked"
                        owner_reason = "已关联活动任务，且 worker 心跳正常"
                        kill_block_reason = "进程仍归属于活动任务"
                    elif lease_live or heartbeat_live or bool(matched.get("is_active")):
                        owner_kind = "unknown"
                        owner_reason = "活动任务 lease/heartbeat/session 仍活跃，进入保护态"
                        kill_block_reason = "存在活动运行信号，禁止手工终止"
                    else:
                        owner_kind = "unknown"
                        owner_reason = "活动任务存在但 worker 心跳缺失，进入保护态"
                        kill_block_reason = "任务可能仍在切换或退出宽限期"
                else:
                    if bool(matched.get("is_active")):
                        owner_kind = "unknown"
                        owner_reason = "终态任务但 session 仍活跃，暂不允许终止"
                        kill_block_reason = "存在 live session，进入保护态"
                    else:
                        owner_kind = "orphan"
                        owner_reason = "仅匹配终态任务/失活会话，且无活动 worker"
                        kill_allowed = True
                        kill_block_reason = None
                processes.append(AgentProcessSnapshot(
                    pod_name=POD_NAME,
                    pid=int(proc["pid"]),
                    pgid=proc.get("pgid"),
                    ppid=proc.get("ppid"),
                    command=str(proc.get("command") or ""),
                    cwd=proc.get("cwd"),
                    rss_bytes=proc.get("rss_bytes"),
                    session_file=matched.get("relative_path"),
                    session_id=str((matched.get("session_header") or {}).get("id") or matched.get("session_name") or "") or None,
                    task_id=matched.get("task_id"),
                    task_name=matched.get("task_name"),
                    task_status=matched.get("task_status"),
                    stage_key=matched.get("stage_key"),
                    role_kind=matched.get("role"),
                    owner_kind=owner_kind,
                    owner_reason=owner_reason,
                    kill_allowed=kill_allowed,
                    kill_block_reason=kill_block_reason,
                    termination_state="live",
                ).__dict__)
            else:
                processes.append(AgentProcessSnapshot(
                    pod_name=POD_NAME,
                    pid=int(proc["pid"]),
                    pgid=proc.get("pgid"),
                    ppid=proc.get("ppid"),
                    command=str(proc.get("command") or ""),
                    cwd=proc.get("cwd"),
                    rss_bytes=proc.get("rss_bytes"),
                    session_file=None,
                    session_id=None,
                    task_id=None,
                    task_name=None,
                    task_status=None,
                    stage_key=None,
                    role_kind=None,
                    owner_kind="unknown",
                    owner_reason="未匹配到任务或会话",
                    kill_allowed=False,
                    kill_block_reason="仅明确孤儿进程可手工终止",
                    termination_state="live",
                ).__dict__)
        sessions = [{
            "pod_name": POD_NAME,
            "session_file": str(node.get("relative_path") or ""),
            "session_id": str((node.get("session_header") or {}).get("id") or node.get("session_name") or "") or None,
            "task_id": node.get("task_id"),
            "task_name": node.get("task_name"),
            "stage_key": node.get("stage_key"),
            "role_kind": node.get("role"),
            "display_name": str(node.get("display_name") or node.get("relative_path") or ""),
            "line_count": int(node.get("line_count") or 0),
            "last_event_at": node.get("last_event_at"),
            "live": bool(node.get("is_active")),
            "has_process": any(str(node.get("relative_path") or "") == str(proc.get("session_file") or "") for proc in processes),
            "process_pid": next((int(proc["pid"]) for proc in processes if str(proc.get("session_file") or "") == str(node.get("relative_path") or "")), None),
            "orphan_session": not bool(node.get("is_active")),
            "parse_warnings": list(node.get("warnings") or []),
        } for node in session_nodes]
        tasks = []
        for row in task_rows:
            linked_sessions = [item for item in sessions if item.get("task_id") == row.task_id]
            linked_processes = [item for item in processes if item.get("task_id") == row.task_id]
            tasks.append({
                "task_id": row.task_id,
                "task_name": row.task_name,
                "task_status": row.status,
                "stage_key": "",
                "pod_name": POD_NAME,
                "process_count": len(linked_processes),
                "session_count": len(linked_sessions),
                "agent_roles": sorted({str(item.get("role_kind") or "") for item in linked_processes if item.get("role_kind")}),
                "process_pids": [int(item["pid"]) for item in linked_processes],
                "session_ids": [str(item["session_id"]) for item in linked_sessions if item.get("session_id")],
                "ownership_status": "partial" if linked_sessions and not linked_processes else "healthy",
            })
        return {
            "summary": {
                "pod_name": POD_NAME,
                "active_processes": len([item for item in processes if item.get("owner_kind") == "tracked"]),
                "orphan_processes": len([item for item in processes if item.get("owner_kind") == "orphan"]),
                "unknown_processes": len([item for item in processes if item.get("owner_kind") == "unknown"]),
                "killable_orphan_processes": len([item for item in processes if item.get("kill_allowed")]),
                "orphan_sessions": len([item for item in sessions if item.get("orphan_session") and not item.get("has_process")]),
                "scanned_at": time.time(),
                "scan_errors": 0,
            },
            "processes": processes,
            "sessions": sessions,
            "tasks": tasks,
            "pods": [{
                "pod_name": POD_NAME,
                "process_count": len(processes),
                "orphan_process_count": len([item for item in processes if item.get("owner_kind") == "orphan"]),
                "session_count": len(sessions),
                "orphan_session_count": len([item for item in sessions if item.get("orphan_session") and not item.get("has_process")]),
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
