from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Callable, Iterable


def find_pi_command() -> list[str]:
    pi_bin = os.environ.get("PI_BIN")
    if pi_bin and os.path.isfile(pi_bin):
        return [pi_bin]
    pi_path = shutil.which("pi")
    if pi_path:
        return [pi_path]
    npx = shutil.which("npx")
    if npx:
        return [npx, "pi"]
    raise FileNotFoundError(
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent"
    )


def process_group_id(proc: asyncio.subprocess.Process) -> int | None:
    try:
        return os.getpgid(proc.pid)
    except ProcessLookupError:
        return None
    except Exception:
        return None


def process_group_exists(pgid: int | None) -> bool:
    if pgid is None:
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def _read_proc_name(pid: int, field: str) -> str:
    try:
        return (pathlib.Path("/proc") / str(pid) / field).read_text(
            encoding="utf-8",
            errors="replace",
        ).strip()
    except Exception:
        return ""


def _safe_readlink(path: pathlib.Path) -> str:
    try:
        return os.readlink(path)
    except Exception:
        return ""


def _read_proc_environ(pid: int) -> dict[str, str]:
    try:
        raw = (pathlib.Path("/proc") / str(pid) / "environ").read_bytes()
    except Exception:
        return {}
    payload: dict[str, str] = {}
    for item in raw.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            payload[key.decode("utf-8", errors="replace")] = value.decode("utf-8", errors="replace")
        except Exception:
            continue
    return payload


def _read_proc_cmdline(pid: int) -> str:
    try:
        raw = (pathlib.Path("/proc") / str(pid) / "cmdline").read_bytes()
    except Exception:
        return ""
    return raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()


def _read_ppid(status_text: str) -> int | None:
    for line in status_text.splitlines():
        if line.startswith("PPid:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def _read_pgid(pid: int) -> int | None:
    try:
        return int(
            subprocess.check_output(
                ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                text=True,
            ).strip()
        )
    except Exception:
        return None


def _normalize_path(path_value: str | None) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    try:
        return os.path.realpath(os.path.abspath(raw))
    except Exception:
        return raw


def _path_within(path_value: str | None, root_value: str | None) -> bool:
    path = _normalize_path(path_value)
    root = _normalize_path(root_value)
    if not path or not root:
        return False
    return path == root or path.startswith(root + os.sep)


@dataclass(frozen=True)
class AgentCleanupTarget:
    task_id: str | None = None
    task_root: str | None = None
    run_root: str | None = None
    worker_id: str | None = None
    execution_epoch: int | None = None


@dataclass(frozen=True)
class AgentRuntimeSnapshot:
    task_id: str
    owner_id: str | None = None
    execution_epoch: int | None = None
    status: str | None = None
    task_root: str | None = None
    run_root: str | None = None
    last_progress_at: float | None = None
    lease_heartbeat_at: float | None = None
    execution_heartbeat_at: float | None = None
    execution_lease_until: float | None = None
    active_context: bool = False


@dataclass(frozen=True)
class AgentProcessInfo:
    pid: int
    ppid: int | None
    pgid: int | None
    comm: str
    exe: str
    cwd: str
    cmdline: str
    environ: dict[str, str]


@dataclass(frozen=True)
class OrphanSweepDecision:
    should_kill: bool
    reason: str
    active_context_matched: bool = False
    recent_activity_seconds: float | None = None
    db_owner_id: str | None = None
    db_execution_epoch: int | None = None
    db_status: str | None = None
    session_kind: str | None = None


def _iter_agent_processes() -> list[AgentProcessInfo]:
    candidates: list[AgentProcessInfo] = []
    proc_root = pathlib.Path("/proc")
    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            status = (proc_dir / "status").read_text(encoding="utf-8", errors="replace")
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe = os.path.basename(_safe_readlink(proc_dir / "exe"))
        except Exception:
            continue
        if comm != "pi" and exe != "node":
            continue
        candidates.append(
            AgentProcessInfo(
                pid=pid,
                ppid=_read_ppid(status),
                pgid=_read_pgid(pid),
                comm=comm,
                exe=exe,
                cwd=_safe_readlink(proc_dir / "cwd"),
                cmdline=_read_proc_cmdline(pid),
                environ=_read_proc_environ(pid),
            )
        )
    return candidates


def _matches_target(info: AgentProcessInfo, target: AgentCleanupTarget | None) -> bool:
    task_id = str(info.environ.get("DFA_TASK_ID") or "").strip()
    task_root = str(info.environ.get("DFA_TASK_ROOT") or "").strip()
    run_root = str(info.environ.get("DFA_TASK_RUN_ROOT") or "").strip()
    worker_id = str(info.environ.get("DFA_WORKER_ID") or "").strip()
    if target is None:
        return bool(task_id or task_root or run_root or "DFA_TASK_ID=" in info.cmdline)
    if target.task_id and task_id == target.task_id:
        return True
    if target.worker_id and worker_id and worker_id == target.worker_id:
        if target.task_id:
            return task_id == target.task_id
        return True
    if target.run_root and (_path_within(info.cwd, target.run_root) or _path_within(run_root, target.run_root)):
        return True
    if target.task_root and (
        _path_within(info.cwd, target.task_root)
        or _path_within(task_root, target.task_root)
        or _path_within(run_root, target.task_root)
    ):
        return True
    return False


def _env_task_id(info: AgentProcessInfo) -> str:
    return str(info.environ.get("DFA_TASK_ID") or "").strip()


def _env_run_root(info: AgentProcessInfo) -> str:
    return str(info.environ.get("DFA_TASK_RUN_ROOT") or "").strip()


def _env_task_root(info: AgentProcessInfo) -> str:
    return str(info.environ.get("DFA_TASK_ROOT") or "").strip()


def _env_execution_epoch(info: AgentProcessInfo) -> int | None:
    raw = str(info.environ.get("DFA_EXECUTION_EPOCH") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _env_session_kind(info: AgentProcessInfo) -> str | None:
    raw = str(info.environ.get("DFA_SESSION_KIND") or "").strip()
    return raw or None


def _session_activity_mtime(run_root: str | None) -> float | None:
    normalized = _normalize_path(run_root)
    if not normalized:
        return None
    sessions_dir = pathlib.Path(normalized) / "sessions"
    latest = 0.0
    try:
        if sessions_dir.is_dir():
            for session_file in sessions_dir.glob("*.jsonl"):
                try:
                    latest = max(latest, session_file.stat().st_mtime)
                except OSError:
                    continue
    except Exception:
        return None
    return latest or None


def _normalize_runtime_snapshots(
    runtime_snapshots: Iterable[AgentRuntimeSnapshot | dict[str, object]] | None,
) -> dict[str, AgentRuntimeSnapshot]:
    normalized: dict[str, AgentRuntimeSnapshot] = {}
    for item in runtime_snapshots or ():
        if isinstance(item, AgentRuntimeSnapshot):
            normalized[item.task_id] = item
            continue
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("task_id") or "").strip()
        if not task_id:
            continue
        epoch_raw = item.get("execution_epoch", item.get("epoch"))
        try:
            execution_epoch = int(epoch_raw) if epoch_raw is not None else None
        except (TypeError, ValueError):
            execution_epoch = None
        normalized[task_id] = AgentRuntimeSnapshot(
            task_id=task_id,
            owner_id=str(item.get("owner_id") or item.get("execution_owner_id") or "").strip() or None,
            execution_epoch=execution_epoch,
            status=str(item.get("status") or "").strip() or None,
            task_root=str(item.get("task_root") or "").strip() or None,
            run_root=str(item.get("run_root") or "").strip() or None,
            last_progress_at=float(item["last_progress_at"]) if item.get("last_progress_at") is not None else None,
            lease_heartbeat_at=float(item["lease_heartbeat_at"]) if item.get("lease_heartbeat_at") is not None else None,
            execution_heartbeat_at=float(item["execution_heartbeat_at"]) if item.get("execution_heartbeat_at") is not None else None,
            execution_lease_until=float(item["execution_lease_until"]) if item.get("execution_lease_until") is not None else None,
            active_context=bool(item.get("active_context") or item.get("execution_alive")),
        )
    return normalized


def _orphan_min_age_seconds() -> float:
    return float(os.environ.get("DFA_AGENT_ORPHAN_MIN_AGE_SECONDS", "900"))


def _orphan_recent_activity_seconds() -> float:
    return float(os.environ.get("DFA_AGENT_ORPHAN_RECENT_ACTIVITY_SECONDS", "120"))


def _orphan_confirm_rounds() -> int:
    return max(1, int(os.environ.get("DFA_AGENT_ORPHAN_CONFIRM_ROUNDS", "2")))


def _task_process_started_at(info: AgentProcessInfo) -> float | None:
    candidates = [
        _session_activity_mtime(_env_run_root(info)),
        _session_activity_mtime(info.cwd),
    ]
    filtered = [value for value in candidates if value is not None]
    if filtered:
        return min(filtered)
    return None


def _build_orphan_sweep_decision(
    info: AgentProcessInfo,
    *,
    now_ts: float,
    runtime_snapshots: dict[str, AgentRuntimeSnapshot],
    owner_id: str | None,
    recent_activity_seconds: float,
) -> OrphanSweepDecision:
    task_id = _env_task_id(info)
    run_root = _env_run_root(info)
    env_epoch = _env_execution_epoch(info)
    session_kind = _env_session_kind(info)
    snapshot = runtime_snapshots.get(task_id) if task_id else None
    recent_activity_at = _session_activity_mtime(run_root) or _session_activity_mtime(info.cwd)
    activity_age = None if recent_activity_at is None else max(0.0, now_ts - recent_activity_at)
    if info.ppid != 1:
        return OrphanSweepDecision(False, "ppid_not_1", session_kind=session_kind)
    if not _matches_target(info, None):
        return OrphanSweepDecision(False, "not_dfa_agent", session_kind=session_kind)
    if snapshot is not None:
        owner_match = not owner_id or not snapshot.owner_id or snapshot.owner_id == owner_id
        epoch_match = env_epoch is None or snapshot.execution_epoch is None or snapshot.execution_epoch == env_epoch
        active_running = snapshot.active_context or str(snapshot.status or "").lower() == "running"
        if owner_match and epoch_match and active_running:
            return OrphanSweepDecision(
                False,
                "active_runtime_snapshot",
                active_context_matched=bool(snapshot.active_context),
                recent_activity_seconds=activity_age,
                db_owner_id=snapshot.owner_id,
                db_execution_epoch=snapshot.execution_epoch,
                db_status=snapshot.status,
                session_kind=session_kind,
            )
    if activity_age is not None and activity_age <= recent_activity_seconds:
        return OrphanSweepDecision(
            False,
            "recent_session_activity",
            recent_activity_seconds=activity_age,
            db_owner_id=snapshot.owner_id if snapshot else None,
            db_execution_epoch=snapshot.execution_epoch if snapshot else None,
            db_status=snapshot.status if snapshot else None,
            session_kind=session_kind,
        )
    if snapshot is not None and str(snapshot.status or "").lower() == "running":
        return OrphanSweepDecision(
            False,
            "running_without_recent_activity_but_snapshot_alive",
            active_context_matched=bool(snapshot.active_context),
            recent_activity_seconds=activity_age,
            db_owner_id=snapshot.owner_id,
            db_execution_epoch=snapshot.execution_epoch,
            db_status=snapshot.status,
            session_kind=session_kind,
        )
    return OrphanSweepDecision(
        True,
        "owner_missing_and_no_recent_activity",
        recent_activity_seconds=activity_age,
        db_owner_id=snapshot.owner_id if snapshot else None,
        db_execution_epoch=snapshot.execution_epoch if snapshot else None,
        db_status=snapshot.status if snapshot else None,
        session_kind=session_kind,
    )


def _kill_process_group(
    logger: Callable[[str], None],
    *,
    label: str,
    info: AgentProcessInfo,
    reason: str,
) -> bool:
    logger(
        f"cleaning agent process [{label}] pid={info.pid} pgid={info.pgid if info.pgid is not None else 'unknown'} "
        f"task_id={info.environ.get('DFA_TASK_ID') or '-'} cwd={info.cwd or '-'} reason={reason}"
    )
    try:
        if info.pgid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(info.pgid, signal.SIGTERM)
            time.sleep(0.2)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(info.pgid, signal.SIGKILL)
        else:
            with contextlib.suppress(ProcessLookupError):
                os.kill(info.pid, signal.SIGTERM)
            time.sleep(0.2)
            with contextlib.suppress(ProcessLookupError):
                os.kill(info.pid, signal.SIGKILL)
        return True
    except Exception:
        return False


def cleanup_task_agent_processes(
    logger: Callable[[str], None],
    *,
    label: str,
    task_id: str | None = None,
    task_root: str | None = None,
    run_root: str | None = None,
    worker_id: str | None = None,
) -> int:
    target = AgentCleanupTarget(
        task_id=task_id or None,
        task_root=task_root or None,
        run_root=run_root or None,
        worker_id=worker_id or None,
    )
    killed = 0
    seen_pgids: set[tuple[str, int]] = set()
    for info in _iter_agent_processes():
        if not _matches_target(info, target):
            continue
        key = ("pg", info.pgid) if info.pgid is not None else ("pid", info.pid)
        if key in seen_pgids:
            continue
        seen_pgids.add(key)
        if _kill_process_group(logger, label=label, info=info, reason="task_targeted_cleanup"):
            killed += 1
    return killed


def cleanup_orphan_pi_processes(
    logger: Callable[[str], None],
    *,
    label: str,
    owner_id: str | None = None,
    runtime_snapshots: Iterable[AgentRuntimeSnapshot | dict[str, object]] | None = None,
    state_tracker: dict[str, int] | None = None,
) -> int:
    killed = 0
    seen_pgids: set[tuple[str, int]] = set()
    snapshots = _normalize_runtime_snapshots(runtime_snapshots)
    tracker = state_tracker if state_tracker is not None else {}
    now_ts = time.time()
    min_age_seconds = _orphan_min_age_seconds()
    recent_activity_seconds = _orphan_recent_activity_seconds()
    confirm_rounds = _orphan_confirm_rounds()
    for info in _iter_agent_processes():
        key = ("pg", info.pgid) if info.pgid is not None else ("pid", info.pid)
        if key in seen_pgids:
            continue
        seen_pgids.add(key)
        decision = _build_orphan_sweep_decision(
            info,
            now_ts=now_ts,
            runtime_snapshots=snapshots,
            owner_id=owner_id,
            recent_activity_seconds=recent_activity_seconds,
        )
        tracker_key = f"{info.pgid or info.pid}:{_env_task_id(info)}:{_env_execution_epoch(info) or ''}"
        if not decision.should_kill:
            tracker.pop(tracker_key, None)
            if decision.reason not in {"ppid_not_1", "not_dfa_agent"}:
                logger(
                    f"preserving agent process [{label}] pid={info.pid} pgid={info.pgid if info.pgid is not None else 'unknown'} "
                    f"task_id={_env_task_id(info) or '-'} session_kind={decision.session_kind or '-'} "
                    f"decision={decision.reason} active_context_matched={decision.active_context_matched} "
                    f"recent_activity_seconds={decision.recent_activity_seconds if decision.recent_activity_seconds is not None else 'unknown'} "
                    f"db_status={decision.db_status or '-'} db_owner_id={decision.db_owner_id or '-'} "
                    f"db_execution_epoch={decision.db_execution_epoch if decision.db_execution_epoch is not None else 'unknown'}"
                )
            continue
        process_started_at = _task_process_started_at(info)
        if process_started_at is not None and (now_ts - process_started_at) < min_age_seconds:
            tracker.pop(tracker_key, None)
            logger(
                f"preserving agent process [{label}] pid={info.pid} pgid={info.pgid if info.pgid is not None else 'unknown'} "
                f"task_id={_env_task_id(info) or '-'} session_kind={decision.session_kind or '-'} "
                f"decision=min_age_guard age_seconds={now_ts - process_started_at:.1f}"
            )
            continue
        observed = tracker.get(tracker_key, 0) + 1
        tracker[tracker_key] = observed
        if observed < confirm_rounds:
            logger(
                f"suspected orphan agent process [{label}] pid={info.pid} pgid={info.pgid if info.pgid is not None else 'unknown'} "
                f"task_id={_env_task_id(info) or '-'} session_kind={decision.session_kind or '-'} "
                f"decision={decision.reason} confirm_round={observed}/{confirm_rounds}"
            )
            continue
        tracker.pop(tracker_key, None)
        if _kill_process_group(logger, label=label, info=info, reason=f"orphan_cleanup:{decision.reason}"):
            killed += 1
    return killed


@dataclass
class AgentProcessHandle:
    proc: asyncio.subprocess.Process
    label: str
    logger: Callable[[str], None]
    pgid: int | None

    @classmethod
    async def spawn(
        cls,
        *args: str,
        cwd: str,
        env: dict[str, str] | None,
        stdout,
        stderr,
        stdin,
        logger: Callable[[str], None],
        label: str,
    ) -> "AgentProcessHandle":
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            start_new_session=True,
        )
        return cls(proc=proc, label=label, logger=logger, pgid=process_group_id(proc))

    async def terminate_tree(
        self,
        *,
        reason: str,
        term_timeout: float = 5.0,
        kill_timeout: float = 5.0,
        force_if_group_still_exists: bool = True,
    ) -> None:
        if self.proc.returncode is not None:
            if force_if_group_still_exists and process_group_exists(self.pgid):
                self.logger(
                    f"cleaning leaked pi process group [{self.label}] "
                    f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
                )
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.pgid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.proc.wait(), timeout=1.0)
            return

        if self.pgid is not None:
            self.logger(
                f"terminating pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGTERM)
        else:
            self.logger(
                f"terminating pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.terminate()

        try:
            await asyncio.wait_for(self.proc.wait(), timeout=term_timeout)
        except asyncio.TimeoutError:
            pass
        except ProcessLookupError:
            return
        else:
            if not force_if_group_still_exists or not process_group_exists(self.pgid):
                return

        if self.pgid is not None:
            self.logger(
                f"force killing pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGKILL)
        else:
            self.logger(
                f"force killing pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.kill()

        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.proc.wait(), timeout=kill_timeout)
