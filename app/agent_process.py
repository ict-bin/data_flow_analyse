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
from typing import Callable


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
class AgentProcessInfo:
    pid: int
    ppid: int | None
    pgid: int | None
    comm: str
    exe: str
    cwd: str
    cmdline: str
    environ: dict[str, str]


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
) -> int:
    killed = 0
    seen_pgids: set[tuple[str, int]] = set()
    for info in _iter_agent_processes():
        if info.ppid != 1:
            continue
        if not _matches_target(info, None):
            continue
        key = ("pg", info.pgid) if info.pgid is not None else ("pid", info.pid)
        if key in seen_pgids:
            continue
        seen_pgids.add(key)
        if _kill_process_group(logger, label=label, info=info, reason="orphan_cleanup"):
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
