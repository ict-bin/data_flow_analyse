"""
data_flow_analyse — Agent 子进程执行器（RPC 模式）

两种执行模式：
  1. Worker（保持上下文）：使用 --session <file> 保持会话历史
  2. Judge（重置上下文）：使用 --no-session 每轮全新

重试机制（双层）：
  外层 — pi 进程级重试（pi_max_retries）：
    进程拉起失败、崩溃、信号杀死 → 重新拉起
    致命错误（Model not found, Unauthorized）→ 不重试，立即终止
  内层 — API 级重试（max_retries）：
    连接超时、限流、服务器错误 → 重新调用
  两层独立计数、独立退避，-1 表示无限重试
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage

logger = logging.getLogger("dfa.runner")

_MAX_BACKOFF = 300  # 退避上限 5 分钟
_DEFAULT_CONTEXT_WINDOW = 128_000
_SINGLE_INPUT_CONTEXT_RATIO = 0.75
_PROMPT_TOKEN_OVERHEAD = 128
_COMPACTION_TRIGGER_PROMPT = (
    "请立即触发一次当前会话的自动压缩（compaction），"
    "仅保留后续继续执行任务所需的关键结论、约束和待办。"
    "不要继续业务分析，只回复 COMPACTION_OK。"
)
_CONTEXT_WINDOW_BY_MODEL = {
    "gpt-5.4": 128_000,
    "gpt-5.4-mini": 128_000,
    "gpt-5.5": 256_000,
    "gpt-5.3-codex": 128_000,
    "gpt-5.2": 200_000,
    "minimax/minimax-m2.5": 163_804,
    "minimax-m2.5": 163_804,
    "minimax-m2.7": 128_000,
    "glm-5.1": 128_000,
    "zai-org/glm-5": 128_000,
}


# ─── 结果类 ───────────────────────────────────────────────────────────────────


class AgentResult:
    """单个 Agent 执行的结果。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None
        self.fatal: bool = False  # 致命错误（配置/环境问题，不可重试）


# ─── 内部异常 ─────────────────────────────────────────────────────────────────


class _PiProcessError(Exception):
    """pi 进程级错误（非 API 错误），由内层向外层传递。"""

    pass


class PiFatalError(Exception):
    """pi 致命错误（不可重试），调用者应终止流水线。"""

    pass


# ─── 日志工具 ─────────────────────────────────────────────────────────────────


def _log_error(msg: str) -> None:
    logger.error(msg)


def _log_warn(msg: str) -> None:
    logger.warning(msg)


def _log_info(msg: str) -> None:
    logger.info(msg)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────


def _backoff(base_delay: float, attempt: int) -> float:
    """指数退避，带上限。attempt 从 1 开始。"""
    return min(base_delay * (2 ** min(attempt - 1, 6)), _MAX_BACKOFF)


def _fmt_max(n: int) -> str:
    return "∞" if n < 0 else str(n)


def _normalize_timeout_seconds(timeout_seconds: float | int | None) -> float | None:
    if timeout_seconds is None:
        return None
    try:
        value = float(timeout_seconds)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _should_retry(
    failures: int, max_retries: int, cancel: asyncio.Event | None
) -> bool:
    if cancel and cancel.is_set():
        return False
    if max_retries < 0:
        return True
    return failures <= max_retries


def _cmd_preview(args: list[str]) -> str:
    """命令预览（截断过长参数）。"""
    parts = []
    for a in args:
        parts.append(a[:80] + "…" if len(a) > 100 else a)
    return " ".join(parts)


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ascii_chars = sum(1 for ch in text if ord(ch) < 128)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, (ascii_chars + 3) // 4 + non_ascii_chars)


def _model_context_window(model: str) -> int:
    normalized = str(model or "").strip().lower()
    for key, value in _CONTEXT_WINDOW_BY_MODEL.items():
        if key in normalized:
            return value
    return _DEFAULT_CONTEXT_WINDOW


def _single_input_token_estimate(system_prompt: str, prompt: str, post_skill_prompt: str | None = None) -> int:
    return (
        _estimate_tokens(system_prompt)
        + _estimate_tokens(prompt)
        + _estimate_tokens(post_skill_prompt or "")
        + _PROMPT_TOKEN_OVERHEAD
    )


def _single_input_token_limit(context_window: int) -> int:
    return max(1, int(context_window * _SINGLE_INPUT_CONTEXT_RATIO))


def _parse_context_overflow_details(error_text: str | None) -> dict[str, int]:
    text = str(error_text or "")
    lowered = text.lower()
    details = {
        "input_tokens": 0,
        "requested_output_tokens": 0,
        "context_length": 0,
        "max_input_tokens": 0,
    }
    if "context length" not in lowered and "input tokens" not in lowered:
        return details
    patterns = {
        "input_tokens": r"passed\s+(\d+)\s+input tokens",
        "requested_output_tokens": r"requested\s+(\d+)\s+output tokens",
        "context_length": r"context length is only\s+(\d+)\s+tokens",
        "max_input_tokens": r"maximum input length(?: of)?\s+(\d+)\s+tokens",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details[key] = int(match.group(1))
    return details


def _is_context_overflow_error(error_text: str | None) -> bool:
    details = _parse_context_overflow_details(error_text)
    if details["context_length"] > 0:
        return True
    lowered = str(error_text or "").lower()
    return (
        "context length" in lowered
        and "input tokens" in lowered
        and ("badrequesterror" in lowered or "400" in lowered)
    )


def _format_context_overflow_failure(
    original_error: str | None,
    *,
    context_window: int,
    single_input_tokens: int,
    single_input_limit: int,
    compaction_attempted: bool,
) -> str:
    action = "已先触发一次会话自动压缩并重试" if compaction_attempted else "未能触发会话自动压缩"
    return (
        f"{action}，但当前单次输入估算约 {single_input_tokens} tokens，"
        f"超过上下文窗口 75% 阈值 {single_input_limit}/{context_window}，"
        f"本次请求不再继续重试。原始错误: {original_error or 'unknown'}"
    )


def _find_pi_command() -> list[str]:
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


def _build_args(
    pi_cmd: list[str],
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
) -> list[str]:
    """构造 pi RPC 模式启动参数（不含 system_prompt 和 prompt）。

    使用 --mode rpc：pi 保持运行，prompt 通过 stdin JSONL 发送，
    彻底绕过 Linux ARG_MAX 限制，支持任意大小的 prompt/system_prompt。
    """
    args = [*pi_cmd, "--mode", "rpc"]
    if session_file:
        args.extend(["--session", session_file])
    else:
        args.append("--no-session")
    if model:
        args.extend(["--model", model])
    if tools:
        args.extend(["--tools", ",".join(tools)])
    if thinking_level and thinking_level != "off":
        args.extend(["--thinking", thinking_level])
    return args


def _write_temp_markdown(
    tmp_dir: str | None,
    prefix: str,
    filename: str,
    content: str,
) -> tuple[str, str]:
    """将 prompt 写入临时 markdown 文件，返回 (tmp_dir, file_path)。"""
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix=prefix)
    file_path = os.path.join(tmp_dir, filename)
    Path(file_path).write_text(content, encoding="utf-8")
    return tmp_dir, file_path


# ─── 错误分类 ─────────────────────────────────────────────────────────────────

# 致命错误：配置/环境问题，重试无意义
_FATAL_PATTERNS = [
    ("model", "not found"),
    ("not found", "use --list"),
    ("invalid", "model"),
    ("invalid", "api key"),
    ("invalid", "api_key"),
    ("unauthorized",),
    ("authentication", "failed"),
    ("401",),
]

# API 可重试错误
_RETRYABLE_API_PATTERNS = [
    "connection",
    "timeout",
    "timed out",
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "ENOTFOUND",
    "socket hang up",
    "fetch failed",
    "rate limit",
    "429",
    "503",
    "502",
    "500",
    "overloaded",
    "capacity",
    "temporarily unavailable",
    "server error",
    "internal error",
    "bad gateway",
    "service unavailable",
    "request failed",
    "network_error",        # gptplus5 并发超限
    "finish_reason",        # provider finish_reason: network_error
    "too many requests",    # 429 另一种表达
    "ENOBUFS",              # pipe buffer 满（大响应导致）
    "EPIPE",                # 管道断裂
]

# 速率限制模式：这些关键词匹配时延长待机时间
_RATE_LIMIT_PATTERNS = ["rate limit", "429", "too many requests", "network_error", "finish_reason"]
_RATE_LIMIT_EXTRA_DELAY = 60   # 限流时额外等彥60s

# pi 进程崩溃关键词
_PI_CRASH_PATTERNS = [
    "cannot find module",
    "module not found",
    "syntaxerror",
    "referenceerror",
    "typeerror",
    "segmentation fault",
    "segfault",
    "killed",
    "signal",
    "enoent",
    "eacces",
    "eperm",
    "heap out of memory",
    "allocation failed",
    "oom",
    "out of memory",
    "spawn",
    "execvp",
    "core dump",
    "bus error",
    "permission denied",
    "no such file",
]


def _is_fatal_error(result: AgentResult) -> bool:
    """致命错误：配置/环境问题，不可重试。"""
    error_text = (result.error or "").lower()
    for pattern in _FATAL_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_retryable_api_error(result: AgentResult) -> bool:
    """API 级可重试错误。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True
    return False


def _is_pi_crash(result: AgentResult) -> bool:
    """pi 进程级崩溃（非 API 错误）。"""
    if result.exit_code == 0:
        return False
    # 有正常消息输出 → pi 本身正常运行
    if result.messages:
        return False
    # API 错误交给内层处理
    if _is_retryable_api_error(result):
        return False
    # 无消息 + 非零退出 = 进程崩溃
    return True


async def _run_with_context_overflow_recovery(
    *,
    pi_cmd: list[str],
    args: list[str],
    prompt: str,
    system_prompt: str,
    post_skill_prompt: str | None,
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
    cwd: str,
    env: dict[str, str] | None,
    on_stream: Callable[[str], None] | None,
    cancel_event: asyncio.Event | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
) -> AgentResult:
    result = await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        env=env,
        prompt=prompt,
        post_skill_prompt=post_skill_prompt,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
    )
    if not _is_context_overflow_error(result.error):
        return result

    overflow = _parse_context_overflow_details(result.error)
    context_window = overflow["context_length"] or _model_context_window(model)
    single_input_tokens = _single_input_token_estimate(system_prompt, prompt, post_skill_prompt)
    single_input_limit = _single_input_token_limit(context_window)
    compaction_attempted = False

    if session_file:
        compaction_attempted = True
        msg = (
            "检测到智能体单次请求触发上下文超限，先触发一次会话自动压缩，"
            "随后重试原请求。"
        )
        _log_warn(msg)
        if on_stream:
            on_stream(f"\n⚠️ {msg}\n")
        compaction_args = _build_args(pi_cmd, model, tools, thinking_level, session_file)
        await _run_with_pi_retry(
            args=compaction_args,
            cwd=cwd,
            env=env,
            prompt=_COMPACTION_TRIGGER_PROMPT,
            post_skill_prompt=None,
            cancel_event=cancel_event,
            on_stream=None,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pi_max_retries=pi_max_retries,
            pi_retry_delay=pi_retry_delay,
        )

    if single_input_tokens > single_input_limit:
        result.error = _format_context_overflow_failure(
            result.error,
            context_window=context_window,
            single_input_tokens=single_input_tokens,
            single_input_limit=single_input_limit,
            compaction_attempted=compaction_attempted,
        )
        return result

    if not session_file:
        return result

    return await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        env=env,
        prompt=prompt,
        post_skill_prompt=post_skill_prompt,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
    )


# ═════════════════════════════════════════════════════════════════════════════
# 公开接口
# ═════════════════════════════════════════════════════════════════════════════


async def run_agent(
    prompt: str,
    *,
    model: str,
    tools: list[str],
    system_prompt: str = "",
    cwd: str = ".",
    env: dict[str, str] | None = None,
    thinking_level: str = "off",
    session_file: str | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    post_skill_prompt: str | None = None,
    max_retries: int = 3,  # API 错误最大重试（-1=无限）
    retry_delay: float = 10.0,  # API 重试首次等待
    run_timeout_seconds: float | int = 3600,
    timeout_retry_enabled: bool = True,
    timeout_max_retries: int = 3,
    pi_max_retries: int = -1,  # pi 进程最大重试（-1=无限）
    pi_retry_delay: float = 10.0,  # pi 进程重试首次等待
) -> AgentResult:
    """
    运行单个 pi Agent 子进程（双层重试 + 致命错误检测）。

    外层：pi 进程级重试（拉起失败、崩溃、被 kill）
    内层：API 级重试（连接超时、限流、服务器错误）
    致命：Model not found / Unauthorized → 不重试，result.fatal=True
    """
    # DRYRUN 模式：不调模型，写入模拟文件，验证控制流
    if os.environ.get('DRYRUN') == '1':
        from .dryrun import run_agent_dryrun as _dr
        return await _dr(prompt, cwd=cwd, session_file=session_file,
                         post_skill_prompt=post_skill_prompt,
                         on_stream=on_stream)
    try:
        pi_cmd = _find_pi_command()
    except FileNotFoundError as e:
        _log_error(f"pi 可执行文件未找到: {e}")
        r = AgentResult()
        r.error = str(e)
        r.exit_code = -1
        r.fatal = True
        return r

    args = _build_args(pi_cmd, model, tools, thinking_level, session_file)

    # System/User Prompt → 临时文件，避免超长 argv 导致 Argument list too long
    tmp_dir: str | None = None
    sys_tmp_file: str | None = None
    prompt_tmp_file: str | None = None
    if system_prompt.strip():
        _sp_path = os.path.join(os.path.abspath(cwd), ".system_prompt.md")
        try:
            Path(_sp_path).write_text(system_prompt, encoding="utf-8")
            sys_tmp_file = _sp_path
        except OSError:
            tmp_dir, sys_tmp_file = _write_temp_markdown(
                tmp_dir, "dfa-", "system.md", system_prompt
            )
        args.extend(["--system-prompt", sys_tmp_file])

    timeout_seconds = _normalize_timeout_seconds(
        run_timeout_seconds if run_timeout_seconds is not None else os.environ.get("DFA_AGENT_TIMEOUT_SECONDS", "3600")
    )
    timeout_failures = 0
    try:
        while True:
            try:
                coro = _run_with_context_overflow_recovery(
                    pi_cmd=pi_cmd,
                    args=args,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    post_skill_prompt=post_skill_prompt,
                    model=model,
                    tools=tools,
                    thinking_level=thinking_level,
                    session_file=session_file,
                    cwd=os.path.abspath(cwd),
                    env=env,
                    cancel_event=cancel_event,
                    on_stream=on_stream,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    pi_max_retries=pi_max_retries,
                    pi_retry_delay=pi_retry_delay,
                )
                return await asyncio.wait_for(coro, timeout=timeout_seconds) if timeout_seconds else await coro
            except asyncio.TimeoutError:
                timeout_failures += 1
                r = AgentResult()
                r.error = (
                    f"agent step timed out after {timeout_seconds:.0f}s"
                    if timeout_seconds else
                    "agent step timed out"
                )
                r.exit_code = -1
                can_retry = timeout_retry_enabled and (
                    timeout_max_retries < 0 or timeout_failures <= timeout_max_retries
                )
                if not can_retry or (cancel_event and cancel_event.is_set()):
                    return r
                delay = _backoff(retry_delay, timeout_failures)
                if on_stream:
                    on_stream(
                        f"\n⏱️ 智能体执行超时，{delay:.0f}s 后重试 "
                        f"({timeout_failures}/{_fmt_max(timeout_max_retries)})...\n"
                    )
                await asyncio.sleep(delay)
    finally:
        pass  # .system_prompt.md is in workspace cwd, cleaned with it


# ─── 外层：pi 进程级重试 ─────────────────────────────────────────────────────


async def _run_with_pi_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None = None,
    prompt: str,
    post_skill_prompt: str | None = None,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
) -> AgentResult:
    """外层循环：处理 pi 进程拉起失败、崩溃、致命错误。"""
    # cwd 不存在是致命错误（目录被删除等），不进入重试
    if not os.path.isdir(cwd):
        _log_error(f"cwd 目录不存在（不可重试）: {cwd}")
        r = AgentResult()
        r.error = f"cwd directory does not exist: {cwd}"
        r.exit_code = -1
        r.fatal = True
        return r

    pi_attempt = 0

    while True:
        if cancel_event and cancel_event.is_set():
            r = AgentResult()
            r.error = "cancelled"
            return r

        try:
            result = await _run_with_api_retry(
                args=args,
                cwd=cwd,
                env=env,
                prompt=prompt,
                post_skill_prompt=post_skill_prompt,
                cancel_event=cancel_event,
                on_stream=on_stream,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

            # ── 致命错误检测（在 pi 进程重试前拦截）──
            if _is_fatal_error(result):
                result.fatal = True
                _log_error(f"pi 致命错误（不可重试）: {result.error}")
                return result

            # ── pi 进程崩溃 → 交由外层重试 ──
            if _is_pi_crash(result):
                raise _PiProcessError(
                    f"exit_code={result.exit_code}: "
                    f"{result.error or '(no error message)'}"
                )

            return result

        except (OSError, FileNotFoundError, PermissionError, _PiProcessError) as exc:
            pi_attempt += 1
            label = f"{pi_attempt}/{_fmt_max(pi_max_retries)}"

            if cancel_event and cancel_event.is_set():
                _log_error(f"pi 进程失败 (cancelled): {exc}")
                r = AgentResult()
                r.error = f"cancelled after pi error: {exc}"
                return r

            # ── 检查 stderr 中是否藏着致命错误 ──
            err_lower = str(exc).lower()
            for pattern in _FATAL_PATTERNS:
                if all(p in err_lower for p in pattern):
                    _log_error(f"pi 致命错误（不可重试）[{label}]: {exc}")
                    r = AgentResult()
                    r.error = str(exc)
                    r.exit_code = -1
                    r.fatal = True
                    return r

            if _should_retry(pi_attempt, pi_max_retries, cancel_event):
                delay = _backoff(pi_retry_delay, pi_attempt)
                _log_warn(
                    f"pi 进程失败 [{label}], {delay:.0f}s 后重试: {exc}\n"
                    f"    命令: {_cmd_preview(args)}"
                )
                if on_stream:
                    on_stream(
                        f"\n❌ pi 进程失败 (exit={getattr(exc, 'exit_code', '?')})，"
                        f"{delay:.0f}s 后重试 ({label})...\n"
                    )
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(f"pi 进程重试耗尽 [{label}]: {exc}")
                r = AgentResult()
                r.exit_code = -1
                r.error = f"pi process failed after {pi_attempt} retries: {exc}"
                return r


# ─── 内层：API 级重试 ────────────────────────────────────────────────────────


async def _run_with_api_retry(
    *,
    args: list[str],
    cwd: str,
    env: dict[str, str] | None = None,
    prompt: str,
    post_skill_prompt: str | None = None,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
) -> AgentResult:
    """内层循环：启动 pi 子进程，处理 API 级错误重试。"""
    api_attempt = 0

    while True:
        result = AgentResult()

        # ── 拉起子进程（OSError 由外层 catch）──
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,  # RPC: 通过 stdin 发送 prompt
        )

        cancel_task = None
        if cancel_event:

            async def _cancel_monitor():
                await cancel_event.wait()
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass

            cancel_task = asyncio.create_task(_cancel_monitor())

        # ── RPC: 发送 prompt，读取事件直到 agent_end ──
        agent_ended = False
        try:
            assert proc.stdin is not None
            assert proc.stdout is not None

            # 发送初始 prompt（无 ARG_MAX 限制）
            prompt_cmd = json.dumps(
                {"type": "prompt", "message": prompt},
                ensure_ascii=False,
            ) + chr(10)
            proc.stdin.write(prompt_cmd.encode("utf-8"))
            await proc.stdin.drain()

            buffer = b""
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    ended = _process_line(
                        line.decode("utf-8", errors="replace"), result, on_stream
                    )
                    if ended:
                        agent_ended = True
                        break
                if agent_ended:
                    break
            if buffer.strip():
                _process_line(
                    buffer.decode("utf-8", errors="replace"), result, on_stream
                )

            # agent_ended 后必须继续 drain stdout 直到 EOF，
            # 否则 pi 继续写导致 pipe buffer 满 → ENOBUFS
            # RPC 第二轮：分析完成后强制调用 skill 输出结果
            if agent_ended and post_skill_prompt and proc.stdin and not proc.stdin.is_closing():
                try:
                    _skill_cmd = json.dumps(
                        {"type": "prompt", "message": post_skill_prompt},
                        ensure_ascii=False,
                    ) + chr(10)
                    proc.stdin.write(_skill_cmd.encode("utf-8"))
                    await proc.stdin.drain()
                    _buf2 = b""
                    while True:
                        try:
                            _chunk2 = await asyncio.wait_for(
                                proc.stdout.read(4096), timeout=180.0)
                        except asyncio.TimeoutError:
                            break
                        if not _chunk2:
                            break
                        _buf2 += _chunk2
                        while b"\n" in _buf2:
                            _l2, _buf2 = _buf2.split(b"\n", 1)
                            if _process_line(_l2.decode("utf-8", errors="replace"),
                                             result, on_stream):
                                break
                        else:
                            continue
                        break
                except Exception as _se:
                    _log_warn(f"post_skill RPC second turn error (ignored): {_se}")
            if agent_ended:
                try:
                    async def _drain_stdout():
                        assert proc.stdout is not None
                        while True:
                            chunk = await proc.stdout.read(65536)
                            if not chunk:
                                break
                    await asyncio.wait_for(_drain_stdout(), timeout=10.0)
                except (asyncio.TimeoutError, Exception):
                    pass

            # 关闭 stdin → pi 检测 EOF 后退出
            try:
                proc.stdin.close()
            except Exception:
                pass

            assert proc.stderr is not None
            try:
                stderr_data = await asyncio.wait_for(proc.stderr.read(), timeout=10.0)
                stderr_text = stderr_data.decode("utf-8", errors="replace").strip()
                if stderr_text and not result.error:
                    result.error = stderr_text
            except asyncio.TimeoutError:
                pass

            try:
                await asyncio.wait_for(proc.wait(), timeout=15.0)
                result.exit_code = proc.returncode or 0
            except asyncio.TimeoutError:
                _log_warn("pi 进程未在 15s 内退出，强制终止")
                proc.kill()
                await proc.wait()
                result.exit_code = -1

        except asyncio.CancelledError:
            _log_warn("agent run cancelled, terminating pi process")
            with contextlib.suppress(Exception):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                with contextlib.suppress(Exception):
                    proc.kill()
                with contextlib.suppress(Exception):
                    await proc.wait()
            raise

        except asyncio.CancelledError:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            raise
        except Exception as e:
            # 管道断裂、进程被杀等
            _log_warn(f"pi 进程读取异常: {e}")
            result.error = f"pi process read error: {e}"
            result.exit_code = -1
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        finally:
            if cancel_task:
                cancel_task.cancel()
                try:
                    await cancel_task
                except asyncio.CancelledError:
                    pass

        # ── 提取输出 ──
        for msg in reversed(result.messages):
            if msg.get("role") == "assistant":
                texts = [
                    c["text"]
                    for c in (msg.get("content") or [])
                    if c.get("type") == "text"
                ]
                result.output = "\n".join(texts)
                break

        if cancel_event and cancel_event.is_set():
            return result

        # ── pi 崩溃 → 不在内层重试，交给外层 ──
        if _is_pi_crash(result):
            if result.error:
                _log_warn(
                    f"pi 进程崩溃 (exit={result.exit_code}): {result.error[:300]}"
                )
            return result

        # ── 致命错误 → 不重试，直接返回让外层处理 ──
        if _is_fatal_error(result):
            return result

        # ── API 可重试错误 ──
        if _is_retryable_api_error(result):
            api_attempt += 1
            can_retry = (max_retries == -1) or (api_attempt <= max_retries)
            if can_retry:
                delay = _backoff(retry_delay, api_attempt)
                # 限流错误额外等待，避免连续冲击并发限制
                err_lower = (result.error or "").lower()
                is_rate_limit = any(p in err_lower for p in _RATE_LIMIT_PATTERNS)
                if is_rate_limit:
                    delay = max(delay, _RATE_LIMIT_EXTRA_DELAY)
                label = f"{api_attempt}/{_fmt_max(max_retries)}"
                kind = "限流" if is_rate_limit else "API"
                _log_warn(
                    f"{kind}错误 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(f"\n⚠️ {kind}错误，{delay:.0f}s 后重试 ({label})...\n")
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(
                    f"API 重试耗尽 [{api_attempt}/{max_retries}]: "
                    f"{(result.error or '')[:200]}"
                )
                result.error = (
                    result.error or ""
                ) + f" [API 重试耗尽: {api_attempt} 次失败]"
                return result

        # ── 成功或不可重试的未知错误 ──
        if result.exit_code != 0 and result.error:
            err_lower = (result.error or "").lower()
            # ENOBUFS/EPIPE 是可重试的管道错误，不属于“不可重试”
            if any(p in err_lower for p in ("enobufs", "epipe", "broken pipe")):
                api_attempt += 1
                can_retry = (max_retries == -1) or (api_attempt <= max_retries)
                if can_retry:
                    delay = _backoff(retry_delay, api_attempt)
                    _log_warn(
                        f"管道错误 [{api_attempt}/{_fmt_max(max_retries)}], {delay:.0f}s 后重试: "
                        f"{(result.error or '')[:200]}"
                    )
                    await asyncio.sleep(delay)
                    continue
            _log_warn(
                f"pi 退出码 {result.exit_code} (有输出，不重试): {result.error[:200]}"
            )
        return result


# ─── JSON Lines 解析 ──────────────────────────────────────────────────────────


def _process_line(
    line: str,
    result: AgentResult,
    on_stream: Callable[[str], None] | None,
) -> bool:
    """解析一行 JSONL。返回 True 表示收到 agent_end（调用方应停止读取）。"""
    line = line.strip()
    if not line:
        return False
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return False

    etype = event.get("type")

    # RPC mode: 过滤命令响应和与 agent 无关的事件
    if etype in (
        "response",
        "session",
        "queue_update",
        "compaction_start",
        "compaction_end",
        "auto_retry_start",
        "auto_retry_end",
    ):
        return False

    # agent_end 信号本轮完成
    if etype == "agent_end":
        # agent_end 含全量 messages，可备用但不重复处理
        return True

    if etype == "message_update":
        ae = event.get("assistantMessageEvent", {})
        if ae.get("type") == "text_delta" and on_stream:
            on_stream(ae.get("delta", ""))

    if etype == "message_end" and event.get("message"):
        msg = event["message"]
        result.messages.append(msg)

        if msg.get("role") == "assistant":
            usage = msg.get("usage", {})
            result.token_usage.input += usage.get("input", 0)
            result.token_usage.output += usage.get("output", 0)
            result.token_usage.cache_read += usage.get("cacheRead", 0)
            result.token_usage.cache_write += usage.get("cacheWrite", 0)
            cost = usage.get("cost", {})
            if isinstance(cost, dict):
                result.token_usage.cost += cost.get("total", 0)
            elif isinstance(cost, (int, float)):
                result.token_usage.cost += cost

            if msg.get("stopReason") == "error":
                result.error = msg.get("errorMessage", "Unknown error")

    return False


# ─── 并行执行 ────────────────────────────────────────────────────────────────


async def run_agents_parallel(
    tasks: list[dict],
    concurrency: int = 4,
) -> list[AgentResult]:
    semaphore = asyncio.Semaphore(concurrency)
    results: list[AgentResult | None] = [None] * len(tasks)

    async def _run(index: int, kwargs: dict):
        async with semaphore:
            results[index] = await run_agent(**kwargs)

    await asyncio.gather(*[_run(i, t) for i, t in enumerate(tasks)])
    return results  # type: ignore[return-value]
