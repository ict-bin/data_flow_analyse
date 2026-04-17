"""
data_flow_analyse — Agent 子进程执行器

两种执行模式：
  1. Worker（保持上下文）：使用 --session <file> 保持会话历史
     - 第一轮: pi --mode json -p --session ./sessions/worker-0.jsonl "任务"
     - 第二轮: pi --mode json -p --session ./sessions/worker-0.jsonl "改进指令"
     → 第二轮能看到第一轮的完整对话历史

  2. Judge（重置上下文）：使用 --no-session 每轮全新
     - 每轮: pi --mode json -p --no-session "评审内容"
     → 每次都是干净的上下文，独立评审

设计依据（来自 pi 源码分析）：
  - pi --session <path> 加载 JSONL 会话文件，恢复消息历史到 agent state
  - pi -p 是 print 模式，执行完退出但 session 文件已保存
  - 下次用相同 --session 指向同一文件时，历史消息自动恢复

重试机制（双层）：
  - pi 进程级重试（pi_max_retries）：进程拉起失败、崩溃、信号杀死 → 重新拉起
  - API 级重试（max_retries）：连接超时、限流、服务器错误 → 重新调用
  - 两者独立计数、独立退避，-1 表示无限重试
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage

logger = logging.getLogger("dfa.runner")


class AgentResult:
    """单个 Agent 执行的结果。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None
        self.fatal: bool = False  # 致命错误（配置/环境问题，不可重试）


# ─── 日志工具 ─────────────────────────────────────────────────────────────────

def _log_error(msg: str):
    logger.error(msg)
    print(f"❌ [pi-runner] {msg}", file=sys.stderr, flush=True)


def _log_warn(msg: str):
    logger.warning(msg)
    print(f"⚠️  [pi-runner] {msg}", file=sys.stderr, flush=True)


def _log_info(msg: str):
    logger.info(msg)
    print(f"ℹ️  [pi-runner] {msg}", file=sys.stderr, flush=True)


# ─── 重试工具 ─────────────────────────────────────────────────────────────────

_MAX_BACKOFF = 300  # 退避上限 5 分钟


def _should_retry(failure_count: int, max_retries: int, cancel_event: asyncio.Event | None = None) -> bool:
    """判断是否应该继续重试。failure_count 从 1 开始（第一次失败后）。"""
    if cancel_event and cancel_event.is_set():
        return False
    if max_retries < 0:  # -1 = 无限重试
        return True
    return failure_count <= max_retries


def _backoff(base_delay: float, attempt: int) -> float:
    """指数退避，带上限。"""
    return min(base_delay * (2 ** min(attempt - 1, 6)), _MAX_BACKOFF)


def _fmt_max(max_retries: int) -> str:
    return "∞" if max_retries < 0 else str(max_retries)


def _cmd_preview(args: list[str]) -> str:
    """命令预览（截断过长的 prompt 参数）。"""
    parts = []
    for a in args:
        if len(a) > 100:
            parts.append(a[:80] + "…")
        else:
            parts.append(a)
    return " ".join(parts)


# ─── pi 可执行文件定位 ────────────────────────────────────────────────────────

def _find_pi_command() -> list[str]:
    """找到 pi 可执行文件。"""
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


# ─── 错误分类 ─────────────────────────────────────────────────────────────────

_RETRYABLE_API_PATTERNS = [
    "connection", "timeout", "timed out", "ECONNREFUSED", "ECONNRESET",
    "ETIMEDOUT", "ENOTFOUND", "socket hang up", "fetch failed",
    "rate limit", "429", "503", "502", "500",
    "overloaded", "capacity", "temporarily unavailable",
    "server error", "internal error", "bad gateway",
    "service unavailable", "request failed",
]


def _is_retryable_api_error(result: AgentResult) -> bool:
    """判断是否为可重试的 API 错误（连接/限流/服务器错误）。"""
    if result.exit_code == 0 and not result.error:
        return False  # 成功，不需重试

    error_text = (result.error or "").lower()

    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True

    return False


# ─── 致命错误（不可重试，来自 system_analyse）─────────────────────────────────

_FATAL_PATTERNS: list[tuple[str, ...]] = [
    ("model", "not found"),             # Model "xxx" not found
    ("not found", "use --list"),         # ... Use --list-models to see available models
    ("invalid", "model"),               # Invalid model
    ("invalid", "api key"),             # Invalid API key
    ("invalid", "api_key"),
    ("unauthorized",),                  # 401
    ("authentication", "failed"),
    ("403", "forbidden"),               # 403
    ("does not exist",),              # 404 The model `xxx` does not exist
    ("cannot find module",),            # Node.js 模块缺失
    ("syntax error",),                  # Node.js 语法错误
    ("syntaxerror",),
]


def _is_fatal_error(result: AgentResult) -> bool:
    """
    判断是否为致命错误（配置/环境问题，绝不应重试）。

    典型场景：model not found、API key 无效、Node.js 模块缺失。
    这些错误重试多少次结果都一样，应立即终止并报告。

    注意：pi 对不存在的模型会 exit_code=0 但通过 stopReason=error
    在 JSON 消息中报错，因此不能仅依赖 exit_code。
    """
    if not result.error:
        return False

    error_lower = (result.error or "").lower()

    for pattern in _FATAL_PATTERNS:
        if all(p in error_lower for p in pattern):
            return True

    return False


def _check_stderr_for_errors(stderr_text: str, result: AgentResult) -> None:
    """
    主动扫描 stderr，检测 pi CLI 自身的致命错误。

    pi 的 CLI 错误格式通常为 "Error: ..."，stderr 中出现这类信息
    时应覆盖 result.error 以便后续正确分类。
    """
    text_lower = stderr_text.lower()
    if "error:" not in text_lower:
        return

    for pattern in _FATAL_PATTERNS:
        if all(p in text_lower for p in pattern):
            # 致命错误：用 stderr 内容覆盖 error 字段
            result.error = stderr_text.strip()
            return


def _is_pi_failure(result: AgentResult) -> bool:
    """
    判断是否为 pi 进程级失败（崩溃、信号杀死、拉起失败）。

    与 API 错误互斥：如果匹配 API 错误模式，交给 API 重试处理。
    如果已有有意义的输出（messages + output），认为 pi 完成了工作，不算进程失败。
    """
    if result.exit_code == 0:
        return False

    # 如果匹配 API 错误模式，交给 API 重试处理
    if _is_retryable_api_error(result):
        return False

    # 致命错误交给 _is_fatal_error 处理，不在这里重试
    if _is_fatal_error(result):
        return False

    # 如果已有有意义的输出，pi 完成了工作（可能退出不干净但结果可用）
    if result.messages and result.output:
        return False

    # 以下都是 pi 进程级失败：

    # 被信号杀死（Linux: 负值或 128+signal）
    if result.exit_code < 0 or result.exit_code >= 128:
        return True

    # 非零退出且无有效输出
    if not result.messages and not result.output:
        return True

    # 错误信息包含进程级关键词
    error_lower = (result.error or "").lower()
    pi_failure_patterns = [
        # 系统/进程级
        "segfault", "segmentation fault", "killed", "oom", "out of memory",
        "cannot allocate", "spawn", "enoent", "eacces", "eperm",
        "no such file", "permission denied", "not found",
        "abnormal", "core dump", "bus error", "illegal instruction",
        # Node.js 运行时（来自 entry_analyse）
        "referenceerror", "typeerror", "rangeerror",
        "heap out of memory", "allocation failed",
        "fatal error", "javascript heap",
        "execvp",
    ]
    for p in pi_failure_patterns:
        if p in error_lower:
            return True

    # 其他非零退出（兜底）
    return True


# ─── 核心执行器 ───────────────────────────────────────────────────────────────

async def run_agent(
    prompt: str,
    *,
    model: str,
    tools: list[str],
    system_prompt: str = "",
    cwd: str = ".",
    thinking_level: str = "off",
    session_file: str | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    max_retries: int = 3,               # API 错误最大重试次数（-1=无限）
    retry_delay: float = 10.0,          # API 重试首次等待（秒），指数退避
    pi_max_retries: int = 3,            # pi 进程拉起失败最大重试次数（-1=无限）
    pi_retry_delay: float = 10.0,       # pi 进程重试首次等待（秒），指数退避
) -> AgentResult:
    """
    运行单个 pi Agent 子进程。

    参数：
      session_file:    None → --no-session（Judge 模式，每次全新）
                       指定路径 → --session <path>（Worker 模式，累积上下文）
      max_retries:     API 级错误（连接/限流/500）最大重试次数，-1=无限
      retry_delay:     API 重试首次等待秒数
      pi_max_retries:  pi 进程级失败（崩溃/拉起失败）最大重试次数，-1=无限
      pi_retry_delay:  pi 进程重试首次等待秒数
    """
    result = AgentResult()

    try:
        pi_cmd = _find_pi_command()
    except FileNotFoundError as e:
        _log_error(f"pi 可执行文件未找到: {e}")
        result.error = str(e)
        result.exit_code = -1
        return result

    # ── 构造命令行参数 ────────────────────────────────────────
    args = [*pi_cmd, "--mode", "json", "-p"]

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

    # ── System Prompt → 临时文件 ──────────────────────────────
    tmp_dir: str | None = None
    tmp_file: str | None = None

    if system_prompt.strip():
        tmp_dir = tempfile.mkdtemp(prefix="dfa-")
        tmp_file = os.path.join(tmp_dir, "system.md")
        Path(tmp_file).write_text(system_prompt, encoding="utf-8")
        args.extend(["--append-system-prompt", tmp_file])

    args.append(prompt)

    try:
        pi_failures = 0
        api_failures = 0

        while True:
            # ── 检查取消 ──────────────────────────────────────
            if cancel_event and cancel_event.is_set():
                break

            result = AgentResult()

            # ── 尝试拉起 pi 子进程 ───────────────────────────
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    cwd=os.path.abspath(cwd),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    stdin=asyncio.subprocess.DEVNULL,
                )
            except (OSError, FileNotFoundError, PermissionError) as e:
                # pi 进程拉起失败（二进制不存在、权限不足、资源不足等）
                pi_failures += 1
                _log_error(
                    f"pi 进程拉起失败 ({pi_failures}/{_fmt_max(pi_max_retries)}): {e}\n"
                    f"  命令: {_cmd_preview(args)}"
                )
                result.error = f"pi launch failed: {e}"
                result.exit_code = -1

                if _should_retry(pi_failures, pi_max_retries, cancel_event):
                    delay = _backoff(pi_retry_delay, pi_failures)
                    _log_warn(f"将在 {delay:.0f}s 后重试拉起 pi...")
                    if on_stream:
                        on_stream(f"\n❌ pi 拉起失败，{delay:.0f}s 后重试 ({pi_failures}/{_fmt_max(pi_max_retries)})...\n")
                    await asyncio.sleep(delay)
                    continue
                else:
                    _log_error(f"pi 拉起重试已耗尽 ({pi_failures} 次失败)")
                    break

            # ── 取消监控 ──────────────────────────────────────
            async def _cancel_monitor():
                if cancel_event:
                    await cancel_event.wait()
                    try:
                        proc.terminate()
                    except ProcessLookupError:
                        pass

            cancel_task = asyncio.create_task(_cancel_monitor()) if cancel_event else None

            # ── 读取 JSON Lines 输出 ─────────────────────────
            try:
                assert proc.stdout is not None
                buffer = b""
                while True:
                    chunk = await proc.stdout.read(4096)
                    if not chunk:
                        break
                    buffer += chunk

                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        _process_line(line.decode("utf-8", errors="replace"), result, on_stream)

                if buffer.strip():
                    _process_line(buffer.decode("utf-8", errors="replace"), result, on_stream)

                # 读取 stderr
                assert proc.stderr is not None
                stderr_data = await proc.stderr.read()
                if stderr_data:
                    stderr_text = stderr_data.decode("utf-8", errors="replace").strip()
                    if stderr_text:
                        # 主动扫描 stderr 中的致命错误（来自 system_analyse）
                        _check_stderr_for_errors(stderr_text, result)
                        if not result.error:
                            result.error = stderr_text

                await proc.wait()
                result.exit_code = proc.returncode or 0

            except Exception as e:
                # 读取过程中异常（进程被杀、管道断裂等）
                pi_failures += 1
                _log_error(
                    f"pi 进程读取异常 ({pi_failures}/{_fmt_max(pi_max_retries)}): {e}\n"
                    f"  命令: {_cmd_preview(args)}"
                )
                result.error = f"pi process read error: {e}"
                result.exit_code = -1

                # 确保子进程被清理
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

            # ── 提取最后一条 assistant 消息作为输出 ──────────
            for msg in reversed(result.messages):
                if msg.get("role") == "assistant":
                    texts = [
                        c["text"]
                        for c in (msg.get("content") or [])
                        if c.get("type") == "text"
                    ]
                    result.output = "\n".join(texts)
                    break

            # ── 检查取消 ──────────────────────────────────────
            if cancel_event and cancel_event.is_set():
                break

            # ── 错误分类与重试 ────────────────────────────────

            # 0) 致命错误（配置/环境问题，绝不重试）
            if _is_fatal_error(result):
                result.fatal = True
                _log_error(
                    f"致命错误（不可重试）: {(result.error or 'unknown')[:300]}\n"
                    f"  请检查模型名称、API Key、pi 安装等配置"
                )
                break

            # 1) pi 进程级失败（崩溃、信号杀死、无输出）
            if _is_pi_failure(result):
                pi_failures += 1
                _log_error(
                    f"pi 进程失败 ({pi_failures}/{_fmt_max(pi_max_retries)}), "
                    f"exit_code={result.exit_code}: "
                    f"{(result.error or 'unknown error')[:300]}\n"
                    f"  命令: {_cmd_preview(args)}"
                )

                if _should_retry(pi_failures, pi_max_retries, cancel_event):
                    delay = _backoff(pi_retry_delay, pi_failures)
                    _log_warn(f"将在 {delay:.0f}s 后重试 pi 进程...")
                    if on_stream:
                        on_stream(f"\n❌ pi 进程失败 (exit={result.exit_code})，{delay:.0f}s 后重试 ({pi_failures}/{_fmt_max(pi_max_retries)})...\n")
                    await asyncio.sleep(delay)
                    continue
                else:
                    _log_error(f"pi 进程重试已耗尽 ({pi_failures} 次失败)")
                    result.error = (result.error or "") + f" [pi 重试已耗尽: {pi_failures} 次失败]"
                    break

            # 2) API 级可重试错误（连接/限流/服务器错误）
            if _is_retryable_api_error(result):
                api_failures += 1
                err_preview = (result.error or "")[:200]
                _log_warn(
                    f"API 错误 ({api_failures}/{_fmt_max(max_retries)}): {err_preview}"
                )

                if _should_retry(api_failures, max_retries, cancel_event):
                    delay = _backoff(retry_delay, api_failures)
                    _log_warn(f"将在 {delay:.0f}s 后重试 API 调用...")
                    if on_stream:
                        on_stream(f"\n⚠️ API 错误，{delay:.0f}s 后重试 ({api_failures}/{_fmt_max(max_retries)})...\n")
                    await asyncio.sleep(delay)
                    continue
                else:
                    _log_error(f"API 重试已耗尽 ({api_failures} 次失败)")
                    result.error = (result.error or "") + f" [API 重试已耗尽: {api_failures} 次失败]"
                    break

            # 3) 成功或不可重试的错误
            if result.exit_code != 0 and result.error:
                # 非零退出但有输出（可能是 pi 的非致命警告），记录但不重试
                _log_warn(
                    f"pi 退出码 {result.exit_code} (有输出，不重试): "
                    f"{result.error[:200]}"
                )
            break

        return result

    finally:
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.unlink(tmp_file)
            except OSError:
                pass
        if tmp_dir and os.path.exists(tmp_dir):
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass


def _process_line(
    line: str,
    result: AgentResult,
    on_stream: Callable[[str], None] | None,
) -> None:
    """解析 pi --mode json 输出的单行 JSON 事件。"""
    line = line.strip()
    if not line:
        return
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return

    etype = event.get("type")

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


async def run_agents_parallel(
    tasks: list[dict],
    concurrency: int = 4,
) -> list[AgentResult]:
    """并行运行多个 Agent，限制并发。"""
    semaphore = asyncio.Semaphore(concurrency)
    results: list[AgentResult | None] = [None] * len(tasks)

    async def _run(index: int, kwargs: dict):
        async with semaphore:
            results[index] = await run_agent(**kwargs)

    await asyncio.gather(*[_run(i, t) for i, t in enumerate(tasks)])
    return results  # type: ignore[return-value]
