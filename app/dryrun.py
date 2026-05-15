"""
dryrun.py — Dryrun mock for runner.run_agent

Set env var DRYRUN=1 to skip actual model calls.
Writes minimal valid mock files and returns immediately.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from pathlib import Path
from typing import Callable


async def run_agent_dryrun(
    prompt: str,
    *,
    cwd: str = ".",
    session_file: str | None = None,
    post_skill_prompt: str | None = None,
    on_stream: Callable[[str], None] | None = None,
) -> "AgentResult":
    """Dryrun: skip model calls, write mock files, verify control flow."""
    from .runner import AgentResult

    await asyncio.sleep(0.05)   # simulate async yield

    cwd_path = Path(os.path.abspath(cwd))
    result = AgentResult()
    result.exit_code = 0
    nonce = uuid.uuid4().hex[:6]

    # Classify prompt type
    is_judge   = "评分" in prompt or "Round" in prompt or "评审" in prompt
    is_taint   = "深入追踪污点参数" in prompt or "阶段二" in prompt
    is_summary = "汇总所有污点" in prompt or "阶段三" in prompt
    # is_base: anything else (function read phase)

    # ── Write session stub ────────────────────────────────────────────────────
    if session_file:
        sess = Path(session_file)
        existing = sess.read_text(encoding="utf-8").strip().split("\n") \
                   if sess.exists() and sess.stat().st_size > 0 else []
        new_events = [
            json.dumps({"type": "session", "version": 3, "id": nonce,
                        "timestamp": "2026-01-01T00:00:00Z", "cwd": cwd}),
            json.dumps({"type": "message", "message": {
                "role": "user",
                "content": [{"type": "text", "text": prompt[:80]}]}}),
            json.dumps({"type": "message", "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": f"[DRYRUN-{nonce}]"}],
                "stopReason": "stop"}}),
        ]
        has_session = any("\"session\"" in l for l in existing)
        all_ev = new_events if not has_session else (existing + new_events[1:])
        sess.write_text("\n".join(all_ev) + "\n", encoding="utf-8")

    # ── Taint session: write taint-flow-{param}.md ────────────────────────────
    if is_taint and post_skill_prompt:
        m = re.search(r"污点参数 `([^`]+)`", prompt)
        param = m.group(1) if m else "param"
        safe_p = re.sub(r"[^A-Za-z0-9_]", "_", param)
        (cwd_path / f"taint-flow-{safe_p}.md").write_text(
            f"# 污点流: {param}\n\n"
            f"## 污点源\n- {param} 🔴 TAINTED\n\n"
            f"## 传播路径\n"
            f"├── [L230] op({param}) → result 🔴 TAINTED\n"
            f"└── [L240] SubFunc(result) → 📎 子函数\n\n"
            f"## 接收此污点的子函数\n"
            f"| 函数 | 调用位置 | 接收的形参 |\n"
            f"|------|---------|------------|\n"
            f"| SubFunc | L240 | subParam |\n",
            encoding="utf-8",
        )
        if on_stream:
            on_stream(f"[DRYRUN] wrote taint-flow-{safe_p}.md\n")

    # ── Summary session: write dataflow-*.md + tainted.list ──────────────────
    if is_summary and post_skill_prompt:
        m2 = re.search(r"`([^`]+?)` 函数", prompt)
        func_name = m2.group(1) if m2 else "UnknownFunc"
        taint_files = re.findall(r"taint-flow-(\w+)\.md", prompt)
        (cwd_path / f"dataflow-{func_name}.md").write_text(
            f"# 数据流追踪: {func_name}\n\n"
            f"## 函数信息\n- 签名: `void {func_name}(...)` \n\n"
            f"## 数据流树状图\n\n"
            + "".join(
                f"### INPUT-{i+1}: {p} 🔴 TAINTED\n"
                f"└── [DRYRUN] propagation path\n\n"
                for i, p in enumerate(taint_files or ["param1"])
            )
            + "## 污点终点汇总\n| 脏数据 | 终点 | 位置 |\n|------|------|------|\n",
            encoding="utf-8",
        )
        (cwd_path / "tainted.list").write_text(
            "-###DryrunSubFunc###L240###dryParam\n", encoding="utf-8")
        (cwd_path / "taintvars.json").write_text("[]\n", encoding="utf-8")
        if on_stream:
            on_stream(f"[DRYRUN] wrote dataflow-{func_name}.md + tainted.list + taintvars.json\n")

    # ── Judge: return pass ────────────────────────────────────────────────────
    if is_judge:
        result.output = (
            "## 评分: 75\n## 通过: 是\n"
            "## 评审意见\n[DRYRUN] 控制流验证通过\n"
            "## 改进指令\n无"
        )
    else:
        result.output = f"<result>[DRYRUN-{nonce}] ok</result>"

    if on_stream:
        on_stream(result.output[:80])
    return result
