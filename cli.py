#!/usr/bin/env python3
"""
data_flow_analyse CLI

用户使用方式：
  python3 cli.py "对 vfpfwd_board.c 的 VFP_ReceivePktFromNpByPcie 函数完成数据流分析"

服务配置由 /data/config/config.json 或 /opt/data_flow_analyse/config.example.json 提供。
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import build_task_config, load_service_config
from app.models import SwarmEvent
from app.orchestrator import Orchestrator


# ─── 美化 CLI 渲染器 ─────────────────────────────────────────────────────────

class CliRenderer:
    """有状态的 CLI 事件渲染器，产出紧凑、层次清晰的输出。"""

    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self._depth = 0                 # 当前追踪深度
        self._root_id = ""              # 根任务 ID
        self._func_count = 0            # 已分析函数数
        self._skipped: list[str] = []   # 跳过的函数（攒起来批量显示）
        self._round_t0: float = 0       # 当前 round 开始时间
        self._round_evals: list[dict] = []  # 当前 round 的 judge 评分
        self._t0 = time.time()          # 全局开始时间
        self._in_round = False          # 是否在 round 中（控制换行）

    def __call__(self, event: SwarmEvent):
        if self.quiet:
            return
        self._render(event)

    # ── 缩进工具 ──────────────────────────────────────────────

    def _prefix(self, depth: int | None = None) -> str:
        """基于深度生成树状缩进前缀。"""
        d = depth if depth is not None else self._depth
        if d <= 0:
            return "  "
        return "  " + "│  " * (d - 1) + "├─ "

    def _cont(self, depth: int | None = None) -> str:
        """续行前缀（不带 ├─）。"""
        d = depth if depth is not None else self._depth
        if d <= 0:
            return "  "
        return "  " + "│  " * d

    # ── 事件分发 ──────────────────────────────────────────────

    def _render(self, event: SwarmEvent):
        t = event.type
        d = event.data

        if t == "trace_start":
            depth = d.get("depth", 0)
            func = d.get("function", "?")
            self._depth = depth
            self._func_count += 1
            if depth == 0:
                self._root_id = event.task_id
                print(f"\n{'━' * 60}")
                print(f"  ▶ {func}")
                print(f"{'━' * 60}")
            else:
                print(f"\n{self._prefix(depth)}[d{depth}] {func}")

        elif t == "task_start":
            if not self._root_id:
                self._root_id = event.task_id

        elif t == "round_start":
            rnd = d.get("round", 1)
            self._round_t0 = time.time()
            self._round_evals = []
            self._in_round = True
            prefix = self._cont()
            print(f"{prefix}R{rnd}: ", end="", flush=True)

        elif t == "worker_done":
            df = "✓" if d.get("dataflow_found") else "∅"
            print(f"W[{df}] ", end="", flush=True)

        elif t == "judge_eval":
            score = d.get("score", 0)
            passed = d.get("passed", False)
            self._round_evals.append({"score": score, "passed": passed})

        elif t == "round_end":
            elapsed = time.time() - self._round_t0
            passed = d.get("passed", False)
            pc = d.get("pass_count", 0)
            tc = d.get("total_judges", 1)
            scores = "/".join(str(e["score"]) for e in self._round_evals)
            icon = "✅" if passed else "❌"
            print(f"→ J[{scores}] {icon} {pc}/{tc} ({elapsed:.0f}s)")
            self._in_round = False

        elif t == "round_reflection":
            prefix = self._cont()
            print(f"{prefix}   ↻ 强制反思轮")

        elif t == "trace_callees":
            funcs = d.get("callees", [])
            prefix = self._cont()
            if funcs:
                preview = ", ".join(funcs[:5])
                more = f" +{len(funcs)-5}" if len(funcs) > 5 else ""
                print(f"{prefix}→ {len(funcs)} callees: {preview}{more}")

        elif t == "trace_skip":
            func = d.get("function", "?")
            reason = d.get("reason", "")
            if "no definition" in reason:
                tag = "extern"
            elif "already" in reason:
                tag = "dup"
            else:
                tag = reason[:15]
            self._skipped.append(f"{func}({tag})")

        elif t == "trace_depth_limit":
            pass  # depth limit 在 round_end 后已清晰，不需额外输出

        elif t == "merge_start":
            n = d.get("file_count", 0)
            print(f"\n  🔀 Merging {n} documents... ", end="", flush=True)

        elif t == "merge_done":
            size = d.get("size", 0)
            unit = "KB" if size > 1024 else "B"
            val = size / 1024 if size > 1024 else size
            print(f"✅ ({val:.1f}{unit})")

        elif t == "merge_failed":
            err = d.get("error", "")[:80]
            print(f"❌ {err}")

        elif t == "task_end":
            # 只有根任务打印最终汇总
            if event.task_id == self._root_id:
                self._print_summary(d)

        elif t == "error":
            prefix = self._cont()
            print(f"\n{prefix}❗ {d.get('error', '')[:200]}", file=sys.stderr)

    # ── 最终汇总 ──────────────────────────────────────────────

    def _print_summary(self, d: dict):
        status = d.get("status", "?").upper()
        elapsed = time.time() - self._t0
        icon = "✅" if status == "PASSED" else "❌" if status == "FAILED" else "⚠️"

        print(f"\n{'═' * 60}")
        print(f"  {icon} {status}  │  {self._func_count} functions  │  {elapsed:.0f}s")
        if d.get("result_file"):
            print(f"  📄 {d['result_file']}")
        if d.get("archive"):
            print(f"  📦 {d['archive']}")
        if self._skipped:
            preview = ", ".join(self._skipped[:8])
            more = f" +{len(self._skipped)-8}" if len(self._skipped) > 8 else ""
            print(f"  ⏭  Skipped: {preview}{more}")
        print(f"{'═' * 60}")


# ─── 查找服务配置文件 ─────────────────────────────────────────────────────────

# 从环境变量读取路径配置
_CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
_CONFIG_SEARCH_PATHS = [
    f"{_CONFIG_DIR}/config.json",
    "/opt/data_flow_analyse/config.example.json",
    "./config.json",
    "./config.example.json",
]


def find_service_config() -> str:
    for p in _CONFIG_SEARCH_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(
        "找不到服务配置文件。请在以下位置之一放置 config.json：\n"
        + "\n".join(f"  - {p}" for p in _CONFIG_SEARCH_PATHS)
    )


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("""用法:
  python3 cli.py "对 xxx.c 的 yyy 函数完成数据流分析"
  python3 cli.py "分析 firmware.c 中 parse_packet 的外部输入数据流"

选项:
  --config <path>    指定服务配置文件（默认自动搜索）
  --quiet            安静模式
  --cwd <path>       指定待分析文件所在目录（默认 /data/target）
""")
        sys.exit(0)

    # 解析参数
    quiet = "--quiet" in sys.argv

    # 提取位置参数（跳过 --key value 对）
    _OPTS_WITH_VALUE = {"--config", "--cwd"}
    skip_next = False
    args = []
    for a in sys.argv[1:]:
        if skip_next:
            skip_next = False
            continue
        if a in _OPTS_WITH_VALUE:
            skip_next = True
            continue
        if a.startswith("--"):
            continue
        args.append(a)
    prompt = args[0] if args else ""

    if not prompt:
        print("错误：请提供分析任务描述", file=sys.stderr)
        sys.exit(1)

    config_path = None
    cwd = os.environ.get("TARGET_DIR", "/data/target")
    for i, a in enumerate(sys.argv):
        if a == "--config" and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
        if a == "--cwd" and i + 1 < len(sys.argv):
            cwd = sys.argv[i + 1]

    # 加载服务配置
    if not config_path:
        config_path = find_service_config()

    svc = load_service_config(config_path)
    cfg = build_task_config(svc, prompt, cwd=cwd)

    # 头部信息
    func = cfg.function_name or "(auto)"
    src = cfg.source_file or "(auto)"
    models = set(a.model for a in cfg.workers.agents) | set(a.model for a in cfg.judges.agents)
    model_str = ", ".join(models)

    max_r = '∞' if cfg.max_rounds < 0 else str(cfg.max_rounds)
    print(f"""
┌─────────────────────────────────────────────────┐
│  data_flow_analyse                              │
├─────────────────────────────────────────────────┤
│  {func:<48}│
│  {src:<48}│
│  W={cfg.worker_count} J={cfg.judge_count}  rounds={cfg.min_rounds}~{max_r}  depth≤{cfg.max_trace_depth:<12}│
│  {model_str:<48}│
└─────────────────────────────────────────────────┘""")

    renderer = CliRenderer(quiet=quiet)
    orch = Orchestrator(config=cfg, on_event=renderer)
    result = await orch.execute_recursive()

    # 如果渲染器没触发 task_end（异常情况），补一个摘要
    if result.status.value not in ("passed",) or renderer._func_count == 0:
        pass  # renderer 已处理

    print(f"\n  Tokens: in={result.total_tokens.input} out={result.total_tokens.output}  cost=${result.total_tokens.cost:.4f}")

    sys.exit(0 if result.status.value == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())
