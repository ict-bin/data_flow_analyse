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

import dataclasses
from typing import Optional


# ─── 进度状态数据类 ──────────────────────────────────────────────────────────

@dataclasses.dataclass
class _RndState:
    """单轮次进度：污点 session + summary + judge 结果。"""
    num: int
    # safe_param → "·"(运行中) | "✓"(完成)
    taints: dict[str, str] = dataclasses.field(default_factory=dict)
    summary: str = ""      # "" | "·" | "✓"
    j_passed: Optional[bool] = None
    j_score: Optional[int] = None

    def fmt(self) -> str:
        items = [f"{p}({s})" for p, s in self.taints.items()]
        if self.summary:
            items.append(f"Σ({self.summary})")
        inner = ",".join(items) if items else "·"
        r = f"R{self.num}({inner})"
        if self.j_passed is not None:
            j = "✓" if self.j_passed else f"✗{self.j_score or ''}"
            r += f"-J{j}"
        return r


@dataclasses.dataclass
class _FuncState:
    """一个函数的全部进度。"""
    task_id: str
    name: str           # 完整函数名
    short: str          # 显示用短名 (≤16 chars)
    depth: int
    rounds: list[_RndState] = dataclasses.field(default_factory=list)
    final: str = ""     # "" | "✅" | "❌"
    t0: float = dataclasses.field(default_factory=time.time)

    @property
    def cur_round(self) -> Optional[_RndState]:
        return self.rounds[-1] if self.rounds else None

    def line(self) -> str:
        hist = "→".join(r.fmt() for r in self.rounds)
        elapsed = f" {time.time()-self.t0:.0f}s"
        return f"  {self.short:<16} {hist}{self.final}{elapsed}"


# ─── 美化 CLI 渲染器 ─────────────────────────────────────────────────────────

class CliRenderer:
    """有状态的 CLI 事件渲染器（含底部实时进度条）。"""

    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self._t0 = time.time()
        self._root_id = ""
        self._func_count = 0
        self._skipped: list[str] = []

        # 函数进度状态
        self._fstate: dict[str, _FuncState] = {}   # task_id → _FuncState
        self._task_depth: dict[str, int] = {}
        self._task_func: dict[str, str] = {}

        # Status block（底部实时行）
        self._tty: bool = sys.stdout.isatty()
        self._slots: list[str] = []                # 有序 task_id 列表
        self._slot_text: dict[str, str] = {}       # task_id → 当前行文本
        self._n_rendered: int = 0                  # 当前渲染的行数

    def __call__(self, event: SwarmEvent):
        if self.quiet:
            return
        self._render(event)

    # -- status block --

    def _clr(self):
        n = self._n_rendered
        if n > 0 and self._tty:
            sys.stdout.write(f'\033[{n}A')
            for _ in range(n):
                sys.stdout.write('\033[2K\r\n')
            sys.stdout.write(f'\033[{n}A')
        self._n_rendered = 0

    def _draw(self):
        if not self._tty or not self._slots:
            return
        for tid in self._slots:
            sys.stdout.write(self._slot_text.get(tid, '') + '\n')
        sys.stdout.flush()
        self._n_rendered = len(self._slots)

    def _print(self, msg: str):
        self._clr()
        print(msg)
        self._draw()

    def _push(self, task_id: str):
        if task_id not in self._slots:
            self._slots.append(task_id)
        self._refresh(task_id)

    def _refresh(self, task_id: str):
        fs = self._fstate.get(task_id)
        if fs:
            self._slot_text[task_id] = fs.line()
        self._clr()
        self._draw()

    def _commit(self, task_id: str):
        fs = self._fstate.get(task_id)
        self._clr()
        self._slots = [t for t in self._slots if t != task_id]
        self._slot_text.pop(task_id, None)
        if fs:
            print(fs.line())
        self._draw()

    @staticmethod
    def _short(name: str, maxlen: int = 16) -> str:
        parts = name.split('::')
        s = parts[-1] if len(parts) >= 2 else name
        return s[:maxlen]

    @staticmethod
    def _prefix_tree(depth: int) -> str:
        if depth <= 0:
            return "  "
        return "  " + "\u2502  " * (depth - 1) + "\u251c\u2500 "

    def _render(self, event: SwarmEvent):
        t   = event.type
        d   = event.data
        tid = event.task_id

        if t == "trace_start":
            depth = d.get("depth", 0)
            func  = d.get("function", "?")
            self._task_depth[tid] = depth
            self._task_func[tid]  = func
            self._func_count += 1
            if not self._root_id:
                self._root_id = tid
            fs = _FuncState(task_id=tid, name=func,
                            short=self._short(func), depth=depth)
            self._fstate[tid] = fs
            if depth == 0:
                self._print("\n" + "\u2501" * 60)
                self._print("  \u25b6 " + func)
                self._print("\u2501" * 60)
            else:
                self._print(self._prefix_tree(depth) + f"[d{depth}] " + func)
            self._push(tid)

        elif t == "task_start":
            if not self._root_id:
                self._root_id = tid

        elif t == "round_start":
            fs = self._fstate.get(tid)
            if fs:
                fs.rounds.append(_RndState(num=d.get("round", len(fs.rounds) + 1)))
                self._refresh(tid)

        elif t == "worker_start":
            wid = d.get("worker_id", "")
            fs  = self._fstate.get(tid)
            if not fs:
                return
            rnd = fs.cur_round
            if not rnd:
                rnd = _RndState(num=1)
                fs.rounds.append(rnd)
            if wid.startswith("worker-taint-"):
                rnd.taints[wid[len("worker-taint-"):]] = "\u00b7"
            elif wid == "worker-summary":
                rnd.summary = "\u00b7"
            self._refresh(tid)

        elif t == "worker_stream":
            pass

        elif t == "worker_done":
            wid = d.get("worker_id", "")
            fs  = self._fstate.get(tid)
            if not fs:
                return
            rnd = fs.cur_round
            if not rnd:
                return
            if wid.startswith("worker-taint-"):
                rnd.taints[wid[len("worker-taint-"):]] = "\u2713"
            elif wid == "worker-summary":
                rnd.summary = "\u2713"
            self._refresh(tid)

        elif t in ("judge_start", "judge_done"):
            pass

        elif t in ("judge_result", "judge_eval"):
            passed = d.get("passed", False)
            score  = d.get("score",  0)
            fs = self._fstate.get(tid)
            if not fs:
                return
            rnd = fs.cur_round
            if not rnd:
                return
            rnd.j_passed = passed
            rnd.j_score  = score
            if passed:
                fs.final = " \u2705"
                self._refresh(tid)
                self._commit(tid)
            else:
                self._refresh(tid)

        elif t == "round_end":
            passed = d.get("passed", False)
            fs = self._fstate.get(tid)
            if not fs:
                return
            if passed:
                if not fs.final:
                    fs.final = " \u2705"
                self._commit(tid)

        elif t == "trace_callees":
            funcs = d.get("callees", [])
            if funcs:
                depth   = self._task_depth.get(tid, 0)
                prefix  = "  " + "\u2502  " * depth
                preview = ", ".join(funcs[:5])
                more    = f" +{len(funcs)-5}" if len(funcs) > 5 else ""
                self._print(prefix + "  \u2192 " + str(len(funcs)) + " callees: " + preview + more)

        elif t == "trace_skip":
            func   = d.get("function", "?")
            reason = d.get("reason", "")
            tag = ("extern" if "no definition" in reason
                   else "dup" if "already" in reason
                   else reason[:12])
            self._skipped.append(f"{func}({tag})")

        elif t == "merge_start":
            n = d.get("file_count", 0)
            self._print(f"\n  \U0001f500 Merging {n} documents...")

        elif t == "merge_done":
            size = d.get("size", 0)
            unit = "KB" if size > 1024 else "B"
            val  = size / 1024 if size > 1024 else size
            self._print(f"  \u2705 Merged ({val:.1f}{unit})")

        elif t == "merge_failed":
            err = d.get("error", "")[:80]
            self._print(f"  \u274c Merge failed: {err}")

        elif t == "task_end":
            if tid == self._root_id:
                self._print_summary(d)

        elif t == "error":
            self._print("  \u2757 " + d.get("error", "")[:200])

    def _print_summary(self, d: dict):
        status  = d.get("status", "?").upper()
        elapsed = time.time() - self._t0
        icon = "\u2705" if status == "PASSED" else "\u274c" if status == "FAILED" else "\u26a0\ufe0f"
        self._print("\n" + "\u2550" * 60)
        self._print(f"  {icon} {status}  \u2502  {self._func_count} functions  \u2502  {elapsed:.0f}s")
        if d.get("result_file"):
            self._print("  \U0001f4c4 " + d["result_file"])
        if d.get("archive"):
            self._print("  \U0001f4e6 " + d["archive"])
        if self._skipped:
            preview = ", ".join(self._skipped[:8])
            more    = f" +{len(self._skipped)-8}" if len(self._skipped) > 8 else ""
            self._print("  \u23ed  Skipped: " + preview + more)
        self._print("\u2550" * 60)


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
