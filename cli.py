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


def _fmt_stat(t_in: int, t_out: int, secs: float) -> str:
    """Format [Xs,i:NNK,o:NNK] stats tag."""
    def k(n):
        return f"{n//1000}K" if n >= 1000 else str(n)
    if t_in == 0 and t_out == 0:
        return f"[{secs:.0f}s]"
    return f"[{secs:.0f}s,i:{k(t_in)},o:{k(t_out)}]"


@dataclasses.dataclass
class _TaintStat:
    """Per-taint-param progress: status + timing + tokens."""
    status: str = "·"   # · running  ✓ done
    t0: float = dataclasses.field(default_factory=time.time)
    secs: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0

    def fmt(self) -> str:
        if self.status == "✓" and (self.secs > 0 or self.tokens_in > 0):
            return f"({self.status}){_fmt_stat(self.tokens_in, self.tokens_out, self.secs)}"
        return f"({self.status})"


@dataclasses.dataclass
class _RndState:
    """Per-round progress: taint sessions + summary + judge."""
    num: int
    taints: dict = dataclasses.field(default_factory=dict)  # safe_param -> _TaintStat
    # summary
    sum_status: str = ""     # "" | "·" | "✓"
    sum_t0: float = 0.0
    sum_secs: float = 0.0
    sum_in: int = 0
    sum_out: int = 0
    # judge
    j_passed: Optional[bool] = None
    j_score: Optional[int] = None
    j_secs: float = 0.0
    j_in: int = 0
    j_out: int = 0
    j_t0: float = 0.0

    def fmt(self) -> str:
        # Taint parts
        parts = [ts.fmt() for p, ts in self.taints.items()]
        label = ",".join(f"{p}{ts.fmt()}" for p, ts in self.taints.items())
        # Summary
        if self.sum_status:
            if self.sum_status == "✓" and (self.sum_secs > 0 or self.sum_in > 0):
                label += f",Σ(✓){_fmt_stat(self.sum_in, self.sum_out, self.sum_secs)}"
            else:
                label += f",Σ({self.sum_status})"
        r = f"R{self.num}({label})" if label else f"R{self.num}(·)"
        # Round-level stats bracket (shown when summary done, before judge)
        # Judge
        if self.j_passed is not None:
            j = "✓" if self.j_passed else f"✗{self.j_score or ''}"
            r += f"-J{j}{_fmt_stat(self.j_in, self.j_out, self.j_secs)}"
        return r


@dataclasses.dataclass
class _FuncState:
    """Full progress for one function."""
    task_id: str
    name: str
    short: str
    depth: int
    rounds: list = dataclasses.field(default_factory=list)  # list[_RndState]
    final: str = ""
    t0: float = dataclasses.field(default_factory=time.time)

    @property
    def cur_round(self) -> Optional[_RndState]:
        return self.rounds[-1] if self.rounds else None

    def line(self, ts: bool = False) -> str:
        hist = "→".join(r.fmt() for r in self.rounds)
        elapsed = f" {time.time()-self.t0:.0f}s"
        prefix = f"[{time.strftime('%H:%M:%S')}] " if ts else "  "
        return f"{prefix}{self.short:<16} {hist}{self.final}{elapsed}"



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
            sep = "━" * 60
            if depth == 0:
                self._print("\n" + sep)
                self._print("  ▶ " + func)
                self._print(sep)
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
                self._refresh(tid, force=not self._tty)

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
                param = wid[len("worker-taint-"):]
                rnd.taints[param] = _TaintStat(status="·")
            elif wid == "worker-summary":
                rnd.sum_status = "·"
                rnd.sum_t0 = time.time()
            self._refresh(tid)

        elif t == "worker_stream":
            pass

        elif t == "worker_done":
            wid   = d.get("worker_id", "")
            t_in  = d.get("tokens_in", 0)
            t_out = d.get("tokens_out", 0)
            fs    = self._fstate.get(tid)
            if not fs:
                return
            rnd = fs.cur_round
            if not rnd:
                return
            if wid.startswith("worker-taint-"):
                param = wid[len("worker-taint-"):]
                ts = rnd.taints.get(param)
                if ts is None:
                    ts = _TaintStat()
                    rnd.taints[param] = ts
                ts.status = "✓"
                ts.secs = time.time() - ts.t0
                ts.tokens_in  = t_in
                ts.tokens_out = t_out
                self._refresh(tid, force=not self._tty)
            elif wid == "worker-summary":
                rnd.sum_status = "✓"
                rnd.sum_secs = time.time() - rnd.sum_t0
                rnd.sum_in  = t_in
                rnd.sum_out = t_out
                self._refresh(tid, force=not self._tty)

        elif t == "judge_start":
            fs = self._fstate.get(tid)
            if fs and fs.cur_round:
                fs.cur_round.j_t0 = time.time()

        elif t == "judge_done":
            t_in  = d.get("tokens_in", 0)
            t_out = d.get("tokens_out", 0)
            fs = self._fstate.get(tid)
            if fs and fs.cur_round:
                rnd = fs.cur_round
                rnd.j_in   = t_in
                rnd.j_out  = t_out
                rnd.j_secs = time.time() - (rnd.j_t0 or time.time())

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
            if not rnd.j_secs:
                rnd.j_secs = time.time() - (rnd.j_t0 or time.time())
            if passed:
                fs.final = " ✅"
                self._refresh(tid, force=not self._tty)
                self._commit(tid)
            else:
                self._refresh(tid, force=not self._tty)

        elif t == "round_end":
            passed = d.get("passed", False)
            fs = self._fstate.get(tid)
            if not fs:
                return
            if passed:
                if not fs.final:
                    fs.final = " ✅"
                self._commit(tid)

        elif t == "trace_callees":
            funcs = d.get("callees", [])
            if funcs:
                depth   = self._task_depth.get(tid, 0)
                prefix  = "  " + "│  " * depth
                preview = ", ".join(funcs[:5])
                more    = f" +{len(funcs)-5}" if len(funcs) > 5 else ""
                self._print(prefix + "  → " + str(len(funcs)) + " callees: " + preview + more)

        elif t == "trace_skip":
            func   = d.get("function", "?")
            reason = d.get("reason", "")
            tag = ("extern" if "no definition" in reason
                   else "dup" if "already" in reason
                   else reason[:12])
            self._skipped.append(f"{func}({tag})")

        elif t == "merge_start":
            n = d.get("file_count", 0)
            self._print(f"\n  🔀 Merging {n} documents...")

        elif t == "merge_done":
            size = d.get("size", 0)
            unit = "KB" if size > 1024 else "B"
            val  = size / 1024 if size > 1024 else size
            self._print(f"  ✅ Merged ({val:.1f}{unit})")

        elif t == "merge_failed":
            err = d.get("error", "")[:80]
            self._print(f"  ❌ Merge failed: {err}")

        elif t == "task_end":
            if tid == self._root_id:
                self._print_summary(d)

        elif t == "error":
            self._print("  ❗ " + d.get("error", "")[:200])


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
