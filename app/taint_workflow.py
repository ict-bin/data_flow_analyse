"""
PerTaintWorkflow — 多 session 并行污点分析工作流

架构:
  Phase 1: base_session     — 阅读目标函数代码
    └─ fork ──┬─ taint_session[param1]  — 深入分析单个污点
              ├─ taint_session[param2]
              ├─ ...
              └─ summary_session         — 汇总所有污点 → 最终报告

  Phase 2: 并行执行所有 taint_sessions (每个分析一个污点参数)
  Phase 3: summary_session 读取所有 taint-flow 文件，生成最终报告
  Phase 4: Judge 评审 → 根据反馈路由到对应 session 重新分析

Session 文件结构:
  {out_dir}/sessions/
    worker-0-base.jsonl
    worker-0-taint-{param}.jsonl
    worker-0-summary.jsonl
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Callable

from .models import (
    AgentInstanceConfig,
    CalleeRef,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
    WorkerEvaluation,
    WorkerResult,
    make_id,
)
from .runner import run_agent



def _extract_function_body(ws, src_file: str, func_name: str,
                           line_hint: str = "") -> str:
    """Orchestrator extracts function body. Inject into prompt so model skips file reading.
    line_hint: e.g. 'L228' — used to prefer the overload at or after that line.
    """
    import subprocess
    cmd = ['extract_func', src_file, func_name]
    if line_hint:
        cmd += ['--line', line_hint.lstrip('Ll')]
    try:
        r = subprocess.run(cmd, cwd=str(ws), capture_output=True, text=True, timeout=15)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    # fallback: grep function range from file, respecting line_hint
    from pathlib import Path as _Path
    hint_num = 0
    if line_hint:
        try:
            hint_num = int(line_hint.lstrip('Ll'))
        except ValueError:
            pass
    for cand in [_Path(str(ws)) / src_file, _Path(str(ws)) / src_file.split("/")[-1]]:
        if not cand.exists():
            continue
        try:
            fl = cand.read_text(encoding="utf-8", errors="replace").splitlines()
            short = func_name.split("::")[-1]
            matches = [i for i, ln in enumerate(fl)
                       if short in ln and "(" in ln and not ln.strip().startswith("/")]
            if hint_num > 0:
                preferred = [i for i in matches if i + 1 >= hint_num]
                matches = preferred + [i for i in matches if i + 1 < hint_num]
            if matches:
                i = matches[0]
                return chr(10).join(fl[max(0, i-2):min(len(fl), i+100)])
        except OSError:
            pass
    return ""


def _find_df_file(worker_cwd: str, function_name: str = "") -> str:
    """Thin wrapper — locate dataflow file in task output dir."""
    from .orchestrator import _find_dataflow_file
    return _find_dataflow_file(worker_cwd, function_name)


def _safe_param(p: str) -> str:
    return re.sub(r'[^A-Za-z0-9_]', '_', p)


def _extract_result_text(output: str) -> str:
    m = re.match(r'<result>(.*?)</result>', output, re.DOTALL)
    return m.group(1).strip() if m else output


# ─── 分阶段 Prompt 构造 ────────────────────────────────────────────────────────

def _build_base_prompt(func_name: str, src_file: str, taint_params: list[str],
                       taint_ctx: str = "", depth: int = 0, max_depth: int = 0) -> str:
    import uuid
    nonce = uuid.uuid4().hex[:8]
    params_str = "、".join(f"`{p}`" for p in taint_params)
    ctx_block = f"\n\n# 调用者传入的脏数据\n{taint_ctx}" if taint_ctx else ""
    depth_note = f"\n\n# 当前追踪深度: {depth}/{max_depth}" if max_depth > 0 else ""
    return (
        f"<!-- {nonce} -->\n"
        f"# 任务\n\n"
        f"对 `{src_file}` 中的 `{func_name}` 函数进行静态污点分析。\n"
        f"污点参数: {params_str}\n\n"
        f"# 阶段一：阅读源码\n\n"
        f"使用 `read` 或 `bash extract_func` 读取 `{func_name}` 的完整代码。\n"
        f"阅读完成后，列出函数签名和所有需要追踪的污点参数，不要开始分析。"
        f"{ctx_block}{depth_note}"
    )


def _build_taint_prompt(param: str, func_name: str,
                        func_body: str = "",
                        feedback: str = "", rnd: int = 1) -> str:
    import uuid
    nonce = uuid.uuid4().hex[:8]
    safe_p = _safe_param(param)
    fb_block = ""
    if rnd > 1 and feedback:
        fb_block = (
            chr(10)*2 + "# Round " + str(rnd) + " feedback for `" + param + "`"
            + chr(10)*2 + feedback + chr(10)*2 + "Please revise your analysis."
        )
    src_block = ""
    if func_body:
        src_block = (
            "## Function Source Code (provided -- DO NOT read files)" + chr(10)*2
            + "```cpp" + chr(10) + func_body + chr(10) + "```" + chr(10)*2
        )
    return (
        "<!-- " + nonce + " -->" + chr(10)
        + "# Deep taint analysis: `" + param + "` in `" + func_name + "`" + chr(10)*2
        + "**Analyze ONLY this one parameter within the current function.**" + chr(10)*2
        + src_block
        + "Requirements:" + chr(10)
        + "- Trace every use of `" + param + "` line-by-line" + chr(10)
        + "- Mark derived variables with 🔴 TAINTED" + chr(10)
        + "- Identify sub-function calls receiving `" + param + "` or derived values" + chr(10)
        + "- Do NOT analyze sub-function internals" + chr(10)
        + "- Source provided -- **DO NOT use read/bash to open files**" + chr(10)*2
        + "After analysis write `taint-flow-" + safe_p + ".md` using the write tool."
        + fb_block
    )

def _build_taint_post_skill(param: str) -> str:
    safe_p = _safe_param(param)
    return (
        f"Based on your analysis of `{param}` above, write `taint-flow-{safe_p}.md`.\n\n"
        f"Use the **write** tool. Format:\n"
        f"```\n"
        f"# 污点流: {param}\n\n"
        f"## 污点源\n- {param} 🔴 TAINTED\n\n"
        f"## 传播路径\n[tree diagram with 🔴 marks]\n\n"
        f"## 接收此污点的子函数\n"
        f"| 函数 | 调用位置 | 接收的形参 |\n"
        f"|------|---------|----------|\n"
        f"| Class::Method | L??? | paramName |\n"
        f"```\n\n"
        f"Write ONLY `taint-flow-{safe_p}.md` — use the write tool now."
    )


def _build_summary_prompt(func_name: str, taint_params: list[str],
                          src_file: str, feedback: str = "", rnd: int = 1) -> str:
    import uuid
    nonce = uuid.uuid4().hex[:8]
    taint_files = ", ".join(
        f"`taint-flow-{_safe_param(p)}.md`" for p in taint_params
    )
    fb_block = ""
    if rnd > 1 and feedback:
        fb_block = f"\n\n# 上一轮反馈（针对汇总报告）\n\n{feedback}\n\n请修正汇总报告。"
    return (
        f"<!-- {nonce} -->\n"
        f"# 阶段三：汇总所有污点分析\n\n"
        f"请读取以下各污点的分析文件并汇总：{taint_files}\n\n"
        f"使用 `read` 工具依次读取每个文件，然后：\n"
        f"1. 将所有污点传播路径合并到一份完整报告 `dataflow-{func_name}.md`\n"
        f"2. 从各文件的「接收此污点的子函数」表格汇总 `tainted.list`\n\n"
        f"**输出步骤（必须按顺序）：**\n"
        f"1. 用 write 工具写入 `dataflow-{func_name}.md`\n"
        f"2. 用 write 工具写入 `tainted.list`（格式：`file###Class::Func###L行号###形参`）"
        f"{fb_block}"
    )


def _build_summary_post_skill(func_name: str, taint_params: list[str]) -> str:
    taint_files = ", ".join(f"taint-flow-{_safe_param(p)}.md" for p in taint_params)
    return (
        f"Based on your merged analysis above, now write the two output files:\n\n"
        f"**File 1**: `dataflow-{func_name}.md` — complete merged dataflow report\n"
        f"**File 2**: `tainted.list` — callee functions that receive tainted params\n\n"
        f"For tainted.list, one line per callee:\n"
        f"`file_path###Class::FuncName###L_line###param1,param2`\n\n"
        f"Use unknown fields: `-` for path/line, `*` for params.\n"
        f"Only include functions that actually receive tainted data (no getters, no conditions).\n\n"
        f"Source files were: {taint_files}\n"
        f"Write BOTH files now using the write tool."
    )


# ─── Judge 评估 Prompt ────────────────────────────────────────────────────────

def _build_taint_eval_prompt(param: str, rnd: int, taint_file: str) -> str:
    safe_p = _safe_param(param)
    return (
        f"# 评审 污点 `{param}` 的分析 (Round {rnd})\n\n"
        f"读取文件: `{taint_file}`\n\n"
        f"**评审标准（只评当前函数范围内的 {param} 分析）：**\n\n"
        f"| 维度 | 分值 |\n"
        f"|------|------|\n"
        f"| {param} 的使用点是否完整覆盖 | 40分 |\n"
        f"| 派生变量是否正确标记 🔴 | 30分 |\n"
        f"| 接收此污点的子函数识别是否准确 | 30分 |\n\n"
        f"❌ 禁止要求展开子函数内部实现！\n\n"
        f"输出格式：\n"
        f"## 评分: <整数>\n"
        f"## 通过: <是/否>\n"
        f"## 评审意见\n<具体问题>\n"
        f"## 改进指令\n<针对 {param} 分析的改进，不要要求追踪子函数>"
    )


def _build_summary_eval_prompt(func_name: str, rnd: int, taint_params: list[str],
                                task: str) -> str:
    taint_files = ", ".join(
        f"`taint-flow-{_safe_param(p)}.md`" for p in taint_params
    )
    return (
        f"# 评审汇总报告 (Round {rnd})\n\n"
        f"## 任务要求\n\n{task}\n\n"
        f"## 需要读取的文件\n\n"
        f"1. 汇总报告: `worker-0-dataflow.md`\n"
        f"2. tainted.list: 通过工作目录或 round-{rnd}/workers 查找\n"
        f"3. 各污点分析: {taint_files}（可选，用于验证）\n\n"
        f"**评审标准：**\n\n"
        f"| 维度 | 分值 |\n"
        f"|------|------|\n"
        f"| F1: 汇总报告文件存在且有内容 | 强制 |\n"
        f"| F2: 报告包含正确函数名 | 强制 |\n"
        f"| 外部输入识别 | 20分 |\n"
        f"| 当前函数内污点追踪完整性 | 35分 |\n"
        f"| 子函数正确识别（tainted.list） | 25分 |\n"
        f"| 文档规范（🔴标记、树状图、行号） | 20分 |\n\n"
        f"❌ 禁止要求展开子函数内部！\n\n"
        f"## 改进指令路由规范\n"
        f"改进指令请注明针对哪个 session：\n"
        f"- `[TAINT:{','.join(taint_params)}]` - 某污点分析有问题\n"
        f"- `[SUMMARY]` - 汇总报告有问题\n\n"
        f"输出格式：\n"
        f"## 评分: <整数>\n## 通过: <是/否>\n## 评审意见\n...\n## 改进指令\n..."
    )


# ─── Feedback 路由 ────────────────────────────────────────────────────────────

def _parse_feedback_routing(feedback: str, taint_params: list[str]
                            ) -> dict[str, str]:
    """从 Judge 改进指令中解析路由。
    返回 {session_name: feedback_text}
    其中 session_name 为 'summary' 或 taint param 名。
    """
    routing: dict[str, str] = {}

    # 显式路由标签 [TAINT:param] 或 [SUMMARY]
    explicit_taints = re.findall(r'\[TAINT:([^\]]+)\]', feedback)
    for t_str in explicit_taints:
        for p in [x.strip() for x in t_str.split(',')]:
            matching = [tp for tp in taint_params if tp.lower() == p.lower()]
            if matching:
                routing[matching[0]] = feedback

    if re.search(r'\[SUMMARY\]', feedback):
        routing['summary'] = feedback

    # 没有显式路由时：如果提到某个污点参数名，路由到对应 session
    if not routing:
        for tp in taint_params:
            if tp in feedback:
                routing[tp] = feedback
        if not routing:
            routing['summary'] = feedback  # 默认给 summary

    return routing


# ─── 主工作流类 ───────────────────────────────────────────────────────────────

class PerTaintWorkflow:
    """多 session 并行污点分析工作流。"""

    def __init__(
        self,
        cfg: TaskConfig,
        func_name: str,
        src_file: str,
        line_hint: str = "",
        taint_params: list[str] = None,
        taint_ctx: str = "",
        task_id: str = "",
        out_dir: Path = None,
        dep: int = 0,
        max_depth: int = 5,
        on_event: Callable | None = None,
    ):
        self.cfg = cfg
        self.func_name = func_name
        self.src_file = src_file
        self.line_hint = line_hint
        self.taint_params = taint_params if taint_params else ["all"]
        self.taint_ctx = taint_ctx
        self.task_id = task_id
        self.out_dir = out_dir
        self.dep = dep
        self.max_depth = max_depth
        self.on_event = on_event

        # Session 文件路径
        self.sess_dir = out_dir / "sessions"
        self.sess_dir.mkdir(parents=True, exist_ok=True)
        self.base_sess = str(self.sess_dir / "worker-0-base.jsonl")
        self.taint_sess = {
            p: str(self.sess_dir / f"worker-0-taint-{_safe_param(p)}.jsonl")
            for p in self.taint_params
        }
        self.summary_sess = str(self.sess_dir / "worker-0-summary.jsonl")

        # Workspace（所有 session 共享）
        target_dir = os.path.abspath(cfg.cwd)
        self.ws = out_dir / "workspace-worker-0"
        self.ws.mkdir(exist_ok=True)
        if os.path.isdir(target_dir):
            for item in os.listdir(target_dir):
                src = os.path.join(target_dir, item)
                dst = str(self.ws / item)
                if not os.path.exists(dst):
                    try:
                        os.symlink(src, dst)
                    except OSError:
                        pass

        # system prompt
        from .config import resolve_system_prompt, load_system_prompts
        worker_prompts = load_system_prompts(cfg.workers.system_prompt_dir, 1)
        self.system_prompt = resolve_system_prompt(0, cfg.workers.agents[0], worker_prompts)
        judge_prompts = load_system_prompts(cfg.judges.system_prompt_dir, 1)
        self.judge_system_prompt = resolve_system_prompt(0, cfg.judges.agents[0], judge_prompts)

        self.worker_model = cfg.workers.agents[0].model
        self.judge_model = cfg.judges.agents[0].model
        self.worker_tools = cfg.workers.agents[0].tools or cfg.workers.default_tools
        self.judge_tools = cfg.judges.agents[0].tools or cfg.judges.default_tools

    def _emit(self, etype: str, **data):
        if self.on_event:
            try:
                self.on_event(etype, self.task_id, **data)
            except Exception:
                pass

    def _agent_kwargs(self, session_file: str | None, is_judge: bool = False) -> dict:
        model = self.judge_model if is_judge else self.worker_model
        tools = self.judge_tools if is_judge else self.worker_tools
        sys_prompt = self.judge_system_prompt if is_judge else self.system_prompt
        return dict(
            model=model,
            tools=tools,
            system_prompt=sys_prompt,
            cwd=str(self.ws),
            thinking_level=(self.cfg.workers.agents[0].thinking_level
                            or self.cfg.workers.default_thinking_level),
            session_file=session_file,
            cancel_event=None,
            max_retries=self.cfg.agent_max_retries,
            retry_delay=self.cfg.agent_retry_delay,
            pi_max_retries=self.cfg.pi_max_retries,
            pi_retry_delay=self.cfg.pi_retry_delay,
        )

    async def run(self) -> TaskResult:
        """主执行循环。"""
        cfg = self.cfg
        max_rounds = cfg.max_rounds if cfg.max_rounds > 0 else 20

        # 跟踪各 session 已完成的轮次（用于反馈路由）
        taint_feedbacks: dict[str, str] = {p: "" for p in self.taint_params}
        summary_feedback: str = ""
        # Extract function body ONCE on the orchestrator side.
        # Inject into taint prompts so model never needs to read files.
        func_body = _extract_function_body(self.ws, self.src_file, self.func_name,
                                           self.line_hint)
        func_body_log = f"({len(func_body)}B)" if func_body else "(empty)"
        self._emit("debug", function=self.func_name,
                   message="func_body extracted " + func_body_log)

        # Concurrency limit: taint sessions share the same worker slot.
        # Max parallel taint pi processes = worker_count (from config).
        _taint_concurrency = max(1, self.cfg.worker_count)
        _taint_sem = asyncio.Semaphore(_taint_concurrency)

        for rnd in range(1, max_rounds + 1):
            rnd_dir = self.out_dir / f"round-{rnd}"
            rnd_dir.mkdir(exist_ok=True)
            rnd_workers_dir = rnd_dir / "workers"
            rnd_workers_dir.mkdir(exist_ok=True)

            self._emit("round_start", round=rnd)

            # ── Phase 2: 并行 taint sessions（受 _taint_sem 并发限制）──────────
            async def run_taint(param: str) -> tuple[str, object]:
                async with _taint_sem:
                    fb = taint_feedbacks.get(param, "")
                    prompt = _build_taint_prompt(param, self.func_name, func_body, fb, rnd)
                    post_skill = _build_taint_post_skill(param)
                    self._emit("worker_start",
                               worker_id=f"worker-taint-{_safe_param(param)}",
                               model=self.worker_model, round=rnd,
                               function=f"{self.func_name}[{param}]")
                    res = await run_agent(
                        prompt=prompt,
                        post_skill_prompt=post_skill,
                        **self._agent_kwargs(self.taint_sess[param])
                    )
                    self._emit("worker_done",
                               worker_id=f"worker-taint-{_safe_param(param)}",
                               output=res.output[:200])
                    return (param, res)

            taint_results_raw = await asyncio.gather(*[
                run_taint(p) for p in self.taint_params
            ])
            taint_results: dict[str, object] = dict(taint_results_raw)

            # Archive taint outputs
            for param, res in taint_results.items():
                safe_p = _safe_param(param)
                (rnd_workers_dir / f"worker-0-taint-{safe_p}-output.md").write_text(
                    res.output, encoding="utf-8")
                taint_f = self.ws / f"taint-flow-{safe_p}.md"
                if taint_f.exists():
                    (rnd_workers_dir / f"taint-flow-{safe_p}.md").write_text(
                        taint_f.read_text(encoding="utf-8"), encoding="utf-8")

            # ── Phase 3: Summary session ───────────────────────────────────────
            summary_prompt = _build_summary_prompt(
                self.func_name, self.taint_params, self.src_file,
                summary_feedback, rnd
            )
            summary_post = _build_summary_post_skill(self.func_name, self.taint_params)
            self._emit("worker_start", worker_id="worker-summary",
                       model=self.worker_model, round=rnd,
                       function=f"{self.func_name}[summary]")
            summary_result = await run_agent(
                prompt=summary_prompt,
                post_skill_prompt=summary_post,
                **self._agent_kwargs(self.summary_sess)
            )
            self._emit("worker_done", worker_id="worker-summary",
                       output=summary_result.output[:200])

            # Archive summary output
            df_file = _find_df_file(str(self.out_dir), self.func_name)
            df_content = ""
            if df_file:
                try:
                    df_content = Path(df_file).read_text(encoding="utf-8")
                    (rnd_workers_dir / "worker-0-dataflow.md").write_text(
                        df_content, encoding="utf-8")
                except OSError:
                    pass
            (rnd_workers_dir / "worker-0-summary-output.md").write_text(
                summary_result.output, encoding="utf-8")

            # ── Phase 4: Judge 评审汇总报告 ────────────────────────────────────
            eval_prompt = _build_summary_eval_prompt(
                self.func_name, rnd, self.taint_params, cfg.task
            )
            judge_dir = rnd_dir / "judges" / "judge-0"
            judge_dir.mkdir(parents=True, exist_ok=True)
            # 将汇总文件复制到 judge workspace
            j_dir = judge_dir
            if df_file and df_content:
                (j_dir / "worker-0-dataflow.md").write_text(df_content, encoding="utf-8")
            # 复制 taint-flow 文件
            for p in self.taint_params:
                tf = self.ws / f"taint-flow-{_safe_param(p)}.md"
                if tf.exists():
                    (j_dir / tf.name).write_text(
                        tf.read_text(encoding="utf-8"), encoding="utf-8")
            # tainted.list
            tl = self.ws / "tainted.list"
            if tl.exists():
                (j_dir / "tainted.list").write_text(
                    tl.read_text(encoding="utf-8"), encoding="utf-8")

            self._emit("judge_start", judge_id="judge-0",
                       model=self.judge_model, round=rnd,
                       function=self.func_name)
            judge_result = await run_agent(
                prompt=eval_prompt,
                **self._agent_kwargs(None, is_judge=True)
            )
            self._emit("judge_done", judge_id="judge-0",
                       output=judge_result.output[:200])

            # Parse judge output
            from .orchestrator import _parse_eval_md as _pem
            parsed = _pem(judge_result.output)
            passed = parsed.get("pass", False)
            score = parsed.get("score", 0)
            feedback_text = parsed.get("feedback", "") + "\n" + parsed.get("refinement", "")

            # Archive judge eval
            (j_dir / "eval-worker-0.md").write_text(
                f"# judge-0 → worker-0 (Round {rnd})\n\n"
                f"- **Model**: {self.judge_model}\n"
                f"- **Pass**: {passed}\n"
                f"- **Score**: {score}\n\n"
                f"## Feedback\n\n{parsed.get('feedback','')}\n\n"
                f"## Refinement\n\n{parsed.get('refinement','')}",
                encoding="utf-8"
            )

            self._emit("judge_result", judge_id="judge-0",
                       passed=passed, score=score, round=rnd,
                       function=self.func_name)

            if passed:
                # 生成 tainted.list fallback（如 LLM 未写）
                self._ensure_tainted_list(df_content)
                return self._make_result(df_content, summary_result, passed=True)

            # ── Phase 5: 路由反馈 ──────────────────────────────────────────────
            routing = _parse_feedback_routing(feedback_text, self.taint_params)
            for target, fb in routing.items():
                if target == 'summary':
                    summary_feedback = fb
                elif target in taint_feedbacks:
                    taint_feedbacks[target] = fb
                    # 补充说明注入到对应 taint session
                    supplement_note = (
                        f"\n\n[系统通知] 关于 `{target}` 的分析收到反馈，"
                        f"请在下一轮补充分析。反馈摘要：{fb[:200]}"
                    )
                    taint_feedbacks[target] = fb + supplement_note

            # 最大轮次
            if rnd >= max_rounds:
                self._ensure_tainted_list(df_content)
                return self._make_result(df_content, summary_result, passed=False)

        return self._make_result("", summary_result if 'summary_result' in dir() else None,
                                 passed=False)

    def _ensure_tainted_list(self, df_content: str):
        """如 tainted.list 未生成，从 dataflow 内容中提取。"""
        from .orchestrator import _parse_callees as _pc
        tl = self.ws / "tainted.list"
        if not tl.exists() and df_content:
            callees = _pc(df_content)
            if callees:
                lines = [f"{c.file or '-'}###{c.function_name}###{c.line or '-'}###{c.tainted_params or '*'}"
                         for c in callees]
                try:
                    tl.write_text("\n".join(lines) + "\n", encoding="utf-8")
                except OSError:
                    pass

    def _make_result(self, df_content: str, summary_result, passed: bool) -> TaskResult:
        from .models import TaskStatus
        status = TaskStatus.PASSED if passed else TaskStatus.FAILED
        final_output = df_content or (summary_result.output if summary_result else "")
        return TaskResult(
            task_id=self.task_id,
            task=self.cfg.task,
            status=status,
            final_output=final_output,
        )
