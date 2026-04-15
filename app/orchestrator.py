"""
data_flow_analyse — 编排引擎

═══════════════════════════════════════════════════════════════════
工作流（每 Round）：

  1. X 个 Worker 并行执行同一任务（各自独立，各自 session 保持上下文）
     → 输出归档为 round-N/workers/worker-i-output.md

  2. 每个 Judge 依次评判每个 Worker（Judge 内用临时 session 做多轮对话）：
     a) 提示词 1: "评判 worker-0 的输出"  → eval-worker-0.md
     b) 提示词 2: "评判 worker-1 的输出"  → eval-worker-1.md
     c) 提示词 3（≥2 worker 时）: "对比总结，哪个做得更好" → summary.md
     → Judge 临时 session 在 round 结束后归档而非删除

  3. 汇总投票：
     - 每个 Judge 的 overall_passed 计为一票
     - pass_count >= pass_threshold → 任务通过

  4. 未通过 → 生成 feedback.md（含最佳 worker + 各 worker 改进建议）
     → 下一轮注入所有 Worker（Worker 有 session 能看到历史）

  5. 通过 → 取最佳 Worker 输出作为 final_output
═══════════════════════════════════════════════════════════════════

归档目录结构：
  output/{task_id}/
  ├── round-1/
  │   ├── workers/
  │   │   ├── worker-0-output.md
  │   │   └── worker-1-output.md
  │   ├── judges/
  │   │   ├── judge-0/
  │   │   │   ├── eval-worker-0.md
  │   │   │   ├── eval-worker-1.md
  │   │   │   └── summary.md
  │   │   └── judge-1/
  │   │       ├── eval-worker-0.md
  │   │       ├── eval-worker-1.md
  │   │       └── summary.md
  │   └── feedback.md
  ├── round-2/
  │   └── ...
  ├── sessions/
  │   ├── worker-0.jsonl
  │   ├── worker-1.jsonl
  │   ├── judge-0-round-1.jsonl
  │   └── judge-0-round-2.jsonl
  ├── output.md
  ├── report.md
  └── result.json
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import time
from collections import Counter
from pathlib import Path
from typing import Callable

from .config import load_system_prompts, resolve_system_prompt
from .models import (
    AgentInstanceConfig,
    CalleeRef,
    JudgeRoundResult,
    JudgeSummary,
    RoundResult,
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
    TraceNode,
    WorkerEvaluation,
    WorkerResult,
    make_id,
)
from .runner import run_agent, run_agents_parallel

WORKER_CONCURRENCY = 4


# ─── 解析工具 ─────────────────────────────────────────────────────────────────

def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _find_dataflow_file(worker_cwd: str, function_name: str = "") -> str:
    """从 Worker 工作目录搜索 dataflow-*.md 文件。"""
    cwd = Path(worker_cwd)
    candidates: list[Path] = []

    # 搜索当前目录和常见位置
    for search_dir in [cwd, Path("/tmp")]:
        if search_dir.is_dir():
            candidates.extend(search_dir.glob("dataflow-*.md"))
            candidates.extend(search_dir.glob("dataflow_*.md"))

    if not candidates:
        return ""

    # 优先匹配函数名
    if function_name:
        for c in candidates:
            if function_name.lower() in c.name.lower():
                return str(c)

    # 取最新修改的
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(candidates[0])


def _get_best_output(worker: WorkerResult) -> str:
    """获取最佳 Worker 的输出：优先用 dataflow 文件，回退用 result 摘要。"""
    if worker.dataflow_file:
        try:
            content = Path(worker.dataflow_file).read_text(encoding="utf-8")
            if content.strip():
                return content
        except OSError:
            pass
    return worker.output


def _parse_callees(dataflow_content: str) -> list[CalleeRef]:
    """从 Worker 的 dataflow 文件中解析'需要跟入的函数调用'表格。"""
    callees: list[CalleeRef] = []
    in_table = False
    for line in dataflow_content.split("\n"):
        stripped = line.strip()
        if "需要跟入的函数调用" in stripped:
            in_table = True
            continue
        if in_table and stripped.startswith("|"):
            cells = [c.strip() for c in stripped.split("|")]
            cells = [c for c in cells if c]
            if len(cells) < 2:
                continue
            if cells[0] in ("函数名", "Function") or cells[0].startswith("---"):
                continue
            desc = cells[4] if len(cells) > 4 else ""
            # 检查所有列是否含有“未找到定义”/“EXPORT”/“extern”
            all_cols = " ".join(cells)
            if "未找到定义" in all_cols or "EXPORT" in all_cols.upper() or "extern" in all_cols.lower():
                continue
            # 函数名有效性检查：必须是合法标识符
            fname = cells[0]
            if not re.match(r'^[A-Za-z_]\w*$', fname) or fname in ('N/A', 'NA', 'None', 'null', 'void'):
                continue
            callees.append(CalleeRef(
                function_name=cells[0],
                file=cells[1] if len(cells) > 1 else "",
                line=cells[2] if len(cells) > 2 else "",
                tainted_params=cells[3] if len(cells) > 3 else "",
                description=desc,
            ))
        elif in_table and stripped and not stripped.startswith("|"):
            if not stripped.startswith("---") and not stripped.startswith("*"):
                in_table = False
    return callees


def _extract_json_object(text: str, required_key: str) -> dict | None:
    """从文本中提取包含指定 key 的 JSON 对象。支持多行、嵌套引号、转义字符。"""
    # 先尝试从 code block 中提取
    code_match = re.search(r"```(?:json)?\s*\n(.*?)\n\s*```", text, re.DOTALL)
    if code_match:
        try:
            obj = json.loads(code_match.group(1))
            if isinstance(obj, dict) and required_key in obj:
                return obj
        except json.JSONDecodeError:
            pass

    # 找所有 '{' 的位置，尝试从每个位置开始解析完整 JSON
    for i, ch in enumerate(text):
        if ch != '{':
            continue
        # 快速跳过明显不是目标 JSON 的（如 C 代码的 {）
        ahead = text[i:i+100]
        if required_key not in ahead and '"' not in ahead[:30]:
            continue
        # 尝试匹配平衡的 {}
        depth = 0
        in_str = False
        escape = False
        for j in range(i, len(text)):
            c = text[j]
            if escape:
                escape = False
                continue
            if c == '\\':
                if in_str:
                    escape = True
                continue
            if c == '"' and not escape:
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    candidate = text[i:j+1]
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict) and required_key in obj:
                            return obj
                    except json.JSONDecodeError:
                        pass
                    break
    return None


def _parse_eval_md(output: str) -> dict:
    """从 Judge 的输出中解析评审结果。优先解析 markdown，回退到 JSON。"""
    score = 0
    passed = False
    feedback = ""
    refinement = ""

    # ═══ 尝试 markdown 解析 ═══

    # 提取评分
    m = re.search(r'##\s*评分[::=：]\s*(\d+)', output)
    if not m:
        m = re.search(r'##\s*[Ss]core[::=：]\s*(\d+)', output)
    if m:
        score = min(int(m.group(1)), 100)

    # 提取通过/不通过
    m = re.search(r'##\s*通过[::=：]\s*(是|否|true|false|yes|no|pass|fail)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
    elif score >= 70:
        passed = True

    # 提取评审意见
    m = re.search(r'##\s*评审意见\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Ff]eedback\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    # 提取改进指令
    m = re.search(r'##\s*改进指令\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Rr]efinement\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        refinement = m.group(1).strip()

    # markdown 解析成功（至少拿到了分数）
    if score > 0:
        if not feedback:
            feedback = output[:500]
        return {"pass": passed, "score": score, "feedback": feedback, "refinement": refinement}

    # ═══ 回退 JSON 解析 ═══

    obj = _extract_json_object(output, "pass")
    if obj:
        return {
            "pass": bool(obj.get("pass", False)),
            "score": int(obj.get("score", 0)),
            "feedback": str(obj.get("feedback", "")),
            "refinement": str(obj.get("refinement", "")),
        }

    # ═══ 最后尝试从任意文本中抽取分数 ═══

    sm = re.search(r'(\d{1,3})\s*/\s*100|\b(\d{2,3})分', output)
    if sm:
        score = int(sm.group(1) or sm.group(2))
        passed = score >= 70
        return {"pass": passed, "score": score, "feedback": output[:500], "refinement": ""}

    return {"pass": False, "score": 0, "feedback": output[:500], "refinement": ""}


def _parse_summary_md(output: str) -> dict:
    """从 Judge 的输出中解析综合对比结果。优先 markdown，回退 JSON。"""
    best_worker = ""
    overall_passed = False
    reasoning = ""

    # ═══ 尝试 markdown 解析 ═══

    m = re.search(r'##\s*最佳\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Bb]est\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    m = re.search(r'##\s*整体通过[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Oo]verall.*?[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        overall_passed = m.group(1).lower() in ('是', 'true', 'yes')

    m = re.search(r'##\s*(?:对比理由|理由|[Rr]easoning)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        reasoning = m.group(1).strip()

    if best_worker:
        if not reasoning:
            reasoning = output[:500]
        return {"best_worker": best_worker, "reasoning": reasoning, "overall_passed": overall_passed}

    # ═══ 回退 JSON 解析 ═══

    obj = _extract_json_object(output, "best_worker")
    if obj:
        return {
            "best_worker": str(obj.get("best_worker", obj.get("best_worker_id", ""))),
            "reasoning": str(obj.get("reasoning", "")),
            "overall_passed": bool(obj.get("overall_passed", obj.get("pass", False))),
        }

    # ═══ 最后尝试从任意文本中找 worker-X ═══

    m = re.search(r'(worker-\d+)\s*(?:最优|最好|胜出|best|winner)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'(?:最优|最好|胜出|best|winner).*?(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    return {"best_worker": best_worker, "reasoning": output[:500], "overall_passed": overall_passed}


# ─── 编排器 ───────────────────────────────────────────────────────────────────

class Orchestrator:

    def __init__(
        self,
        config: TaskConfig,
        on_event: Callable[[SwarmEvent], None] | None = None,
        session_dir: str = "./sessions",
    ):
        self.cfg = config
        self.on_event = on_event or (lambda e: None)
        self.session_dir = os.path.abspath(session_dir)
        self._cancel_event: asyncio.Event | None = None

    def _emit(self, etype: str, task_id: str, **data):
        try:
            self.on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
        except Exception:
            pass

    async def execute(self, task_id: str | None = None, *, archive: bool = True) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)  # /data/target（只读，源文件在这里）
        threshold = cfg.pass_threshold or math.ceil(cfg.judge_count / 2)
        self._cancel_event = asyncio.Event()

        out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
        out_dir.mkdir(parents=True, exist_ok=True)
        sess_dir = out_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)

        # 每个 Worker 独立可写工作目录（包含 target 文件的符号链接）
        worker_cwds: list[str] = []
        for i in range(cfg.worker_count):
            wdir = out_dir / f"workspace-worker-{i}"
            wdir.mkdir(exist_ok=True)
            # 将 target 目录下的文件链接到 worker 工作目录
            if os.path.isdir(target_dir):
                for item in os.listdir(target_dir):
                    src = os.path.join(target_dir, item)
                    dst = str(wdir / item)
                    if not os.path.exists(dst):
                        try:
                            os.symlink(src, dst)
                        except OSError:
                            pass
            worker_cwds.append(str(wdir))

        worker_dir_prompts = load_system_prompts(cfg.workers.system_prompt_dir, cfg.worker_count)
        judge_dir_prompts = load_system_prompts(cfg.judges.system_prompt_dir, cfg.judge_count)

        # Worker session 文件（跨轮保持）
        worker_sessions = [str(sess_dir / f"worker-{i}.jsonl") for i in range(cfg.worker_count)]

        result = TaskResult(task_id=task_id, status=TaskStatus.RUNNING,
                            task=cfg.task, config_snapshot=cfg.model_dump())

        agents_desc = ([f"worker-{i}={a.model}" for i, a in enumerate(cfg.workers.agents)]
                       + [f"judge-{i}={a.model}" for i, a in enumerate(cfg.judges.agents)])
        self._emit("task_start", task_id, task=cfg.task, agents=agents_desc)

        try:
            feedback_for_workers = ""

            for rnd_num in range(1, cfg.max_rounds + 1):
                if self._cancel_event.is_set():
                    break

                self._emit("round_start", task_id, round=rnd_num)
                rnd_dir = out_dir / f"round-{rnd_num}"
                rnd_workers_dir = rnd_dir / "workers"
                rnd_judges_dir = rnd_dir / "judges"
                rnd_workers_dir.mkdir(parents=True, exist_ok=True)
                rnd_judges_dir.mkdir(parents=True, exist_ok=True)

                # ═══════════════════════════════════════════════════════
                # 1. Workers 并行执行
                # ═══════════════════════════════════════════════════════

                worker_prompt = self._build_worker_prompt(
                    cfg.task, cfg.context, rnd_num, feedback_for_workers)

                w_tasks = []
                for i, acfg in enumerate(cfg.workers.agents):
                    wid = f"worker-{i}"
                    self._emit("worker_start", task_id, worker_id=wid,
                               model=acfg.model, round=rnd_num)
                    w_tasks.append({
                        "prompt": worker_prompt,
                        "model": acfg.model,
                        "tools": acfg.tools or cfg.workers.default_tools,
                        "system_prompt": resolve_system_prompt(i, acfg, worker_dir_prompts),
                        "cwd": worker_cwds[i],
                        "thinking_level": acfg.thinking_level or cfg.workers.default_thinking_level,
                        "session_file": worker_sessions[i],
                        "cancel_event": self._cancel_event,
                        "max_retries": cfg.agent_max_retries,
                        "retry_delay": cfg.agent_retry_delay,
                        "on_stream": lambda d, wid=wid: self._emit(
                            "worker_stream", task_id, worker_id=wid, delta=d),
                    })

                w_raw = await run_agents_parallel(w_tasks, concurrency=WORKER_CONCURRENCY)

                round_workers: list[WorkerResult] = []
                for i, wr in enumerate(w_raw):
                    wid = f"worker-{i}"
                    output = _extract_result(wr.output)
                    result.total_tokens += wr.token_usage

                    # 从 Worker 工作目录搜索 dataflow-*.md 文件
                    df_file = _find_dataflow_file(worker_cwds[i], cfg.function_name)
                    df_content = ""
                    if df_file:
                        try:
                            df_content = Path(df_file).read_text(encoding="utf-8")
                        except OSError:
                            pass

                    self._emit("worker_done", task_id, worker_id=wid,
                               output=output[:500],
                               dataflow_found=bool(df_file))
                    round_workers.append(WorkerResult(
                        worker_id=wid, model=cfg.workers.agents[i].model,
                        output=output, dataflow_file=df_file or "",
                        token_usage=wr.token_usage, error=wr.error))

                    # 归档 worker 摘要输出
                    (rnd_workers_dir / f"{wid}-output.md").write_text(output, encoding="utf-8")
                    # 归档 dataflow 文件（如果存在）
                    if df_content:
                        (rnd_workers_dir / f"{wid}-dataflow.md").write_text(df_content, encoding="utf-8")

                # ═══════════════════════════════════════════════════════
                # 2. Judges 逐个评判（每个 Judge 内多轮对话）
                # ═══════════════════════════════════════════════════════

                # Judge 之间并行，每个 Judge 内部串行（逐个评 Worker → 总结）
                for j_idx, j_acfg in enumerate(cfg.judges.agents):
                    self._emit("judge_start", task_id, judge_id=f"judge-{j_idx}",
                               model=j_acfg.model, round=rnd_num)

                async def _run_one_judge(j_idx: int, j_acfg: AgentInstanceConfig) -> JudgeRoundResult:
                    return await self._run_judge_evaluation(
                        judge_idx=j_idx,
                        judge_cfg=j_acfg,
                        judge_sys_prompt=resolve_system_prompt(j_idx, j_acfg, judge_dir_prompts),
                        round_workers=round_workers,
                        task_id=task_id,
                        rnd_num=rnd_num,
                        cwd=target_dir,
                        sess_dir=sess_dir,
                        rnd_judges_dir=rnd_judges_dir,
                    )

                judge_tasks_async = [
                    _run_one_judge(j_idx, j_acfg)
                    for j_idx, j_acfg in enumerate(cfg.judges.agents)
                ]
                round_judges: list[JudgeRoundResult] = list(await asyncio.gather(*judge_tasks_async))

                # 汇总事件 + token
                for j_idx, j_result in enumerate(round_judges):
                    jid = f"judge-{j_idx}"
                    result.total_tokens += j_result.token_usage
                    for ev in j_result.evaluations:
                        self._emit("judge_eval", task_id, judge_id=jid,
                                   worker_id=ev.worker_id, passed=ev.passed,
                                   score=ev.score, feedback=ev.feedback[:200])
                    if j_result.summary:
                        self._emit("judge_summary", task_id, judge_id=jid,
                                   best=j_result.summary.best_worker_id,
                                   overall_passed=j_result.summary.overall_passed,
                                   reasoning=j_result.summary.reasoning[:200])

                # ═══════════════════════════════════════════════════════
                # 3. 汇总投票
                # ═══════════════════════════════════════════════════════

                pass_count = sum(1 for j in round_judges
                                 if j.summary and j.summary.overall_passed)
                # 对于单 worker 场景，用每个 judge 对该 worker 的 passed
                if cfg.worker_count == 1:
                    pass_count = sum(
                        1 for j in round_judges
                        if j.evaluations and j.evaluations[0].passed)

                is_passed = pass_count >= threshold

                # 找出最佳 worker（多数票）
                best_votes: Counter[str] = Counter()
                for j in round_judges:
                    if j.summary and j.summary.best_worker_id:
                        best_votes[j.summary.best_worker_id] += 1
                best_wid = best_votes.most_common(1)[0][0] if best_votes else round_workers[0].worker_id

                # 生成 feedback.md
                feedback_md = self._build_feedback_md(
                    round_workers, round_judges, best_wid, rnd_num)
                (rnd_dir / "feedback.md").write_text(feedback_md, encoding="utf-8")

                rnd = RoundResult(
                    round=rnd_num,
                    worker_results=round_workers,
                    judge_results=round_judges,
                    pass_count=pass_count,
                    total_judges=cfg.judge_count,
                    passed=is_passed,
                    best_worker_id=best_wid,
                    feedback_to_workers=feedback_md,
                )
                result.rounds.append(rnd)

                self._emit("round_end", task_id, round=rnd_num,
                           passed=is_passed, pass_count=pass_count,
                           total_judges=cfg.judge_count, best_worker=best_wid)

                if is_passed and rnd_num >= cfg.min_rounds:
                    result.status = TaskStatus.PASSED
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = _get_best_output(best_w)
                    break

                if is_passed and rnd_num < cfg.min_rounds:
                    self._emit("round_reflection", task_id, round=rnd_num,
                               message=f"Round {rnd_num} passed but min_rounds={cfg.min_rounds}, forcing reflection")

                # 下一轮的反馈
                feedback_for_workers = feedback_md
                if rnd_num == cfg.max_rounds:
                    result.status = TaskStatus.FAILED
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = _get_best_output(best_w)

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # ═══════════════════════════════════════════════════════════════
        # 最终处理：归档 + 格式化输出 + 压缩 + 清理
        # ═══════════════════════════════════════════════════════════════

        # 1) 写入报告到工作目录
        (out_dir / "report.md").write_text(self._report(result), encoding="utf-8")
        (out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        if not archive:
            # 子任务模式：不压缩/不清理/不写 result_dir，留给根任务统一处理
            self._emit("task_end", task_id,
                       status=result.status.value, function=cfg.function_name)
            self._cancel_event = None
            return result

        # 2) 格式化最终输出 → 写到 result_dir（挂载的输出目录）
        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        cleaned_output = self._format_final_output(result)
        result_filename = self._make_result_filename(cfg, "md")
        (result_dir / result_filename).write_text(cleaned_output, encoding="utf-8")
        result.final_output = cleaned_output

        # 3) 压缩全部工作过程 → archive_dir/<source_file>_<function_name>_log.zip
        archive_dir = Path(os.path.abspath(cfg.archive_dir))
        archive_dir.mkdir(parents=True, exist_ok=True)
        zip_name = self._make_result_filename(cfg, "zip", suffix="_log")
        zip_path = archive_dir / zip_name
        shutil.make_archive(
            str(zip_path).removesuffix(".zip"),  # base name without .zip
            "zip",
            root_dir=str(out_dir.parent),
            base_dir=out_dir.name,
        )

        # 4) 清理工作目录（压缩包已归档）
        shutil.rmtree(out_dir, ignore_errors=True)

        self._emit("task_end", task_id,
                    status=result.status.value,
                    archive=str(zip_path),
                    result_file=str(result_dir / result_filename))
        self._cancel_event = None
        return result

    def abort(self):
        if self._cancel_event:
            self._cancel_event.set()





    # ═══════════════════════════════════════════════════════════════════════
    # 递归分析入口
    # ═══════════════════════════════════════════════════════════════════════

    async def execute_recursive(
        self,
        task_id: str | None = None,
        depth: int = 0,
        tainted_context: str = "",
        _analyzed: set[str] | None = None,
        _root_out_dir: Path | None = None,
    ) -> TaskResult:
        """递归分析：当前函数 Worker+Judge → 解析子函数 → 递归 → merge。"""
        cfg = self.cfg
        max_depth = cfg.max_trace_depth
        is_root = (depth == 0)
        analyzed = _analyzed if _analyzed is not None else set()

        # 防止重复分析
        func_key = cfg.source_file + "::" + cfg.function_name
        if func_key in analyzed:
            self._emit("trace_skip", task_id or "",
                       function=cfg.function_name, reason="already analyzed")
            skip_out = "# " + cfg.function_name + "\n\n(已在其他分支中分析，跳过)"
            return TaskResult(
                task_id=task_id or make_id(), status=TaskStatus.PASSED,
                task=cfg.task, final_output=skip_out)
        analyzed.add(func_key)

        # 注入深度和污染上下文
        if depth > 0:
            depth_note = "\n\n# 追踪深度: " + str(depth) + "/" + str(max_depth)
        else:
            depth_note = ""
        if tainted_context:
            cfg.context = (cfg.context or "") + "\n\n# 调用者传入的脏数据\n" + tainted_context + depth_note
        elif depth_note:
            cfg.context = (cfg.context or "") + depth_note

        # 子任务的 output_dir 放在根任务目录下
        if _root_out_dir and not is_root:
            sub_dir_name = "depth" + str(depth) + "-" + cfg.function_name[:40]
            cfg.output_dir = str(_root_out_dir / sub_dir_name)

        self._emit("trace_start", task_id or "",
                   function=cfg.function_name, depth=depth, max_depth=max_depth)

        # ─── 步骤1：当前函数的 Worker+Judge 分析（子任务不归档）───
        result = await self.execute(task_id, archive=is_root and max_depth == 0)

        # 确定根任务工作目录
        root_out_dir = _root_out_dir
        if is_root and root_out_dir is None:
            root_out_dir = Path(os.path.abspath(cfg.output_dir)) / result.task_id

        # ─── 步骤2：解析子函数列表 ───
        if depth >= max_depth:
            self._emit("trace_depth_limit", result.task_id,
                       function=cfg.function_name, depth=depth)
            if is_root:
                self._do_final_archive(result, root_out_dir)
            return result

        callees = _parse_callees(result.final_output)
        # 防护：过滤自递归 + 已分析 + 单层上限
        MAX_CALLEES_PER_LEVEL = 10
        filtered: list[CalleeRef] = []
        for c in callees:
            c_key = (c.file or cfg.source_file) + "::" + c.function_name
            if c.function_name == cfg.function_name:
                continue
            if c_key in analyzed:
                continue
            filtered.append(c)
        callees = filtered[:MAX_CALLEES_PER_LEVEL]

        if callees:
            self._emit("trace_callees", result.task_id,
                       function=cfg.function_name,
                       callees=[c.function_name for c in callees], depth=depth)

        # ─── 步骤3：递归分析每个子函数 ───
        sub_dataflow_files: list[tuple[str, str]] = []  # (func_name, dataflow_path)

        for callee in callees:
            sub_file = callee.file or cfg.source_file
            sub_prompt = (
                "分析文件 " + sub_file + " 中函数 " + callee.function_name + " 的数据流。"
                + " 只追踪以下被污染的参数: " + (callee.tainted_params or "所有参数")
            )

            sub_cfg = cfg.model_copy(deep=True)
            sub_cfg.task = sub_prompt
            sub_cfg.function_name = callee.function_name
            sub_cfg.source_file = sub_file
            ctx_base = cfg.context or ""
            if "# 调用者传入的脏数据" in ctx_base:
                ctx_base = ctx_base.split("# 调用者传入的脏数据")[0].strip()
            sub_cfg.context = ctx_base

            tainted_ctx = ("函数 " + callee.function_name + " 被 " + cfg.function_name
                           + " 在 " + callee.line + " 调用。\n"
                           + "污染参数: " + callee.tainted_params + "\n"
                           + "说明: " + callee.description)

            sub_orch = Orchestrator(config=sub_cfg, on_event=self.on_event)
            sub_id = result.task_id + "-d" + str(depth + 1) + "-" + callee.function_name[:30]

            sub_result = await sub_orch.execute_recursive(
                task_id=sub_id, depth=depth + 1,
                tainted_context=tainted_ctx, _analyzed=analyzed,
                _root_out_dir=root_out_dir)

            result.total_tokens += sub_result.total_tokens
            result.total_duration_ms += sub_result.total_duration_ms

            # 收集子函数 dataflow 路径
            if sub_result.final_output:
                # 将子函数输出写入根工作目录
                if root_out_dir:
                    safe_name = re.sub(r'[^A-Za-z0-9_-]', '_', callee.function_name)
                    sub_df_path = root_out_dir / ("dataflow-" + safe_name + ".md")
                    sub_df_path.write_text(sub_result.final_output, encoding="utf-8")
                    sub_dataflow_files.append((callee.function_name, str(sub_df_path)))

        # ─── 步骤4：根层合并 + 归档 ───
        if is_root:
            # 将根函数的 dataflow 也保存
            if root_out_dir and result.final_output:
                root_df_path = root_out_dir / ("dataflow-" + cfg.function_name + ".md")
                root_df_path.write_text(result.final_output, encoding="utf-8")
                sub_dataflow_files.insert(0, (cfg.function_name, str(root_df_path)))

            # 运行 merge agent 合并所有 dataflow
            if sub_dataflow_files and root_out_dir:
                merged = await self._run_merge_agent(
                    root_function=cfg.function_name,
                    dataflow_files=sub_dataflow_files,
                    cwd=str(root_out_dir),
                    result=result)
                if merged:
                    result.final_output = merged

            self._do_final_archive(result, root_out_dir)

        return result

    def _do_final_archive(self, result: TaskResult, root_out_dir: Path | None):
        """统一归档：写报告 + 压缩 + 输出结果文件 + 清理。"""
        cfg = self.cfg
        if not root_out_dir or not root_out_dir.exists():
            return

        # 写报告
        (root_out_dir / "report.md").write_text(self._report(result), encoding="utf-8")
        (root_out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        # 格式化最终输出
        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        cleaned_output = self._format_final_output(result)
        result_filename = self._make_result_filename(cfg, "md")
        (result_dir / result_filename).write_text(cleaned_output, encoding="utf-8")
        result.final_output = cleaned_output

        # 压缩全部（包含所有子任务工作目录）
        archive_dir = Path(os.path.abspath(cfg.archive_dir))
        archive_dir.mkdir(parents=True, exist_ok=True)
        zip_name = self._make_result_filename(cfg, "zip", suffix="_log")
        zip_path = archive_dir / zip_name
        shutil.make_archive(
            str(zip_path).removesuffix(".zip"),
            "zip",
            root_dir=str(root_out_dir.parent),
            base_dir=root_out_dir.name,
        )

        # 清理
        shutil.rmtree(root_out_dir, ignore_errors=True)

        self._emit("task_end", result.task_id,
                    status=result.status.value,
                    archive=str(zip_path),
                    result_file=str(result_dir / result_filename))

    async def _run_merge_agent(
        self,
        root_function: str,
        dataflow_files: list[tuple[str, str]],
        cwd: str,
        result: TaskResult,
    ) -> str | None:
        """合并所有子函数 dataflow 为统一的完整数据流文档。"""
        cfg = self.cfg
        if not dataflow_files:
            return None

        # 生成 trace-tree.md 供 merge agent 读取
        tree_lines = ["# 调用树结构\n"]
        for name, path in dataflow_files:
            tree_lines.append("- `" + name + "` → `" + os.path.basename(path) + "`")
        tree_path = Path(cwd) / "trace-tree.md"
        tree_path.write_text("\n".join(tree_lines), encoding="utf-8")

        # 文件列表
        file_list = "\n".join(
            "- `" + os.path.basename(path) + "` — " + name
            for name, path in dataflow_files
        )
        merge_prompt = (
            "# 合并任务\n\n"
            "根函数: " + root_function + "\n"
            "共 " + str(len(dataflow_files)) + " 个数据流文档需要合并。\n\n"
            "请使用 `read` 工具读取以下文件，然后按照 system prompt 中的格式规范合并：\n\n"
            + file_list + "\n\n"
            "调用树结构文件: `trace-tree.md`\n\n"
            "合并结果写入: `merged-dataflow.md`"
        )

        # 加载 merge 专用 system prompt
        merge_prompt_dir = os.path.join(
            os.path.dirname(cfg.workers.system_prompt_dir), "merge")
        sys_prompt = ""
        for p in [os.path.join(merge_prompt_dir, "default.md"),
                  "/opt/data_flow_analyse/prompts/merge/default.md"]:
            if os.path.isfile(p):
                sys_prompt = Path(p).read_text(encoding="utf-8")
                break

        self._emit("merge_start", result.task_id, function=root_function,
                    file_count=len(dataflow_files))

        w_cfg = cfg.workers.agents[0] if cfg.workers.agents else AgentInstanceConfig(model="")
        ar = await run_agent(
            prompt=merge_prompt,
            model=w_cfg.model,
            tools=["read", "write", "bash"],
            system_prompt=sys_prompt,
            cwd=cwd,
            thinking_level=w_cfg.thinking_level or "off",
            session_file=None,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
        )

        result.total_tokens += ar.token_usage

        # 搜索合并后的文件
        merged_path = Path(cwd) / "merged-dataflow.md"
        if not merged_path.exists():
            merged_path = Path(cwd) / ("merged-dataflow-" + root_function + ".md")
        if merged_path.exists():
            content = merged_path.read_text(encoding="utf-8")
            self._emit("merge_done", result.task_id, size=len(content))
            return content

        if ar.output and len(ar.output) > 200:
            self._emit("merge_done", result.task_id, size=len(ar.output))
            return ar.output

        self._emit("merge_failed", result.task_id, error=ar.error or "no output")
        return None

    # ═══════════════════════════════════════════════════════════════════════
    # Judge 多轮评判逻辑
    # ═══════════════════════════════════════════════════════════════════════

    async def _run_judge_evaluation(
        self,
        judge_idx: int,
        judge_cfg,
        judge_sys_prompt: str,
        round_workers: list[WorkerResult],
        task_id: str,
        rnd_num: int,
        cwd: str,
        sess_dir: Path,
        rnd_judges_dir: Path,
    ) -> JudgeRoundResult:
        """
        一个 Judge 在一轮中的完整评审流程（每步独立上下文）：
          1. 对每个 Worker：新起上下文 → 评测 → 写 eval 文件
          2. 新起上下文 → 读取所有 eval 文件 → 综合对比 → 写 summary

        设计目的：防止 Worker 之间的评审互相影响。
        """
        cfg = self.cfg
        jid = f"judge-{judge_idx}"

        j_dir = rnd_judges_dir / jid
        j_dir.mkdir(parents=True, exist_ok=True)

        j_result = JudgeRoundResult(
            judge_id=jid,
            model=judge_cfg.model,
        )

        base_kwargs = {
            "model": judge_cfg.model,
            "tools": judge_cfg.tools or cfg.judges.default_tools,
            "system_prompt": judge_sys_prompt,
            "cwd": str(j_dir),   # Judge 的 cwd 指向自己的输出目录
            "thinking_level": judge_cfg.thinking_level or cfg.judges.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
        }

        # ═══ 步骤0：准备 Worker 输出文件（放入 Judge 工作目录）═══

        for w in round_workers:
            # 摘要输出
            (j_dir / f"{w.worker_id}-output.md").write_text(
                w.output, encoding="utf-8")
            # dataflow 文件
            df_dst = j_dir / f"{w.worker_id}-dataflow.md"
            if w.dataflow_file:
                try:
                    df_content = Path(w.dataflow_file).read_text(encoding="utf-8")
                    df_dst.write_text(df_content, encoding="utf-8")
                except OSError:
                    df_dst.write_text(
                        f"# ⚠️ Dataflow file not found: {w.dataflow_file}",
                        encoding="utf-8")
            else:
                df_dst.write_text(
                    "# ⚠️ Worker did not produce a dataflow file",
                    encoding="utf-8")

        # ═══ 步骤1：逐个评判（每个 Worker 独立上下文）═══════════

        for w in round_workers:
            eval_prompt = self._build_eval_prompt(
                cfg.task, cfg.criteria, w, rnd_num,
                output_path=f"{w.worker_id}-output.md",
                dataflow_path=f"{w.worker_id}-dataflow.md",
            )

            # 独立上下文：session_file=None → --no-session
            ar = await run_agent(
                prompt=eval_prompt, **base_kwargs, session_file=None)
            j_result.token_usage += ar.token_usage

            parsed = _parse_eval_md(ar.output)
            ev = WorkerEvaluation(
                worker_id=w.worker_id,
                passed=parsed["pass"],
                score=parsed["score"],
                feedback=parsed["feedback"],
                refinement=parsed["refinement"],
            )
            j_result.evaluations.append(ev)

            # 归档 eval 结果
            (j_dir / f"eval-{w.worker_id}.md").write_text(
                f"# {jid} → {w.worker_id} (Round {rnd_num})\n\n"
                f"- **Model**: {judge_cfg.model}\n"
                f"- **Pass**: {ev.passed}\n"
                f"- **Score**: {ev.score}\n\n"
                f"## Feedback\n\n{ev.feedback}\n\n"
                f"## Refinement\n\n{ev.refinement}\n",
                encoding="utf-8",
            )

        # ═══ 步骤2：综合对比（新上下文，读取 eval 文件）═══════════

        if len(round_workers) >= 2:
            eval_files = [f"eval-{w.worker_id}.md" for w in round_workers]
            summary_prompt = self._build_summary_prompt(
                round_workers, j_result.evaluations, eval_files)

            # 独立上下文
            ar = await run_agent(
                prompt=summary_prompt, **base_kwargs, session_file=None)
            j_result.token_usage += ar.token_usage

            parsed = _parse_summary_md(ar.output)
            j_result.summary = JudgeSummary(
                best_worker_id=parsed["best_worker"],
                reasoning=parsed["reasoning"],
                overall_passed=parsed["overall_passed"],
            )

            (j_dir / "summary.md").write_text(
                f"# {jid} Summary (Round {rnd_num})\n\n"
                f"- **Best Worker**: {j_result.summary.best_worker_id}\n"
                f"- **Overall Passed**: {j_result.summary.overall_passed}\n\n"
                f"## Reasoning\n\n{j_result.summary.reasoning}\n",
                encoding="utf-8",
            )
        else:
            ev = j_result.evaluations[0]
            j_result.summary = JudgeSummary(
                best_worker_id=ev.worker_id,
                reasoning=ev.feedback,
                overall_passed=ev.passed,
            )

        return j_result

    # ═══════════════════════════════════════════════════════════════════════
    # 提示词
    # ═══════════════════════════════════════════════════════════════════════

    def _build_worker_prompt(self, task, context, rnd, feedback):
        parts = [f"# Task\n\n{task}"]
        if context:
            parts.append(f"# Additional Context\n\n{context}")
        if rnd > 1 and feedback:
            parts.append(
                f"# Feedback from Round {rnd - 1}\n\n"
                f"Your previous work was evaluated. Here is the full feedback report:\n\n"
                f"{feedback}\n\n"
                f"Address ALL issues. Improve your output based on this feedback.")
        parts.append("Wrap your final deliverable in <result>...</result> tags.")
        return "\n\n".join(parts)

    def _build_eval_prompt(self, task, criteria, worker: WorkerResult, rnd,
                           output_path: str = "", dataflow_path: str = ""):
        parts = [
            f"# Evaluate {worker.worker_id} (Round {rnd})",
            f"## Task Requirements\n\n{task}",
        ]
        if criteria:
            parts.append(f"## Evaluation Criteria\n\n{criteria}")

        parts.append(
            f"## {worker.worker_id}'s Output Files\n\n"
            f"Worker 的摘要输出文件: `{output_path}`\n"
            f"Worker 的数据流分析文档: `{dataflow_path}`\n\n"
            f"**请使用 read 工具读取以上两个文件，然后进行评测。**"
        )

        parts.append(
            "评测完成后，请严格按以下 markdown 格式输出结果：\n\n"
            "```\n"
            "## 评分: <0-100的整数>\n"
            "## 通过: <是/否>\n"
            "## 评审意见\n"
            "<详细评审，引用具体行号、变量名、函数名>\n"
            "## 改进指令\n"
            "<按优先级列出可操作的改进项，如果通过则写“无”>\n"
            "```")
        return "\n\n".join(parts)

    def _build_summary_prompt(self, workers: list[WorkerResult],
                               evals: list[WorkerEvaluation],
                               eval_files: list[str]):
        parts = ["# Compare All Workers\n"]
        parts.append("You have evaluated each worker individually. "
                     "Read the evaluation files below, then compare them.\n")
        for ev, fpath in zip(evals, eval_files):
            parts.append(
                f"- **{ev.worker_id}**: Score {ev.score}, "
                f"{'PASS' if ev.passed else 'FAIL'} — evaluation file: `{fpath}`")
        parts.append(
            "\n**请使用 read 工具读取以上所有 eval 文件，然后给出综合对比。**\n"
            "\n对比完成后，请严格按以下 markdown 格式输出：\n\n"
            "```\n"
            "## 最佳Worker: <worker-X>\n"
            "## 整体通过: <是/否>\n"
            "## 对比理由\n"
            "<解释为什么这个 worker 最好，以及整体是否达标>\n"
            "```\n"
            "注意: `整体通过` 写 `是` 仅当最佳 worker 的输出满足所有要求。")
        return "\n".join(parts)

    # ═══════════════════════════════════════════════════════════════════════
    # feedback.md 生成
    # ═══════════════════════════════════════════════════════════════════════

    def _build_feedback_md(
        self,
        workers: list[WorkerResult],
        judges: list[JudgeRoundResult],
        best_wid: str,
        rnd: int,
    ) -> str:
        lines = [
            f"# Round {rnd} Feedback",
            "",
            f"**Best Worker**: {best_wid}",
            "",
        ]

        # 汇总各 Judge 对最佳 worker 的评价
        lines.append("## Why Best")
        for j in judges:
            if j.summary:
                lines.append(f"- {j.judge_id} ({j.model}): {j.summary.reasoning[:300]}")
        lines.append("")

        # 每个 worker 的具体反馈
        for w in workers:
            lines.append(f"## Feedback for {w.worker_id} ({w.model})")
            if w.worker_id == best_wid:
                lines.append(f"*You were rated the best this round. Keep up the good work.*\n")
            else:
                lines.append(f"*{best_wid} was rated better. Study the differences and improve.*\n")

            for j in judges:
                ev = next((e for e in j.evaluations if e.worker_id == w.worker_id), None)
                if ev:
                    lines.append(f"### {j.judge_id} ({j.model}) — Score: {ev.score}")
                    lines.append(f"**Feedback**: {ev.feedback}")
                    if ev.refinement:
                        lines.append(f"**To improve**: {ev.refinement}")
                    lines.append("")

        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════════════════
    # 报告
    # ═══════════════════════════════════════════════════════════════════════

    def _report(self, result: TaskResult) -> str:
        L = [
            f"# Task Report: {result.task_id}", "",
            f"- **Status**: {result.status.value}",
            f"- **Task**: {result.task}",
            f"- **Rounds**: {len(result.rounds)}",
            f"- **Duration**: {result.total_duration_ms / 1000:.1f}s",
            f"- **Cost**: ${result.total_tokens.cost:.4f}", "",
            "## Agent Models", "",
        ]
        for i, a in enumerate(self.cfg.workers.agents):
            L.append(f"- worker-{i}: `{a.model}`")
        for i, a in enumerate(self.cfg.judges.agents):
            L.append(f"- judge-{i}: `{a.model}`")
        L.append("")

        for rnd in result.rounds:
            icon = "✅ PASSED" if rnd.passed else "❌ FAILED"
            L.append(f"## Round {rnd.round}  —  {icon} ({rnd.pass_count}/{rnd.total_judges})")
            L.append(f"**Best Worker**: {rnd.best_worker_id}\n")

            L.append("### Worker Outputs\n")
            for w in rnd.worker_results:
                L.append(f"#### {w.worker_id} (`{w.model}`)")
                L.append(f"```\n{w.output[:2000]}\n```\n")

            L.append("### Judge Evaluations\n")
            for j in rnd.judge_results:
                L.append(f"#### {j.judge_id} (`{j.model}`)\n")
                for ev in j.evaluations:
                    p = "✅" if ev.passed else "❌"
                    L.append(f"- {ev.worker_id}: {p} Score {ev.score} — {ev.feedback[:200]}")
                if j.summary:
                    L.append(f"\n**Summary**: Best={j.summary.best_worker_id}, "
                             f"Passed={j.summary.overall_passed}")
                    L.append(f"> {j.summary.reasoning[:300]}\n")

            if rnd.feedback_to_workers:
                L.append(f"### Feedback to Workers\n")
                L.append(f"{rnd.feedback_to_workers[:2000]}\n")

        if result.error:
            L.append(f"## Error\n\n{result.error}")
        return "\n".join(L)

    # ═══════════════════════════════════════════════════════════════════
    # 格式化输出 + 文件命名
    # ═══════════════════════════════════════════════════════════════════

    @staticmethod
    def _format_final_output(result: TaskResult) -> str:
        """
        格式化最终通过的 Worker 输出：
        - 去除 <result> 标签
        - 清理多余空行
        - 添加元信息头
        """
        raw = result.final_output
        # 去除残留的 <result> 标签
        raw = re.sub(r"</?result>", "", raw)
        # 清理连续空行（>2 行压缩为 2 行）
        raw = re.sub(r"\n{3,}", "\n\n", raw).strip()

        best_wid = ""
        best_model = ""
        final_round = 0
        if result.rounds:
            last = result.rounds[-1]
            final_round = last.round
            best_wid = last.best_worker_id
            bw = next((w for w in last.worker_results if w.worker_id == best_wid), None)
            if bw:
                best_model = bw.model

        header = (
            f"---\n"
            f"task_id: {result.task_id}\n"
            f"status: {result.status.value}\n"
            f"best_worker: {best_wid}\n"
            f"model: {best_model}\n"
            f"rounds: {final_round}\n"
            f"duration: {result.total_duration_ms / 1000:.1f}s\n"
            f"cost: ${result.total_tokens.cost:.4f}\n"
            f"---\n\n"
        )
        return header + raw

    @staticmethod
    def _make_result_filename(cfg: TaskConfig, ext: str, suffix: str = "") -> str:
        """
        生成输出文件名：<source_file>_<function_name><suffix>.<ext>
        如：firmware_parse_packet_log.zip 或 firmware_parse_packet.md
        """
        src = cfg.source_file or "unknown"
        func = cfg.function_name or "unknown"
        # 清理文件名中的不安全字符
        src = re.sub(r"[^\w.-]", "_", Path(src).stem)
        func = re.sub(r"[^\w.-]", "_", func)
        return f"{src}_{func}{suffix}.{ext}"
