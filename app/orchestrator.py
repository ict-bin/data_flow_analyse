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
    JudgeRoundResult,
    JudgeSummary,
    RoundResult,
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
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


def _parse_eval_json(output: str) -> dict:
    """从 Judge 输出中解析单个 Worker 的评价 JSON。"""
    code_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", output, re.DOTALL)
    text = code_match.group(1) if code_match else output
    json_match = re.search(r'\{[^{}]*"pass"[^{}]*\}', text, re.DOTALL)
    if not json_match:
        json_match = re.search(r"\{[\s\S]*?\}", text)
    if not json_match:
        return {"pass": False, "score": 0, "feedback": f"无效 JSON: {output[:500]}", "refinement": ""}
    try:
        p = json.loads(json_match.group(0))
        return {"pass": bool(p.get("pass", False)), "score": int(p.get("score", 0)),
                "feedback": str(p.get("feedback", "")), "refinement": str(p.get("refinement", ""))}
    except (json.JSONDecodeError, ValueError, TypeError):
        return {"pass": False, "score": 0, "feedback": f"JSON 解析失败: {json_match.group(0)[:300]}", "refinement": ""}


def _parse_summary_json(output: str) -> dict:
    """从 Judge 总结中解析对比结果。"""
    code_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", output, re.DOTALL)
    text = code_match.group(1) if code_match else output
    json_match = re.search(r'\{[^{}]*"best_worker"[^{}]*\}', text, re.DOTALL)
    if not json_match:
        json_match = re.search(r"\{[\s\S]*?\}", text)
    if not json_match:
        return {"best_worker": "", "reasoning": output[:500], "overall_passed": False}
    try:
        p = json.loads(json_match.group(0))
        return {
            "best_worker": str(p.get("best_worker", p.get("best_worker_id", ""))),
            "reasoning": str(p.get("reasoning", "")),
            "overall_passed": bool(p.get("overall_passed", p.get("pass", False))),
        }
    except (json.JSONDecodeError, ValueError, TypeError):
        return {"best_worker": "", "reasoning": "解析失败", "overall_passed": False}


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

    async def execute(self, task_id: str | None = None) -> TaskResult:
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

                if is_passed:
                    result.status = TaskStatus.PASSED
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = _get_best_output(best_w)
                    break

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
        一个 Judge 在一轮中的完整评审流程：
          1. 逐个评判每个 Worker（使用临时 session 保持 Judge 内上下文）
          2. ≥2 个 Worker 时，发送对比总结提示词
          3. 归档所有评判文件 + session 文件
        """
        cfg = self.cfg
        jid = f"judge-{judge_idx}"

        # Judge 临时 session（每轮一个新文件，轮结束后归档保留）
        judge_sess_file = str(sess_dir / f"{jid}-round-{rnd_num}.jsonl")

        j_dir = rnd_judges_dir / jid
        j_dir.mkdir(parents=True, exist_ok=True)

        j_result = JudgeRoundResult(
            judge_id=jid,
            model=judge_cfg.model,
        )

        common_kwargs = {
            "model": judge_cfg.model,
            "tools": judge_cfg.tools or cfg.judges.default_tools,
            "system_prompt": judge_sys_prompt,
            "cwd": cwd,
            "thinking_level": judge_cfg.thinking_level or cfg.judges.default_thinking_level,
            "session_file": judge_sess_file,  # 轮内保持上下文
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
        }

        # ── 逐个评判 ─────────────────────────────────────────

        for w in round_workers:
            eval_prompt = self._build_eval_prompt(cfg.task, cfg.criteria, w, rnd_num)

            ar = await run_agent(prompt=eval_prompt, **common_kwargs)
            j_result.token_usage += ar.token_usage

            parsed = _parse_eval_json(ar.output)
            ev = WorkerEvaluation(
                worker_id=w.worker_id,
                passed=parsed["pass"],
                score=parsed["score"],
                feedback=parsed["feedback"],
                refinement=parsed["refinement"],
            )
            j_result.evaluations.append(ev)

            # 归档
            (j_dir / f"eval-{w.worker_id}.md").write_text(
                f"# {jid} → {w.worker_id} (Round {rnd_num})\n\n"
                f"- **Model**: {judge_cfg.model}\n"
                f"- **Pass**: {ev.passed}\n"
                f"- **Score**: {ev.score}\n\n"
                f"## Feedback\n\n{ev.feedback}\n\n"
                f"## Refinement\n\n{ev.refinement}\n",
                encoding="utf-8",
            )

        # ── 对比总结（≥2 worker）────────────────────────────

        if len(round_workers) >= 2:
            summary_prompt = self._build_summary_prompt(round_workers, j_result.evaluations)

            ar = await run_agent(prompt=summary_prompt, **common_kwargs)
            j_result.token_usage += ar.token_usage

            parsed = _parse_summary_json(ar.output)
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
            # 单 worker：直接用 eval 结果
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

    def _build_eval_prompt(self, task, criteria, worker: WorkerResult, rnd):
        parts = [
            f"# Evaluate {worker.worker_id} (Round {rnd})",
            f"## Task Requirements\n\n{task}",
        ]
        if criteria:
            parts.append(f"## Evaluation Criteria\n\n{criteria}")
        parts.append(
            f"## {worker.worker_id}'s Output (model: {worker.model})\n\n"
            f"{worker.output}")
        parts.append(
            "Evaluate this worker's output. Respond with JSON only:\n"
            '{"pass": true/false, "score": 0-100, "feedback": "...", "refinement": "..."}')
        return "\n\n".join(parts)

    def _build_summary_prompt(self, workers: list[WorkerResult], evals: list[WorkerEvaluation]):
        parts = ["# Compare All Workers\n"]
        parts.append("You have evaluated each worker individually. Now compare them.\n")
        for ev in evals:
            parts.append(f"- **{ev.worker_id}**: Score {ev.score}, {'PASS' if ev.passed else 'FAIL'}")
        parts.append(
            "\nRespond with JSON:\n"
            "```json\n"
            '{"best_worker": "worker-X", "reasoning": "Why this worker is best", '
            '"overall_passed": true/false}\n'
            "```\n"
            "`overall_passed` = true only if the best worker's output meets all requirements.")
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
