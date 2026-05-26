"""
orchestrator.py — 核心编排引擎

Orchestrator: 管理单函数分析的 Worker+Judge 轮次循环
execute_recursive: BFS 队列 + Worker Pool 递归分析调用链
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import time
import hashlib
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
    TraceNode,
    WorkerEvaluation,
    WorkerResult,
    CalleeRef,
    make_id,
    normalize_max_rounds_exceeded_review_strategy,
)
from .runner import run_agent, run_agents_parallel
from .taint_workflow import PerTaintWorkflow
from .judge_runner import JudgeMixin
from .parsers import (
    _extract_result,
    _find_dataflow_file,
    _read_tainted_list,
    _parse_callees,
    _parse_eval_md,
    _parse_summary_md,
    _STDLIB_SKIP,
    _get_best_output,
)
from .cpp_resolver import _function_has_definition, _resolve_cpp_name, _get_definition_line
from .prompt_builder import (
    _build_worker_prompt,
    _build_eval_prompt,
    _build_summary_prompt,
    _build_feedback_md,
    _report,
    _format_final_output,
    _make_result_filename,
    _build_combined_report,
)



def _round_dir_name(round_num: int) -> str:
    return f"round_{round_num:03d}"


def _safe_dir_name(value: str, *, max_len: int = 120) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    if not safe:
        safe = "item"
    if len(safe) <= max_len:
        return safe
    digest = hashlib.sha1(value.encode("utf-8", errors="replace")).hexdigest()[:10]
    return f"{safe[:max_len - 11]}-{digest}"


def _nested_function_run_dir(root_out_dir: Path, tid: str, dep: int, func_name: str) -> Path:
    if dep <= 0:
        return root_out_dir
    logical = f"{tid}-{func_name}"
    return root_out_dir / "subtasks" / f"depth_{dep:02d}" / _safe_dir_name(logical)


def _relativize_round_artifacts(result: TaskResult, round_root: Path, root_out_dir: Path) -> None:
    for rnd in result.rounds or []:
        for worker in rnd.worker_results or []:
            if getattr(worker, "session_file", None):
                try:
                    worker.session_file = str((round_root / worker.session_file).resolve().relative_to(root_out_dir.resolve())).replace("\\", "/")
                except Exception:
                    try:
                        worker.session_file = str(Path(worker.session_file).resolve().relative_to(root_out_dir.resolve())).replace("\\", "/")
                    except Exception:
                        pass
        for judge in rnd.judge_results or []:
            if getattr(judge, "session_file", None):
                try:
                    judge.session_file = str((round_root / judge.session_file).resolve().relative_to(root_out_dir.resolve())).replace("\\", "/")
                except Exception:
                    try:
                        judge.session_file = str(Path(judge.session_file).resolve().relative_to(root_out_dir.resolve())).replace("\\", "/")
                    except Exception:
                        pass

class Orchestrator(JudgeMixin):

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

    def _is_cancelled(self) -> bool:
        return bool(self._cancel_event and self._cancel_event.is_set())

    def _raise_if_cancelled(self) -> None:
        if self._is_cancelled():
            raise asyncio.CancelledError("orchestrator cancelled")

    async def execute(
        self,
        task_id: str | None = None,
        *,
        archive: bool = True,
        depth: int = 0,
        max_depth: int = 0,
        run_dir: Path | None = None,
    ) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)  # /data/target(只读,源文件在这里)
        threshold = cfg.pass_threshold if cfg.pass_threshold is not None else math.ceil(cfg.judge_count / 2)
        self._cancel_event = asyncio.Event()
        max_rounds_strategy = normalize_max_rounds_exceeded_review_strategy(
            getattr(cfg, "max_rounds_exceeded_review_strategy", None)
        )

        # 任务目录结构: output_dir/task_id/input|run|output。递归函数子任务只在根 run/subtasks 下建中间目录。
        if run_dir is not None:
            out_dir = run_dir
            task_base = out_dir.parent
        else:
            task_base = Path(os.path.abspath(cfg.output_dir)) / task_id
            out_dir = task_base / "run"
        out_dir.mkdir(parents=True, exist_ok=True)

        # 归档模式且非子任务:立即写 flag=0 到 output/ 目录
        if archive:
            output_path = task_base / "output"
            output_path.mkdir(parents=True, exist_ok=True)
            (output_path / "flag").write_text("0", encoding="utf-8")

        sess_dir = out_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)

        # 每个 Worker 独立可写工作目录(包含 target 文件的符号链接 + chroot 式环境隔离)
        worker_cwds: list[str] = []
        worker_envs: list[dict] = []
        for i in range(cfg.worker_count):
            wdir = out_dir / f"workspace-worker-{i}"
            wdir.mkdir(exist_ok=True)
            # tmp 子目录隔离临时文件
            wtmp = wdir / "tmp"
            wtmp.mkdir(exist_ok=True)
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
            worker_envs.append({**os.environ, "HOME": str(wdir), "TMPDIR": str(wtmp)})


        worker_dir_prompts = load_system_prompts(cfg.workers.system_prompt_dir, cfg.worker_count)
        judge_dir_prompts = load_system_prompts(cfg.judges.system_prompt_dir, cfg.judge_count)

        # Worker session 文件(跨轮保持)
        worker_sessions = [str(sess_dir / f"worker-{i}.jsonl") for i in range(cfg.worker_count)]

        result = TaskResult(task_id=task_id, status=TaskStatus.RUNNING,
                            task=cfg.task, config_snapshot=cfg.model_dump())

        agents_desc = ([f"worker-{i}={a.model}" for i, a in enumerate(cfg.workers.agents)]
                       + [f"judge-{i}={a.model}" for i, a in enumerate(cfg.judges.agents)])
        self._emit("task_start", task_id, task=cfg.task, agents=agents_desc,
                   function=cfg.function_name, depth=depth)

        # 将 depth/max_depth 注入 context,Worker 据此决定是否需要追踪 callee
        # max_depth=0 时不注入(外部调用者未指定深度)
        if max_depth > 0:
            _depth_note = f"\n\n# 当前追踪深度: {depth}/{max_depth}" + (
                "(已达最大深度,callee 表格仍需填写但系统不会递归)"
                if depth >= max_depth else ""
            )
            cfg.context = (cfg.context or "").rstrip() + _depth_note

        try:
            feedback_for_workers = ""

            for rnd_num in (range(1, cfg.max_rounds + 1) if cfg.max_rounds >= 0 else __import__('itertools').count(1)):
                if self._is_cancelled():
                    break

                self._emit("round_start", task_id, round=rnd_num,
                           function=cfg.function_name, depth=depth)
                rnd_dir = out_dir / _round_dir_name(rnd_num)
                rnd_workers_dir = rnd_dir / "workers"
                rnd_judges_dir = rnd_dir / "judges"
                rnd_workers_dir.mkdir(parents=True, exist_ok=True)
                rnd_judges_dir.mkdir(parents=True, exist_ok=True)

                # ═══════════════════════════════════════════════════════
                # 1. Workers 并行执行
                # ═══════════════════════════════════════════════════════

                worker_prompt = _build_worker_prompt(
                    cfg.task, cfg.context, rnd_num, feedback_for_workers,
                    function_name=cfg.function_name,
                    source_file=cfg.source_file,
                    function_description=cfg.function_description,
                    function_description_source=cfg.function_description_source,
                    entry_reason=cfg.entry_reason,
                    entry_reason_source=cfg.entry_reason_source,
                    taint_details=cfg.taint_details,
                )

                w_tasks = []
                # write-dataflow skill 已安装到 ~/.pi/agent/skills/（支持该机制的模型可自动发现）
                # GLM-5 不支持 /skill: 命令，阶段4指令已直接内嵌入 Worker system prompt
                for i, acfg in enumerate(cfg.workers.agents):
                    self._raise_if_cancelled()
                    wid = f"worker-{i}"
                    self._emit("worker_start", task_id, worker_id=wid,
                               model=acfg.model, round=rnd_num,
                               function=cfg.function_name)
                    w_tasks.append({
                        "prompt": worker_prompt,
                        "model": acfg.model,
                        "tools": acfg.tools or cfg.workers.default_tools,
                        "system_prompt": resolve_system_prompt(i, acfg, worker_dir_prompts),
                        "cwd": worker_cwds[i],
                        "env": worker_envs[i],
                        "thinking_level": acfg.thinking_level or cfg.workers.default_thinking_level,
                        "session_file": worker_sessions[i],
                        # RPC 第二轮：分析完成后强制写入 tainted.list
                        "post_skill_prompt": (
                            "Based on your taint analysis above, now write the tainted.list file.\n\n"
                            "Use the **write** tool to create `tainted.list` in the current directory.\n"
                            "Format — one line per callee function that receives tainted parameters:\n\n"
                            "```\n"
                            "file_path###Class::FuncName###L_line###param1,param2\n"
                            "```\n\n"
                            "Rules:\n"
                            "- Only functions where tainted data flows IN as arguments\n"
                            "- NO getters, condition checks, logging, or stdlib functions\n"
                            "- file_path: path relative to workspace root (e.g. src-vul/openthread/...)\n"
                            "- Class::FuncName: fully qualified name\n"
                            "- params: the CALLEE's formal parameter names (not caller's variable names)\n"
                            "- Unknown field: use `-` for path/line, `*` for params\n\n"
                            "If no functions need follow-up (leaf function), write:\n"
                            "`# no callees`\n\n"
                            "Write ONLY tainted.list — do not rewrite the dataflow file."
                        ),
                        "cancel_event": self._cancel_event,
                        "max_retries": cfg.agent_max_retries,
                        "retry_delay": cfg.agent_retry_delay,
                        "run_timeout_seconds": cfg.agent_run_timeout_seconds,
                        "timeout_retry_enabled": cfg.agent_timeout_retry_enabled,
                        "timeout_max_retries": cfg.agent_timeout_max_retries,
                        "pi_max_retries": cfg.pi_max_retries,
                        "pi_retry_delay": cfg.pi_retry_delay,
                        "on_stream": lambda d, wid=wid: self._emit(
                            "worker_stream", task_id, worker_id=wid, delta=d),
                    })

                self._raise_if_cancelled()
                w_raw = await run_agents_parallel(w_tasks, concurrency=cfg.worker_count)
                self._raise_if_cancelled()

                round_workers: list[WorkerResult] = []
                for i, wr in enumerate(w_raw):
                    self._raise_if_cancelled()
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

                            pass
                    # 后置校验:检查 dataflow 文件结构完整性
                    df_issues: list[str] = []
                    if not df_file or len(df_content.strip()) < 100:
                        df_issues.append(
                            f"[F1] {wid} 未将分析结果写入 dataflow-*.md 文件(或文件为空)\n"
                            f"     请使用 write 工具将完整分析写入 dataflow-{cfg.function_name}.md"
                        )
                    else:
                        # 检查是否包含目标函数名
                        func_short = cfg.function_name.split("::")[-1]
                        if func_short not in df_content and cfg.function_name not in df_content:
                            df_issues.append(
                                f"[F2] dataflow 文件内容不包含目标函数名 '{cfg.function_name}',"
                                f"可能分析了错误的函数\n     请确认分析的是 {cfg.source_file} 中的 {cfg.function_name}"
                            )
                    self._emit("worker_done", task_id, worker_id=wid,
                               output=output[:500],
                               dataflow_found=bool(df_file) and not df_issues,
                               df_issues=df_issues,
                               function=cfg.function_name)
                    round_workers.append(WorkerResult(
                        worker_id=wid, model=cfg.workers.agents[i].model,
                        output=output, dataflow_file=df_file or "",
                        token_usage=wr.token_usage, error=wr.error,
                        df_issues=df_issues))


                    # 每轮结束归档 session(不管成败都保留,供调试和下轮继续利用)
                    _sess_src = Path(worker_sessions[i])
                    if _sess_src.exists():
                        try:
                            import shutil as _shu
                            _shu.copy2(str(_sess_src), str(rnd_workers_dir / f"{wid}-session.jsonl"))
                        except OSError:
                            pass

                    # 归档 worker 摘要输出
                    (rnd_workers_dir / f"{wid}-output.md").write_text(output, encoding="utf-8")
                    # 归档 dataflow 文件(如果存在)
                    if df_content:
                        (rnd_workers_dir / f"{wid}-dataflow.md").write_text(df_content, encoding="utf-8")

                # ═══════════════════════════════════════════════════════
                # 2. Judges 逐个评判(每个 Judge 内多轮对话)
                # ═══════════════════════════════════════════════════════

                # Judge 之间并行,每个 Judge 内部串行(逐个评 Worker → 总结)
                for j_idx, j_acfg in enumerate(cfg.judges.agents):
                    self._emit("judge_start", task_id, judge_id=f"judge-{j_idx}",
                               model=j_acfg.model, round=rnd_num,
                               function=cfg.function_name)

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
                self._raise_if_cancelled()
                round_judges: list[JudgeRoundResult] = list(await asyncio.gather(*judge_tasks_async))
                self._raise_if_cancelled()

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
                # 对于单 worker 场景,用每个 judge 对该 worker 的 passed
                if cfg.worker_count == 1:
                    pass_count = sum(
                        1 for j in round_judges
                        if j.evaluations and j.evaluations[0].passed)

                is_passed = pass_count >= threshold

                # 找出最佳 worker(多数票)
                best_votes: Counter[str] = Counter()
                for j in round_judges:
                    if j.summary and j.summary.best_worker_id:
                        best_votes[j.summary.best_worker_id] += 1
                best_wid = best_votes.most_common(1)[0][0] if best_votes else round_workers[0].worker_id

                # 生成 feedback.md
                feedback_md = _build_feedback_md(
                    round_workers, round_judges, best_wid, rnd_num)
                (rnd_dir / "feedback.md").write_text(feedback_md, encoding="utf-8")

                rnd = RoundResult(
                    round=rnd_num,
                    worker_results=round_workers,
                    judge_results=round_judges,
                    pass_count=pass_count,
                    total_judges=cfg.judge_count,
                    passed=is_passed,
                    status="passed" if is_passed else "failed",
                    best_worker_id=best_wid,
                    feedback_to_workers=feedback_md,
                    completion_reason="passed" if is_passed else "",
                )
                result.rounds.append(rnd)

                self._emit("round_end", task_id, round=rnd_num,
                           passed=is_passed, pass_count=pass_count,
                           total_judges=cfg.judge_count, best_worker=best_wid,
                           function=cfg.function_name)

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
                if cfg.max_rounds >= 0 and rnd_num == cfg.max_rounds:
                    best_w = next((w for w in round_workers if w.worker_id == best_wid), round_workers[0])
                    result.final_output = _get_best_output(best_w)
                    if max_rounds_strategy == "treat_as_passed":
                        result.status = TaskStatus.PASSED
                        rnd.status = "passed_with_max_rounds_policy"
                        rnd.module_completed = True
                        rnd.completion_reason = "max_rounds_exceeded_treated_as_passed"
                        result.completion_reason = "max_rounds_exceeded_treated_as_passed"
                    else:
                        result.status = TaskStatus.FAILED
                        rnd.completion_reason = "max_rounds_exceeded"
                        result.completion_reason = "max_rounds_exceeded"
                    break

        except asyncio.CancelledError:
            result.status = TaskStatus.ERROR
            result.error = "cancelled"
            self._emit("error", task_id, error="cancelled")
        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # ═══════════════════════════════════════════════════════════════
        # 最终处理:归档 + 格式化输出 + 压缩 + 清理
        # ═══════════════════════════════════════════════════════════════

        # 1) 写入报告到工作目录 (run/)
        (out_dir / "report.md").write_text(_report(result, cfg), encoding="utf-8")
        (out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        if not archive:
            # 子任务模式:不写 output/，由根任务统一处理
            # ★ 关键:将 dataflow 文件内容写入 final_output 供 callee 解析
            _df_path = _find_dataflow_file(out_dir, cfg.function_name)
            if _df_path:
                try:
                    _df_text = Path(_df_path).read_text(encoding="utf-8")
                    if len(_df_text.strip()) > len((result.final_output or "").strip()):
                        result.final_output = _df_text
                except OSError:
                    pass
            self._cancel_event = None
            return result

        # 2) 格式化最终输出 → 写到 task_id/output/ 目录
        output_path = task_base / "output"
        output_path.mkdir(parents=True, exist_ok=True)
        cleaned_output = _format_final_output(result)
        result_filename = "final_report.md"
        (output_path / result_filename).write_text(cleaned_output, encoding="utf-8")
        result.final_output = cleaned_output

        # 3) 写 flag 文件(仅 PASSED 覆盖为 1,其他保持入口处写的 0)
        if result.status == TaskStatus.PASSED:
            (output_path / "flag").write_text("1", encoding="utf-8")

        self._emit("task_end", task_id,
                    status=result.status.value,
                    run_dir=str(out_dir),
                    result_file=str(output_path / result_filename))
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
        _root_output_dir: Path | None = None,
        resume: bool = False,
    ) -> TaskResult:
        """BFS 队列 + 工作池架构:
        A 进入队列 → Worker 执行 W+J 分析 → 解析 callee B,C,D → 加入队列 → Worker 执行 B,C,D...
        并发数 = callee_concurrency(worker 数量),无 Semaphore 死锁问题。
        """
        cfg = self.cfg
        is_root = (depth == 0)

        # ── 非根任务:直接执行 W+J 并返回,由根任务的工作池调度 ──────────────
        if not is_root:
            logical_task_id = task_id or make_id()
            out_dir = (
                _nested_function_run_dir(_root_out_dir, logical_task_id, depth, cfg.function_name)
                if _root_out_dir is not None
                else Path(os.path.abspath(cfg.output_dir)) / logical_task_id / "run"
            )
            result = await self.execute(
                logical_task_id,
                archive=False,
                depth=depth,
                max_depth=cfg.max_trace_depth,
                run_dir=out_dir,
            )
            df_path = _find_dataflow_file(out_dir, cfg.function_name)
            if df_path:
                try:
                    df_text = Path(df_path).read_text(encoding="utf-8")
                    if len(df_text.strip()) > len((result.final_output or "").strip()):
                        result.final_output = df_text
                except OSError:
                    pass
            return result

        # ── 根任务:BFS 队列 + 工作池 ────────────────────────────────────────
        max_depth = cfg.max_trace_depth
        # BFS 并发度：直接使用 callee_concurrency（1-64，1=串行，-1=自动）
        # 控制同时进行 W+J 分析的 BFS 工作池大小，与 taint 内部并行正交
        # callee_concurrency=-1 表示自动（默认 4）
        n_workers = max(1, cfg.callee_concurrency) if cfg.callee_concurrency > 0 else 4
        analyzed: set[str] = _analyzed if _analyzed is not None else set()
        MAX_CALLEES_PER_LEVEL = 10

        # 初始化根目录
        root_task_id = task_id or make_id()
        if _root_out_dir is not None:
            # 已经是 run/ 子目录
            root_out_dir = _root_out_dir
        else:
            root_out_dir = Path(os.path.abspath(cfg.output_dir)) / root_task_id / "run"
        root_out_dir.mkdir(parents=True, exist_ok=True)

        # flag=0 写入 output/ 目录
        root_output_path = root_out_dir.parent / "output"
        root_output_path.mkdir(parents=True, exist_ok=True)
        (root_output_path / "flag").write_text("0", encoding="utf-8")

        queue: asyncio.Queue = asyncio.Queue()
        all_results: dict[str, TaskResult] = {}   # func_key -> TaskResult
        sub_dataflow_files: list[tuple[str, str]] = []

        # 注册根函数
        root_key = cfg.source_file + "::" + cfg.function_name
        analyzed.add(root_key)

        self._emit("task_start", root_task_id, task=cfg.task,
                   agents=f"W={cfg.worker_count} J={cfg.judge_count}",
                   function=cfg.function_name, source_file=cfg.source_file)
        self._emit("trace_start", root_task_id,
                   function=cfg.function_name, source_file=cfg.source_file,
                   depth=0, max_depth=max_depth)

        # 根任务入队
        await queue.put((cfg.function_name, cfg.source_file, cfg.line_hint,
                         cfg.model_copy(deep=True), root_task_id, 0, tainted_context))

        async def process_item(item: tuple) -> None:
            self._raise_if_cancelled()
            func_name, src_file, line_hint, task_cfg, tid, dep, taint_ctx = item

            # 通知 CLI: 新函数开始
            self._emit("trace_start", tid,
                       function=func_name, source_file=src_file,
                       depth=dep, max_depth=max_depth)

            # 注入 tainted_context
            if taint_ctx and dep > 0:
                ctx_base = task_cfg.context or ""
                if "# 调用者传入的脏数据" in ctx_base:
                    ctx_base = ctx_base.split("# 调用者传入的脏数据")[0].strip()
                task_cfg.context = ctx_base + "\n\n# 调用者传入的脏数据\n" + taint_ctx

            # dataflow 输出目录
            df_dir = root_out_dir / "dataflow"
            df_dir.mkdir(exist_ok=True)
            safe_func = re.sub(r'[^A-Za-z0-9_:.-]', '_', func_name)
            out_dir = _nested_function_run_dir(root_out_dir, tid, dep, func_name)
            out_dir.mkdir(parents=True, exist_ok=True)

            # ── 断点续跑：检查是否已有缓存分析结果 ─────────────────────────
            result: TaskResult | None = None
            if resume:
                _cached_df = _find_dataflow_file(out_dir, func_name)
                if not _cached_df:
                    _cached_df_path = df_dir / f"{safe_func}.md"
                    if _cached_df_path.exists():
                        _cached_df = str(_cached_df_path)
                if _cached_df:
                    try:
                        cached_output = Path(_cached_df).read_text(encoding="utf-8")
                        result = TaskResult(
                            task_id=tid,
                            status=TaskStatus.PASSED,
                            task=task_cfg.task,
                            final_output=cached_output,
                        )
                        self._emit("trace_skip", tid, function=func_name,
                                   reason="resume: cached result reused", depth=dep)
                    except OSError:
                        result = None

            if result is None:
                self._raise_if_cancelled()
                # 执行：使用 PerTaintWorkflow 实现多 session 并行污点分析
                _taint_match = re.search(r'外部输入参数.*?为[:：]\s*([^\n]+)',
                                        task_cfg.task or "")
                _taint_list: list[str] = []
                if _taint_match:
                    raw_taints = _taint_match.group(1)
                    _taint_list = [re.sub(r'[。，,（(].*', '', t).strip().strip('`')
                                   for t in raw_taints.split(',') if t.strip()]
                    _taint_list = [t for t in _taint_list if t and re.match(r'^[A-Za-z_]', t)]
                if not _taint_list:
                    _tc_m = re.search(r'污染参数[:\uff1a]\s*([^\n]+)', taint_ctx or "")
                    if _tc_m:
                        _taint_list = [t.strip().strip('`') for t in _tc_m.group(1).split(',') if t.strip()]
                if not _taint_list:
                    _taint_list = ["all"]

                workflow = PerTaintWorkflow(
                    cfg=task_cfg,
                    func_name=func_name,
                    src_file=src_file,
                    line_hint=line_hint,
                    taint_params=_taint_list,
                    taint_ctx=taint_ctx or "",
                    task_id=tid,
                    out_dir=out_dir,
                    dep=dep,
                    max_depth=max_depth,
                    on_event=self.on_event,
                    cancel_event=self._cancel_event,
                )
                result = await workflow.run()
                self._raise_if_cancelled()

            _relativize_round_artifacts(result, out_dir, root_out_dir)

            # 通知 CLI: 函数分析完成
            self._emit("round_end", tid,
                       passed=(result.status.value == "passed"),
                       function=func_name, depth=dep)
            df_path = _find_dataflow_file(out_dir, func_name)
            if df_path:
                try:
                    df_text = Path(df_path).read_text(encoding="utf-8")
                    if len(df_text.strip()) > len((result.final_output or "").strip()):
                        result.final_output = df_text
                except OSError:
                    pass

            # 强制生成 tainted.list：不依赖 LLM 自觉写文件
            # 优先保留 LLM 已写的版本（文件路径/参数名更准确），否则由 Orchestrator 从 _parse_callees 生成
            if result.final_output:
                _callees_for_list = _parse_callees(result.final_output)
                if _callees_for_list:
                    for _ws in out_dir.glob("workspace-worker-*/"):
                        _tainted_path = _ws / "tainted.list"
                        if not _tainted_path.exists():
                            _lines = []
                            for _c in _callees_for_list:
                                _f = _c.file or "-"
                                _l = _c.line or "-"
                                _p = _c.tainted_params or "*"
                                _lines.append(f"{_f}###{_c.function_name}###{_l}###{_p}")
                            try:
                                _tainted_path.write_text(
                                    "\n".join(_lines) + "\n", encoding="utf-8")
                            except OSError:
                                pass

            # 保存 dataflow/funcname.md(供 callee 解析 + merge)
            if result.final_output:
                dest = df_dir / f"{safe_func}.md"
                try:
                    dest.write_text(result.final_output, encoding="utf-8")
                    sub_dataflow_files.append((func_name, str(dest)))
                except OSError:
                    pass

            func_key = src_file + "::" + func_name
            all_results[func_key] = result

            # ── 解析 callee 并加入队列 ─────────────────────────────────────
            if dep < max_depth and result.final_output:
                # 优先读取 tainted.list，无则 fallback 到解析 dataflow 文件
                worker_cwd = str(out_dir)
                callees = _read_tainted_list(worker_cwd)
                if not callees:
                    callees = _parse_callees(result.final_output)
                target_dir = os.path.abspath(task_cfg.cwd)
                valid: list[CalleeRef] = []
                for callee in callees:
                    # 标准化 c_key:只用函数名(不含文件),避免路径差异导致误 dup
                    c_key = callee.function_name
                    # 跳过自引用（完整名或短名匹配）
                    if callee.function_name == func_name:
                        continue
                    if callee.function_name.split("::")[-1] == func_name.split("::")[-1]:
                        continue
                    if c_key in analyzed:
                        self._emit("trace_skip", tid, function=callee.function_name,
                                   reason="already analyzed")
                        continue
                    if callee.function_name in _STDLIB_SKIP:
                        continue
                    if not _function_has_definition(target_dir, callee.function_name):
                        self._emit("trace_skip", tid, function=callee.function_name,
                                   reason="no definition found")
                        continue
                    analyzed.add(c_key)
                    valid.append(callee)

                # Fallback: 若 tainted.list 全被过滤（如自引用），尝试解析 taint-flow-*.md
                if not valid:
                    _taint_flow_callees: list[CalleeRef] = []
                    for _tf in out_dir.glob("workspace-worker-*/taint-flow-*.md"):
                        try:
                            _taint_flow_callees.extend(
                                _parse_callees(_tf.read_text(encoding="utf-8")))
                        except OSError:
                            pass
                    for callee in _taint_flow_callees:
                        c_key = callee.function_name
                        if callee.function_name == func_name:
                            continue
                        if callee.function_name.split("::")[-1] == func_name.split("::")[-1]:
                            continue
                        if c_key in analyzed:
                            continue
                        if callee.function_name in _STDLIB_SKIP:
                            continue
                        if not _function_has_definition(target_dir, callee.function_name):
                            continue
                        analyzed.add(c_key)
                        valid.append(callee)
                    if valid:
                        self._emit("debug", tid,
                                   message=f"taint-flow fallback: found {len(valid)} callees for {func_name}")

                if valid:
                    self._emit("trace_callees", tid, function=func_name,
                               callees=[c.function_name for c in valid], depth=dep)

                for callee in valid[:MAX_CALLEES_PER_LEVEL]:
                    self._raise_if_cancelled()
                    sub_file = callee.file or src_file
                    sub_cfg = task_cfg.model_copy(deep=True)
                    sub_cfg.function_name = callee.function_name
                    sub_cfg.source_file = sub_file
                    sub_cfg.task = (
                        f"对 {sub_file} 的 {callee.function_name} 函数进行静态污点分析,"
                        f"外部输入参数(已污染)为:{callee.tainted_params or '所有参数'}"
                    )
                    ctx_base = task_cfg.context or ""
                    if "# 调用者传入的脏数据" in ctx_base:
                        ctx_base = ctx_base.split("# 调用者传入的脏数据")[0].strip()
                    sub_cfg.context = ctx_base
                    tainted_ctx_str = (
                        f"函数 {callee.function_name} 被 {func_name} 在 {callee.line} 调用。\n"
                        f"污染参数: {callee.tainted_params}\n说明: {callee.description}"
                    )
                    sub_tid = tid + f"-d{dep + 1}-{callee.function_name[:25]}"
                    sub_line_hint = _get_definition_line(
                        target_dir, callee.function_name, sub_file)
                    if not sub_line_hint:
                        # fallback: 调用点行号（粗略）
                        sub_line_hint = callee.line if callee.line.startswith("L") else (
                            "L" + callee.line.lstrip("L") if callee.line else "")
                    if self._is_cancelled():
                        break
                    await queue.put((callee.function_name, sub_file, sub_line_hint, sub_cfg,
                                     sub_tid, dep + 1, tainted_ctx_str))

        async def worker(wid: int) -> None:
            while True:
                if self._is_cancelled() and queue.empty():
                    break
                item = await queue.get()
                if item is None:      # sentinel → 退出
                    queue.task_done()
                    break
                try:
                    await process_item(item)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    tid = item[3] if len(item) > 3 else "?"
                    self._emit("error", tid, error=str(e))
                finally:
                    queue.task_done()

        # 启动工作池(n_workers 个并发 Worker+Judge 会话)
        workers = [asyncio.create_task(worker(i)) for i in range(n_workers)]

        # 等待所有任务处理完毕
        await queue.join()
        if self._is_cancelled():
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise asyncio.CancelledError("recursive orchestration cancelled")

        # 发送终止 sentinel
        for _ in range(n_workers):
            await queue.put(None)
        await asyncio.gather(*workers)

        # ── 根函数结果 ────────────────────────────────────────────────────────
        root_result = all_results.get(root_key)
        if root_result is None:
            root_result = TaskResult(task_id=root_task_id, task=cfg.task,
                                     status=TaskStatus.ERROR,
                                     error="root function analysis failed")
        combined_rounds = []
        combined_tokens = TokenUsage()
        total_duration_ms = 0.0
        for item in all_results.values():
            combined_rounds.extend(item.rounds or [])
            combined_tokens += item.total_tokens
            total_duration_ms += float(item.total_duration_ms or 0.0)
        root_result.rounds = combined_rounds
        root_result.total_tokens = combined_tokens
        if total_duration_ms > 0:
            root_result.total_duration_ms = total_duration_ms

        # ── 综合报告:程序化构建(主) + LLM 增强(可选) ────────────────────────
        if sub_dataflow_files:
            # 1. 程序化合并 — 始终成功，产出完整跨函数报告
            root_result.final_output = _build_combined_report(
                root_function=cfg.function_name,
                dataflow_files=sub_dataflow_files,
            )
            # 2. 尝试 LLM merge 增强 — 失败时保留程序化报告，不阻断流程
            try:
                self._raise_if_cancelled()
                merged = await self._run_merge_agent(
                    root_function=cfg.function_name,
                    dataflow_files=sub_dataflow_files,
                    cwd=str(root_out_dir),
                    result=root_result)
                # 只在 LLM 产出明显更丰富时才替换（避免空输出或过短输出覆盖）
                if merged and len(merged.strip()) > len(root_result.final_output) // 2:
                    root_result.final_output = merged
            except Exception as e:
                self._emit("merge_skipped", root_task_id,
                           error=f"LLM merge failed, keeping programmatic report: {e}")

        # ── 最终归档 ──────────────────────────────────────────────────────────
        self._do_final_archive(root_result, root_out_dir, _root_output_dir)
        return root_result

    def _do_final_archive(self, result: TaskResult, root_out_dir: Path | None, root_output_dir: Path | None = None):
        """统一归档:写报告 + 输出结果文件到 output/ 目录 + 写 flag。不创建压缩包,不清理工作目录。"""
        cfg = self.cfg
        if not root_out_dir or not root_out_dir.exists():
            return

        # 写报告到 run/ 目录
        (root_out_dir / "report.md").write_text(_report(result, cfg), encoding="utf-8")
        (root_out_dir / "result.json").write_text(result.model_dump_json(indent=2), encoding="utf-8")

        # output/ 目录存放最终输出
        output_path = root_output_dir or (root_out_dir.parent / "output")
        output_path.mkdir(parents=True, exist_ok=True)

        # 格式化最终输出 → output/
        cleaned_output = _format_final_output(result)
        result_filename = "final_report.md"
        (output_path / result_filename).write_text(cleaned_output, encoding="utf-8")
        result.final_output = cleaned_output

        # 将 dataflow/ 文件夹复制到 output/dataflow/
        df_folder = root_out_dir / "dataflow"
        if df_folder.exists():
            dest_df = output_path / "dataflow"
            if dest_df.exists():
                shutil.rmtree(dest_df, ignore_errors=True)
            try:
                shutil.copytree(str(df_folder), str(dest_df))
            except OSError:
                pass

        # 写 flag 文件(仅 PASSED 为 1,其他保持 0)
        if result.status == TaskStatus.PASSED:
            (output_path / "flag").write_text("1", encoding="utf-8")

        self._emit("task_end", result.task_id,
                    status=result.status.value,
                    run_dir=str(root_out_dir),
                    result_file=str(output_path / result_filename))
