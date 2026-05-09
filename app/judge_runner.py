"""
judge_runner.py — Judge 评审逻辑 (Mixin for Orchestrator)
"""
from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path

from .models import (
    AgentInstanceConfig, JudgeRoundResult, JudgeSummary,
    TaskConfig, WorkerEvaluation, WorkerResult, TokenUsage,
)
from .runner import run_agent
from .parsers import _parse_eval_md, _parse_summary_md
from .prompt_builder import _build_eval_prompt, _build_summary_prompt


class JudgeMixin:
    """Judge 评审相关方法，通过多继承注入 Orchestrator。"""

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
            "- `" + os.path.basename(path) + "` - " + name
            for name, path in dataflow_files
        )
        merge_prompt = (
            "# 数据流分析合并任务\n\n"
            "根函数: " + root_function + "\n"
            "共 " + str(len(dataflow_files)) + " 个数据流分析文档需要合并。\n\n"
            "**要求**:\n"
            "1. 用 `read` 工具逐个读取以下全部文件\n"
            "2. 输出一份**详细完整**的合并报告,要求:\n"
            "   - 每个函数的完整污点传播路径(保留原文格式)\n"
            "   - 调用链树状图(含深度和污点状态)\n"
            "   - 每个函数的关键污点变量汇总表\n"
            "   - 污点终点汇总(EXPORT/USED/CLEANED/DEFERRED)\n"
            "3. 用 `write` 工具将完整报告写入 `merged-dataflow.md`\n\n"
            "文件列表:\n" + file_list + "\n\n"
            "调用树结构文件: `trace-tree.md`"
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
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
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
        一个 Judge 在一轮中的完整评审流程(每步独立上下文):
          1. 对每个 Worker:新起上下文 → 评测 → 写 eval 文件
          2. 新起上下文 → 读取所有 eval 文件 → 综合对比 → 写 summary

        设计目的:防止 Worker 之间的评审互相影响。
        """
        cfg = self.cfg
        jid = f"judge-{judge_idx}"

        j_dir = rnd_judges_dir / jid
        j_dir.mkdir(parents=True, exist_ok=True)

        # 将源码目录文件链接到 Judge 工作目录，使 Judge 能够读取源文件验证分析
        target_dir = os.path.abspath(cwd)
        if os.path.isdir(target_dir):
            for item in os.listdir(target_dir):
                src = os.path.join(target_dir, item)
                dst = str(j_dir / item)
                if not os.path.exists(dst):
                    try:
                        os.symlink(src, dst)
                    except OSError:
                        pass

        j_result = JudgeRoundResult(
            judge_id=jid,
            model=judge_cfg.model,
        )

        base_kwargs = {
            "model": judge_cfg.model,
            "tools": judge_cfg.tools or cfg.judges.default_tools,
            "system_prompt": judge_sys_prompt,
            "cwd": str(j_dir),   # Judge 的 cwd 指向自己的输出目录（含源码符号链接）
            "thinking_level": judge_cfg.thinking_level or cfg.judges.default_thinking_level,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
        }

        # ═══ 步骤0:准备 Worker 输出文件(放入 Judge 工作目录)═══

        for w in round_workers:
            # dataflow 文件放入 Judge 工作目录(output.md 不放入,避免冗余干扰)
            # dataflow 文件
            df_dst = j_dir / f"{w.worker_id}-dataflow.md"

            # 如果有结构性问题,写入问题描述作为代替文件
            if w.df_issues:
                df_dst.write_text(
                    "# ⚠️ 结构性检查失败 - Worker 未正确交付\n\n"
                    + "\n".join(w.df_issues),
                    encoding="utf-8")
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

        # ═══ 步骤1:并行评判所有 Worker(每个 Worker 独立上下文)═════════

        async def _eval_one_worker(w: WorkerResult) -> tuple[WorkerEvaluation, object]:
            # 结构性问题:直接生成 fail,不调用 LLM
            if w.df_issues:
                issues_text = "\n".join(w.df_issues)
                ev = WorkerEvaluation(
                    worker_id=w.worker_id,
                    passed=False,
                    score=0,
                    feedback=f"结构性检查失败,自动不通过:\n{issues_text}",
                    refinement=issues_text,
                )
                (j_dir / f"eval-{w.worker_id}.md").write_text(
                    f"# {jid} → {w.worker_id} (Round {rnd_num}) - 自动不通过\n\n"
                    f"- **原因**: 结构性检查失败\n\n"
                    f"## 问题列表\n\n{issues_text}\n",
                    encoding="utf-8",
                )
                return ev, TokenUsage()  # 不消耗 token
            eval_prompt = _build_eval_prompt(
                cfg.task, w, rnd_num,
                output_path=f"{w.worker_id}-output.md",
                dataflow_path=f"{w.worker_id}-dataflow.md",
            )
            ar = await run_agent(
                prompt=eval_prompt, **base_kwargs, session_file=None)
            parsed = _parse_eval_md(ar.output)
            ev = WorkerEvaluation(
                worker_id=w.worker_id,
                passed=parsed["pass"],
                score=parsed["score"],
                feedback=parsed["feedback"],
                refinement=parsed["refinement"],
            )
            (j_dir / f"eval-{w.worker_id}.md").write_text(
                f"# {jid} \u2192 {w.worker_id} (Round {rnd_num})\n\n"
                f"- **Model**: {judge_cfg.model}\n"
                f"- **Pass**: {ev.passed}\n"
                f"- **Score**: {ev.score}\n\n"
                f"## Feedback\n\n{ev.feedback}\n\n"
                f"## Refinement\n\n{ev.refinement}\n",
                encoding="utf-8",
            )
            return ev, ar.token_usage

        eval_pairs = await asyncio.gather(*[_eval_one_worker(w) for w in round_workers])
        for ev, tokens in eval_pairs:
            j_result.evaluations.append(ev)
            j_result.token_usage += tokens

        # ═══ 步骤2:综合对比(新上下文,读取 eval 文件)═══════════

        if len(round_workers) >= 2:
            eval_files = [f"eval-{w.worker_id}.md" for w in round_workers]
            summary_prompt = _build_summary_prompt(
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

