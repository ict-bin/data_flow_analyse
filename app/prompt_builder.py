"""
prompt_builder.py — Worker/Judge Prompt 构造 + 结果格式化
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from .models import TaskConfig, TaskResult, TaskStatus, WorkerResult, WorkerEvaluation


def _build_worker_prompt(task, context, rnd, feedback,
                          function_name: str = "", source_file: str = ""):
    import uuid
    # 随机 nonce 确保每次请求 token 前缀不同,破坏 vllm prefix cache
    # (防止 temperature=0 加 prefix cache 导致确定性复现"不调 write"的行为)
    nonce = uuid.uuid4().hex[:8]
    # 主任务描述,显式注入输出文件名和只读警告
    task_block = task
    if function_name:
        safe_fn = function_name
        task_block += (
            f"\n\n❗️ **必读:输出文件要求**\n"
            f"- 使用 `write` 工具将分析写入:`dataflow-{safe_fn}.md`(**当前目录下**)\n"
            f"- 文件名就是 `dataflow-{safe_fn}.md`,**不要**写成 `{safe_fn}.dataflow.md` 或加其他路径\n"
            f"- `src-vul/` 目录是只读挂载,导新任何写入都会失败!请将文件写到当前目录"
        )
    parts = [f"<!-- {nonce} -->\n# Task\n\n{task_block}"]
    if context:
        parts.append(f"# Additional Context\n\n{context}")
    if rnd > 1 and feedback:
        # 结构性问题(F1/F2/F3)置顶,避免被长上下文忽视
        is_structural = any(tag in feedback for tag in ("[F1]", "[F2]", "[F3]"))
        if is_structural:
            parts.insert(0,
                f"⚠️ 上一轮交付失败,必须首先修复以下问题(否则本轮仍会自动不通过):\n\n"
                f"{feedback}\n\n"
                f"修复完成后再输出分析内容和 <result>。")
        else:
            parts.append(
                f"# 第 {rnd - 1} 轮反馈\n\n"
                f"上一轮工作已评审,请针对以下反馈改进:\n\n"
                f"{feedback}\n\n"
                f"确保全面解决所有问题。")
    parts.append("用 <result>...</result> 包裹摘要信息。")
    return "\n\n".join(parts)



def _build_eval_prompt(task, worker: WorkerResult, rnd,
                       output_path: str = "", dataflow_path: str = ""):
    CRITERIA = (
        "重点评判维度:外部输入识别完整性、污点追踪深度(子函数必须跟入)、"
        "数据处理函数覆盖、文档规范性、需要跟入的函数列表完整性"
    )
    parts = [
        f"# Evaluate {worker.worker_id} (Round {rnd})",
        f"## Task Requirements\n\n{task}",
        f"## Evaluation Criteria\n\n{CRITERIA}",
    ]

    parts.append(
        f"## {worker.worker_id}'s Output Files\n\n"
        f"`{dataflow_path}` 是 Worker 生成的数据流分析文档(归档名,非原始文件名)。\n\n"
        f"**请使用 read 工具读取该文件,然后进行评测。**\n"
        f"评判时以文件内容为准,不需关注存档路径名。"
    )

    parts.append(
        "评测完成后,请严格按以下 markdown 格式输出结果:\n\n"
        "```\n"
        "## 评分: <0-100的整数>\n"
        "## 通过: <是/否>\n"
        "## 评审意见\n"
        "<详细评审,引用具体行号、变量名、函数名>\n"
        "## 改进指令\n"
        "<按优先级列出可操作的改进项,如果通过则写『无』>\n"
        "```")
    return "\n\n".join(parts)



def _build_summary_prompt(workers: list[WorkerResult],
                           evals: list[WorkerEvaluation],
                           eval_files: list[str]):
    parts = ["# Compare All Workers\n"]
    parts.append("You have evaluated each worker individually. "
                 "Read the evaluation files below, then compare them.\n")
    for ev, fpath in zip(evals, eval_files):
        parts.append(
            f"- **{ev.worker_id}**: Score {ev.score}, "
            f"{'PASS' if ev.passed else 'FAIL'} - evaluation file: `{fpath}`")
    parts.append(
        "\n**请使用 read 工具读取以上所有 eval 文件,然后给出综合对比。**\n"
        "\n对比完成后,请严格按以下 markdown 格式输出:\n\n"
        "```\n"
        "## 最佳Worker: <worker-X>\n"
        "## 整体通过: <是/否>\n"
        "## 对比理由\n"
        "<解释为什么这个 worker 最好,以及整体是否达标>\n"
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
                lines.append(f"### {j.judge_id} ({j.model}) - Score: {ev.score}")
                lines.append(f"**Feedback**: {ev.feedback}")
                if ev.refinement:
                    lines.append(f"**To improve**: {ev.refinement}")
                lines.append("")

    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════
# 报告
# ═══════════════════════════════════════════════════════════════════════



def _report(result: TaskResult, cfg: TaskConfig) -> str:
    L = [
        f"# Task Report: {result.task_id}", "",
        f"- **Status**: {result.status.value}",
        f"- **Task**: {result.task}",
        f"- **Rounds**: {len(result.rounds)}",
        f"- **Duration**: {result.total_duration_ms / 1000:.1f}s",
        f"- **Cost**: ${result.total_tokens.cost:.4f}", "",
        "## Agent Models", "",
    ]
    for i, a in enumerate(cfg.workers.agents):
        L.append(f"- worker-{i}: `{a.model}`")
    for i, a in enumerate(cfg.judges.agents):
        L.append(f"- judge-{i}: `{a.model}`")
    L.append("")

    for rnd in result.rounds:
        icon = "✅ PASSED" if rnd.passed else "❌ FAILED"
        L.append(f"## Round {rnd.round}  -  {icon} ({rnd.pass_count}/{rnd.total_judges})")
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
                L.append(f"- {ev.worker_id}: {p} Score {ev.score} - {ev.feedback[:200]}")
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
    格式化最终通过的 Worker 输出:
    - 去除 <result> 标签
    - 清理多余空行
    - 添加元信息头
    """
    raw = result.final_output
    # 去除残留的 <result> 标签
    raw = re.sub(r"</?result>", "", raw)
    # 清理连续空行(>2 行压缩为 2 行)
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

def _build_combined_report(
    root_function: str,
    dataflow_files: list[tuple[str, str]],
) -> str:
    """从所有子函数 dataflow 文件程序化组装最终综合报告。
    不依赖 LLM merge agent，始终可靠地生成完整的跨函数分析文档。
    """
    total = len(dataflow_files)
    # root 排第一，其余按 BFS 顺序保持原位
    ordered: list[tuple[str, str]] = []
    rest: list[tuple[str, str]] = []
    for name, path in dataflow_files:
        if name == root_function:
            ordered.insert(0, (name, path))
        else:
            rest.append((name, path))
    ordered.extend(rest)

    lines: list[str] = [
        f"# 完整数据流分析: {root_function}",
        "",
        "## 分析概览",
        "",
        f"- **根函数**: `{root_function}`",
        f"- **跟踪函数总数**: {total}",
        "",
        "## 调用链函数列表",
        "",
    ]
    for i, (name, _) in enumerate(ordered):
        marker = "📌 根函数" if name == root_function else "└─ 被跟入"
        lines.append(f"{i + 1}. `{name}` {marker}")
    lines += ["", "---", ""]

    for i, (func_name, path) in enumerate(ordered):
        section_label = "根函数" if func_name == root_function else "被跟入函数"
        lines.append(f"## [{i + 1}/{total}] {func_name}  ·  {section_label}")
        lines.append("")
        try:
            content = Path(path).read_text(encoding="utf-8").strip()
            # 去除重复的 H1 标题行（格式：# 数据流追踪: FuncName）
            content = re.sub(r"^#\s+数据流追踪[：:][^\n]*\n", "", content, count=1)
            lines.append(content)
        except OSError:
            lines.append(f"> ⚠️ 文件读取失败: `{path}`")
        lines += ["", "---", ""]

    return "\n".join(lines)


@staticmethod


def _make_result_filename(cfg: TaskConfig, ext: str, suffix: str = "") -> str:
    """
    生成输出文件名:<source_file>_<function_name><suffix>.<ext>
    如:firmware_parse_packet_log.zip 或 firmware_parse_packet.md
    """
    src = cfg.source_file or "unknown"
    func = cfg.function_name or "unknown"
    # 清理文件名中的不安全字符
    src = re.sub(r"[^\w.-]", "_", Path(src).stem)
    func = re.sub(r"[^\w.-]", "_", func)
    return f"{src}_{func}{suffix}.{ext}"
