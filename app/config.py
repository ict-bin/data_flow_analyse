"""
data_flow_analyse — 配置加载 + prompt 解析
"""

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path

from .models import AgentInstanceConfig, RoleConfig, ServiceConfig, TaskConfig

# 容器内固定挂载路径（可通过环境变量覆盖）
# ENV: TARGET_DIR, CONFIG_DIR, OUTPUT_DIR
TARGET_DIR = os.environ.get("TARGET_DIR", "/data/target")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/output")


def load_service_config(config_path: str) -> ServiceConfig:
    """加载服务配置（管理员配置的长期文件）。"""
    p = Path(config_path)
    if not p.is_file():
        raise FileNotFoundError(f"服务配置文件不存在: {config_path}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ServiceConfig(**raw)


def build_task_config(svc: ServiceConfig, prompt: str, cwd: str = None) -> TaskConfig:
    """从服务配置 + 用户一句话 prompt 构造运行时 TaskConfig。

    prompt 示例：
      "对 vfpfwd_board.c 的 VFP_ReceivePktFromNpByPcie 函数完成数据流分析"
      "分析 firmware.c 中 parse_packet 的外部输入数据流"
    """
    if cwd is None:
        cwd = TARGET_DIR
    source_file, function_name = parse_prompt(prompt)

    cfg = TaskConfig(
        task=prompt,
        source_file=source_file,
        function_name=function_name,
        cwd=cwd,
        max_rounds=svc.max_rounds,
        min_rounds=svc.min_rounds,
        pass_threshold=svc.pass_threshold,
        agent_max_retries=svc.agent_max_retries,
        agent_retry_delay=svc.agent_retry_delay,
        pi_max_retries=svc.pi_max_retries,
        pi_retry_delay=svc.pi_retry_delay,
        max_trace_depth=svc.max_trace_depth,
        workers=svc.workers.model_copy(deep=True),
        judges=svc.judges.model_copy(deep=True),
        output_dir=svc.output_dir,
        archive_dir=svc.archive_dir,
        result_dir=svc.result_dir,
    )

    _backfill_role(cfg.workers)
    _backfill_role(cfg.judges)

    if cfg.pass_threshold is None:
        cfg.pass_threshold = math.ceil(cfg.judge_count / 2)

    return cfg


def parse_prompt(prompt: str) -> tuple[str, str]:
    """
    从用户的一句话 prompt 中提取文件名和函数名。

    支持的格式（中英文均可）：
      "对 xxx.c 的 yyy 函数完成数据流分析"
      "分析 xxx.c 中 yyy 的外部输入"
      "分析文件 xxx.c 中函数 yyy 的数据流"
      "analyze xxx.c function yyy"
    """
    source_file = ""
    function_name = ""

    # 尝试匹配文件名（含路径，如 src/foo.c）
    file_patterns = [
        r'(?:文件|file|对|分析)\s+([\w./-]+\.\w+)',          # "对 xxx.c" / "文件 xxx.c"
        r'([\w./-]+\.(?:c|h|cpp|cc|cxx|py|java|go|rs))\b',  # 任何 xxx.c 格式
    ]
    for pat in file_patterns:
        m = re.search(pat, prompt, re.IGNORECASE)
        if m:
            source_file = m.group(1)
            break

    # 尝试匹配函数名
    func_patterns = [
        r'(?:函数|function|的)\s+([A-Za-z_]\w*(?:::[A-Za-z_]\w*)+)',  # C++ 方法名 "Class::Method"
        r'(?:函数|function|的)\s+(\w+)',        # "函数 xxx" / "的 xxx"
        r'(?:中|的)\s+([A-Za-z_]\w*(?:::[A-Za-z_]\w*)+)\s*(?:函数|的|$)',  # C++ 方法名
        r'(?:中|的)\s+(\w+)\s*(?:函数|的|$)',    # "中 xxx 函数"
        r'([A-Za-z_]\w*(?:::[A-Za-z_]\w*)+)\s+(?:函数|function)',  # C++ 方法名
        r'(\w+)\s+(?:函数|function)',            # "xxx 函数"
    ]
    for pat in func_patterns:
        m = re.search(pat, prompt, re.IGNORECASE)
        if m:
            candidate = m.group(1)
            # 排除常见非函数名词
            if candidate not in ("数据流", "外部", "输入", "完成", "分析", "进行",
                                 "data", "flow", "analysis", "the", "input"):
                function_name = candidate
                break

    return source_file, function_name


def _backfill_role(role: RoleConfig) -> None:
    for agent in role.agents:
        if not agent.model:
            agent.model = role.default_model
        if agent.tools is None:
            agent.tools = role.default_tools[:]
        if agent.thinking_level is None:
            agent.thinking_level = role.default_thinking_level


def load_system_prompts(prompt_dir: str, count: int) -> list[str]:
    """从文件夹加载 system prompt。"""
    prompt_dir = os.path.abspath(prompt_dir)
    prompts: list[str] = [""] * count

    if not os.path.isdir(prompt_dir):
        return prompts

    files: dict[str, str] = {}
    for f in sorted(Path(prompt_dir).glob("*.md")):
        files[f.stem] = f.read_text(encoding="utf-8").strip()

    default_text = files.get("default", "")
    prompts = [default_text] * count

    for i in range(count):
        for prefix in [f"worker-{i}", f"judge-{i}", f"{i}"]:
            if prefix in files:
                prompts[i] = files[prefix]
                break

    return prompts


def resolve_system_prompt(
    agent_idx: int,
    agent_cfg: AgentInstanceConfig,
    prompts_from_dir: list[str],
) -> str:
    if agent_cfg.system_prompt:
        return agent_cfg.system_prompt
    if agent_idx < len(prompts_from_dir):
        return prompts_from_dir[agent_idx]
    return ""
