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
    """从 Worker 工作目录搜索数据流分析文件。
    兼容多种命名惯例：dataflow-*.md / *.dataflow.md / *dataflow*.md / <funcname>*.md
    """
    cwd = Path(worker_cwd)
    candidates: list[Path] = []

    for search_dir in [cwd, Path("/tmp")]:
        if not search_dir.is_dir():
            continue
        # 常见命名惯例（当前目录）
        for pat in ["dataflow-*.md", "dataflow_*.md", "*.dataflow.md",
                    "*dataflow*.md", "*_analysis.md"]:
            candidates.extend(search_dir.glob(pat))
        # 递归搜索子目录（Worker 可能将文件写到源码子目录下）
        if function_name:
            short = function_name.split("::")[-1]
            candidates.extend(search_dir.rglob(f"*{short}*.md"))
            candidates.extend(search_dir.rglob("*.dataflow.md"))
            candidates.extend(search_dir.glob(f"{short}*.md"))
            candidates.extend(search_dir.glob(f"*{short}*.md"))

    # 去重
    seen: set[str] = set()
    uniq: list[Path] = []
    for c in candidates:
        k = str(c)
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    candidates = uniq

    if not candidates:
        return ""

    # 优先匹配函数名，且内容 > 100 bytes
    if function_name:
        short = function_name.split("::")[-1].lower()
        func_lower = function_name.lower()
        # 首先尝试内容匹配（文件名可能不包含函数名）
        for c in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                sz = c.stat().st_size
                if sz > 200:  # 排除空骨架
                    head = c.read_text(encoding='utf-8', errors='replace')[:500]
                    if short in head.lower() or func_lower in head.lower():
                        # 如果文件不在 cwd 根目录，将内容拷贝到正确路径
                        correct = cwd / f"dataflow-{function_name}.md"
                        if c.resolve() != correct.resolve() and not correct.exists():
                            try:
                                correct.write_text(
                                    c.read_text(encoding='utf-8', errors='replace'),
                                    encoding='utf-8')
                                return str(correct)
                            except OSError:
                                pass
                        return str(c)
            except OSError:
                pass
        # 备选：文件名包含函数名
        for c in candidates:
            if short in c.name.lower() or func_lower in c.name.lower():
                try:
                    if c.stat().st_size > 200:
                        return str(c)
                except OSError:
                    pass

    # 取最新且 > 200 bytes 的文件
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for c in candidates:
        try:
            if c.stat().st_size > 200:
                return str(c)
        except OSError:
            pass
    return str(candidates[0]) if candidates else ""


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
    """从 Worker 的 dataflow 文件中解析'需要跟入的函数调用'表格。
    兼容多种列格式（自动检测函数名列位置）。"""
    callees: list[CalleeRef] = []
    in_table = False
    func_col = -1
    file_col = -1
    line_col = -1
    param_col = -1
    desc_col = -1
    skip_col  = -1  # “是否需要跟入”列

    for line in dataflow_content.split(chr(10)):
        stripped = line.strip()
        # markdown 标题行（# 开头）才触发 callee 表，防止嵌入文本起头的错误匹配
        if stripped.startswith("#") and any(kw in stripped for kw in [
                "函数调用", "跟入", "跟进", "callee", "Callee"]):
            in_table = True
            func_col = -1
            continue
        if in_table and stripped.startswith("##") and not stripped.startswith("###"):
            if not any(kw in stripped for kw in ["函数调用", "跟入", "跟进", "callee"]):
                in_table = False
        if not in_table:
            continue
        if not stripped.startswith("|"):
            continue

        cells = [c.strip() for c in stripped.split("|")]
        cells = [c for c in cells if c]
        if len(cells) < 2:
            continue
        if all(c.startswith("---") or c.startswith(":--") for c in cells):
            continue

        # 检测表头行 → 确定各列位置
        lower_cells = [c.lower() for c in cells]
        is_header = False
        for i, lc in enumerate(lower_cells):
            if lc in ("函数名", "函数调用", "调用函数", "function", "func", "func_name", "callee"):
                func_col = i
                is_header = True
            elif lc in ("序号", "no", "#", "idx", "index"):
                is_header = True  # 序号列不是函数名列
            elif "是否" in lc or "需要跟" in lc or "track" in lc or "follow" in lc:
                skip_col = i
                is_header = True
            elif lc in ("文件", "file"):
                file_col = i
            elif "调用位置" in lc or "行号" in lc or "line" in lc or "call" in lc:
                line_col = i
            elif "污染" in lc or "taint" in lc or "参数" in lc:
                param_col = i
            elif "说明" in lc or "desc" in lc:
                desc_col = i
        if is_header:
            continue

        # 未检测到表头时默认第一列
        if func_col == -1:
            func_col = 0

        # 提取各字段
        # 跳过明确标注不需要跟入的行（包含 ❌ / 否）
        if 0 <= skip_col < len(cells):
            sv = cells[skip_col].strip()
            if "❌" in sv or "否" in sv or sv.lower() in ("no", "false", "0", "不"):
                continue

        fname = cells[func_col] if func_col < len(cells) else ""
        # 清理函数名：去掉反引号、-> 或 . 前缀的对象名
        fname = fname.strip('`').strip()
        # 处理 obj->Method() 或 obj.Method() → 取最后一个标识符
        if '->' in fname:
            fname = fname.split('->')[-1]
        elif '.' in fname and '::' not in fname:
            fname = fname.split('.')[-1]
        # 去掉括号及参数
        fname = re.sub(r'\(.*', '', fname).strip()
        ffile = cells[file_col] if 0 <= file_col < len(cells) else ""
        fline = cells[line_col] if 0 <= line_col < len(cells) else ""
        fparam = cells[param_col] if 0 <= param_col < len(cells) else ""
        fdesc = cells[desc_col] if 0 <= desc_col < len(cells) else ""

        # 双重校验：如果“污染参数”列明确为空/无，说明未有污点流入，跳过
        # （补充 Worker 提示词的防线）
        if param_col >= 0 and fparam:
            param_lower = fparam.lower().strip('` ')
            if param_lower in ("无", "none", "null", "不传入污点", "no taint",
                               "无污点", "无直接污点", "-", "—"):
                continue

        # 过滤外部函数
        all_cols = " ".join(cells)
        if "未找到定义" in all_cols or "EXPORT" in all_cols.upper() or "extern" in all_cols.lower():
            continue
        # 文件列标记为外部的函数（Worker 常输出 "外部函数"、"external" 等）
        if ffile and ("外部" in ffile or "external" in ffile.lower() or "未找到" in ffile):
            continue
        # 函数名有效性：至少3字符的合法标识符
        if not re.match(r'^[A-Za-z_]\w{2,}$', fname):
            continue
        if fname in ('None', 'null', 'void', 'return', 'break', 'continue'):
            continue
        # 标准库函数直接过滤
        if fname in _STDLIB_SKIP:
            continue

        callees.append(CalleeRef(
            function_name=fname, file=ffile, line=fline,
            tainted_params=fparam, description=fdesc))
    return callees


# 标准库 / 编译器内置函数黑名单，这些函数不在项目源码中有定义，无需追踪
_STDLIB_SKIP: frozenset[str] = frozenset({
    # 内存
    'memcpy', 'memset', 'memmove', 'memcmp', 'memchr', 'memrchr',
    # 字符串
    'strlen', 'strcpy', 'strncpy', 'strcat', 'strncat', 'strcmp', 'strncmp',
    'strchr', 'strrchr', 'strstr', 'strtok', 'strtok_r',
    'strtol', 'strtoul', 'strtoll', 'strtoull', 'strtod', 'strtof',
    'sprintf', 'snprintf', 'printf', 'fprintf', 'vprintf', 'vsprintf', 'vsnprintf',
    'scanf', 'sscanf', 'fscanf',
    # 内存管理
    'malloc', 'calloc', 'realloc', 'free', 'alloca', 'valloc',
    'new', 'delete',
    # 文件 IO
    'fopen', 'fclose', 'fread', 'fwrite', 'fgets', 'fputs', 'fflush',
    'fseek', 'ftell', 'rewind', 'feof', 'ferror', 'clearerr', 'fileno',
    'open', 'close', 'read', 'write', 'lseek',
    # 数学
    'abs', 'labs', 'llabs', 'fabs', 'fabsf', 'sqrt', 'sqrtf',
    'sin', 'cos', 'tan', 'pow', 'log', 'log2', 'log10',
    # 类型转换
    'atoi', 'atol', 'atof', 'atoll',
    # 控制
    'assert', 'abort', 'exit', '_exit', 'atexit', 'rand', 'srand',
    # POSIX
    'pthread_create', 'pthread_join', 'pthread_mutex_lock', 'pthread_mutex_unlock',
    'pthread_mutex_init', 'pthread_mutex_destroy',
    'sleep', 'usleep', 'nanosleep', 'getpid', 'getppid',
    # 网络
    'socket', 'bind', 'connect', 'listen', 'accept', 'send', 'recv',
    'sendto', 'recvfrom', 'setsockopt', 'getsockopt', 'htons', 'ntohs', 'htonl', 'ntohl',
    # 其他 C++ 内置
    'operator', 'swap',
})


def _function_has_definition(target_dir: str, function_name: str) -> bool:
    """快速 grep 检查函数定义（非 extern 声明）是否存在于目标目录的源文件中。"""
    import subprocess
    if function_name in _STDLIB_SKIP:
        return False
    try:
        exts = ["--include=*.c", "--include=*.h", "--include=*.hpp",
                "--include=*.cpp", "--include=*.cc", "--include=*.cxx"]
        # 第一步：全词匹配函数名，避免 ltc_memcpy 误匹配 memcpy
        result = subprocess.run(
            ["grep", "-rl", "-w"] + exts + [function_name, target_dir],
            capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            return False
        # 第二步：搜索函数定义行（返回类型 + 空白 + 函数名(）
        result2 = subprocess.run(
            ["grep", "-rn", "-P"] + exts + [
             r"^[A-Za-z_][A-Za-z0-9_ *&:<>\[\]]*[\s*:]" + re.escape(function_name) + r"\s*\(",
             target_dir],
            capture_output=True, text=True, timeout=5)
        if result2.returncode != 0 or not result2.stdout.strip():
            # 内联定义 fallback：匹配 { 同行内联定义
            result3 = subprocess.run(
                ["grep", "-rn", "-P"] + exts + [
                 re.escape(function_name) + r"\s*\([^)]*\)\s*(?:const)?\s*\{?",
                 target_dir],
                capture_output=True, text=True, timeout=5)
            if result3.returncode != 0 or not result3.stdout.strip():
                return False
            lines = result3.stdout.strip().split("\n")
        else:
            lines = result2.stdout.strip().split("\n")
        for line in lines:
            code_part = line.split(":", 2)[-1] if ":" in line else line
            if not re.search(r'\bextern\b', code_part, re.IGNORECASE):
                return True
        return False
    except (subprocess.TimeoutExpired, OSError):
        return True  # 超时/出错时保守返回 True，不跳过


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
    elif score >= 60:
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

    async def execute(self, task_id: str | None = None, *, archive: bool = True, depth: int = 0, max_depth: int = 0) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)  # /data/target（只读，源文件在这里）
        threshold = cfg.pass_threshold or math.ceil(cfg.judge_count / 2)
        self._cancel_event = asyncio.Event()

        # 归档模式且非子任务：立即写 flag=0
        if archive:
            flag_dir = Path(os.path.abspath(cfg.result_dir))
            flag_dir.mkdir(parents=True, exist_ok=True)
            (flag_dir / "flag").write_text("0", encoding="utf-8")

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
        self._emit("task_start", task_id, task=cfg.task, agents=agents_desc,
                   function=cfg.function_name, depth=depth)

        # 将 depth/max_depth 注入 context，Worker 据此决定是否需要追踪 callee
        # max_depth=0 时不注入（外部调用者未指定深度）
        if max_depth > 0:
            _depth_note = f"\n\n# 当前追踪深度: {depth}/{max_depth}" + (
                "（已达最大深度，callee 表格仍需填写但系统不会递归）"
                if depth >= max_depth else ""
            )
            cfg.context = (cfg.context or "").rstrip() + _depth_note

        try:
            feedback_for_workers = ""

            for rnd_num in (range(1, cfg.max_rounds + 1) if cfg.max_rounds >= 0 else __import__('itertools').count(1)):
                if self._cancel_event.is_set():
                    break

                self._emit("round_start", task_id, round=rnd_num,
                           function=cfg.function_name, depth=depth)
                rnd_dir = out_dir / f"round-{rnd_num}"
                rnd_workers_dir = rnd_dir / "workers"
                rnd_judges_dir = rnd_dir / "judges"
                rnd_workers_dir.mkdir(parents=True, exist_ok=True)
                rnd_judges_dir.mkdir(parents=True, exist_ok=True)

                # ═══════════════════════════════════════════════════════
                # 1. Workers 并行执行
                # ═══════════════════════════════════════════════════════

                worker_prompt = self._build_worker_prompt(
                    cfg.task, cfg.context, rnd_num, feedback_for_workers,
                    function_name=cfg.function_name, source_file=cfg.source_file)

                w_tasks = []
                for i, acfg in enumerate(cfg.workers.agents):
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
                        "thinking_level": acfg.thinking_level or cfg.workers.default_thinking_level,
                        "session_file": worker_sessions[i],
                        "cancel_event": self._cancel_event,
                        "max_retries": cfg.agent_max_retries,
                        "retry_delay": cfg.agent_retry_delay,
                        "pi_max_retries": cfg.pi_max_retries,
                        "pi_retry_delay": cfg.pi_retry_delay,
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

                            pass
                    # 后置校验：检查 dataflow 文件结构完整性
                    df_issues: list[str] = []
                    if not df_file or len(df_content.strip()) < 100:
                        df_issues.append(
                            f"[F1] {wid} 未将分析结果写入 dataflow-*.md 文件（或文件为空）\n"
                            f"     请使用 write 工具将完整分析写入 dataflow-{cfg.function_name}.md"
                        )
                    else:
                        # 检查是否包含目标函数名
                        func_short = cfg.function_name.split("::")[-1]
                        if func_short not in df_content and cfg.function_name not in df_content:
                            df_issues.append(
                                f"[F2] dataflow 文件内容不包含目标函数名 '{cfg.function_name}'，"
                                f"可能分析了错误的函数\n     请确认分析的是 {cfg.source_file} 中的 {cfg.function_name}"
                            )
                        # 检查是否包含 callee 表格
                        if not any(kw in df_content for kw in ["函数调用", "callee", "跟入", "跟进"]):
                            df_issues.append(
                                "[F3] dataflow 文件缺少函数调用跟入表格（## 需要跟入的函数调用）\n"
                                "     此表格是系统递归分析子函数的关键依据，即使为空也必须保留表头"
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


                    # 每轮结束归档 session（不管成败都保留，供调试和下轮继续利用）
                    _sess_src = Path(worker_sessions[i])
                    if _sess_src.exists():
                        try:
                            import shutil as _shu
                            _shu.copy2(str(_sess_src), str(rnd_workers_dir / f"{wid}-session.jsonl"))
                        except OSError:
                            pass

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
            # 子任务模式：不压缩/不清理/不写 result_dir，由根任务统一处理
            # 不发 task_end（避免 callee 分析前过早触发 CLI banner）
            #
            # ★ 关键：将 dataflow 文件内容写入 final_output 供 callee 解析
            # Worker 的 <result> 摘要只是简短总结，不含完整 callee 表格
            # 真正的数据流分析和 callee 表格在 workspace 的 dataflow-*.md 里
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

        # 5) 写 flag 文件（仅 PASSED 覆盖为 1，其他保持入口处写的 0）
        if result.status == TaskStatus.PASSED:
            (result_dir / "flag").write_text("1", encoding="utf-8")

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
        """BFS 队列 + 工作池架构：
        A 进入队列 → Worker 执行 W+J 分析 → 解析 callee B,C,D → 加入队列 → Worker 执行 B,C,D...
        并发数 = callee_concurrency（worker 数量），无 Semaphore 死锁问题。
        """
        cfg = self.cfg
        is_root = (depth == 0)

        # ── 非根任务：直接执行 W+J 并返回，由根任务的工作池调度 ──────────────
        if not is_root:
            result = await self.execute(task_id, archive=False, depth=depth, max_depth=cfg.max_trace_depth)
            out_dir = Path(os.path.abspath(cfg.output_dir)) / (task_id or result.task_id)
            df_path = _find_dataflow_file(out_dir, cfg.function_name)
            if df_path:
                try:
                    df_text = Path(df_path).read_text(encoding="utf-8")
                    if len(df_text.strip()) > len((result.final_output or "").strip()):
                        result.final_output = df_text
                except OSError:
                    pass
            return result

        # ── 根任务：BFS 队列 + 工作池 ────────────────────────────────────────
        max_depth = cfg.max_trace_depth
        n_workers = max(1, cfg.callee_concurrency) if cfg.callee_concurrency > 0 else 4
        analyzed: set[str] = _analyzed if _analyzed is not None else set()
        MAX_CALLEES_PER_LEVEL = 10

        # 初始化根目录
        root_task_id = task_id or make_id()
        root_out_dir = _root_out_dir
        if root_out_dir is None:
            root_out_dir = Path(os.path.abspath(cfg.output_dir)) / root_task_id
        root_out_dir.mkdir(parents=True, exist_ok=True)

        # flag=0
        flag_dir = Path(os.path.abspath(cfg.result_dir))
        flag_dir.mkdir(parents=True, exist_ok=True)
        (flag_dir / "flag").write_text("0", encoding="utf-8")

        queue: asyncio.Queue = asyncio.Queue()
        all_results: dict[str, TaskResult] = {}   # func_key -> TaskResult
        sub_dataflow_files: list[tuple[str, str]] = []

        # 注册根函数
        root_key = cfg.source_file + "::" + cfg.function_name
        analyzed.add(root_key)

        self._emit("task_start", root_task_id, task=cfg.task,
                   agents=f"W={cfg.worker_count} J={cfg.judge_count}")
        self._emit("trace_start", root_task_id,
                   function=cfg.function_name, depth=0, max_depth=max_depth)

        # 根任务入队
        await queue.put((cfg.function_name, cfg.source_file,
                         cfg.model_copy(deep=True), root_task_id, 0, tainted_context))

        async def process_item(item: tuple) -> None:
            func_name, src_file, task_cfg, tid, dep, taint_ctx = item

            # 注入 tainted_context
            if taint_ctx and dep > 0:
                ctx_base = task_cfg.context or ""
                if "# 调用者传入的脏数据" in ctx_base:
                    ctx_base = ctx_base.split("# 调用者传入的脏数据")[0].strip()
                task_cfg.context = ctx_base + "\n\n# 调用者传入的脏数据\n" + taint_ctx

            # ── 解析污点参数列表 ──────────────────────────────────────────
            _taint_match = re.search(r'外部输入参数.*?为[：:]\s*([^\n]+)', task_cfg.task or "")
            _taint_list: list[str] = []
            if _taint_match:
                _taint_list = [t.strip() for t in _taint_match.group(1).split(',') if t.strip()]
            if not _taint_list:
                _tc_match = re.search(r'污染参数[：:]\s*([^\n]+)', taint_ctx or "")
                if _tc_match:
                    _taint_list = [t.strip() for t in _tc_match.group(1).split(',') if t.strip()]
            if not _taint_list:
                _taint_list = ["all"]

            # dataflow 输出目录
            df_dir = root_out_dir / "dataflow"
            df_dir.mkdir(exist_ok=True)
            safe_func = re.sub(r'[^A-Za-z0-9_:.-]', '_', func_name)

            # ── 单次 execute，多阶段 task prompt（同一 session 内完成所有污点分析）──
            # 构建分阶段 task：阶读源码→逐污点分析→写报告
            taint_steps = "\n".join(
                f"### 阶段{i+2}：追踪污点参数 `{t}`\n"
                f"只追踪 `{t}` 的传播路径，标记所有接收 `{t}` 的函数调用（排除条件判断函数）。"
                for i, t in enumerate(_taint_list)
            )
            taint_names = "、".join(f"`{t}`" for t in _taint_list)
            final_step_idx = len(_taint_list) + 2

            task_cfg.task = (
                f"对 {src_file} 的 {func_name} 函数进行静态污点分析，"
                f"污点参数为：{taint_names}\n\n"
                f"## 工作流程（严格按阶段顺序执行）\n\n"
                f"### 阶段1：阅读源码\n"
                f"使用 `read` 或 `bash extract_func` 读取 {func_name} 函数的完整代码。\n\n"
                f"{taint_steps}\n\n"
                f"### 阶段{final_step_idx}：汇总并写入报告\n"
                f"综合以上各污点分析，使用 `write` 工具写入 `dataflow-{func_name}.md`。\n"
                f"报告必须包含：\n"
                f"- 每个污点的数据流树状图\n"
                f"- `## 需要跟入的函数调用` 表格（**只列接收了污点参数的函数**，条件判断/纯getter不列）\n"
                f"- `是否需要跟入` 列：不接收污点参数的函数填 `❌ 否`\n\n"
                f"## 交付清单（write 前逐项确认）\n"
                f"- [ ] `## 需要跟入的函数调用` 表格已填写（哪怕空表也要有表头）\n"
                f"- [ ] 表格里每行的 `是否需要跟入` 列已填写 ✅/❌\n"
                f"- [ ] `write` 工具已调用，文件名 `dataflow-{func_name}.md`\n"
                f"- [ ] `<result>...</result>` 已包裹摘要"
            )

            # 执行（单次 execute，单会话内多轮直到 Judge 通过）
            orch = Orchestrator(config=task_cfg, on_event=self.on_event)
            result = await orch.execute(tid, archive=False, depth=dep, max_depth=max_depth)

            # 读取 dataflow 文件更新 final_output
            out_dir = Path(os.path.abspath(task_cfg.output_dir)) / tid
            df_path = _find_dataflow_file(out_dir, func_name)
            if df_path:
                try:
                    df_text = Path(df_path).read_text(encoding="utf-8")
                    if len(df_text.strip()) > len((result.final_output or "").strip()):
                        result.final_output = df_text
                except OSError:
                    pass

            # 保存 dataflow/funcname.md（供 callee 解析 + merge）
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
                callees = _parse_callees(result.final_output)
                target_dir = os.path.abspath(task_cfg.cwd)
                valid: list[CalleeRef] = []
                for callee in callees:
                    # 标准化 c_key：只用函数名（不含文件），避免路径差异导致误 dup
                    c_key = callee.function_name
                    if callee.function_name == func_name:
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

                if valid:
                    self._emit("trace_callees", tid, function=func_name,
                               callees=[c.function_name for c in valid], depth=dep)

                for callee in valid[:MAX_CALLEES_PER_LEVEL]:
                    sub_file = callee.file or src_file
                    sub_cfg = task_cfg.model_copy(deep=True)
                    sub_cfg.function_name = callee.function_name
                    sub_cfg.source_file = sub_file
                    sub_cfg.task = (
                        f"对 {sub_file} 的 {callee.function_name} 函数进行静态污点分析，"
                        f"外部输入参数（已污染）为：{callee.tainted_params or '所有参数'}"
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
                    await queue.put((callee.function_name, sub_file, sub_cfg,
                                     sub_tid, dep + 1, tainted_ctx_str))

        async def worker(wid: int) -> None:
            while True:
                item = await queue.get()
                if item is None:      # sentinel → 退出
                    queue.task_done()
                    break
                try:
                    await process_item(item)
                except Exception as e:
                    tid = item[3] if len(item) > 3 else "?"
                    self._emit("error", tid, error=str(e))
                finally:
                    queue.task_done()

        # 启动工作池（n_workers 个并发 Worker+Judge 会话）
        workers = [asyncio.create_task(worker(i)) for i in range(n_workers)]

        # 等待所有任务处理完毕
        await queue.join()

        # 发送终止 sentinel
        for _ in range(n_workers):
            await queue.put(None)
        await asyncio.gather(*workers)

        # ── 根函数结果 ────────────────────────────────────────────────────────
        root_result = all_results.get(root_key)
        if root_result is None:
            root_result = TaskResult(task_id=root_task_id, status=TaskStatus.ERROR,
                                     error="root function analysis failed")

        # ── merge agent ───────────────────────────────────────────────────────
        if sub_dataflow_files:
            try:
                merged = await self._run_merge_agent(
                    root_function=cfg.function_name,
                    dataflow_files=sub_dataflow_files,
                    cwd=str(root_out_dir),
                    result=root_result)
                if merged:
                    root_result.final_output = merged
            except Exception as e:
                self._emit("error", root_task_id, error=f"merge failed: {e}")

        # ── 最终归档 ──────────────────────────────────────────────────────────
        self._do_final_archive(root_result, root_out_dir)
        return root_result

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
        # 将属于根任务的子任务目录移入 subtasks/ 子目录
        subtasks_dir = root_out_dir / "subtasks"
        output_parent = root_out_dir.parent
        root_prefix = root_out_dir.name + "-"
        for sibling in sorted(output_parent.iterdir()):
            if sibling.is_dir() and sibling.name.startswith(root_prefix):
                try:
                    subtasks_dir.mkdir(exist_ok=True)
                    shutil.move(str(sibling), str(subtasks_dir / sibling.name))
                except OSError:
                    pass
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

        # 将 dataflow/ 文件夹复制到 result_dir（小切文件 + 已归档到 zip）
        df_folder = root_out_dir / "dataflow"
        if df_folder.exists():
            dest_df = result_dir / "dataflow"
            if dest_df.exists():
                shutil.rmtree(dest_df, ignore_errors=True)
            try:
                shutil.copytree(str(df_folder), str(dest_df))
            except OSError:
                pass

        # 清理
        shutil.rmtree(root_out_dir, ignore_errors=True)

        # 写 flag 文件（仅 PASSED 覆盖为 1，其他保持入口处写的 0）
        if result.status == TaskStatus.PASSED:
            (result_dir / "flag").write_text("1", encoding="utf-8")

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
            "# 数据流分析合并任务\n\n"
            "根函数: " + root_function + "\n"
            "共 " + str(len(dataflow_files)) + " 个数据流分析文档需要合并。\n\n"
            "**要求**：\n"
            "1. 用 `read` 工具逐个读取以下全部文件\n"
            "2. 输出一份**详细完整**的合并报告，要求：\n"
            "   - 每个函数的完整污点传播路径（保留原文格式）\n"
            "   - 调用链树状图（含深度和污点状态）\n"
            "   - 每个函数的关键污点变量汇总表\n"
            "   - 污点终点汇总（EXPORT/USED/CLEANED/DEFERRED）\n"
            "3. 用 `write` 工具将完整报告写入 `merged-dataflow.md`\n\n"
            "文件列表：\n" + file_list + "\n\n"
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
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
        }

        # ═══ 步骤0：准备 Worker 输出文件（放入 Judge 工作目录）═══

        for w in round_workers:
            # 摘要输出
            (j_dir / f"{w.worker_id}-output.md").write_text(
                w.output, encoding="utf-8")
            # dataflow 文件
            df_dst = j_dir / f"{w.worker_id}-dataflow.md"

            # 如果有结构性问题，写入问题描述作为代替文件
            if w.df_issues:
                df_dst.write_text(
                    "# ⚠️ 结构性检查失败 — Worker 未正确交付\n\n"
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

        # ═══ 步骤1：并行评判所有 Worker（每个 Worker 独立上下文）═════════

        async def _eval_one_worker(w: WorkerResult) -> tuple[WorkerEvaluation, object]:
            # 结构性问题：直接生成 fail，不调用 LLM
            if w.df_issues:
                issues_text = "\n".join(w.df_issues)
                ev = WorkerEvaluation(
                    worker_id=w.worker_id,
                    passed=False,
                    score=0,
                    feedback=f"结构性检查失败，自动不通过：\n{issues_text}",
                    refinement=issues_text,
                )
                (j_dir / f"eval-{w.worker_id}.md").write_text(
                    f"# {jid} → {w.worker_id} (Round {rnd_num}) — 自动不通过\n\n"
                    f"- **原因**: 结构性检查失败\n\n"
                    f"## 问题列表\n\n{issues_text}\n",
                    encoding="utf-8",
                )
                return ev, TokenUsage()  # 不消耗 token
            eval_prompt = self._build_eval_prompt(
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

    def _build_worker_prompt(self, task, context, rnd, feedback,
                              function_name: str = "", source_file: str = ""):
        import uuid
        # 随机 nonce 确保每次请求 token 前缀不同，破坏 vllm prefix cache
        # （防止 temperature=0 加 prefix cache 导致确定性复现“不调 write”的行为）
        nonce = uuid.uuid4().hex[:8]
        # 主任务描述，显式注入输出文件名和只读警告
        task_block = task
        if function_name:
            safe_fn = function_name
            task_block += (
                f"\n\n❗️ **必读：输出文件要求**\n"
                f"- 使用 `write` 工具将分析写入：`dataflow-{safe_fn}.md`（**当前目录下**）\n"
                f"- 文件名就是 `dataflow-{safe_fn}.md`，**不要**写成 `{safe_fn}.dataflow.md` 或加其他路径\n"
                f"- `src-vul/` 目录是只读挂载，导新任何写入都会失败！请将文件写到当前目录"
            )
        parts = [f"<!-- {nonce} -->\n# Task\n\n{task_block}"]
        if context:
            parts.append(f"# Additional Context\n\n{context}")
        if rnd > 1 and feedback:
            # 结构性问题（F1/F2/F3）置顶，避免被长上下文忽视
            is_structural = any(tag in feedback for tag in ("[F1]", "[F2]", "[F3]"))
            if is_structural:
                parts.insert(0,
                    f"⚠️ 上一轮交付失败，必须首先修复以下问题（否则本轮仍会自动不通过）:\n\n"
                    f"{feedback}\n\n"
                    f"修复完成后再输出分析内容和 <result>。")
            else:
                parts.append(
                    f"# 第 {rnd - 1} 轮反馈\n\n"
                    f"上一轮工作已评审，请针对以下反馈改进：\n\n"
                    f"{feedback}\n\n"
                    f"确保全面解决所有问题。")
        parts.append("用 <result>...</result> 包裹摘要信息。")
        return "\n\n".join(parts)

    def _build_eval_prompt(self, task, worker: WorkerResult, rnd,
                           output_path: str = "", dataflow_path: str = ""):
        CRITERIA = (
            "重点评判维度：外部输入识别完整性、污点追踪深度（子函数必须跟入）、"
            "数据处理函数覆盖、文档规范性、需要跟入的函数列表完整性"
        )
        parts = [
            f"# Evaluate {worker.worker_id} (Round {rnd})",
            f"## Task Requirements\n\n{task}",
            f"## Evaluation Criteria\n\n{CRITERIA}",
        ]

        parts.append(
            f"## {worker.worker_id}'s Output Files\n\n"
            f"注意: 以下是归档文件名（不是 Worker 创建的原始文件名），**请勿按存档名评判文件命名是否正确**：\n"
            f"- Worker 摘要输出（`{output_path}`）\n"
            f"- Worker 数据流分析文档（`{dataflow_path}`）\n\n"
            f"**请使用 read 工具读取以上两个文件，然后进行评测。"
            f"评判文件命名时应以文件内容中的函数名为准，不需关注存档路径名。**"
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
