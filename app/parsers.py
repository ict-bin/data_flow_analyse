"""
parsers.py — 输出文件解析 + 评审结果解析工具
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

from .models import CalleeRef, WorkerResult


def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output




def _find_dataflow_file(worker_cwd: str, function_name: str = "") -> str:
    """从 Worker 工作目录搜索数据流分析文件。
    兼容多种命名惯例:dataflow-*.md / *.dataflow.md / *dataflow*.md / <funcname>*.md
    """
    cwd = Path(worker_cwd)
    candidates: list[Path] = []

    for search_dir in [cwd, Path("/tmp")]:
        if not search_dir.is_dir():
            continue
        # 常见命名惯例(当前目录)
        for pat in ["dataflow-*.md", "dataflow_*.md", "*.dataflow.md",
                    "*dataflow*.md", "*_analysis.md"]:
            candidates.extend(search_dir.glob(pat))
        # 递归搜索子目录(Worker 可能将文件写到源码子目录下)
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

    # 优先匹配函数名,且内容 > 100 bytes
    if function_name:
        short = function_name.split("::")[-1].lower()
        func_lower = function_name.lower()
        # 首先尝试内容匹配(文件名可能不包含函数名)
        for c in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                sz = c.stat().st_size
                if sz > 200:  # 排除空骨架
                    head = c.read_text(encoding='utf-8', errors='replace')[:500]
                    if short in head.lower() or func_lower in head.lower():
                        # 如果文件不在 cwd 根目录,将内容拷贝到正确路径
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
        # 备选:文件名包含函数名
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




def _read_tainted_list(worker_cwd: str) -> list[CalleeRef]:
    """读取 tainted.list 文件，返回 CalleeRef 列表。

    搜索顺序: workspace-worker-*/ → round_*/workers/ → round-*/workers/ → 任意子目录
    """
    callees: list[CalleeRef] = []
    task_dir = Path(worker_cwd)
    # 搜索所有可能位置
    candidates: list[Path] = []
    candidates.extend(task_dir.glob("workspace-worker-*/tainted.list"))
    candidates.extend(task_dir.glob("round_*/workers/*/tainted.list"))
    candidates.extend(task_dir.glob("round-*/workers/*/tainted.list"))
    candidates.extend(task_dir.rglob("tainted.list"))
    # 去重并按修改时间排序
    seen: set[str] = set()
    unique: list[Path] = []
    for c in candidates:
        k = str(c.resolve())
        if k not in seen:
            seen.add(k)
            unique.append(c)
    if not unique:
        return []
    unique.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    tainted_file = unique[0]
    try:
        content = tainted_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("###")
        if len(parts) < 2:
            continue
        fpath  = parts[0].strip() if parts[0].strip() not in ("-", "") else ""
        fname  = parts[1].strip() if len(parts) > 1 else ""
        fline  = parts[2].strip() if len(parts) > 2 else ""
        fparam = parts[3].strip() if len(parts) > 3 else "*"
        if not fname or fname in _STDLIB_SKIP:
            continue
        # 清理函数名: 去尾括号
        fname = re.sub(r'\(.*', '', fname).strip()
        if fparam == "*":
            fparam = ""
        callees.append(CalleeRef(
            function_name=fname, file=fpath, line=fline,
            tainted_params=fparam, description=""))
    return callees


def _get_best_output(worker: WorkerResult) -> str:
    """获取最佳 Worker 的输出:优先用 dataflow 文件,回退用 result 摘要。"""
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
    兼容多种列格式(自动检测函数名列位置)。"""
    callees: list[CalleeRef] = []
    in_table = False
    func_col = -1
    file_col = -1
    line_col = -1
    param_col = -1
    desc_col  = -1

    for line in dataflow_content.split(chr(10)):
        stripped = line.strip()
        # markdown 标题行(# 开头)才触发 callee 表,防止嵌入文本起头的错误匹配
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
            elif lc in ("文件", "file") or ("file" in lc and "pos" not in lc and "loc" not in lc):
                file_col = i
            elif "调用位置" in lc or "调用行" in lc or "行号" in lc or lc in ("line", "line no", "lineno"):
                line_col = i
            elif lc in ("污染参数", "污点参数", "已污染参数", "tainted", "tainted params",
                        "tainted_params", "taint params", "污染实参"):
                param_col = i
            elif "taint" in lc and "param" in lc:
                param_col = i
            elif "是否传递" in lc or "是否传入" in lc or "是否把污" in lc:
                # "是否传递污染数据": 値如 "是 (offset, length)" 或 "否"
                # 兼当 skip_col + param_col
                param_col = i  # 后面专長解析
                is_header = True
            elif "说明" in lc or "备注" in lc or "原因" in lc or "desc" in lc or "remark" in lc:
                desc_col = i
            elif "数据" in lc and ("传播" in lc or "流动" in lc):
                desc_col = i
        if is_header:
            continue

        # 未检测到表头时默认第一列
        if func_col == -1:
            func_col = 0

        # 提取各字段
        # 跳过明确标注「不需要跟入」的行：如果第4列包含 ❌ / 否（历史格式兼容）
        if len(cells) >= 4:
            fourth = cells[3].strip()
            if "❌" in fourth or fourth.lower() in ("否", "no", "false"):
                continue

        fname = cells[func_col] if func_col < len(cells) else ""
        # 清理函数名:去掉反引号、-> 或 . 前缀的对象名
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
        fdesc  = cells[desc_col]  if 0 <= desc_col  < len(cells) else ""

        # 处理 "是否传递污染数据" 式列:値如 "否", "是 (aOffset, aLength)" 等
        if fparam:
            fp_s = fparam.strip().strip('*').strip()
            if fp_s.startswith('否') or fp_s.lower().startswith('no'):
                continue  # 明确不传递污点,跳过
            # 提取括号内的形参名: "是 (aOffset, aLength)" → "aOffset, aLength"
            m_paren = re.search(r'[((]([^))]{1,80})[))]', fp_s)
            if m_paren:
                fparam = m_paren.group(1).strip()
            elif fp_s.startswith('是') or fp_s.startswith('yes') or fp_s.startswith('true'):
                fparam = ""  # 带 "是" 但无括号--保留空字符串,后续 fallback 到 所有参数

        # 双重校验:如果"污染参数"列明确为空/无,说明未有污点流入,跳过
        # (补充 Worker 提示词的防线)
        if param_col >= 0 and fparam:
            param_lower = fparam.lower().strip('` ')
            if param_lower in ("无", "none", "null", "不传入污点", "no taint",
                               "无污点", "无直接污点", "-", "-"):
                continue

        # 过滤外部函数
        all_cols = " ".join(cells)
        if "未找到定义" in all_cols or "EXPORT" in all_cols.upper() or "extern" in all_cols.lower():
            continue
        # 文件列标记为外部的函数(Worker 常输出 "外部函数"、"external" 等)
        if ffile and ("外部" in ffile or "external" in ffile.lower() or "未找到" in ffile):
            continue
        # 函数名有效性:至少3字符的合法标识符
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

    # Fallback: 如果表格解析为空，尝试从 "已跟入函数分析" 章节标题中提取函数名
    # 支持 "### N.N FuncName(args)" 和 "#### FuncName" 格式
    if not callees:
        for line in dataflow_content.split(chr(10)):
            s = line.strip()
            # 匹配 "已跟入" 等关键词所在的 markdown 标题
            if s.startswith('#') and any(kw in s for kw in [
                    '已跟入', '跟入分析', '追踪分析', '子函数']):
                # 提取标题中的函数名: '### 2.1 SetCommissioning(...)' → 'SetCommissioning'
                m = re.search(r'[`\"\']?([A-Za-z_][\w:<>*&\s]*?)\s*\(', s)
                if m:
                    fn = re.sub(r'\(.*', '', m.group(1)).strip()
                    fn = fn.split()[-1] if ' ' in fn else fn  # take last token
                    fn = re.sub(r'[^A-Za-z0-9_:.]', '', fn)
                    if fn and len(fn) >= 3 and fn not in _STDLIB_SKIP:
                        callees.append(CalleeRef(
                            function_name=fn, file='', line='',
                            tainted_params='*', description='已跟入内联分析'))
    return callees


# 标准库 / 编译器内置函数黑名单,这些函数不在项目源码中有定义,无需追踪


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

    # 找所有 '{' 的位置,尝试从每个位置开始解析完整 JSON
    for i, ch in enumerate(text):
        if ch != '{':
            continue
        # 快速跳过明显不是目标 JSON 的(如 C 代码的 {)
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
    """从 Judge 的输出中解析评审结果。优先解析 markdown,回退到 JSON。"""
    score = 0
    passed = False
    feedback = ""
    refinement = ""

    # ═══ 尝试 markdown 解析 ═══

    # 提取评分
    m = re.search(r'##\s*评分[::=:]\s*(\d+)', output)
    if not m:
        m = re.search(r'##\s*[Ss]core[::=:]\s*(\d+)', output)
    if m:
        score = min(int(m.group(1)), 100)

    # 提取通过/不通过
    m = re.search(r'##\s*通过[::=:]\s*(是|否|true|false|yes|no|pass|fail)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Pp]ass[::=:]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
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

    # markdown 解析成功(至少拿到了分数)
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
    """从 Judge 的输出中解析综合对比结果。优先 markdown,回退 JSON。"""
    best_worker = ""
    overall_passed = False
    reasoning = ""

    # ═══ 尝试 markdown 解析 ═══

    m = re.search(r'##\s*最佳\s*[Ww]orker[::=:]\s*(worker-\d+)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Bb]est\s*[Ww]orker[::=:]\s*(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    m = re.search(r'##\s*整体通过[::=:]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Oo]verall.*?[Pp]ass[::=:]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
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
