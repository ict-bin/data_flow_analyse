"""
cpp_resolver.py — C++ 函数定义搜索与限定名解析
"""
from __future__ import annotations

import os
import re
import subprocess
from typing import Optional

from .parsers import _STDLIB_SKIP


def _find_function_file(target_dir: str, function_name: str) -> "str | None":
    """在项目源码中搜索函数/方法定义所在文件，返回相对路径。

    支持两种形式:
      - "Class::Method" → 直接搜索 Class::Method(
      - "Method"        → 搜索 ::Method( 找到实现类
    """
    if function_name in _STDLIB_SKIP:
        return None
    method = function_name.split('::')[-1]
    exts = ["--include=*.c", "--include=*.h", "--include=*.hpp",
            "--include=*.cpp", "--include=*.cc", "--include=*.cxx"]

    def _first_cpp_file(output: str) -> "str | None":
        files = [f for f in output.strip().split('\n') if f]
        if not files:
            return None
        cpp = [f for f in files if f.endswith(('.cpp', '.cc', '.cxx', '.c'))]
        best = cpp[0] if cpp else files[0]
        return os.path.relpath(best, target_dir)

    try:
        if '::' in function_name:
            pat = re.escape(function_name) + r'\s*\('
        else:
            pat = r'::' + re.escape(method) + r'\s*\('
        r = subprocess.run(["grep", "-rlnP"] + exts + [pat, target_dir],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            return _first_cpp_file(r.stdout)
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


def _resolve_cpp_name(target_dir: str, raw_name: str,
                      file_hint: str = "") -> tuple[str, str]:
    """将 callee 表格中的原始名称解析为 (qualified_name, rel_file)。

    处理:
      - "Read"         → grep ::Read( → ("Message::Read", "common/message.cpp")
      - "aMsg.Read"   → obj前缀小写表示实例，取 Read 再解析
      - "Msg.Read"    → obj前缀大写像类名，转为 "Msg::Read"再解析
      - "Cls::Method" → 直接验证
    Returns: (qualified_name, rel_file) 或 (method_name, "")
    """
    name = raw_name.strip().strip('`')
    name = re.sub(r'\(.*', '', name).strip()

    obj_prefix = ""
    if '::' in name:
        qualified, method = name, name.split('::')[-1]
    elif '->' in name:
        obj_prefix, method = name.rsplit('->', 1)
        qualified = (obj_prefix + '::' + method) if obj_prefix and obj_prefix[0].isupper() else ''
    elif '.' in name:
        obj_prefix, method = name.rsplit('.', 1)
        qualified = (obj_prefix + '::' + method) if obj_prefix and obj_prefix[0].isupper() else ''
    else:
        qualified, method = '', name

    if not re.match(r'^[A-Za-z_]\w*$', method):
        return (name, '')

    exts = ["--include=*.c", "--include=*.h", "--include=*.hpp",
            "--include=*.cpp", "--include=*.cc", "--include=*.cxx"]

    def _grep(pat: str) -> str:
        try:
            r = subprocess.run(["grep", "-rnP"] + exts + [pat, target_dir],
                               capture_output=True, text=True, timeout=5)
            return r.stdout if r.returncode == 0 else ''
        except (subprocess.TimeoutExpired, OSError):
            return ''

    if qualified:
        out = _grep(re.escape(qualified) + r'\s*\(')
        for ln in out.strip().split('\n'):
            if not ln or 'extern' in ln.lower():
                continue
            fpath = ln.split(':')[0]
            if '/usr/' not in fpath:
                rel = os.path.relpath(fpath, target_dir)
                return (qualified, rel)

    out2 = _grep(r'::' + re.escape(method) + r'\s*\(')
    seen: dict[str, str] = {}
    for ln in out2.strip().split('\n'):
        if not ln or 'extern' in ln.lower():
            continue
        parts = ln.split(':', 2)
        if len(parts) < 3:
            continue
        fpath, _, code = parts
        if '/usr/' in fpath:
            continue
        m = re.search(r'(\w+)::' + re.escape(method) + r'\s*\(', code)
        if m:
            qn = m.group(1) + '::' + method
            rel = os.path.relpath(fpath, target_dir)
            if qn not in seen:
                seen[qn] = rel
    if seen:
        for qn, rel in seen.items():
            if rel.endswith(('.cpp', '.cc', '.cxx', '.c')):
                return (qn, rel)
        return next(iter(seen.items()))

    return (method, '')


def _get_definition_line(target_dir: str, function_name: str, file_hint: str = "") -> str:
    """返回函数定义的起始行号，格式 'L123'。
    优先在 file_hint 文件中搜索；失败则全局搜索。
    找不到时返回空字符串。
    """
    import subprocess, os, re
    method = function_name.split('::')[-1]
    pat = re.escape(method) + r'\s*\('

    def _search_file(fpath: str) -> str:
        try:
            r = subprocess.run(['grep', '-nP', pat, fpath],
                               capture_output=True, text=True, timeout=5)
            for ln in r.stdout.strip().split('\n'):
                if not ln:
                    continue
                lineno, _, code = ln.partition(':')
                # 跳过声明（无大括号、含 extern/;）
                if re.search(r';\s*$', code.strip()) and '{' not in code:
                    continue
                if 'extern' in code:
                    continue
                return 'L' + lineno.strip()
        except (subprocess.TimeoutExpired, OSError):
            pass
        return ''

    if file_hint:
        full = os.path.join(target_dir, file_hint)
        if os.path.isfile(full):
            hit = _search_file(full)
            if hit:
                return hit

    exts = ['--include=*.cpp', '--include=*.cc', '--include=*.cxx', '--include=*.c']
    try:
        r = subprocess.run(['grep', '-rlnP'] + exts + [pat, target_dir],
                           capture_output=True, text=True, timeout=5)
        for f in r.stdout.strip().split('\n'):
            if not f or '/usr/' in f:
                continue
            hit = _search_file(f)
            if hit:
                return hit
    except (subprocess.TimeoutExpired, OSError):
        pass
    return ''


def _function_has_definition(target_dir: str, function_name: str) -> bool:
    import subprocess
    if function_name in _STDLIB_SKIP:
        return False
    # 不得把 C/C++ 关键字 / 语法标识误判为函数定义
    _KEYWORDS = frozenset({
        'if', 'else', 'while', 'for', 'do', 'switch', 'case', 'break',
        'continue', 'return', 'goto', 'typedef', 'struct', 'union', 'enum',
        'class', 'namespace', 'template', 'typename', 'sizeof', 'typeof',
        'static', 'extern', 'inline', 'void', 'int', 'char', 'long',
        'unsigned', 'signed', 'const', 'volatile', 'auto', 'register',
    })
    if function_name in _KEYWORDS:
        return False
    # 函数名必须是合法标识符，且长度 >= 3
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_:]*$', function_name) or len(function_name) < 3:
        return False
    try:
        exts = ["--include=*.c", "--include=*.h", "--include=*.hpp",
                "--include=*.cpp", "--include=*.cc", "--include=*.cxx"]
        # 第一步:全词匹配函数名,避免 ltc_memcpy 误匹配 memcpy
        result = subprocess.run(
            ["grep", "-rl", "-w"] + exts + [function_name, target_dir],
            capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            return False
        # 第二步:搜索函数定义行(返回类型 + 空白 + 函数名()
        result2 = subprocess.run(
            ["grep", "-rn", "-P"] + exts + [
             r"^[A-Za-z_][A-Za-z0-9_ *&:<>\[\]]*[\s*:]" + re.escape(function_name) + r"\s*\(",
             target_dir],
            capture_output=True, text=True, timeout=5)
        if result2.returncode != 0 or not result2.stdout.strip():
            # 内联定义 fallback:匹配 { 同行内联定义
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
        return True  # 超时/出错时保守返回 True,不跳过


