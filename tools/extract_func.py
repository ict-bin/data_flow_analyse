#!/usr/bin/env python3
"""extract_func — 从 C/C++ 源文件中精确提取指定函数的完整代码

用法:
    extract_func <file> <FunctionName>
    extract_func <file> <Class::Method>
    extract_func <file> <FunctionName> --context N   # 附带前 N 行上下文
    extract_func <file> --list                        # 列出文件中所有函数名

输出:
    函数完整源代码，第一行注释标注文件路径和行号范围
"""

import sys
import re
import os


# ─── 括号计数提取 ─────────────────────────────────────────────────────────────

def _count_braces(line: str, depth: int, in_block: bool) -> tuple[int, bool]:
    """对单行做括号深度计数，跳过字符串/字符字面量/注释。
    返回 (new_depth, new_in_block_comment)"""
    i = 0
    while i < len(line):
        if in_block:
            if line[i] == '*' and i + 1 < len(line) and line[i+1] == '/':
                in_block = False
                i += 2
            else:
                i += 1
            continue

        c = line[i]

        # 块注释开始
        if c == '/' and i + 1 < len(line) and line[i+1] == '*':
            in_block = True
            i += 2
            continue

        # 行注释 → 忽略本行剩余
        if c == '/' and i + 1 < len(line) and line[i+1] == '/':
            break

        # 字符串字面量
        if c == '"':
            i += 1
            while i < len(line):
                if line[i] == '\\':
                    i += 2
                    continue
                if line[i] == '"':
                    break
                i += 1
            i += 1
            continue

        # 字符字面量
        if c == "'":
            i += 1
            while i < len(line):
                if line[i] == '\\':
                    i += 2
                    continue
                if line[i] == "'":
                    break
                i += 1
            i += 1
            continue

        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1

        i += 1

    return depth, in_block


# ─── 函数查找 ─────────────────────────────────────────────────────────────────

def find_function_start(lines: list[str], func_name: str) -> list[int]:
    """返回所有可能是函数定义/声明起始行的行号列表（0-indexed）。"""
    # 处理 Class::Method 形式：只用最后一段匹配
    short_name = func_name.split('::')[-1]
    # 也接受带类名前缀的全名匹配
    escaped_short = re.escape(short_name)
    escaped_full  = re.escape(func_name)

    # 匹配模式：行中含有 func_name( 或 short_name(，且行首是标识符字符（返回类型）
    pat_full  = re.compile(r'\b' + escaped_full  + r'\s*\(')
    pat_short = re.compile(r'\b' + escaped_short + r'\s*\(')

    candidates = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        # 跳过纯注释行
        if stripped.startswith('//') or stripped.startswith('*') or stripped.startswith('/*'):
            continue
        # 跳过预处理指令
        if stripped.startswith('#'):
            continue
        if pat_full.search(line) or pat_short.search(line):
            candidates.append(i)

    return candidates


def extract_function(lines: list[str], start: int) -> tuple[list[str], int, int]:
    """从 start 行开始提取完整函数体（到匹配的 } 结束）。
    返回 (function_lines, start_lineno_1indexed, end_lineno_1indexed)"""
    depth = 0
    found_open = False
    in_block = False

    for i in range(start, len(lines)):
        depth, in_block = _count_braces(lines[i], depth, in_block)
        if not found_open and depth > 0:
            found_open = True
        if found_open and depth == 0:
            return lines[start:i+1], start + 1, i + 1

    # 没找到完整函数体（纯声明）
    return lines[start:start+1], start + 1, start + 1


# ─── 列出函数 ─────────────────────────────────────────────────────────────────

def list_functions(lines: list[str]) -> list[tuple[int, str]]:
    """粗略列出文件中所有函数定义（返回 [(lineno_1indexed, signature), ...]）。"""
    results = []
    # 匹配：行首为标识符（返回类型），行中含 identifier( 且下几行有 {
    sig_pat = re.compile(r'^[A-Za-z_][A-Za-z0-9_: *&<>\[\]]*\s+\*?([A-Za-z_]\w*(?:::\w+)*)\s*\(')
    for i, line in enumerate(lines):
        m = sig_pat.match(line)
        if m:
            func = m.group(1)
            results.append((i + 1, func, line.rstrip()))
    return results


# ─── 主程序 ───────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    if not args or args[0] in ('-h', '--help'):
        print(__doc__)
        sys.exit(0)

    filepath = args[0]
    if not os.path.isfile(filepath):
        print(f"// ERROR: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(filepath, encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"// ERROR reading {filepath}: {e}", file=sys.stderr)
        sys.exit(1)

    # --list 模式
    if len(args) >= 2 and args[1] == '--list':
        funcs = list_functions(lines)
        print(f"// {filepath}  ({len(lines)} lines, {len(funcs)} functions found)")
        for lineno, name, sig in funcs:
            print(f"  L{lineno:5d}  {name}")
        return

    if len(args) < 2:
        print("Usage: extract_func <file> <FunctionName>", file=sys.stderr)
        sys.exit(1)

    func_name = args[1]
    context_n = 0
    line_hint = 0  # if >0, prefer candidates at or after this 1-indexed line
    i = 2
    while i < len(args):
        a = args[i]
        if a == '--context' and i + 1 < len(args):
            try:
                context_n = int(args[i+1])
            except ValueError:
                pass
            i += 2
        elif a == '--line' and i + 1 < len(args):
            try:
                raw = args[i+1].lstrip('Ll')  # accept 'L228' or '228'
                line_hint = int(raw)
            except ValueError:
                pass
            i += 2
        else:
            i += 1

    candidates = find_function_start(lines, func_name)
    if not candidates:
        print(f"// Function '{func_name}' not found in {filepath}")
        sys.exit(0)

    # 如果指定了行号提示，找包含该行的函数：找 start_line 最大且 <= line_hint 的候选
    # （即最近的前驱）；其他候选作为备用
    if line_hint > 0:
        before = [c for c in candidates if c + 1 <= line_hint]
        after  = [c for c in candidates if c + 1 >  line_hint]
        # 最近前驱 → before 按行号降序（最大的在前）
        ordered = list(reversed(before)) + after
    else:
        ordered = candidates

    # 找第一个有函数体的候选
    for start in ordered:
        func_lines, s_line, e_line = extract_function(lines, start)
        full = ''.join(func_lines)
        if '{' in full and '}' in full:
            ctx_start = max(0, start - context_n)
            print(f"// {filepath}  L{s_line}-L{e_line}  ({e_line - s_line + 1} lines)")
            if context_n > 0 and ctx_start < start:
                sys.stdout.write(''.join(lines[ctx_start:start]))
            sys.stdout.write(full)
            return

    # 只有声明
    start = ordered[0]
    print(f"// {filepath}  L{start+1} (declaration only)")
    sys.stdout.write(lines[start])


if __name__ == '__main__':
    main()
