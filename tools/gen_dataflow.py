#!/usr/bin/env python3
"""gen_dataflow — 写入标准格式的数据流分析报告

用法:
  bash gen_dataflow "FuncName" <<'REPORT'
  # 数据流追踪: FuncName
  ...分析内容...
  REPORT

说明:
  从 stdin 读取 markdown 内容，写入 dataflow-<FuncName>.md。
  自动校验必须包含的关键元素（缺失时给出警告但仍写入）。
"""

import sys
import os
import re


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    func_name = sys.argv[1].strip()
    content = sys.stdin.read()

    if not content.strip():
        print(f"❌ 错误：stdin 为空，请通过 heredoc 传入分析内容", file=sys.stderr)
        sys.exit(1)

    filename = f"dataflow-{func_name}.md"
    warnings = []

    # 校验关键元素
    if "🔴" not in content and "TAINTED" not in content:
        warnings.append("⚠️  未发现污点标记 🔴 / TAINTED，污点追踪可能不完整")
    if func_name not in content and func_name.split("::")[-1] not in content:
        warnings.append(f"⚠️  文件内容未包含函数名 '{func_name}'")

    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ 已写入: {filename}  ({len(content)} 字节)")
    for w in warnings:
        print(w)


if __name__ == "__main__":
    main()
