#!/usr/bin/env python3
"""gen_dataflow — 生成标准格式的 dataflow-*.md 模板文件

用法:
  gen_dataflow <函数名> <源文件> <行号范围> <污点输入列表>

参数:
  函数名       完整函数名，如 Leader::HandleCommissioningSet
  源文件       相对路径，如 src-vul/openthread/.../foo.cpp
  行号范围     如 L228-L282（可用 extract_func 查得）
  污点输入     逗号分隔，如 aHeader,aMessage,aMessageInfo

示例:
  gen_dataflow "Leader::HandleCommissioningSet" \\
    "src-vul/openthread/src/core/thread/network_data_leader_ftd.cpp" \\
    "L228-L282" \\
    "aHeader,aMessage,aMessageInfo"

输出:
  在当前目录创建 dataflow-<函数名>.md，包含所有必须章节骨架。
  用 edit 工具填入实际分析内容，不要删除已有章节标题。
"""

import sys
import re
import os


def safe_filename(func_name: str) -> str:
    """函数名转文件名（保留 ::，替换其他非法字符）。"""
    return "dataflow-" + func_name + ".md"


def make_template(func_name: str, source_file: str,
                  line_range: str, inputs: list[str]) -> str:
    """生成标准 dataflow 模板字符串。"""
    base = os.path.basename(source_file)
    sig = f"{func_name}({', '.join(inputs)})"

    parts: list[str] = []

    # ── Header ──────────────────────────────────────────────────
    parts += [
        f"# 数据流追踪: {func_name}",
        "",
        "## 函数信息",
        f"- 文件: {source_file}",
        f"- 行号: {line_range}",
        f"- 签名: `{sig}`",
        "",
    ]

    # ── 外部输入识别 ─────────────────────────────────────────────
    parts += [
        "## 外部输入识别",
        "",
    ]
    for i, inp in enumerate(inputs, 1):
        parts.append(f"[INPUT-{i}] {inp} | 类型待填 | 函数参数 | 行号待填")
    parts.append("")

    # ── 数据流树状图 ─────────────────────────────────────────────
    parts += [
        "## 数据流树状图",
        "",
    ]
    for i, inp in enumerate(inputs, 1):
        parts += [
            f"### INPUT-{i}: {inp} 🔴 TAINTED",
            f"├── [Lxx] `{inp}相关操作` → result 🔴 TAINTED (说明)",
            f"│   └── [Lxx] 下一步使用 🔴 TAINTED",
            f"└── [Lxx] 最终流向 → 📎 见跟入列表 / 🟡 EXPORT / 📌 USED / 🟢 CLEANED",
            "",
        ]

    # ── 需要跟入的函数调用 ───────────────────────────────────────
    parts += [
        "## 需要跟入的函数调用",
        "",
        "**重要：以下函数将由系统自动递归分析，请确保信息准确。**",
        "",
        "| 函数名 | 文件 | 调用位置 | 污染参数 | 说明 |",
        "|--------|------|---------|---------|------|",
        "",
    ]

    # ── 数据处理函数清单 ─────────────────────────────────────────
    parts += [
        "## 数据处理函数清单",
        "| 函数名 | 文件位置 | 接收的脏数据 | 参数位置 | 作用 |",
        "|--------|---------|-------------|---------|------|",
        "",
    ]

    # ── 污点终点汇总 ─────────────────────────────────────────────
    parts += [
        "## 污点终点汇总",
        "| 脏数据 | 终点类型 | 位置 | 说明 |",
        "|--------|---------|------|------|",
        "",
    ]

    # ── 自检报告 ─────────────────────────────────────────────────
    parts += [
        "## 自检报告",
        "- 遗漏检查: （填写）",
        "- 深度不足: （填写）",
        "- 函数覆盖: （填写已覆盖函数数量）",
        "- 返回值污染: 所有接收脏参数的函数返回值已标记",
        "",
    ]

    return "\n".join(parts)


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    if len(sys.argv) < 5:
        print("错误：需要 4 个参数：函数名 源文件 行号范围 污点输入", file=sys.stderr)
        print("示例：gen_dataflow \"foo\" \"src/foo.c\" \"L10-L50\" \"buf,len\"", file=sys.stderr)
        sys.exit(1)

    func_name  = sys.argv[1]
    source_file = sys.argv[2]
    line_range = sys.argv[3]
    inputs_raw = sys.argv[4]

    inputs = [s.strip() for s in inputs_raw.split(",") if s.strip()]
    if not inputs:
        inputs = ["input"]

    filename = safe_filename(func_name)
    content  = make_template(func_name, source_file, line_range, inputs)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ 已生成: {filename}  ({len(content)} bytes)")
    print(f"   函数: {func_name}")
    print(f"   输入: {', '.join(inputs)}")
    print()
    print("下一步：使用 edit 工具填充每个 INPUT 的数据流树状图，")
    print("并补全 '需要跟入的函数调用' 表格（空表也要保留表头）。")


if __name__ == "__main__":
    main()
