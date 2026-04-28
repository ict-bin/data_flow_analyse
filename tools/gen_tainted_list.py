#!/usr/bin/env python3
"""gen_tainted_list — 生成结构化的函数跟入列表

用法:
  bash gen_tainted_list <<'CALLEES'
  文件路径###Class::FuncName###L行号###污点形参1,污点形参2
  CALLEES

每行格式: 文件路径###函数全限定名###L行号###污点形参列表

字段规则:
  - 文件路径: 相对路径（如 src-vul/openthread/src/common/message.cpp），不确定填 -
  - 函数全限定名: Class::Method 格式，不确定类名先 grep 确认
  - L行号: 如 L245，不确定填 -
  - 污点形参: 被调函数的形参名（逗号分隔），不确定填 *

示例:
  src-vul/openthread/src/core/common/message.cpp###Message::Read###L245###aOffset,aLength
  -###LeaderBase::SetCommissioningData###L301###aValue,aValueLength

无需跟入子函数时也必须调用（空输入）:
  echo "" | bash gen_tainted_list

输出: tainted.list（已存在则覆盖）
"""

import sys
import re


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    entries = []
    errors = []

    for i, raw in enumerate(sys.stdin, 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split("###")
        if len(parts) != 4:
            errors.append(f"行{i}: 需要4个###分隔字段，实际{len(parts)}个: {line!r}")
            continue

        fpath, fname, fline, fparams = [p.strip() for p in parts]

        # 清理函数名（去括号及参数）
        fname_clean = re.sub(r"\(.*", "", fname).strip()
        # 验证函数名合法性
        if not re.match(r"^[A-Za-z_][\w:<>~*&]*$", fname_clean):
            errors.append(f"行{i}: 无效函数名 {fname!r}")
            continue

        # 标准化行号格式
        if fline and fline != "-" and not fline.startswith("L"):
            fline = "L" + fline

        entries.append(f"{fpath}###{fname_clean}###{fline}###{fparams}")

    # 无论有无条目都写文件
    with open("tainted.list", "w", encoding="utf-8") as f:
        if entries:
            f.write("\n".join(entries) + "\n")
        else:
            f.write("# 无需跟入子函数\n")

    if errors:
        print(f"⚠️  {len(errors)} 个格式错误（仍已写入有效条目）:", file=sys.stderr)
        for e in errors[:5]:
            print(f"   {e}", file=sys.stderr)

    if entries:
        print(f"✅ 已写入 tainted.list: {len(entries)} 个子函数")
    else:
        print("✅ 已写入 tainted.list: 无需跟入子函数（叶函数）")


if __name__ == "__main__":
    main()
