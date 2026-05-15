#!/usr/bin/env python3
"""gen_tainted_list — 生成结构化的函数跟入列表

用法:
  bash gen_tainted_list <<'CALLEES'
  文件路径###Class::FuncName###L行号###污点形参1,污点形参2
  @taintvar###变量名###L行号###来源函数或说明
  CALLEES

每行格式:
  1) 普通 callee: 文件路径###函数全限定名###L行号###污点形参列表
  2) 新污点对象: @taintvar###变量名###L行号###来源函数或说明

字段规则:
  - 文件路径: 相对路径（如 src-vul/openthread/src/common/message.cpp），不确定填 -
  - 函数全限定名: Class::Method 格式，不确定类名先 grep 确认
  - L行号: 如 L245，不确定填 -
  - 污点形参: 被调函数的形参名（逗号分隔），不确定填 *

示例:
  src-vul/openthread/src/core/common/message.cpp###Message::Read###L245###aOffset,aLength
  -###LeaderBase::SetCommissioningData###L301###aValue,aValueLength
  @taintvar###out_var###L123###RecvPacket(output-param)

无需跟入子函数时也必须调用（空输入）:
  echo "" | bash gen_tainted_list

输出:
  - tainted.list（普通 callee 记录）
  - taintvars.json（新导入污点对象，若存在）
"""

import sys
import re
import json


def _safe_print(text: str, *, err: bool = False) -> None:
    stream = sys.stderr if err else sys.stdout
    try:
        print(text, file=stream)
    except UnicodeEncodeError:
        fallback = text.encode(stream.encoding or "utf-8", errors="replace").decode(stream.encoding or "utf-8", errors="replace")
        print(fallback, file=stream)


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    entries = []
    taintvars = []
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

        # 特殊记录：新污点对象
        if fpath == "@taintvar":
            if not fname:
                errors.append(f"行{i}: @taintvar 缺少变量名")
                continue
            if fline and fline != "-" and not fline.startswith("L"):
                fline = "L" + fline
            taintvars.append({
                "name": fname,
                "line": fline or "-",
                "source": fparams or "-",
                "kind": "output-param",
            })
            continue

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

    with open("taintvars.json", "w", encoding="utf-8") as f:
        json.dump(taintvars, f, ensure_ascii=False, indent=2)

    if errors:
        _safe_print(f"⚠️  {len(errors)} 个格式错误（仍已写入有效条目）:", err=True)
        for e in errors[:5]:
            _safe_print(f"   {e}", err=True)

    if entries:
        _safe_print(f"✅ 已写入 tainted.list: {len(entries)} 个子函数")
    else:
        _safe_print("✅ 已写入 tainted.list: 无需跟入子函数（叶函数）")
    _safe_print(f"✅ 已写入 taintvars.json: {len(taintvars)} 个新污点对象")


if __name__ == "__main__":
    main()
