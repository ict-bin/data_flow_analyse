#!/bin/bash
# 03_dataflow_analyse/run.sh
# 自动读取 02_entry_analyse 的输出，解析指定行的函数信息，启动数据流分析
#
# 用法:
#   ./run.sh                         # 分析默认入口（默认读第9行）
#   ./run.sh --line 9                # 指定行号
#   ./run.sh --entry /path/to/functions.list --line 3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FULL_TEST_DIR="$(dirname "$SCRIPT_DIR")"

# ── 默认参数 ──────────────────────────────────────────────────────────────────
ENTRY_LIST="${FULL_TEST_DIR}/02_entry_analyse/output/functions.list"
TARGET_DIR="${FULL_TEST_DIR}/01_system_analyse/firmware"
CONFIG_DIR="${SCRIPT_DIR}/config"
OUTPUT_DIR="${SCRIPT_DIR}/output"
LINE_NUM=9

# ── 解析命令行参数 ────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --line)    LINE_NUM="$2";   shift 2 ;;
        --entry)   ENTRY_LIST="$2"; shift 2 ;;
        --target)  TARGET_DIR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── 校验 ──────────────────────────────────────────────────────────────────────
if [[ ! -f "$ENTRY_LIST" ]]; then
    echo "ERROR: functions.list not found: $ENTRY_LIST" >&2
    exit 1
fi

# ── 解析指定行 ────────────────────────────────────────────────────────────────
# 格式: <src_file>:<Class>::<Method>:<LineNo>:<param1>,<param2>,...
# 示例: src-vul/openthread/.../network_data_leader_ftd.cpp:Leader::HandleCommissioningSet:L228:aHeader,aMessage,aMessageInfo
LINE_CONTENT="$(sed -n "${LINE_NUM}p" "$ENTRY_LIST")"
if [[ -z "$LINE_CONTENT" ]]; then
    echo "ERROR: line ${LINE_NUM} is empty in $ENTRY_LIST" >&2
    exit 1
fi

# 用 Python 做精确解析（处理 C++ 双冒号方法名）
read -r SRC_FILE FUNC_NAME LINE_NO TAINTED_PARAMS < <(python3 - "$LINE_CONTENT" << 'PYEOF'
import sys, re

line = sys.argv[1].strip()
parts = line.split(':')

# 第一部分是文件路径
src_file = parts[0]

# 从第二段开始，找到第一个匹配 L\d+ 的作为行号
func_parts = []
line_no = ""
param_parts = []
i = 1
while i < len(parts):
    if re.match(r'^L\d+$', parts[i]):
        line_no = parts[i]
        param_parts = parts[i+1:]
        break
    func_parts.append(parts[i])
    i += 1

# 过滤空字符串（C++ :: 分割后会产生空串），再用 :: 拼接
func_name = '::'.join(p for p in func_parts if p)
tainted = ':'.join(param_parts)
print(src_file, func_name, line_no, tainted)
PYEOF
)

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  03_dataflow_analyse — 自动解析入口                      ║"
echo "╠══════════════════════════════════════════════════════════╣"
printf "║  Entry list : %-44s║\n" "$(basename $ENTRY_LIST) (line $LINE_NUM)"
printf "║  Source file: %-44s║\n" "$(basename $SRC_FILE)"
printf "║  Function   : %-44s║\n" "$FUNC_NAME"
printf "║  Line       : %-44s║\n" "$LINE_NO"
printf "║  Tainted    : %-44s║\n" "$TAINTED_PARAMS"
echo "╚══════════════════════════════════════════════════════════╝"

# ── 构造分析 prompt ───────────────────────────────────────────────────────────
PROMPT="对 ${SRC_FILE} 的 ${FUNC_NAME}（${LINE_NO}）函数进行静态污点分析，外部输入参数（已污染）为：${TAINTED_PARAMS}"

# ── 启动容器 ──────────────────────────────────────────────────────────────────
mkdir -p "$OUTPUT_DIR"

echo ""
echo "Starting analysis..."
nohup docker run --rm --network host \
  --name dataflow_03 \
  ${DRYRUN:+-e DRYRUN=1} \
  -v "${TARGET_DIR}:/data/target:ro" \
  -v "${CONFIG_DIR}:/data/config:ro" \
  -v "${OUTPUT_DIR}:/data/output" \
  data_flow_analyse \
  python3 cli.py \
    --config /data/config/config.json \
    "${PROMPT}" \
  > "${SCRIPT_DIR}/run.log" 2>&1 &

echo "Container started (PID=$!), logs: ${SCRIPT_DIR}/run.log"
echo ""
sleep 3
echo "=== Initial log ==="
cat "${SCRIPT_DIR}/run.log"
