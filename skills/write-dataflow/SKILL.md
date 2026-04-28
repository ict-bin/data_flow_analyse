---
name: write-dataflow
description: Output data flow analysis results after completing taint analysis. Writes dataflow report and tainted.list callee file. USE THIS as the final step of every taint analysis task.
---

# Write Dataflow Analysis Output

After completing the taint analysis, you **must** call both tools below.

## Step 1: Write Dataflow Report

```bash
bash gen_dataflow "FunctionName" <<'DATA_FLOW_DOC'
# 数据流追踪: FunctionName

## 函数信息
- 文件: src-vul/openthread/.../foo.cpp
- 行号: L228-L282
- 签名: `ReturnType FunctionName(Type param1, Type param2)`

## 数据流树状图

### INPUT-1: param1 (Type) 🔴 TAINTED
├── [L230] `local = param1.field` → local 🔴 TAINTED
│   └── [L240] SubFunc(local) → 📎 见 tainted.list
└── [L280] result = process(local) → 📌 USED

### INPUT-2: param2 (Type) 🔴 TAINTED
└── [L250] Response(param2) → 📎 见 tainted.list

## 污点终点汇总
| 脏数据 | 终点类型 | 位置 | 说明 |
|--------|---------|------|------|
| param1 | 📎 子函数 | L240 | 传入 SubFunc |
| param2 | 📎 子函数 | L250 | 传入 Response |
DATA_FLOW_DOC
```

## Step 2: Write Tainted Callee List

List only functions that **actually receive tainted parameters**.  
Do NOT list: condition checks, pure getters, logging functions, stdlib.

```bash
bash gen_tainted_list <<'TAINTED_CALLEE_LIST'
src-vul/openthread/src/core/common/message.cpp###Message::Read###L245###aOffset,aLength
-###LeaderBase::SetCommissioningData###L301###aValue,aValueLength
TAINTED_CALLEE_LIST
```

**Format per line**: `file_path###Class::FuncName###L_line###param1,param2`

- `file_path`: relative path from workspace root, or `-` if unknown
- `Class::FuncName`: fully qualified name (grep to confirm class name if unsure)
- `L_line`: call site line number, or `-` if unknown  
- `params`: callee's **formal parameter names** that receive taint, or `*` if unsure

**If no callees** (leaf function), still call with empty input:
```bash
echo "" | bash gen_tainted_list
```
