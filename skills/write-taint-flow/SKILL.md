---
name: write-taint-flow
description: Write per-taint analysis report for one taint parameter. USE AFTER completing analysis of a single taint parameter within the current function.
---

# Write Single-Taint Flow Report

After deeply analyzing ONE taint parameter within the current function, write its report.

Use the **write** tool to create `taint-flow-{PARAM_NAME}.md`:

```
write("taint-flow-PARAM_NAME.md", """
# 污点流: PARAM_NAME

## 污点源
- 参数: PARAM_NAME (Type) 🔴 TAINTED  
- 来源: 外部输入参数

## 当前函数内传播路径

### 直接使用
├── [L???] `code` → result 🔴 TAINTED (说明)
└── [L???] 传入 SubFunc → 📎 子函数接收污点

### 派生变量
- `derived_var = operation(PARAM_NAME)` → 🔴 TAINTED

## 新导入的污点对象
- `out_var` 🔴 TAINTED — 由 `Recv/Read/Get/...(&out_var)` 在某行写入（如适用）
- 若当前函数通过输出参数/缓冲区导入了新的污点对象，必须在此列出，并在后续传播路径中继续追踪该对象

## 接收此污点的子函数
（只列在当前函数内调用的、实际接收此污点数据的函数）

| 函数 | 调用位置 | 接收的形参 |
|------|---------|-----------|
| Class::Method | L??? | paramName |

## 污点终点
| 终点 | 类型 | 位置 |
|------|------|------|
| SubFunc(param) | 📎 子函数 | L??? |
""")
```

Replace `PARAM_NAME` with the actual parameter name.
