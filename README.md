# data_flow_analyse

基于多 Agent 协作的外部输入数据流自动化分析系统。递归追踪函数调用链，每个函数独立经过 Worker+Judge 流水线分析，最终由 Merge Agent 合并为完整的数据流树状图。

## 核心架构

```
用户: "对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析"
                            │
                            ▼
                ┌───────────────────────┐
                │  Orchestrator (递归)   │
                └───────────┬───────────┘
                            │
    ┌───────────────────────┼───────────────────────┐
    ▼                       ▼                       ▼
 depth=0               depth=0                  depth=0
 Worker分析            Judge评审               min_rounds
 IPSEC_SOCKI_PipeMsg   独立上下文评判          强制≥2轮反思
    │                       │
    │ 输出: dataflow.md     │
    │ + 需要跟入的函数列表   │
    │                       │
    ▼                       ▼
 解析子函数列表 ──────── 投票通过
    │
    ├─ depth=1: IPSEC_SOCKI_HandlePipeData
    │   └─ Worker+Judge 流水线（只追踪脏参数）
    │       └─ depth=2: IPSEC_SOCK_ProcPipeData
    │           └─ ...
    ├─ depth=1: IPSEC_Print_File ⏭️ (extern, 跳过)
    └─ depth=1: SSP_Debug ⏭️ (extern, 跳过)
                            │
                            ▼
                ┌───────────────────────┐
                │  Merge Agent          │
                │  读取所有 dataflow    │
                │  合并为统一文档       │
                └───────────┬───────────┘
                            │
                            ▼
            ┌─────────────────────────────┐
            │  最终输出                    │
            │  • merged-dataflow.md       │
            │  • 统一归档 _log.zip        │
            │  • flag (1=成功 / 0=失败)   │
            └─────────────────────────────┘
```

### 单函数分析流水线

每个函数（无论 depth 几）都经过相同的 Worker+Judge 流水线：

```
┌───────────┐     ┌─────────────────────┐     ┌────────────┐
│ Worker(s) │ ──▶ │    文件交换层        │ ──▶ │ Judge(s)   │
│ 并行分析  │     │ worker-X-output.md   │     │ 并行评审   │
│ (session) │     │ worker-X-dataflow.md │     │ (独立上下文)│
└───────────┘     └─────────────────────┘     └─────┬──────┘
                                                     │
                              ┌───────────────────────┘
                              ▼
                    投票 pass_count ≥ threshold
                     │                    │
                  PASSED               FAILED
                     │                    │
              rnd < min_rounds?      反馈注入
                  │        │          下一轮
                 是        否
                  │        │
             强制反思    完成 ──▶ 解析子函数列表 ──▶ 递归
```

### 关键设计

| 特性 | 说明 |
|------|------|
| **递归追踪** | 子函数调用自动触发新的 Worker+Judge 流水线 |
| **深度可控** | `max_trace_depth` 配置递归深度（默认 3） |
| **外部函数过滤** | 双层过滤：grep 排除 extern 声明 + 解析时排除 "外部函数" 标记，避免浪费 |
| **只追踪脏参数** | 子函数分析时只追踪调用者标记的被污染参数 |
| **自动去重** | 同一函数不会被重复分析（跨分支共享 analyzed 集合） |
| **单层上限** | 每个函数最多递归 10 个子函数，防止爆炸 |
| **Worker 保持上下文** | `--session` 跨轮累积，第 2 轮看到第 1 轮全部对话 |
| **Judge 独立上下文** | 每次评审 `--no-session`，防止 Worker 间评审互相影响 |
| **最小轮数** | `min_rounds=2`：即使第 1 轮全票通过，也强制反思 |
| **Merge Agent** | 专用 system prompt + 精确输出模板，合并所有层级 |
| **双层重试** | pi 进程级（崩溃/拉起失败）+ API 级（连接/限流），独立计数，-1=无限 |
| **致命错误识别** | model not found / invalid API key 等配置错误立即终止，不重试 |
| **flag 文件** | 任务开始写 `0`，仅成功覆盖为 `1`，崩溃/中断也保证有 flag |

## 目录结构

```
data_flow_analyse/
├── app/
│   ├── models.py        # 数据模型
│   ├── config.py        # 配置加载 + prompt 解析
│   ├── runner.py        # pi Agent 子进程执行器（双层重试 + 致命错误检测）
│   ├── orchestrator.py  # 多 Agent 编排核心（递归 + merge + 外部函数过滤）
│   └── server.py        # REST API 服务器
├── prompts/
│   ├── workers/default.md   # Worker system prompt（污点追踪 + 子函数列表）
│   ├── judges/default.md    # Judge system prompt（评审规则 + markdown 输出）
│   └── merge/default.md     # Merge system prompt（合并格式模板）
├── cli.py               # CLI 入口（美化输出 + 树状层级显示）
├── main.py              # REST 服务入口
├── config.example.json  # 服务配置示例
├── Dockerfile
└── Dockerfile.full      # 全量构建（首次）
```

## 快速开始

### 1. 服务配置（一次性）

`config.json`：

```json
{
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": 1,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "pi_max_retries": -1,
    "pi_retry_delay": 10,
    "max_trace_depth": 5,
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "agents": [{ "model": "vllm/zai-org/GLM-5" }]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "agents": [{ "model": "vllm/zai-org/GLM-5" }]
    },
    "output_dir": "/data/output"
}
```

`models.json`（同目录下，pi 模型配置）：

```json
{
    "providers": {
        "vllm": {
            "baseUrl": "http://172.31.29.10:8000/v1",
            "api": "openai-completions",
            "apiKey": "1234",
            "models": [{ "id": "zai-org/GLM-5", "name": "GLM-5", "contextWindow": 128000, "maxTokens": 8192 }]
        }
    }
}
```

### 2. 运行分析

```bash
docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse \
  python3 cli.py "对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析"
```

### 3. 查看结果

```
output/
├── flag                                      # 1=成功, 0=失败（脚本对接用）
├── libipsec_IPSEC_SOCKI_PipeMsg.md           # 最终合并的数据流文档
└── libipsec_IPSEC_SOCKI_PipeMsg_log.zip      # 完整归档
```

**flag 文件行为**：任务开始即写入 `0`，仅在最终状态为 PASSED 时覆盖为 `1`。中途崩溃、被 kill、异常退出时 flag 始终为 `0`。

### CLI 输出示例

```
┌─────────────────────────────────────────────────┐
│  data_flow_analyse                              │
├─────────────────────────────────────────────────┤
│  IPSEC_SOCKI_PipeMsg                             │
│  libipsec.c                                      │
│  W=1 J=1  rounds=2~3  depth≤6                   │
│  vllm/zai-org/GLM-5                              │
└─────────────────────────────────────────────────┘

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▶ IPSEC_SOCKI_PipeMsg
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  R1: W[✓] → J[82] ✅ 1/1 (552s)
  R2: W[✓] → J[90] ✅ 1/1 (380s)
  → 1 callees: IPSEC_SOCKI_HandlePipeData

  ├─ [d1] IPSEC_SOCKI_HandlePipeData
  │  R1: W[✓] → J[88] ✅ 1/1 (395s)
  │  → 1 callees: IPSEC_SOCKI_PipeData

  │  ├─ [d2] IPSEC_SOCKI_PipeData
  │  │  R1: W[✓] → J[92] ✅ 1/1 (202s)

  🔀 Merging 3 documents... ✅ (11.7KB)

════════════════════════════════════════════════════════════
  ✅ PASSED  │  3 functions  │  1395s
  📄 /data/output/libipsec_IPSEC_SOCKI_PipeMsg.md
  📦 /data/output/libipsec_IPSEC_SOCKI_PipeMsg_log.zip
  ⏭  Skipped: IPSEC_Print_File(extern), SSP_Debug(extern)
════════════════════════════════════════════════════════════
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_rounds` | 3 | 每个函数最大 Worker+Judge 迭代轮数 |
| `min_rounds` | 2 | 每个函数最少轮数（强制自我反思） |
| `pass_threshold` | `ceil(judges/2)` | 通过所需的 Judge 投票数 |
| `max_trace_depth` | 3 | 函数调用递归追踪最大深度 |
| `agent_max_retries` | 100 | API 错误（连接/限流/500）最大重试次数，-1=无限 |
| `agent_retry_delay` | 30 | API 重试首次等待秒数（指数退避，上限 300s） |
| `pi_max_retries` | 3 | pi 进程拉起失败/崩溃重试次数，-1=无限 |
| `pi_retry_delay` | 10 | pi 进程重试首次等待秒数（指数退避，上限 300s） |
| `workers.agents[]` | - | Worker 实例列表，每个可指定独立模型 |
| `judges.agents[]` | - | Judge 实例列表 |

## 重试机制

双层重试，独立计数，独立退避：

| 层级 | 触发条件 | 配置项 | 退避策略 |
|------|---------|--------|---------|
| **pi 进程级** | 拉起失败 / 崩溃 / 信号杀死 / OOM | `pi_max_retries` | `pi_retry_delay × 2^n`，上限 300s |
| **API 级** | 连接超时 / 429 限流 / 500/502/503 | `agent_max_retries` | `agent_retry_delay × 2^n`，上限 300s |
| **致命错误** | model not found / invalid API key / 401 | 不重试 | 立即终止并报告 |

错误分类优先级：致命错误 > pi 进程失败 > API 错误 > 成功

## 输出格式

最终输出由 Merge Agent 生成，格式固定：

```markdown
# 完整数据流追踪：<根函数名>

## 调用链概览
| 层级 | 函数名 | 文件:行号 | 状态 | 追踪的脏数据 |

## 完整数据流树
### [depth=0] <根函数>
#### ↳ [depth=1] <子函数A>
##### ↳ [depth=2] <孙函数>

## 统一污点终点汇总
| 脏数据来源 | 终点类型 | 位置 | 所在函数 | 说明 |

## 统一数据处理函数清单
| 函数名 | 文件:行号 | 接收的脏数据 | 调用层级 | 作用 |

## 追踪统计
| 指标 | 值 |
```

### 污点标记

| 标记 | 含义 |
|------|------|
| 🔴 TAINTED | 未清洗的脏数据 |
| 🟢 CLEANED | 经过有效校验/清洗 |
| 🟡 EXPORT | 传入外部函数（无源码） |
| 📌 USED | 数据消费点 |
| [DEFERRED] | 存入全局/堆，由其他上下文读取 |

## REST API

```bash
# 启动服务
docker run -d --network host \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse python3 main.py

# 提交分析
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{"prompt": "对 firmware.c 的 parse_packet 函数完成数据流分析"}'
```

| 端点 | 方法 | 说明 |
|------|------|------|
| `/analyse` | POST | 提交分析任务 |
| `/task/{id}` | GET | 查询任务状态 |
| `/task/{id}/stream` | GET | SSE 实时事件流 |
| `/task/{id}/abort` | POST | 中止任务 |
| `/health` | GET | 健康检查 |

## 归档结构

```
task-xxx/
├── round-1/workers/          # 根函数 Worker 输出
├── round-2/judges/           # 根函数 Judge 评审
├── sessions/                 # 根函数 Worker session
├── workspace-worker-0/       # 根函数 Worker 工作目录
├── depth1-FuncA/             # 子函数A完整工作目录
│   └── task-.../rounds/sessions/workspace/
├── depth2-FuncB/             # 孙函数B完整工作目录
├── dataflow-RootFunc.md      # 根函数独立 dataflow
├── dataflow-FuncA.md         # 子函数A独立 dataflow
├── dataflow-FuncB.md         # 孙函数B独立 dataflow
├── trace-tree.md             # 调用树结构
├── merged-dataflow.md        # Merge Agent 合并结果
├── report.md
└── result.json
```

## 挂载说明

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 待分析的源代码文件 | 只读 |
| `/data/config` | `config.json` + `models.json` | 只读 |
| `/data/output` | 分析结果输出（含 flag 文件） | 读写 |

## 部署

```bash
# 全量构建（首次）
docker build --network host -f Dockerfile.full -t data_flow_analyse .

# 增量构建（代码更新后，基于 dfa-base:layer5）
docker build --network host -t data_flow_analyse .
```
