# data_flow_analyse

基于多 Agent 协作的外部输入数据流自动化分析系统。递归追踪函数调用链，每个函数独立经过 Worker+Judge 流水线分析，最终由 Merge Agent 合并为完整的数据流树状图。

## 核心架构

```
用户: "对 libipsec.so.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析"
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
    │           └─ Worker+Judge 流水线
    │               ├─ depth=3: IPSEC_LIBI_HandleInputPktV4
    │               │   └─ depth=4: IPSEC_AH_HandleInputPktV4
    │               │       └─ depth=5: ...
    │               └─ depth=3: IPSEC_SOCK_SendToSocket
    │                   └─ ...
    ├─ depth=1: IPSEC_SOCK_ProcPipeData ⏭️ (已分析，跳过)
    └─ depth=1: IPSEC_MakeDbgCompStrSetter
        └─ ...
                            │
                            ▼
                ┌───────────────────────┐
                │  Merge Agent          │
                │  读取所有 dataflow    │
                │  合并为统一文档       │
                │  (专用 system prompt) │
                └───────────┬───────────┘
                            │
                            ▼
            ┌─────────────────────────────┐
            │  最终输出                    │
            │  • merged-dataflow.md       │
            │  • 统一归档 _log.zip        │
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
| **全量跟入** | 所有函数调用都 grep 搜索定义，找不到才标 EXPORT |
| **只追踪脏参数** | 子函数分析时只追踪调用者标记的被污染参数 |
| **自动去重** | 同一函数不会被重复分析（跨分支共享 analyzed 集合） |
| **单层上限** | 每个函数最多递归 10 个子函数，防止爆炸 |
| **Worker 并行** | 多个 Worker 同时分析同一函数，各自独立工作目录 |
| **Worker 保持上下文** | `--session` 跨轮累积，第 2 轮看到第 1 轮全部对话 |
| **Judge 独立上下文** | 每次评审 `--no-session`，防止 Worker 间评审互相影响 |
| **最小轮数** | `min_rounds=2`：即使第 1 轮全票通过，也强制反思 |
| **Merge Agent** | 专用 system prompt + 精确输出模板，合并所有层级 |
| **统一归档** | 所有子任务工作目录保留在根任务下，最终统一压缩 |
| **错误重试** | API 失败自动重试（可配置次数和间隔） |

## 目录结构

```
data_flow_analyse/
├── app/
│   ├── models.py        # 数据模型
│   ├── config.py        # 配置加载 + prompt 解析
│   ├── runner.py        # pi Agent 子进程执行器（重试机制）
│   ├── orchestrator.py  # 多 Agent 编排核心（递归 + merge）
│   └── server.py        # REST API 服务器
├── prompts/
│   ├── workers/default.md   # Worker system prompt（污点追踪 + 子函数列表）
│   ├── judges/default.md    # Judge system prompt（评审规则 + markdown 输出）
│   └── merge/default.md     # Merge system prompt（合并格式模板）
├── cli.py               # CLI 入口
├── main.py              # REST 服务入口
├── config.example.json  # 服务配置示例
├── Dockerfile
├── deploy.sh            # 一键部署脚本
└── examples/            # 测试结果示例
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
    "output_dir": "/data/output",
    "context": "Ghidra 反编译的嵌入式固件代码",
    "criteria": "重点：外部输入识别完整性、污点追踪深度、数据处理函数覆盖"
}
```

`models.json`（同目录下）：

```json
[{
    "id": "vllm/zai-org/GLM-5",
    "provider": "openai",
    "apiKey": "1234",
    "baseUrl": "http://172.31.29.10:8000/v1"
}]
```

### 2. 运行分析

```bash
docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse \
  python3 cli.py "对 libipsec.so.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析"
```

### 3. 查看结果

```
output/
├── libipsec.so_IPSEC_SOCKI_PipeMsg.md        # 最终合并的数据流文档
└── libipsec.so_IPSEC_SOCKI_PipeMsg_log.zip   # 完整归档
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_rounds` | 3 | 每个函数最大 Worker+Judge 迭代轮数 |
| `min_rounds` | 2 | 每个函数最少轮数（强制自我反思） |
| `pass_threshold` | `ceil(judges/2)` | 通过所需的 Judge 投票数 |
| `max_trace_depth` | 3 | 函数调用递归追踪最大深度 |
| `agent_max_retries` | 100 | API 错误最大重试次数 |
| `agent_retry_delay` | 30 | 首次重试等待秒数（指数退避） |
| `workers.agents[]` | - | Worker 实例列表，每个可指定独立模型 |
| `judges.agents[]` | - | Judge 实例列表 |
| `context` | "" | 全局上下文（如：反编译代码风格说明） |
| `criteria` | "" | 全局评判标准 |

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
| `/data/output` | 分析结果输出 | 读写 |

## 部署

```bash
bash deploy.sh   # 同步代码 → 构建镜像 → 清理残留
```
