# data_flow_analyse

基于多 Agent 协作的外部输入数据流自动化分析系统。多个 Worker 并行分析同一函数，多个 Judge 独立评审，迭代优化直到通过。子函数调用自动递归分析。

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                    Orchestrator                          │
│                                                         │
│  Round 1 (min_rounds=2, 即使通过也强制反思)              │
│  ┌───────────┐  ┌───────────┐                           │
│  │ Worker-0  │  │ Worker-1  │  ← 并行，独立工作目录      │
│  │ (session) │  │ (session) │  ← 保持上下文跨轮累积      │
│  └─────┬─────┘  └─────┬─────┘                           │
│        │ dataflow.md   │ dataflow.md                    │
│        ▼               ▼                                │
│  ┌─────────────────────────────────────────┐            │
│  │           文件交换层                      │            │
│  │  worker-0-output.md  worker-0-dataflow.md│            │
│  │  worker-1-output.md  worker-1-dataflow.md│            │
│  └─────────────────────────────────────────┘            │
│        │                     │                          │
│  ┌─────┴──────┐  ┌──────────┴─┐                         │
│  │  Judge-0   │  │  Judge-1   │  ← 并行                 │
│  │            │  │            │                          │
│  │ [新上下文]  │  │ [新上下文]  │  ← 评 worker-0         │
│  │  eval w-0  │  │  eval w-0  │                          │
│  │ [新上下文]  │  │ [新上下文]  │  ← 评 worker-1         │
│  │  eval w-1  │  │  eval w-1  │                          │
│  │ [新上下文]  │  │ [新上下文]  │  ← 读所有 eval，对比   │
│  │  summary   │  │  summary   │                          │
│  └────────────┘  └────────────┘                         │
│        │                                                │
│        ▼                                                │
│  投票: pass_count >= threshold → PASSED                 │
│  Round < min_rounds → 强制下一轮（自我反思）              │
│                                                         │
│  Round 2+ (带反馈迭代)                                   │
│  Workers 收到 feedback.md → 改进 → Judges 重新评审       │
│                                                         │
│  最终输出: dataflow 树状图 + 归档 zip                    │
└─────────────────────────────────────────────────────────┘
```

### 关键设计

| 特性 | 说明 |
|------|------|
| **递归追踪** | 子函数调用自动触发新的 Worker+Judge 流水线，防止上下文过长和跟踪深度不足 |
| **深度可控** | `max_trace_depth` 配置递归深度（默认 3），自动去重防止循环 |
| **全量跟入** | 所有函数调用都尝试跟入，只有确认找不到定义的才标记 EXPORT |
| **Worker 并行** | 多个 Worker 同时分析同一函数，各自独立工作目录 |
| **Worker 保持上下文** | 使用 `--session` 跨轮累积上下文，第 2 轮能看到第 1 轮的全部对话 |
| **Judge 独立上下文** | 每次评审新起上下文（`--no-session`），防止 Worker 间评审互相影响 |
| **Judge 读文件评审** | Worker 输出以文件形式传递给 Judge，Judge 用 `read` 工具读取后评审 |
| **最小轮数** | `min_rounds=2`：即使第 1 轮全票通过，也强制进行反思迭代轮 |
| **错误重试** | API 调用失败自动重试（可配置次数和间隔） |

## 目录结构

```
data_flow_analyse/
├── app/
│   ├── models.py        # 数据模型（ServiceConfig, TaskConfig, ...）
│   ├── config.py        # 配置加载 + prompt 解析
│   ├── runner.py        # pi Agent 子进程执行器（重试机制）
│   ├── orchestrator.py  # 多 Agent 编排核心
│   └── server.py        # REST API 服务器
├── prompts/
│   ├── workers/default.md   # Worker system prompt（污点追踪规则）
│   └── judges/default.md    # Judge system prompt（评审规则）
├── cli.py               # CLI 入口
├── main.py              # REST 服务入口
├── config.example.json  # 服务配置示例
├── Dockerfile           # 容器构建
├── deploy.sh            # 一键部署脚本
└── scripts/entrypoint.sh
```

## 快速开始

### 1. 准备配置文件

`config.json`（服务提供者配置一次，长期不变）：

```json
{
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": 2,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "agents": [
            { "model": "vllm/zai-org/GLM-5" },
            { "model": "vllm/zai-org/GLM-5" }
        ]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "agents": [
            { "model": "vllm/zai-org/GLM-5" },
            { "model": "vllm/zai-org/GLM-5" }
        ]
    },
    "output_dir": "/data/output",
    "context": "Ghidra 反编译的嵌入式固件代码",
    "criteria": "重点：外部输入识别完整性、污点追踪深度、数据处理函数覆盖"
}
```

### 2. 准备模型配置

`models.json`（放在配置目录下，容器启动时自动链接）：

```json
[
    {
        "id": "vllm/zai-org/GLM-5",
        "provider": "openai",
        "apiKey": "1234",
        "baseUrl": "http://172.31.29.10:8000/v1"
    }
]
```

### 3. 运行分析

**用户只需提供：待分析代码（挂载）+ 一句话 prompt**

```bash
docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse \
  python3 cli.py "对 firmware.c 的 parse_packet 函数完成数据流分析"
```

### 4. 查看结果

分析完成后，输出目录包含：

```
output/
├── firmware_parse_packet.md          # 最终数据流树状图文档
└── firmware_parse_packet_log.zip     # 完整过程归档
```

## 配置说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_rounds` | 3 | 最大迭代轮数 |
| `min_rounds` | 2 | 最少执行轮数（强制自我反思） |
| `pass_threshold` | `ceil(judges/2)` | 通过所需的 Judge 投票数 |
| `agent_max_retries` | 100 | API 错误时最大重试次数 |
| `agent_retry_delay` | 30 | 首次重试等待秒数（指数退避） |
| `max_trace_depth` | 3 | 函数调用递归追踪最大深度 |
| `workers.agents[]` | - | Worker 实例列表，每个可指定独立模型 |
| `judges.agents[]` | - | Judge 实例列表，每个可指定独立模型 |
| `context` | "" | 全局额外上下文（所有任务共用） |
| `criteria` | "" | 全局评判标准 |

## REST API 模式

启动服务：

```bash
docker run -d --network host \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse \
  python3 main.py
```

提交分析：

```bash
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
| `/tasks` | GET | 列出所有任务 |
| `/health` | GET | 健康检查 |

## 输出格式

最终输出为完整的数据流树状图文档（`.md`），包含：

- **元信息头**：task_id、status、轮数、耗时
- **外部输入清单**：所有入口点及类型
- **数据流树状图**：每个输入的完整追踪路径
  - 🔴 TAINTED — 未清洗的脏数据
  - 🟢 CLEANED — 经过有效校验/清洗
  - 🟡 EXPORT — 传入外部函数（跨模块边界）
  - 📌 USED — 数据消费点（循环控制、条件判断等）
  - [DEFERRED] — 需要跟入但本次分析未展开
- **污点终点汇总表**
- **数据处理函数清单**

## 部署

```bash
# 一键部署（同步代码 → 构建镜像 → 清理残留）
bash deploy.sh
```

## 挂载说明

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 待分析的源代码文件 | 只读 |
| `/data/config` | 服务配置 (`config.json` + `models.json`) | 只读 |
| `/data/output` | 分析结果输出 | 读写 |
