# data_flow_analyse

基于 [pi](https://github.com/mariozechner/pi) 多智能体框架的 **C/C++ 数据流污点分析系统**。  
从一个已知入口函数出发，递归追踪外部输入在函数调用链中的传播路径，输出合并后的完整数据流分析报告。

---

## 架构概览

```
┌──────────────────────────────────────────────────────────┐
│  cli.py  ←  用户 prompt（"分析 foo.c 的 Bar 函数"）       │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Orchestrator (app/orchestrator.py)                      │
│                                                          │
│  execute_recursive(depth=0)                              │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Round 1..N  (max_rounds=-1 → ∞)                 │   │
│  │  ┌─────────────────┐   ┌─────────────────┐       │   │
│  │  │  Workers × W    │   │  Judges × J     │       │   │
│  │  │  (并行)          │──▶│  (并行)          │       │   │
│  │  └─────────────────┘   └────────┬────────┘       │   │
│  │                                 │ pass?           │   │
│  └─────────────────────────────────┼────────────────┘   │
│                                    │                      │
│  解析 callee 列表 → 并行递归        │                      │
│  ┌───────────────────────────────┐ │                      │
│  │  callee_1 → execute_recursive│ │                      │
│  │  callee_2 → execute_recursive│◀┘  (asyncio.gather)   │
│  │  callee_3 → execute_recursive│                        │
│  └───────────────────────────────┘                       │
│                                    ▼                      │
│  Merge Agent (合并所有 dataflow-*.md)                     │
└──────────────────────────────────────────────────────────┘
```

### 核心组件

| 文件 | 职责 |
|------|------|
| `cli.py` | CLI 入口，解析 prompt，驱动 Orchestrator，树状进度输出 |
| `app/orchestrator.py` | 编排 Worker+Judge 流水线，递归调用链追踪，并行 callee 分析 |
| `app/runner.py` | 调用 pi 进程执行单个 Agent，处理重试/错误检测/流式输出 |
| `app/config.py` | 解析 config.json，构建 TaskConfig |
| `app/models.py` | Pydantic 数据模型（TaskConfig, ServiceConfig, SwarmEvent 等）|
| `prompts/workers/` | Worker Agent 系统提示词 |
| `prompts/judges/` | Judge Agent 系统提示词 |
| `prompts/merge/` | Merge Agent 系统提示词 |
| `tools/extract_func.py` | 函数提取工具（`extract_func` 命令）|

---

## 执行流程

### 单函数分析（一轮）

```
Worker ─── 读取源码 → 追踪污点路径 → 输出 dataflow-*.md
           ↑
           用 extract_func 精确提取函数代码（而非读整个文件）

Judge ──── 读 Worker 输出 → 打分 (0-100) → pass/fail
           ↑
           通过/失败阈值: pass_threshold（默认：ceil(judges/2)）

Round 通过 → 递归分析 callees
Round 失败 → 生成 feedback → 下一轮注入 Worker
```

### 递归调用链追踪

```
HandleCommissioningSet (depth=0)
├── 第一步：Worker 分析，Judge 通过
├── 解析 callee 表格
│   找到：SetCommissioningData, SendCommissioningSetResponse
│
├── grep 预检（_function_has_definition）
│   过滤：memcpy/malloc 等标准库函数（_STDLIB_SKIP 黑名单）
│   过滤：extern 纯声明
│
└── asyncio.gather 并行启动（受 callee_concurrency 限制）
    ├── SetCommissioningData (depth=1)  ─┐
    └── SendCommissioningSetResponse    ─┤ 同时运行
                                         ↓
                                    Merge Agent 合并
```

### 并行化

| 层次 | 实现 | 说明 |
|------|------|------|
| **多 Worker** | `run_agents_parallel` | 同轮多 Worker 并发分析同一函数 |
| **多 Judge** | `asyncio.gather` | 多 Judge 同时评审，每个 Judge 独立上下文 |
| **Judge 内多 Worker 评判** | `asyncio.gather` | 单 Judge 并发评判各 Worker 输出 |
| **callee 递归** | `asyncio.gather` + Semaphore | 兄弟 callee 并发分析，`callee_concurrency` 限速 |

---

## 配置说明

### config.json 完整字段

```json
{
    "max_rounds": -1,           // 每函数最大轮数，-1=无限
    "min_rounds": 1,            // 最少轮数（即使第1轮通过也继续）
    "pass_threshold": 1,        // Judge 通过票数阈值（默认 ceil(J/2)）
    "max_trace_depth": 5,       // 函数调用递归最大深度
    "callee_concurrency": -1,   // callee 并行数，-1=不限，1=串行，N=最多N个
    "agent_max_retries": 50,    // API 错误重试次数
    "agent_retry_delay": 15,    // 重试初始等待秒（指数退避）
    "pi_max_retries": -1,       // pi 进程拉起失败重试次数，-1=无限
    "pi_retry_delay": 5,        // pi 进程重试等待秒
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "default_thinking_level": "off",
        "agents": [
            { "model": "icsl_vllm_2/MiniMax/MiniMax-M2.5" }
        ]
    },
    "judges": {
        "default_tools": ["read", "bash", "find"],
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "default_thinking_level": "off",
        "agents": [
            { "model": "icsl_vllm_2/MiniMax/MiniMax-M2.5" }
        ]
    },
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir":  "/data/output"
}
```

### models.json（pi 模型配置）

放在 `/data/config/models.json`，容器启动时自动链接到 pi 配置目录。

示例（内网 vllm 模型）：
```json
{
  "models": [
    {
      "name": "icsl_vllm_2/MiniMax/MiniMax-M2.5",
      "provider": "openai-completions",
      "baseUrl": "http://172.31.23.100:8002/v1",
      "model": "MiniMax/MiniMax-M2.5"
    }
  ]
}
```

---

## Docker 部署

### 构建镜像

```bash
docker build --network host -t data_flow_analyse .
```

基础镜像 `dfa-base:layer5` 已预装：
- Node.js + pi agent
- Python 3
- ripgrep（`rg`，避免 pi grep 工具运行时下载）

镜像额外安装：
- `extract_func` 命令（`/usr/local/bin/extract_func`）

### 运行

```bash
docker run --rm --network host \
  -v /path/to/firmware:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  data_flow_analyse \
  python3 cli.py "对 src-vul/openthread/.../foo.cpp 的 Bar::Process 函数完成数据流分析"
```

### 目录挂载

| 容器路径 | 说明 |
|----------|------|
| `/data/target` | 只读，待分析源码目录 |
| `/data/config` | 只读，`config.json` + `models.json` |
| `/data/output` | 读写，分析结果 + 日志压缩包 |

---

## 输出格式

```
/data/output/
├── flag                          # 0=未完成/失败, 1=PASSED
├── <src>_<Func>.md               # 合并后的完整数据流分析报告
└── <src>_<Func>_log.zip          # 所有轮次的 Worker/Judge 交互记录
```

### CLI 输出示例

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ▶ Leader::HandleCommissioningSet
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    [Leader::HandleCommissioningSet] R1:
    [Leader::HandleCommissioningSet] W ........ W[✓] (245s)
    [Leader::HandleCommissioningSet] J ....  J[82] ✅
      → ✅ 1/1 (420s)
  → 2 callees: SetCommissioningData, SendCommissioningSetResponse

  ├─ [d1] SetCommissioningData       ←── 并行启动
  ├─ [d1] SendCommissioningSetResponse ←── 并行启动

    [SetCommissioningData] R1:
    [SetCommissioningData] W ... W[✓] (180s)   ←── 交错输出
    [SendCommissioningSetResponse] R1:
    [SendCommissioningSetResponse] W .... W[✓] (200s)
    ...

  🔀 Merging 3 documents... ✅ (12.4KB)

════════════════════════════════════════════════════════════
  ✅ PASSED  │  3 functions  │  820s
  📄 /data/output/network_data_leader_ftd_Leader__HandleCommissioningSet.md
  📦 /data/output/network_data_leader_ftd_Leader__HandleCommissioningSet_log.zip
════════════════════════════════════════════════════════════
```

---

## `extract_func` 工具

内置于容器的函数提取工具，Worker Agent 可通过 bash 调用，避免读取整个大文件。

```bash
# 提取指定函数
extract_func src-vul/openthread/src/core/thread/network_data_leader_ftd.cpp \
    Leader::HandleCommissioningSet

# 带上下文（前5行）
extract_func file.cpp process_packet --context 5

# 列出文件中所有函数
extract_func file.cpp --list
```

输出示例：
```c
// src-vul/.../network_data_leader_ftd.cpp  L228-L282  (55 lines)
otError Leader::HandleCommissioningSet(
    const Coap::Header &aHeader, Message &aMessage,
    const Ip6::MessageInfo &aMessageInfo)
{
    // ... 函数完整代码 ...
}
```

---

## 错误处理

| 错误类型 | 行为 |
|---------|------|
| API 限流 / 网络超时 | 指数退避重试，最多 `agent_max_retries` 次 |
| pi 进程启动失败 | 重试最多 `pi_max_retries` 次（-1=无限） |
| 模型未找到 / 401 | **致命错误**，立即终止，不重试 |
| Node.js 模块缺失 | **致命错误**，立即终止 |
| callee 无定义 | grep 预检跳过，不启动 Worker+Judge 流水线 |
| 标准库函数 | `_STDLIB_SKIP` 黑名单过滤（memcpy/malloc 等 40+ 函数）|

---

## 开发

### 本地运行（无 Docker）

```bash
pip install -r requirements.txt
python3 cli.py --config config.example.json \
    --cwd /path/to/src \
    "对 foo.c 的 bar_func 完成数据流分析"
```

### 添加新的 Worker/Judge 提示词

在 `prompts/workers/` 或 `prompts/judges/` 目录中放置 `.md` 文件，在 config 的 `system_prompt_dir` 指向该目录即可。多个 Agent 可用各自目录下对应序号的文件（`0.md`, `1.md`，或全部使用 `default.md`）。

### 关键设计决策

- **不使用 pi grep 工具**：pi 的 grep 工具在无外网服务器上运行时会尝试下载 ripgrep，改用 bash grep
- **Docker 不跟随宿主机符号链接**：挂载路径须使用 `readlink -f` 解析真实路径
- **callee 并行安全**：asyncio 单线程，`analyzed` set 无竞争，需在 gather 前预注册防重复
