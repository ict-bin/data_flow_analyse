# data_flow_analyse 容器使用手册

---

## 目录结构准备

在宿主机上准备以下目录：

```
~/my_analysis/
├── target/                    ← 放你的反编译文件
│   ├── firmware.c
│   ├── protocol.c
│   └── include/
│       └── types.h
├── config/                    ← 放配置文件和自定义 prompts
│   ├── config.json
│   └── prompts/               ← 可选，覆盖容器内默认 prompts
│       ├── workers/
│       │   └── default.md
│       └── judges/
│           └── default.md
└── output/                    ← 结果输出到这里（容器自动写入）
```

---

## 第一步：准备配置文件

创建 `config/config.json`：

```json
{
    "task": "分析文件 firmware.c 中函数 parse_network_packet 的外部输入数据流。跟踪所有外部数据从入口到被清洗或传出模块的完整路径，输出数据流树状图。",

    "source_file": "firmware.c",
    "function_name": "parse_network_packet",

    "max_rounds": 3,
    "pass_threshold": 2,

    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/data/config/prompts/workers",
        "default_thinking_level": "high",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514", "thinking_level": "high" },
            { "model": "openai/gpt-4o", "thinking_level": "medium" }
        ]
    },

    "judges": {
        "default_tools": ["read", "bash", "grep", "find", "ls"],
        "system_prompt_dir": "/data/config/prompts/judges",
        "default_thinking_level": "medium",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514" },
            { "model": "openai/gpt-4o" }
        ]
    },

    "cwd": "/data/target",
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output",

    "context": "Ghidra 反编译产物，C 风格伪代码，大量指针操作和手动内存管理",
    "criteria": "重点：外部输入识别完整性、污点追踪深度（子函数必须跟入）、数据处理函数覆盖"
}
```

### 配置文件中的路径说明

| 字段 | 值 | 说明 |
|------|----|------|
| `cwd` | `/data/target` | Agent 的工作目录 = 你挂载进去的待分析文件目录 |
| `output_dir` | `/data/output` | 工作过程文档的临时输出目录 |
| `archive_dir` | `/data/output` | 压缩包 `xxx_log.zip` 输出位置 |
| `result_dir` | `/data/output` | 格式化后的最终结果 `xxx.md` 输出位置 |
| `system_prompt_dir` | `/data/config/prompts/workers` | 如果你挂载了自定义 prompts，指向容器内路径 |

如果不需要自定义 prompts，`system_prompt_dir` 也可以写容器内默认路径：
```json
"system_prompt_dir": "/opt/data_flow_analyse/prompts/workers"
```

---

## 第二步：运行

### 方式一：CMD 模式（跑一次任务，结束自动退出）

```bash
docker run --rm \
  -v ~/my_analysis/target:/data/target:ro \
  -v ~/my_analysis/config:/data/config:ro \
  -v ~/my_analysis/output:/data/output \
  -e ANTHROPIC_API_KEY=sk-ant-xxx \
  -e OPENAI_API_KEY=sk-xxx \
  data_flow_analyse \
  python3 cli.py /data/config/config.json
```

**说明**：
- `--rm`：容器退出后自动删除
- `-v .../target:/data/target:ro`：待分析文件，只读挂载
- `-v .../config:/data/config:ro`：配置文件，只读挂载
- `-v .../output:/data/output`：输出目录，可写
- `-e ANTHROPIC_API_KEY=...`：API Key 通过环境变量传入
- 最后的 `python3 cli.py /data/config/config.json` 覆盖默认 CMD

**控制台输出示例**：
```
╔═══════════════════════════════════════════════════════════╗
║                 data_flow_analyse CLI                     ║
╠═══════════════════════════════════════════════════════════╣
║  Workers:    2     Judges: 2                              ║
║  Max Rounds: 3                                            ║
╚═══════════════════════════════════════════════════════════╝
  worker-0: anthropic/claude-sonnet-4-20250514
  worker-1: openai/gpt-4o
  judge-0:  anthropic/claude-sonnet-4-20250514
  judge-1:  openai/gpt-4o

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Round 1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🔧 worker-0 (anthropic/claude-sonnet-4-20250514) starting...
  🔧 worker-1 (openai/gpt-4o) starting...
  ✅ worker-0 done → ...
  ✅ worker-1 done → ...
  ⚖️  judge-0 evaluating...
     ✅ judge-0→worker-0: PASS (82/100)
     ❌ judge-0→worker-1: FAIL (55/100)
     📊 judge-0 summary: best=worker-0, passed=True
  ...
  ➜ ✅ PASSED  (2/2 judges)
     Best worker: worker-0

📊 Summary:
   Status:   passed
   Rounds:   1
   Duration: 45.2s
   Cost:     $0.1234
   Archive:  /data/output/
   Result:   /data/output/
```

**任务结束后容器自动退出，宿主机 `~/my_analysis/output/` 中找到结果。**


### 方式二：REST API 模式（常驻服务，接受多个任务）

```bash
docker run -d --name dfa \
  -p 3000:3000 \
  -v ~/my_analysis/target:/data/target:ro \
  -v ~/my_analysis/config:/data/config:ro \
  -v ~/my_analysis/output:/data/output \
  -e ANTHROPIC_API_KEY=sk-ant-xxx \
  -e OPENAI_API_KEY=sk-xxx \
  data_flow_analyse
```

然后通过 HTTP 提交任务：

```bash
# 提交任务
curl -X POST http://localhost:3000/task \
  -H "Content-Type: application/json" \
  -d @~/my_analysis/config/config.json

# 响应：
# {"task_id":"task-xxx","status":"accepted","stream":"/task/task-xxx/stream","result":"/task/task-xxx"}

# 实时查看进度（SSE 事件流）
curl -N http://localhost:3000/task/task-xxx/stream

# 查看结果
curl http://localhost:3000/task/task-xxx

# 查看所有任务
curl http://localhost:3000/tasks

# 中止任务
curl -X POST http://localhost:3000/task/task-xxx/abort

# 健康检查
curl http://localhost:3000/health
```

**带回调通知**（任务完成后 POST 到你的服务）：

```bash
curl -X POST "http://localhost:3000/task?callback_url=http://your-server:8080/webhook" \
  -H "Content-Type: application/json" \
  -d @~/my_analysis/config/config.json
```

任务完成时会 POST 到 `callback_url`：
```json
{
  "task_id": "task-xxx",
  "status": "passed",
  "duration_ms": 45200,
  "cost": 0.1234,
  "error": null
}
```

---

## 第三步：查看输出

任务完成后 `~/my_analysis/output/` 中会有：

```
~/my_analysis/output/
├── firmware_parse_network_packet.md           ← 最终结果（格式化清理后）
└── firmware_parse_network_packet_log.zip      ← 完整工作过程归档
```

### 最终结果 `.md`

格式化后的最佳 Worker 输出，带元信息头：

```markdown
---
task_id: task-1712678400-abc123
status: passed
best_worker: worker-0
model: anthropic/claude-sonnet-4-20250514
rounds: 2
duration: 45.2s
cost: $0.1234
---

# 数据流追踪：parse_network_packet

## INPUT-1: pkt_buf (recv() 返回) 🔴 TAINTED

├── [L42] 赋值: header = *(pkt_header_t*)pkt_buf  🔴
│   ├── [L45] header.length → memcpy 第3参数  🔴 → 📌 USED
...
```

### 归档压缩包 `_log.zip`

解压后包含全部工作过程：

```
task-xxx/
├── round-1/
│   ├── workers/
│   │   ├── worker-0-output.md      Worker 0 的数据流追踪
│   │   └── worker-1-output.md      Worker 1 的数据流追踪
│   ├── judges/
│   │   ├── judge-0/
│   │   │   ├── eval-worker-0.md    Judge 0 对 Worker 0 的评价
│   │   │   ├── eval-worker-1.md    Judge 0 对 Worker 1 的评价
│   │   │   └── summary.md          Judge 0 的对比总结
│   │   └── judge-1/
│   │       └── ...
│   └── feedback.md                  汇总反馈
├── round-2/
│   └── ...
├── sessions/
│   ├── worker-0.jsonl               Worker 0 完整对话历史
│   ├── worker-1.jsonl               Worker 1 完整对话历史
│   ├── judge-0-round-1.jsonl        Judge 0 第1轮的多轮对话
│   └── ...
├── report.md                        完整报告
└── result.json                      机器可读数据
```

---

## 常见场景

### 只用一个 Worker + 两个 Judge

```json
{
    "task": "分析 protocol.c 中 handle_message 的数据流",
    "source_file": "protocol.c",
    "function_name": "handle_message",
    "max_rounds": 3,
    "pass_threshold": 2,
    "workers": {
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514", "thinking_level": "high" }
        ]
    },
    "judges": {
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514" },
            { "model": "openai/gpt-4o" }
        ]
    },
    "cwd": "/data/target",
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output"
}
```

### 三个不同模型的 Worker 竞赛

```json
{
    "task": "...",
    "source_file": "parser.c",
    "function_name": "decode_tlv",
    "workers": {
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514", "thinking_level": "high" },
            { "model": "openai/gpt-4o", "thinking_level": "medium" },
            { "model": "google/gemini-2.5-pro", "thinking_level": "medium" }
        ]
    },
    "judges": {
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514" },
            { "model": "openai/gpt-4o" }
        ]
    }
}
```

### 分析多个函数（分别建配置文件，依次运行）

```bash
# config/config_func1.json → parse_header
# config/config_func2.json → process_payload
# config/config_func3.json → handle_response

for cfg in config/config_func*.json; do
  docker run --rm \
    -v ~/my_analysis/target:/data/target:ro \
    -v ~/my_analysis/config:/data/config:ro \
    -v ~/my_analysis/output:/data/output \
    -e ANTHROPIC_API_KEY=sk-ant-xxx \
    data_flow_analyse \
    python3 cli.py /data/config/$(basename $cfg)
done

ls ~/my_analysis/output/
# firmware_parse_header.md
# firmware_parse_header_log.zip
# firmware_process_payload.md
# firmware_process_payload_log.zip
# firmware_handle_response.md
# firmware_handle_response_log.zip
```

---

## 挂载点一览

| 容器路径 | 宿主机挂载 | 权限 | 用途 |
|---------|-----------|------|------|
| `/data/target` | 你的反编译文件目录 | `ro`（只读）| Agent 读取分析 |
| `/data/config` | 配置文件目录 | `ro`（只读）| `config.json` + 自定义 prompts |
| `/data/output` | 结果输出目录 | `rw`（读写）| `.md` 结果 + `_log.zip` 归档 |

---

## 环境变量

| 变量 | 必填 | 说明 |
|------|------|------|
| `ANTHROPIC_API_KEY` | 看配置 | Anthropic 模型的 API Key |
| `OPENAI_API_KEY` | 看配置 | OpenAI 模型的 API Key |
| `PORT` | 否 | REST API 端口（默认 3000）|

**只需要填你配置文件中用到的模型的 Key**。比如 agents 里全是 anthropic 的模型，就只需要 `ANTHROPIC_API_KEY`。

---

## 快速参考

```bash
# CMD 一次性（最常用）
docker run --rm \
  -v ~/target:/data/target:ro \
  -v ~/config:/data/config:ro \
  -v ~/output:/data/output \
  -e ANTHROPIC_API_KEY=xxx \
  data_flow_analyse \
  python3 cli.py /data/config/config.json

# REST API 常驻
docker run -d -p 3000:3000 \
  -v ~/target:/data/target:ro \
  -v ~/config:/data/config:ro \
  -v ~/output:/data/output \
  -e ANTHROPIC_API_KEY=xxx \
  data_flow_analyse

# 查看运行日志
docker logs -f dfa

# 停止
docker stop dfa && docker rm dfa
```
