# data_flow_analyse

多 Agent 协同数据流追踪分析工具 — 基于 [pi-coding-agent](https://github.com/badlogic/pi-mono)

多个 Worker Agent 独立追踪同一函数的外部输入数据流 → Judge Agent 逐个评判并对比 → 未通过则迭代改进 → 直到质量达标 → 打包归档全部工作过程。

```
              config.json 驱动一切
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│                  data_flow_analyse 容器                    │
│                                                          │
│   Round 1                                                │
│   ┌────────────────────────────────────────────┐         │
│   │  Worker 0 (Claude) ──session──► 保持上下文  │         │
│   │  Worker 1 (GPT-4o) ──session──► 保持上下文  │         │
│   │  各自独立追踪同一函数的数据流                 │         │
│   └────────────────┬───────────────────────────┘         │
│                    ▼                                      │
│   ┌────────────────────────────────────────────┐         │
│   │  Judge 0 ──no-session──► 每轮重置           │         │
│   │    ├─ 评判 worker-0 → eval-worker-0.md     │         │
│   │    ├─ 评判 worker-1 → eval-worker-1.md     │         │
│   │    └─ 对比总结 → summary.md                 │         │
│   │  Judge 1 ──no-session──► 每轮重置           │         │
│   │    └─ 同上                                  │         │
│   └────────────────┬───────────────────────────┘         │
│                    ▼                                      │
│              投票通过？                                    │
│              No → feedback.md 注入 Worker → Round 2       │
│              Yes → 打包 output/{task_id}/                 │
└──────────────────────────────────────────────────────────┘
```

## 核心设计

1. **JSON 配置驱动** — 每个 Worker/Judge 实例独立指定模型、工具、思考级别
2. **Worker 保持上下文** — pi session 跨轮累积，改进时能看到完整历史
3. **Judge 每轮重置** — 独立评审，逐个评判每个 Worker，最后对比总结
4. **全过程 .md 归档** — Worker 输出、Judge 评判、反馈、session 全部归档

---

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt
npm install -g @mariozechner/pi-coding-agent

# 配置
cp config.example.json config.json
# 编辑 config.json（填入任务描述、目标函数、模型）
export ANTHROPIC_API_KEY=sk-ant-...

# 运行
python cli.py config.json
```

---

## 配置文件

```json
{
    "task": "分析文件 target/firmware.c 中函数 parse_network_packet 的外部输入数据流",

    "max_rounds": 3,
    "pass_threshold": 2,

    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "./prompts/workers",
        "default_thinking_level": "high",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514", "thinking_level": "high" },
            { "model": "openai/gpt-4o", "thinking_level": "medium" }
        ]
    },

    "judges": {
        "default_tools": ["read", "bash", "grep", "find", "ls"],
        "system_prompt_dir": "./prompts/judges",
        "default_thinking_level": "medium",
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514" },
            { "model": "openai/gpt-4o" }
        ]
    },

    "cwd": "./workspace",
    "output_dir": "./output",
    "context": "Ghidra 反编译的嵌入式固件代码，C 风格，大量指针操作",
    "criteria": "重点：外部输入识别完整性、追踪深度、处理函数覆盖"
}
```

每个 `agents[]` 元素独立指定 model/tools/thinking_level，不填则用角色默认值。

---

## System Prompt 文件夹

```
prompts/
├── workers/
│   ├── default.md        ← 所有 Worker 共用
│   ├── worker-0.md       ← 可选：Worker 0 专用（覆盖 default）
│   └── worker-1.md       ← 可选：Worker 1 专用
└── judges/
    ├── default.md        ← 所有 Judge 共用
    ├── judge-0.md        ← 可选：Judge 0 专用
    └── judge-1.md        ← 可选：Judge 1 专用
```

---

## 输出归档结构

```
output/{task_id}/
├── round-1/
│   ├── workers/
│   │   ├── worker-0-output.md          Worker 0 的数据流追踪结果
│   │   └── worker-1-output.md          Worker 1 的数据流追踪结果
│   ├── judges/
│   │   ├── judge-0/
│   │   │   ├── eval-worker-0.md        Judge 0 对 Worker 0 的评价
│   │   │   ├── eval-worker-1.md        Judge 0 对 Worker 1 的评价
│   │   │   └── summary.md             Judge 0 的对比总结
│   │   └── judge-1/
│   │       └── ...
│   └── feedback.md                     汇总反馈（注入下一轮 Worker）
├── round-2/
│   └── ...
├── sessions/
│   ├── worker-0.jsonl                  Worker 0 完整会话历史
│   ├── worker-1.jsonl                  Worker 1 完整会话历史
│   ├── judge-0-round-1.jsonl           Judge 0 第 1 轮多轮对话
│   └── ...
├── output.md                           最终输出（最佳 Worker 的结果）
├── report.md                           完整报告
└── result.json                         机器可读数据
```

---

## 运行方式

### CLI

```bash
python cli.py config.json
python cli.py config.json --quiet
```

### REST API

```bash
python main.py

curl -X POST http://localhost:3000/task \
  -H "Content-Type: application/json" \
  -d @config.json
```

### Docker

```bash
docker compose up --build

docker run --rm -v ./config.json:/app/config.json \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  data_flow_analyse python cli.py /app/config.json
```

---

## 项目结构

```
data_flow_analyse/
├── app/
│   ├── models.py           Pydantic 数据模型
│   ├── config.py           JSON 配置 + system prompt 加载
│   ├── runner.py           pi 子进程执行器（session/no-session）
│   ├── orchestrator.py     编排引擎（Worker→Judge 循环）
│   └── server.py           FastAPI REST API + SSE
├── prompts/
│   ├── workers/default.md  Worker 角色：数据流追踪 + 污点标记
│   └── judges/default.md   Judge 角色：评审追踪完整性和深度
├── config.example.json
├── cli.py
├── main.py
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

---

## License

MIT
