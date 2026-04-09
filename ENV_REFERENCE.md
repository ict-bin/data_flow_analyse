# `-e` 环境变量参考

## 你的场景

使用 `http://172.31.29.10:8000` 的 `zai-org/GLM-5` 模型，API Key 为 `12345`。

---

## 准备文件

### 1. `config/models.json` — 告诉 pi 你的模型在哪

```json
{
    "providers": {
        "glm": {
            "baseUrl": "http://172.31.29.10:8000/v1",
            "api": "openai-completions",
            "apiKey": "GLM_API_KEY",
            "compat": {
                "supportsDeveloperRole": false,
                "supportsReasoningEffort": false
            },
            "models": [
                {
                    "id": "zai-org/GLM-5",
                    "name": "GLM-5",
                    "reasoning": false,
                    "input": ["text"],
                    "contextWindow": 128000,
                    "maxTokens": 8192
                }
            ]
        }
    }
}
```

**关键字段**：
- `"apiKey": "GLM_API_KEY"` — 这里写的是**环境变量名**，不是密钥本身
- pi 启动时从环境变量 `GLM_API_KEY` 读取实际密钥
- 所以 docker run 时用 `-e GLM_API_KEY=12345` 传入

### 2. `config/config.json` — 引用模型

```json
{
    "task": "分析 firmware.c 中 parse_network_packet 的数据流",
    "source_file": "firmware.c",
    "function_name": "parse_network_packet",
    "max_rounds": 2,
    "pass_threshold": 1,

    "workers": {
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/workers",
        "agents": [
            { "model": "glm/zai-org/GLM-5" },
            { "model": "glm/zai-org/GLM-5" }
        ]
    },
    "judges": {
        "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
        "agents": [
            { "model": "glm/zai-org/GLM-5" }
        ]
    },

    "cwd": "/data/target",
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output"
}
```

**模型引用格式**：`"<provider名>/<model的id>"` → `"glm/zai-org/GLM-5"`

---

## 运行

```bash
docker run --rm \
  -v ~/my_analysis/target:/data/target:ro \
  -v ~/my_analysis/config:/data/config:ro \
  -v ~/my_analysis/output:/data/output \
  -e GLM_API_KEY=12345 \
  data_flow_analyse \
  python3 cli.py /data/config/config.json
```

---

## `-e` 环境变量一览

### API Key 变量

| 变量 | 用途 | 何时需要 |
|------|------|---------|
| `GLM_API_KEY=12345` | 自定义模型的 Key | models.json 中 `"apiKey": "GLM_API_KEY"` |
| `ANTHROPIC_API_KEY=sk-ant-xxx` | Anthropic | agents 中用 anthropic 模型时 |
| `OPENAI_API_KEY=sk-xxx` | OpenAI | agents 中用 openai 模型时 |
| `GOOGLE_API_KEY=xxx` | Google Gemini | agents 中用 google 模型时 |

**规则**：`models.json` 中 `"apiKey"` 字段的值就是环境变量名。你可以随便命名：

```json
"apiKey": "MY_SECRET_KEY"
```
→ docker run 时 `-e MY_SECRET_KEY=actual_password`

### 系统变量

| 变量 | 默认值 | 用途 |
|------|--------|------|
| `PORT` | `3000` | REST API 端口 |
| `OUTPUT_DIR` | `/data/output` | 工作临时目录 |
| `ARCHIVE_DIR` | `/data/output` | 压缩包输出目录 |
| `RESULT_DIR` | `/data/output` | 最终结果输出目录 |
| `SESSION_DIR` | `/data/sessions` | Session 存储目录 |
| `CLEANUP_DELAY` | `300` | REST 模式下任务完成后清理延迟（秒）|

---

## 混合使用多个模型（不同 Provider）

同时用内网 GLM 和 Anthropic Claude：

### models.json

```json
{
    "providers": {
        "glm": {
            "baseUrl": "http://172.31.29.10:8000/v1",
            "api": "openai-completions",
            "apiKey": "GLM_API_KEY",
            "compat": { "supportsDeveloperRole": false, "supportsReasoningEffort": false },
            "models": [
                { "id": "zai-org/GLM-5", "name": "GLM-5", "contextWindow": 128000, "maxTokens": 8192 }
            ]
        }
    }
}
```

### config.json

```json
{
    "workers": {
        "agents": [
            { "model": "glm/zai-org/GLM-5" },
            { "model": "anthropic/claude-sonnet-4-20250514", "thinking_level": "high" }
        ]
    },
    "judges": {
        "agents": [
            { "model": "anthropic/claude-sonnet-4-20250514" }
        ]
    }
}
```

### 运行

```bash
docker run --rm \
  -v ~/target:/data/target:ro \
  -v ~/config:/data/config:ro \
  -v ~/output:/data/output \
  -e GLM_API_KEY=12345 \
  -e ANTHROPIC_API_KEY=sk-ant-xxx \
  data_flow_analyse \
  python3 cli.py /data/config/config.json
```

---

## 多个自定义模型端点

```json
{
    "providers": {
        "glm": {
            "baseUrl": "http://172.31.29.10:8000/v1",
            "api": "openai-completions",
            "apiKey": "GLM_KEY",
            "compat": { "supportsDeveloperRole": false, "supportsReasoningEffort": false },
            "models": [
                { "id": "zai-org/GLM-5", "name": "GLM-5" }
            ]
        },
        "qwen": {
            "baseUrl": "http://172.31.29.20:8001/v1",
            "api": "openai-completions",
            "apiKey": "QWEN_KEY",
            "compat": { "supportsDeveloperRole": false, "supportsReasoningEffort": false },
            "models": [
                { "id": "Qwen/Qwen3-72B", "name": "Qwen3-72B" }
            ]
        },
        "deepseek": {
            "baseUrl": "https://api.deepseek.com/v1",
            "api": "openai-completions",
            "apiKey": "DEEPSEEK_KEY",
            "models": [
                { "id": "deepseek-chat", "name": "DeepSeek V3" }
            ]
        }
    }
}
```

```bash
docker run --rm \
  -v ~/config:/data/config:ro \
  -v ~/target:/data/target:ro \
  -v ~/output:/data/output \
  -e GLM_KEY=12345 \
  -e QWEN_KEY=abcde \
  -e DEEPSEEK_KEY=sk-xxx \
  data_flow_analyse \
  python3 cli.py /data/config/config.json
```

config.json 中引用：`"glm/zai-org/GLM-5"`、`"qwen/Qwen/Qwen3-72B"`、`"deepseek/deepseek-chat"`

---

## `compat` 字段说明

大部分 vLLM / Ollama / 自建 OpenAI 兼容服务都需要设置：

```json
"compat": {
    "supportsDeveloperRole": false,
    "supportsReasoningEffort": false
}
```

如果你的模型支持推理（如 DeepSeek-R1），可以设置：

```json
{
    "id": "deepseek-r1",
    "reasoning": true,
    "compat": {
        "supportsDeveloperRole": false,
        "supportsReasoningEffort": true
    }
}
```

---

## 重要提醒（snap Docker 用户）

此设备的 Docker 是 snap 安装的，**不能挂载 `/tmp` 下的目录**。挂载路径必须在用户 home 目录下：

```bash
# ✅ 正确
-v ~/my_analysis/config:/data/config:ro

# ❌ 不工作（snap 限制）
-v /tmp/config:/data/config:ro
```
