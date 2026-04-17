# data_flow_analyse 使用手册

这份文档面向独立运行 `data_flow_analyse`。如果你是从模块入口清单一路跑到数据流，请优先看根目录 [CHAINED_PIPELINE.md](../CHAINED_PIPELINE.md)。

## 1. 准备目录

```text
~/my-analysis/
├── target/
│   └── ...                  # 待分析源码目录
├── config/
│   ├── config.json
│   └── models.json
└── output/
```

挂载约定：

- `target` -> `/data/target`
- `config` -> `/data/config`
- `output` -> `/data/output`

## 2. 准备配置

`config/config.json` 示例：

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
    "default_thinking_level": "off",
    "agents": [{ "model": "gaiasec/auto" }]
  },
  "judges": {
    "default_tools": ["read", "bash", "grep", "find"],
    "system_prompt_dir": "/opt/data_flow_analyse/prompts/judges",
    "default_thinking_level": "off",
    "agents": [{ "model": "gaiasec/auto" }]
  },
  "output_dir": "/data/output",
  "archive_dir": "/data/output",
  "result_dir": "/data/output"
}
```

`models.json` 需要提供当前模型 provider 配置。

## 3. 运行 CLI

```bash
docker run --rm --network host \
  -v ~/my-analysis/target:/data/target:ro \
  -v ~/my-analysis/config:/data/config:ro \
  -v ~/my-analysis/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  data_flow_analyse \
  python3 cli.py "对 firmware.c 的 parse_network_packet 函数完成数据流分析" \
  --config /data/config/config.json \
  --cwd /data/target
```

`prompt` 需要能明确解析出：

- 源文件名
- 入口函数名

例如：

- `对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析`
- `分析 firmware.c 中 parse_packet 的外部输入数据流`

## 4. 运行 REST API

```bash
docker run -d --name data-flow-analyse \
  -p 3000:3000 \
  -v ~/my-analysis/target:/data/target:ro \
  -v ~/my-analysis/config:/data/config:ro \
  -v ~/my-analysis/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  data_flow_analyse
```

提交任务：

```bash
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "对 firmware.c 的 parse_network_packet 函数完成数据流分析",
    "cwd": "/data/target"
  }'
```

查看任务：

```bash
curl http://localhost:3000/task/<task_id>
curl -N http://localhost:3000/task/<task_id>/stream
```

## 5. 输出说明

```text
output/
├── flag
├── firmware_parse_network_packet.md
└── firmware_parse_network_packet_log.zip
```

其中：

- `*.md` 是最终合并后的数据流报告
- `*_log.zip` 是完整过程归档
- `flag` 为 `1` 表示任务通过

## 6. 常见问题

### 函数没有被解析出来

先检查 prompt 是否足够明确，尤其是文件名和函数名是否都写了。

### 递归深度不够

把 `max_trace_depth` 调大；但要注意层数越深，成本和耗时越高。

### 大量子函数被跳过

常见原因包括：

- 子函数没有源码定义
- 子函数被识别为外部函数
- 已在其他分支中分析过，被去重
