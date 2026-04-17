# data_flow_analyse

`data_flow_analyse` 用于从一个已知入口函数出发，递归追踪外部输入在函数调用链中的传播过程，并输出合并后的数据流分析结果。

典型问题包括：

- 外部输入从哪个参数进入函数
- 哪些子函数继续消费了污染数据
- 哪些路径做了校验、转换、拼接、转发或落盘
- 哪些调用属于外部函数，哪些值得继续跟入

## 核心流程

```text
源文件 + 入口函数
  -> Worker 分析当前函数
  -> Judge 评审
  -> 解析需要继续跟入的子函数
  -> 对子函数递归重复同样流程
  -> Merge Agent 合并所有层级结果
  -> 输出 merged dataflow
```

## 目录结构

```text
04-data_flow_analyse/
├── app/
│   ├── config.py
│   ├── models.py
│   ├── runner.py
│   ├── orchestrator.py
│   └── server.py
├── prompts/
│   ├── workers/
│   ├── judges/
│   └── merge/
├── scripts/
├── cli.py
├── main.py
├── chained_runner.py
├── config.example.json
├── ENV_REFERENCE.md
├── USAGE.md
├── Dockerfile
├── Dockerfile.chain
└── docker-compose.yml
```

## 输入与输出

### 输入

- `/data/target`：待分析源码目录
- prompt：例如 `"对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析"`

### 输出

独立运行时典型输出：

```text
output/
├── flag
├── libipsec_IPSEC_SOCKI_PipeMsg.md
└── libipsec_IPSEC_SOCKI_PipeMsg_log.zip
```

链式运行时典型输出：

```text
/app/.run/04-dataflow/output/
├── tasks/
│   └── <module>__<file>__<func>/
└── summary.json
```

## 快速开始

### 1. CLI 运行

```bash
docker build -t data_flow_analyse .

docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  data_flow_analyse \
  python3 cli.py "对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析" \
  --config /data/config/config.json \
  --cwd /data/target
```

### 2. REST API 运行

```bash
docker run -d --name data-flow-analyse \
  -p 3000:3000 \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  data_flow_analyse
```

提交任务：

```bash
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "对 libipsec.c 的 IPSEC_SOCKI_PipeMsg 函数完成数据流分析",
    "cwd": "/data/target"
  }'
```

常用接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `POST` | `/analyse` | 提交数据流分析任务 |
| `GET` | `/task/{id}` | 查看结果 |
| `GET` | `/task/{id}/stream` | SSE 事件流 |
| `POST` | `/task/{id}/abort` | 中止任务 |
| `GET` | `/tasks` | 列出任务 |

## 递归追踪的几个关键点

- 只对被标记为值得跟入的子函数继续分析
- 通过 `max_trace_depth` 控制最大递归深度
- 对已经分析过的函数做去重，避免环路重复
- 对明显外部函数或无定义函数跳过递归
- 最终由 merge prompt 把多层结果合并成统一文档

## 链式模式中的位置

在根目录链式流水线中，本模块对应 `04-dataflow`。

它会：

1. 读取 `03-entry/output/entrypoints.json`
2. 为每个入口函数创建独立 task 目录
3. 调用本模块 CLI 分析
4. 把每个 task 的结果写到 `.run/04-dataflow/output/tasks/`

## 配置示例

最常用配置见 [config.example.json](config.example.json)，其中重点字段是：

- `max_rounds`
- `min_rounds`
- `pass_threshold`
- `max_trace_depth`
- `workers.agents`
- `judges.agents`

模型和环境变量说明见 [ENV_REFERENCE.md](ENV_REFERENCE.md)。

## 相关文档

- [USAGE.md](USAGE.md)
- [仓库 README](../README.md)
- [CHAINED_PIPELINE.md](../CHAINED_PIPELINE.md)
