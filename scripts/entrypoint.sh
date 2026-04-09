#!/bin/bash
# 容器入口脚本
# 1. 如果挂载了 models.json，链接到 pi 配置目录
# 2. 执行传入的 CMD

set -e

PI_DIR="${PI_CODING_AGENT_DIR:-/root/.pi/agent}"
mkdir -p "$PI_DIR"

# 自动链接 models.json
if [ -f /data/config/models.json ]; then
    ln -sf /data/config/models.json "$PI_DIR/models.json"
    echo "[entrypoint] models.json linked → $PI_DIR/models.json"
fi

# 自动链接自定义 prompts（如果有）
if [ -d /data/config/prompts ]; then
    echo "[entrypoint] custom prompts found at /data/config/prompts/"
fi

exec "$@"
