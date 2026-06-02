ARG SECFLOW_PI_AGENT_RUNTIME_IMAGE=ghcr.io/runshine/secflow-base-pi-agent-runtime:20260602
FROM ${SECFLOW_PI_AGENT_RUNTIME_IMAGE}

ARG SECFLOW_BUILD_VERSION=""
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends zip \
    && rm -rf /var/lib/apt/lists/*

# ═══ 项目代码 ═════════════════════════════════════════════════════════════════
WORKDIR /opt/data_flow_analyse
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -q
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY tools/             ./tools/
COPY skills/            ./skills/
COPY config.example.json .env.example ./
RUN printf '{"build_version":"%s"}\n' "$SECFLOW_BUILD_VERSION" > /opt/data_flow_analyse/build_meta.json
RUN find . -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh 2>/dev/null || true
# 安装工具：extract_func / gen_dataflow / gen_tainted_list 供 Worker 直接调用
RUN cp tools/extract_func.py /usr/local/bin/extract_func \
    && chmod +x /usr/local/bin/extract_func \
    && cp tools/gen_dataflow.py /usr/local/bin/gen_dataflow \
    && chmod +x /usr/local/bin/gen_dataflow \
    && cp tools/gen_tainted_list.py /usr/local/bin/gen_tainted_list \
    && chmod +x /usr/local/bin/gen_tainted_list

# ═══ pi 配置目录 ══════════════════════════════════════════════════════════════
# pi 的全局配置目录，models.json 放这里才能被 pi 识别
# 容器启动脚本会将 /data/config/models.json 链接到此处
ENV PI_CODING_AGENT_DIR=/root/.pi/agent
RUN mkdir -p /root/.pi/agent/bin \
    # 将 write-dataflow skill 安装到 pi 全局发现目录
    # ~/.pi/agent/skills/ 是 pi 全局 skill 目录，任何 cwd 都能发现
    && mkdir -p /root/.pi/agent/skills \
    && ln -sf /opt/data_flow_analyse/skills/write-dataflow /root/.pi/agent/skills/write-dataflow \
    && ln -sf /opt/data_flow_analyse/skills/write-taint-flow /root/.pi/agent/skills/write-taint-flow

RUN ln -sf "$(which rg)" /root/.pi/agent/bin/rg \
    && echo "ripgrep ready: $(rg --version | head -1)"

# ═══ 挂载点 ═══════════════════════════════════════════════════════════════════
#
# /data/target  — 待分析文件（只读）
# /data/config  — config.json + models.json + prompts/（只读）
# /data/output  — 输出目录
#
RUN mkdir -p /data/target /data/config /data/output /data/workspace /data/sessions
# 不声明 VOLUME（避免匿名卷遮盖 bind mount）

ENV PORT=3000
ENV OUTPUT_DIR=/data/output
ENV ARCHIVE_DIR=/data/output
ENV RESULT_DIR=/data/output
ENV SESSION_DIR=/data/sessions

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# ═══ 入口脚本 ═════════════════════════════════════════════════════════════════
# 启动前自动链接 models.json（如果挂载了的话）
COPY scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆盖: python3 cli.py /data/config/config.json
CMD ["python3", "main.py"]
