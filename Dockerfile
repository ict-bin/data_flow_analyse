FROM dfa-base:layer5

ENV PYTHONUNBUFFERED=1

# ═══ 项目代码 ═════════════════════════════════════════════════════════════════
WORKDIR /opt/data_flow_analyse
COPY app/               ./app/
COPY cli.py main.py     ./
COPY prompts/           ./prompts/
COPY scripts/           ./scripts/
COPY config.example.json .env.example ./
RUN chmod +x scripts/*.sh 2>/dev/null || true

# ═══ pi 配置目录 ══════════════════════════════════════════════════════════════
# pi 的全局配置目录，models.json 放这里才能被 pi 识别
# 容器启动脚本会将 /data/config/models.json 链接到此处
ENV PI_CODING_AGENT_DIR=/root/.pi/agent
RUN mkdir -p /root/.pi/agent

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
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# 默认 REST API，覆盖: python3 cli.py /data/config/config.json
CMD ["python3", "main.py"]
