"""data_flow_analyse — REST API 服务器

  Management layer (persistent, project-scoped):
    POST /api/app/dataflow-analyse/tasks          创建任务
    GET  /api/app/dataflow-analyse/tasks          任务列表（project_id 过滤）
    GET  /api/app/dataflow-analyse/tasks/{id}     任务详情
    GET  /api/app/dataflow-analyse/tasks/{id}/logs  实时阶段事件
    POST /api/app/dataflow-analyse/tasks/{id}/cancel   取消任务
    POST /api/app/dataflow-analyse/tasks/{id}/restart  重新运行
    POST /api/app/dataflow-analyse/tasks/{id}/resume   断点续跑
    DELETE /api/app/dataflow-analyse/tasks/{id}        删除任务
    GET  /api/app/dataflow-analyse/prompts        Prompt 模板列表
    POST /api/app/dataflow-analyse/prompts        创建 Prompt 模板
    GET  /api/app/dataflow-analyse/prompts/{id}   Prompt 模板详情
    PUT  /api/app/dataflow-analyse/prompts/{id}   更新 Prompt 模板
    DELETE /api/app/dataflow-analyse/prompts/{id} 删除 Prompt 模板
    POST /api/app/dataflow-analyse/prompts/{id}/clone  克隆 Prompt 模板
    POST /api/app/dataflow-analyse/generate-prompt    根据路径生成 prompt
    GET  /api/app/dataflow-analyse/health         健康检查

  Legacy engine routes (in-memory, backward compat):
    POST /analyse           直接提交分析（CLI 兼容）
    GET  /task/{id}         查询结果
    GET  /task/{id}/stream  SSE 实时事件流
    POST /task/{id}/abort   中止
    GET  /tasks             列出任务
    GET  /health            健康检查
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from threading import Lock
from typing import Any, Callable

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .build_info import build_service_meta
from .config import build_task_config, get_service_yaml, load_service_config
from .logging_utils import configure_container_logging
from .metrics import normalize_http_route, observe_http_request as observe_metrics_request, observe_http_request_inflight, render_aggregate_metrics, render_local_metrics, render_summary_metrics
from .metrics_summary import build_ai_summary, build_generic_observability_summary, build_rest_api_summary, parse_prometheus_metrics
from .models import SwarmEvent, TaskResult, TaskStatus, make_id
from .orchestrator import Orchestrator
from .runtime_context import (
    DISPATCHER_ENABLED,
    EXECUTOR_ENABLED,
    INSTANCE_ID,
    PUBLIC_API_ENABLED,
    REGISTRY_ENABLED,
    ROLE,
)
from .service.runtime_bootstrap import get_runtime_bootstrap
from .service.task_service import get_task_service
from .time_utils import now_local
from .logging_utils import log_event

load_dotenv()
configure_container_logging("01-dataflow_analyse")

# 使用统一的路径配置（优先读取环境变量）
from .config import CONFIG_DIR, TARGET_DIR

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", f"{CONFIG_DIR}/config.json")
CLEANUP_DELAY = int(os.environ.get("CLEANUP_DELAY", "300"))
_SUMMARY_CACHE_TTL_SECONDS = 5.0
_summary_cache: dict[str, tuple[float, Any]] = {}
_summary_cache_lock = Lock()

logger = logging.getLogger("dfa.server")


def _cached_summary(key: str, builder: Callable[[], Any]) -> Any:
    now = time.monotonic()
    with _summary_cache_lock:
        cached = _summary_cache.get(key)
        if cached and now - cached[0] <= _SUMMARY_CACHE_TTL_SECONDS:
            return cached[1]
    value = builder()
    with _summary_cache_lock:
        _summary_cache[key] = (time.monotonic(), value)
    return value


def _aggregate_metrics_rows():
    return parse_prometheus_metrics(render_summary_metrics())


class TaskEntry:
    def __init__(self, orch: Orchestrator, task_id: str, prompt: str):
        self.orch = orch
        self.task_id = task_id
        self.prompt = prompt
        self.result: TaskResult | None = None
        self.events: list[dict] = []
        self.queues: list[asyncio.Queue] = []
        self.done = asyncio.Event()
        self.callback_url: str | None = None


_tasks: dict[str, TaskEntry] = {}


def _forbidden_for_role(feature: str) -> HTTPException:
    return HTTPException(status_code=503, detail=f"{feature} disabled for role={ROLE}")


# ─── Lifespan ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    await get_runtime_bootstrap().start(app)

    yield
    # --- shutdown ---
    await get_runtime_bootstrap().stop()


app = FastAPI(title="data_flow_analyse", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
get_runtime_bootstrap().install_internal_observability_router(app)


@app.middleware("http")
async def collect_request_metrics(request, call_next):
    started = time.perf_counter()
    response = None
    route = request.scope.get("route")
    path = getattr(route, "path", None) or request.url.path
    normalized_route = normalize_http_route(str(path))
    observe_http_request_inflight(request.method, normalized_route, 1)
    try:
        response = await call_next(request)
        return response
    finally:
        status_code = response.status_code if response is not None else 500
        observe_metrics_request(request.method, str(path), status_code, time.perf_counter() - started)
        observe_http_request_inflight(request.method, normalized_route, -1)

# 启动时加载一次服务配置
_svc_config = None


def _get_svc_config():
    global _svc_config
    if _svc_config is None:
        for p in [SERVICE_CONFIG_PATH, "/opt/data_flow_analyse/config.example.json"]:
            if os.path.isfile(p):
                _svc_config = load_service_config(p)
                break
        if _svc_config is None:
            raise RuntimeError(f"服务配置文件不存在: {SERVICE_CONFIG_PATH}")
    return _svc_config


@app.get("/metrics")
@app.get("/api/app/dataflow-analyse/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(render_local_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/api/app/dataflow-analyse/metrics/aggregate", include_in_schema=False)
async def aggregate_metrics():
    return PlainTextResponse(render_aggregate_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/api/app/dataflow-analyse/metrics/summary", include_in_schema=False)
async def metrics_summary():
    return await run_in_threadpool(
        _cached_summary,
        "summary",
        lambda: build_generic_observability_summary(_aggregate_metrics_rows(), title="数据流分析"),
    )


@app.get("/api/app/dataflow-analyse/metrics/rest-api-summary", include_in_schema=False)
async def metrics_rest_api_summary():
    return await run_in_threadpool(
        _cached_summary,
        "rest-api-summary",
        lambda: build_rest_api_summary(_aggregate_metrics_rows()),
    )


@app.get("/api/app/dataflow-analyse/metrics/ai-summary", include_in_schema=False)
async def metrics_ai_summary():
    return await run_in_threadpool(
        _cached_summary,
        "ai-summary",
        lambda: build_ai_summary(_aggregate_metrics_rows(), coverage_text="数据流分析 AI 指标覆盖 trace / round / review / judge 相关调用。"),
    )


# ─── 请求体 ──────────────────────────────────────────────────────────────────

class AnalyseRequest(BaseModel):
    prompt: str = Field(..., description="一句话任务描述，如：对 firmware.c 的 parse_packet 函数完成数据流分析")
    cwd: str = Field(default="", description="待分析文件目录，默认 /data/target")
    callback_url: str = Field(default="", description="任务完成后 POST 通知的 URL")


# ─── 路由 ─────────────────────────────────────────────────────────────────────

@app.get("/health")
@app.get("/api/app/dataflow-analyse/health")
async def health():
    bootstrap = get_runtime_bootstrap().status()
    payload = {
        "status": "ok",
        **build_service_meta(),
        "instance_id": INSTANCE_ID,
        "role": ROLE,
        "public_api_enabled": PUBLIC_API_ENABLED,
        "dispatcher_enabled": DISPATCHER_ENABLED,
        "executor_enabled": EXECUTOR_ENABLED,
        "registry_enabled": REGISTRY_ENABLED,
        "active": sum(1 for t in _tasks.values() if t.result is None),
        "completed": sum(1 for t in _tasks.values() if t.result is not None),
        "dispatcher_running": get_runtime_bootstrap().dispatcher_running(),
        "leased_tasks": get_task_service().local_running_task_count(),
        "startup_phase": bootstrap["phase"],
        "startup_ready": bootstrap["ready"],
        "startup_error": bootstrap["error"],
        "db_ready": bootstrap["db_ready"],
        "management_api_ready": bootstrap["management_api_ready"],
        "bootstrap_attempts": bootstrap["attempts"],
    }
    if bootstrap["ready"]:
        return payload
    payload["status"] = "starting" if bootstrap["error"] is None else "error"
    return JSONResponse(status_code=503, content=payload)


@app.post("/analyse", status_code=202)
async def submit_analyse(body: AnalyseRequest):
    """提交分析任务。只需一句话 prompt。"""
    if not PUBLIC_API_ENABLED:
        raise _forbidden_for_role("legacy submit API")
    if not EXECUTOR_ENABLED:
        raise _forbidden_for_role("legacy in-process executor")
    svc = _get_svc_config()
    cwd = body.cwd or TARGET_DIR
    cfg = build_task_config(svc, body.prompt, cwd=cwd)
    task_id = make_id()

    def on_event(event: SwarmEvent):
        entry = _tasks.get(task_id)
        if not entry:
            return
        d = event.model_dump()
        entry.events.append(d)
        for q in entry.queues:
            try:
                q.put_nowait(d)
            except asyncio.QueueFull:
                pass

    orch = Orchestrator(config=cfg, on_event=on_event)
    entry = TaskEntry(orch, task_id, body.prompt)
    entry.callback_url = body.callback_url or None
    _tasks[task_id] = entry

    async def _run():
        try:
            entry.result = await orch.execute_recursive(task_id)
        except Exception as e:
            entry.result = TaskResult(
                task_id=task_id, status=TaskStatus.ERROR,
                task=body.prompt, error=str(e))
        finally:
            done_data = {
                "type": "done", "task_id": task_id,
                "status": entry.result.status.value if entry.result else "error",
            }
            for q in entry.queues:
                try:
                    q.put_nowait(done_data)
                except asyncio.QueueFull:
                    pass
            entry.done.set()
            if entry.callback_url and entry.result:
                await _notify(entry)
            await asyncio.sleep(CLEANUP_DELAY)
            _tasks.pop(task_id, None)

    asyncio.create_task(_run())
    return {
        "task_id": task_id,
        "source_file": cfg.source_file,
        "function_name": cfg.function_name,
        "status": "accepted",
        "stream": f"/task/{task_id}/stream",
        "result": f"/task/{task_id}",
    }


async def _notify(entry: TaskEntry):
    if not entry.callback_url or not entry.result:
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(entry.callback_url, json={
                "task_id": entry.task_id,
                "status": entry.result.status.value,
                "duration_ms": entry.result.total_duration_ms,
                "cost": entry.result.total_tokens.cost,
            })
    except Exception:
        pass


@app.get("/task/{task_id}")
async def get_task(task_id: str):
    if not PUBLIC_API_ENABLED:
        raise _forbidden_for_role("legacy task API")
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    if entry.result:
        return entry.result.model_dump()
    return {"task_id": task_id, "status": "running", "events_count": len(entry.events)}


@app.get("/task/{task_id}/stream")
async def stream_task(task_id: str):
    if not PUBLIC_API_ENABLED:
        raise _forbidden_for_role("legacy task stream API")
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    entry.queues.append(queue)

    async def gen():
        for evt in entry.events:
            yield {"data": json.dumps(evt, ensure_ascii=False)}
        if entry.result:
            yield {"data": json.dumps({"type": "done", "task_id": task_id})}
            return
        try:
            while True:
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=30)
                    yield {"data": json.dumps(evt, ensure_ascii=False)}
                    if evt.get("type") == "done":
                        return
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            if queue in entry.queues:
                entry.queues.remove(queue)

    return EventSourceResponse(gen())


@app.post("/task/{task_id}/abort")
async def abort_task(task_id: str):
    if not PUBLIC_API_ENABLED:
        raise _forbidden_for_role("legacy task abort API")
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404)
    if entry.result:
        return {"message": "Already completed", "status": entry.result.status.value}
    entry.orch.abort()
    return {"message": "Abort sent", "task_id": task_id}


@app.get("/tasks")
async def list_tasks():
    if not PUBLIC_API_ENABLED:
        raise _forbidden_for_role("legacy task list API")
    return {"tasks": [
        {"task_id": tid, "prompt": e.prompt[:100],
         "status": e.result.status.value if e.result else "running"}
        for tid, e in _tasks.items()
    ]}
