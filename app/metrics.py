from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

from sqlalchemy.orm import Session

from .db.models import AppDfaTask
from .runtime_context import (
    CLUSTER_EXPECTED_WORKER_CAPACITY,
    CLUSTER_EXPECTED_WORKERS,
    DISPATCHER_ENABLED,
    EXECUTOR_ENABLED,
    HEARTBEAT_INTERVAL_SECONDS,
    MAX_LOCAL_RUNNING_TASKS,
    PUBLIC_API_ENABLED,
    REGISTRY_ENABLED,
    ROLE,
)
from .service.task_service import get_task_service
from .service.worker_snapshot import build_worker_cluster_snapshot

_REQUEST_LOCK = threading.Lock()
_HTTP_REQUEST_TOTAL = defaultdict(int)
_HTTP_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0, "buckets": [0] * 13})
_HTTP_REQUEST_INFLIGHT = defaultdict(int)
_LOCAL_EVENT_LOCK = threading.Lock()
_LOCAL_EVENT_TOTAL = defaultdict(int)
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}
_HTTP_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
_PATH_ID_SEGMENT_RE = re.compile(r"/(?:\d+|[0-9a-f]{8,}|[0-9a-f]{8}-[0-9a-f-]{27,})(?=/|$)", re.IGNORECASE)


def normalize_http_route(path: str | None) -> str:
    raw = str(path or "/").strip() or "/"
    return _PATH_ID_SEGMENT_RE.sub("/{id}", raw)


def http_status_class(status_code: int | str | None) -> str:
    try:
        code = int(status_code or 500)
    except (TypeError, ValueError):
        code = 500
    if code < 0:
        return "cancelled"
    return f"{code // 100}xx"


def observe_http_request_inflight(method: str, route: str, delta: int) -> None:
    key = (str(method or "GET").upper(), normalize_http_route(route))
    with _REQUEST_LOCK:
        _HTTP_REQUEST_INFLIGHT[key] += int(delta)
        if _HTTP_REQUEST_INFLIGHT[key] < 0:
            _HTTP_REQUEST_INFLIGHT[key] = 0


def observe_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    normalized_route = normalize_http_route(path)
    http_key = (method.upper(), normalized_route, http_status_class(status_code), str(int(status_code)))
    duration_key = (method.upper(), normalized_route)
    with _REQUEST_LOCK:
        _HTTP_REQUEST_TOTAL[http_key] += 1
        duration_bucket = _HTTP_REQUEST_DURATION[duration_key]
        duration_bucket["count"] += 1
        duration_bucket["sum"] += max(0.0, float(duration_seconds))
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            if duration_seconds <= upper_bound:
                duration_bucket["buckets"][index] += 1


def observe_local_event(event: str, result: str = "success") -> None:
    key = (str(event or "unknown"), str(result or "success"))
    with _LOCAL_EVENT_LOCK:
        _LOCAL_EVENT_TOTAL[key] += 1


def render_metrics() -> str:
    return render_local_metrics()


def render_local_metrics() -> str:
    lines = ["# HELP secflow_dfa_up Service metrics scrape succeeded.", "# TYPE secflow_dfa_up gauge"]
    try:
        lines.append("secflow_dfa_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_local_runtime_metrics())
        lines.extend(_render_agent_observability_metrics())
    except Exception:
        lines.append("secflow_dfa_up 0")
    return "\n".join(lines) + "\n"


def render_aggregate_metrics() -> str:
    lines = [
        "# HELP secflow_dfa_metrics_aggregate_up DFA aggregate metrics scrape succeeded.",
        "# TYPE secflow_dfa_metrics_aggregate_up gauge",
    ]
    try:
        started = time.perf_counter()
        from .api.tasks import _LAST_AGENT_AGGREGATE_META
        lines.append("secflow_dfa_metrics_aggregate_up 1")
        lines.extend(_render_cluster_task_metrics())
        lines.extend([
            "# HELP secflow_dfa_metrics_aggregate_partial Aggregate metrics returned a partial response.",
            "# TYPE secflow_dfa_metrics_aggregate_partial gauge",
            f"secflow_dfa_metrics_aggregate_partial {1 if _LAST_AGENT_AGGREGATE_META.get('partial') else 0}",
            "# HELP secflow_dfa_agent_aggregate_sources Aggregate agent fanout successful source count.",
            "# TYPE secflow_dfa_agent_aggregate_sources gauge",
            f"secflow_dfa_agent_aggregate_sources {int(_LAST_AGENT_AGGREGATE_META.get('sources') or 0)}",
            "# HELP secflow_dfa_agent_aggregate_fanout_errors_total Aggregate agent fanout error count.",
            "# TYPE secflow_dfa_agent_aggregate_fanout_errors_total gauge",
            f"secflow_dfa_agent_aggregate_fanout_errors_total {int(_LAST_AGENT_AGGREGATE_META.get('fanout_errors') or 0)}",
            "# HELP secflow_dfa_agent_aggregate_duration_seconds Aggregate agent fanout duration in seconds.",
            "# TYPE secflow_dfa_agent_aggregate_duration_seconds gauge",
            f"secflow_dfa_agent_aggregate_duration_seconds {_fmt(float(_LAST_AGENT_AGGREGATE_META.get('duration_seconds') or 0.0))}",
            "# HELP secflow_dfa_agent_aggregate_cache_hits_total Aggregate agent snapshot cache hit count.",
            "# TYPE secflow_dfa_agent_aggregate_cache_hits_total counter",
            f"secflow_dfa_agent_aggregate_cache_hits_total {int(_LAST_AGENT_AGGREGATE_META.get('cache_hits') or 0)}",
            "# HELP secflow_dfa_agent_aggregate_cache_misses_total Aggregate agent snapshot cache miss count.",
            "# TYPE secflow_dfa_agent_aggregate_cache_misses_total counter",
            f"secflow_dfa_agent_aggregate_cache_misses_total {int(_LAST_AGENT_AGGREGATE_META.get('cache_misses') or 0)}",
            "# HELP secflow_dfa_metrics_aggregate_duration_seconds Aggregate metrics render duration in seconds.",
            "# TYPE secflow_dfa_metrics_aggregate_duration_seconds gauge",
            f"secflow_dfa_metrics_aggregate_duration_seconds {_fmt(time.perf_counter() - started)}",
        ])
    except Exception:
        lines.append("secflow_dfa_metrics_aggregate_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_dfa_http_requests_total Total normalized HTTP requests observed by this process.",
        "# TYPE secflow_dfa_http_requests_total counter",
        "# HELP secflow_dfa_http_request_duration_seconds Normalized HTTP request duration in seconds.",
        "# TYPE secflow_dfa_http_request_duration_seconds histogram",
        "# HELP secflow_dfa_http_request_inflight Current inflight HTTP requests.",
        "# TYPE secflow_dfa_http_request_inflight gauge",
    ]
    with _REQUEST_LOCK:
        http_totals = dict(_HTTP_REQUEST_TOTAL)
        http_durations = {
            key: {"count": value["count"], "sum": value["sum"], "buckets": list(value["buckets"])}
            for key, value in _HTTP_REQUEST_DURATION.items()
        }
        http_inflight = dict(_HTTP_REQUEST_INFLIGHT)
    for key in sorted(http_totals):
        method, route, status_class, status_code = key
        lines.append(
            f"secflow_dfa_http_requests_total"
            f"{_labels(method=method, route=route, status_class=status_class, status_code=status_code)} {http_totals[key]}"
        )
    for key in sorted(http_durations):
        method, route = key
        labels = _labels(method=method, route=route)
        cumulative = 0
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            cumulative += int(http_durations[key]["buckets"][index])
            lines.append(
                f"secflow_dfa_http_request_duration_seconds_bucket"
                f"{_labels(method=method, route=route, le=_fmt(upper_bound))} {cumulative}"
            )
        lines.append(f"secflow_dfa_http_request_duration_seconds_sum{labels} {_fmt(http_durations[key]['sum'])}")
        lines.append(f"secflow_dfa_http_request_duration_seconds_count{labels} {int(http_durations[key]['count'])}")
    for key in sorted(http_inflight):
        method, route = key
        lines.append(f"secflow_dfa_http_request_inflight{_labels(method=method, route=route)} {int(http_inflight[key])}")
    return lines


def _render_local_runtime_metrics() -> list[str]:
    task_service = get_task_service()
    local_running = int(task_service.local_running_task_count())
    lines = [
        "# HELP secflow_dfa_local_role_info Static role info for this pod.",
        "# TYPE secflow_dfa_local_role_info gauge",
        f"secflow_dfa_local_role_info{_labels(role=ROLE)} 1",
        "# HELP secflow_dfa_local_public_api_enabled Public API enabled flag for this pod.",
        "# TYPE secflow_dfa_local_public_api_enabled gauge",
        f"secflow_dfa_local_public_api_enabled {1 if PUBLIC_API_ENABLED else 0}",
        "# HELP secflow_dfa_local_dispatcher_enabled Dispatcher enabled flag for this pod.",
        "# TYPE secflow_dfa_local_dispatcher_enabled gauge",
        f"secflow_dfa_local_dispatcher_enabled {1 if DISPATCHER_ENABLED else 0}",
        "# HELP secflow_dfa_local_executor_enabled Executor enabled flag for this pod.",
        "# TYPE secflow_dfa_local_executor_enabled gauge",
        f"secflow_dfa_local_executor_enabled {1 if EXECUTOR_ENABLED else 0}",
        "# HELP secflow_dfa_local_registry_enabled Registry enabled flag for this pod.",
        "# TYPE secflow_dfa_local_registry_enabled gauge",
        f"secflow_dfa_local_registry_enabled {1 if REGISTRY_ENABLED else 0}",
        "# HELP secflow_dfa_local_running_tasks Current running tasks in this pod.",
        "# TYPE secflow_dfa_local_running_tasks gauge",
        f"secflow_dfa_local_running_tasks {local_running}",
        "# HELP secflow_dfa_local_running_capacity Configured max running tasks for this pod.",
        "# TYPE secflow_dfa_local_running_capacity gauge",
        f"secflow_dfa_local_running_capacity {MAX_LOCAL_RUNNING_TASKS}",
    ]
    with _LOCAL_EVENT_LOCK:
        local_events = dict(_LOCAL_EVENT_TOTAL)
    lines.extend([
        "# HELP secflow_dfa_local_events_total Local execution events observed by this pod.",
        "# TYPE secflow_dfa_local_events_total counter",
    ])
    for (event, result), value in sorted(local_events.items()):
        lines.append(f"secflow_dfa_local_events_total{_labels(event=event, result=result)} {value}")
    return lines


def _render_cluster_task_metrics() -> list[str]:
    from .db import get_db

    db_up = 0
    rows: list[AppDfaTask] = []
    worker_snapshot = None
    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False)).all()
            worker_snapshot = build_worker_cluster_snapshot(db)
            db_up = 1
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        rows = []

    status_counts: dict[str, int] = defaultdict(int)
    dispatch_counts: dict[str, int] = defaultdict(int)
    queue_count = turnaround_count = execution_count = 0
    queue_sum = turnaround_sum = execution_sum = 0.0
    retry_total = timeout_total = cancel_total = 0
    failure_category_counts: dict[str, int] = defaultdict(int)
    token_input_total = token_output_total = token_cache_read_total = token_cache_write_total = 0
    token_cost_total = 0.0
    token_input_running = token_output_running = 0
    token_cost_running = 0.0
    round_total = judge_total = function_total = 0
    round_duration_sum = judge_duration_sum = 0.0
    cumulative_duration_total = 0.0
    wall_clock_duration_total = 0.0
    trace_depth_max = 0
    trace_callee_total = 0
    leased_tasks = 0
    stale_leases = 0
    orphan_running_tasks = 0
    running_without_owner = 0
    heartbeat_age_max = 0.0
    heartbeat_live = 0
    session_gauge = 0
    observed_active_owners: set[str] = set()
    observed_live_heartbeat_owners: set[str] = set()

    now = datetime.now(timezone.utc).timestamp()
    for row in rows:
        status = str(row.status or "unknown")
        status_counts[status] += 1
        dispatch_counts[str(row.dispatch_status or "unknown")] += 1
        if status == "running":
            if not row.execution_owner_id:
                running_without_owner += 1
            if (not row.execution_owner_id) or (row.execution_lease_until is None) or (
                row.execution_lease_until and row.execution_lease_until.timestamp() < now
            ):
                orphan_running_tasks += 1
        if row.execution_owner_id and row.execution_lease_until and row.execution_lease_until.timestamp() >= now:
            leased_tasks += 1
            observed_active_owners.add(str(row.execution_owner_id))
        elif row.execution_owner_id:
            stale_leases += 1
        if row.started_at and row.created_at:
            queue_sum += _seconds_between(row.created_at, row.started_at)
            queue_count += 1
        if row.finished_at and row.created_at:
            turnaround_sum += _seconds_between(row.created_at, row.finished_at)
            turnaround_count += 1
        if row.started_at and row.finished_at:
            elapsed = _seconds_between(row.started_at, row.finished_at)
            wall_clock_duration_total += elapsed
            execution_sum += elapsed
            execution_count += 1
        result_json = row.result_json if isinstance(row.result_json, dict) else {}
        usage = _token_usage(result_json.get("total_tokens") if isinstance(result_json.get("total_tokens"), dict) else {})
        token_input_total += usage["input"]
        token_output_total += usage["output"]
        token_cache_read_total += usage["cache_read"]
        token_cache_write_total += usage["cache_write"]
        token_cost_total += usage["cost"]
        if status == "running":
            token_input_running += usage["input"]
            token_output_running += usage["output"]
            token_cost_running += usage["cost"]
        cumulative_duration_total += max(0.0, float(result_json.get("total_duration_ms") or 0.0) / 1000.0)

        rounds = result_json.get("rounds") if isinstance(result_json.get("rounds"), list) else []
        if len(rounds) > 1:
            retry_total += len(rounds) - 1
        seen_functions: set[str] = set()
        for item in rounds:
            if not isinstance(item, dict):
                continue
            round_total += 1
            round_duration_sum += max(0.0, float(item.get("duration_ms") or 0.0) / 1000.0)
            function_name = str(item.get("function_name") or item.get("function") or item.get("entry") or "unknown")
            if function_name not in seen_functions:
                seen_functions.add(function_name)
                function_total += 1
            judge_results = item.get("judge_results") if isinstance(item.get("judge_results"), list) else []
            judge_total += len(judge_results)
            for judge in judge_results:
                if not isinstance(judge, dict):
                    continue
                judge_duration_sum += max(0.0, float(judge.get("duration_ms") or 0.0) / 1000.0)
                if judge.get("session_file"):
                    session_gauge += 1

        run_root = _task_run_root(row)
        trace_depth, callee_count = _trace_stats(row, run_root)
        trace_depth_max = max(trace_depth_max, trace_depth)
        trace_callee_total += callee_count
        session_gauge += _count_session_files(run_root / "sessions")

        if row.execution_heartbeat_at:
            age = max(0.0, now - row.execution_heartbeat_at.timestamp())
            heartbeat_age_max = max(heartbeat_age_max, age)
            if age <= HEARTBEAT_INTERVAL_SECONDS * 2:
                heartbeat_live += 1
                if row.execution_owner_id:
                    observed_live_heartbeat_owners.add(str(row.execution_owner_id))

        classification = _classify_failure(row.error, result_json)
        if classification == "timeout":
            timeout_total += 1
        if classification == "cancel":
            cancel_total += 1
        if classification != "none":
            failure_category_counts[classification] += 1

    dispatcher_running = 1 if DISPATCHER_ENABLED else 0
    executor_running = 1 if EXECUTOR_ENABLED else 0
    configured_workers = max(0, int(CLUSTER_EXPECTED_WORKERS))
    configured_capacity_per_worker = max(0, int(CLUSTER_EXPECTED_WORKER_CAPACITY))
    observed_active_worker_count = len(observed_active_owners)
    observed_live_heartbeat_worker_count = len(observed_live_heartbeat_owners)
    actual_worker_count = worker_snapshot.worker_count if worker_snapshot is not None else 0
    actual_healthy_worker_count = worker_snapshot.healthy_workers if worker_snapshot is not None else 0
    actual_stale_worker_count = worker_snapshot.stale_workers if worker_snapshot is not None else 0
    actual_capacity_per_worker = max((worker.max_concurrent_jobs for worker in worker_snapshot.workers), default=0) if worker_snapshot is not None else 0
    configured_slots = worker_snapshot.total_capacity if worker_snapshot is not None else configured_workers * configured_capacity_per_worker
    busy_slots = worker_snapshot.running_jobs if worker_snapshot is not None else max(0, status_counts.get("running", 0))
    free_slots = worker_snapshot.available_slots if worker_snapshot is not None else max(0, configured_slots - busy_slots) if configured_slots > 0 else 0
    heartbeat_stale = max(0, status_counts.get("running", 0) - heartbeat_live)
    slot_utilization_ratio = (busy_slots / configured_slots) if configured_slots > 0 else 0.0
    observed_worker_coverage_ratio = (observed_active_worker_count / configured_workers) if configured_workers > 0 else 0.0
    queue_pressure_ratio = (status_counts.get("pending", 0) / configured_slots) if configured_slots > 0 else 0.0
    lines = [
        "# HELP secflow_dfa_db_up Database query path for metrics is available.",
        "# TYPE secflow_dfa_db_up gauge",
        f"secflow_dfa_db_up {db_up}",
        "# HELP secflow_dfa_cluster_tasks Number of tasks by status.",
        "# TYPE secflow_dfa_cluster_tasks gauge",
    ]
    for status in sorted(status_counts):
        lines.append(f"secflow_dfa_cluster_tasks{_labels(status=status)} {status_counts[status]}")
    finished_count = sum(count for status, count in status_counts.items() if status in _TERMINAL_STATUSES)
    lines.extend([
        "# HELP secflow_dfa_cluster_tasks_pending Pending tasks.",
        "# TYPE secflow_dfa_cluster_tasks_pending gauge",
        f"secflow_dfa_cluster_tasks_pending {status_counts.get('pending', 0)}",
        "# HELP secflow_dfa_cluster_tasks_running Running tasks.",
        "# TYPE secflow_dfa_cluster_tasks_running gauge",
        f"secflow_dfa_cluster_tasks_running {status_counts.get('running', 0)}",
        "# HELP secflow_dfa_cluster_tasks_terminal Terminal tasks.",
        "# TYPE secflow_dfa_cluster_tasks_terminal gauge",
        f"secflow_dfa_cluster_tasks_terminal {finished_count}",
        "# HELP secflow_dfa_cluster_workers Worker counts by state.",
        "# TYPE secflow_dfa_cluster_workers gauge",
        f'secflow_dfa_cluster_workers{{state="configured"}} {configured_workers}',
        f'secflow_dfa_cluster_workers{{state="actual"}} {actual_worker_count}',
        f'secflow_dfa_cluster_workers{{state="healthy"}} {actual_healthy_worker_count}',
        f'secflow_dfa_cluster_workers{{state="stale"}} {actual_stale_worker_count}',
        f'secflow_dfa_cluster_workers{{state="observed_active_owner"}} {observed_active_worker_count}',
        f'secflow_dfa_cluster_workers{{state="observed_live_heartbeat_owner"}} {observed_live_heartbeat_worker_count}',
        "# HELP secflow_dfa_cluster_worker_slots Worker slot counts by kind.",
        "# TYPE secflow_dfa_cluster_worker_slots gauge",
        f'secflow_dfa_cluster_worker_slots{{kind="capacity"}} {configured_slots}',
        f'secflow_dfa_cluster_worker_slots{{kind="busy"}} {busy_slots}',
        f'secflow_dfa_cluster_worker_slots{{kind="free"}} {free_slots}',
        "# HELP secflow_dfa_cluster_worker_capacity_per_pod Configured per-worker task capacity.",
        "# TYPE secflow_dfa_cluster_worker_capacity_per_pod gauge",
        f"secflow_dfa_cluster_worker_capacity_per_pod {actual_capacity_per_worker or configured_capacity_per_worker}",
        "# HELP secflow_dfa_cluster_worker_slot_utilization_ratio Busy slot ratio over configured capacity.",
        "# TYPE secflow_dfa_cluster_worker_slot_utilization_ratio gauge",
        f"secflow_dfa_cluster_worker_slot_utilization_ratio {_fmt(slot_utilization_ratio)}",
        "# HELP secflow_dfa_cluster_worker_observed_coverage_ratio Observed active owners over configured workers.",
        "# TYPE secflow_dfa_cluster_worker_observed_coverage_ratio gauge",
        f"secflow_dfa_cluster_worker_observed_coverage_ratio {_fmt(observed_worker_coverage_ratio)}",
        "# HELP secflow_dfa_cluster_queue_pressure_ratio Pending tasks over configured worker slots.",
        "# TYPE secflow_dfa_cluster_queue_pressure_ratio gauge",
        f"secflow_dfa_cluster_queue_pressure_ratio {_fmt(queue_pressure_ratio)}",
        "# HELP secflow_dfa_cluster_queue_wait_seconds Queue wait duration aggregated over tasks.",
        "# TYPE secflow_dfa_cluster_queue_wait_seconds summary",
        f"secflow_dfa_cluster_queue_wait_seconds_count {queue_count}",
        f"secflow_dfa_cluster_queue_wait_seconds_sum {_fmt(queue_sum)}",
        "# HELP secflow_dfa_cluster_execution_seconds Execution duration aggregated over tasks.",
        "# TYPE secflow_dfa_cluster_execution_seconds summary",
        f"secflow_dfa_cluster_execution_seconds_count {execution_count}",
        f"secflow_dfa_cluster_execution_seconds_sum {_fmt(execution_sum)}",
        "# HELP secflow_dfa_cluster_turnaround_seconds End-to-end turnaround duration aggregated over tasks.",
        "# TYPE secflow_dfa_cluster_turnaround_seconds summary",
        f"secflow_dfa_cluster_turnaround_seconds_count {turnaround_count}",
        f"secflow_dfa_cluster_turnaround_seconds_sum {_fmt(turnaround_sum)}",
        "# HELP secflow_dfa_cluster_rounds Aggregated round count snapshot.",
        "# TYPE secflow_dfa_cluster_rounds gauge",
        f"secflow_dfa_cluster_rounds {round_total}",
        "# HELP secflow_dfa_cluster_judges Aggregated judge count snapshot.",
        "# TYPE secflow_dfa_cluster_judges gauge",
        f"secflow_dfa_cluster_judges {judge_total}",
        "# HELP secflow_dfa_cluster_sessions Aggregated session count snapshot.",
        "# TYPE secflow_dfa_cluster_sessions gauge",
        f"secflow_dfa_cluster_sessions {session_gauge}",
        "# HELP secflow_dfa_cluster_leased_tasks Active leased task count.",
        "# TYPE secflow_dfa_cluster_leased_tasks gauge",
        f"secflow_dfa_cluster_leased_tasks {leased_tasks}",
        "# HELP secflow_dfa_cluster_stale_leases Expired lease count for owned tasks.",
        "# TYPE secflow_dfa_cluster_stale_leases gauge",
        f"secflow_dfa_cluster_stale_leases {stale_leases}",
        "# HELP secflow_dfa_cluster_orphan_running_tasks Running tasks missing a valid execution lease context.",
        "# TYPE secflow_dfa_cluster_orphan_running_tasks gauge",
        f"secflow_dfa_cluster_orphan_running_tasks {orphan_running_tasks}",
        "# HELP secflow_dfa_cluster_running_without_owner Running tasks missing execution_owner_id.",
        "# TYPE secflow_dfa_cluster_running_without_owner gauge",
        f"secflow_dfa_cluster_running_without_owner {running_without_owner}",
        "# HELP secflow_dfa_cluster_dispatcher_enabled Dispatcher feature enabled on this scrape source.",
        "# TYPE secflow_dfa_cluster_dispatcher_enabled gauge",
        f"secflow_dfa_cluster_dispatcher_enabled {dispatcher_running}",
        "# HELP secflow_dfa_cluster_executor_enabled Executor feature enabled on this scrape source.",
        "# TYPE secflow_dfa_cluster_executor_enabled gauge",
        f"secflow_dfa_cluster_executor_enabled {executor_running}",
        "# HELP secflow_dfa_cluster_heartbeat_live_tasks Running tasks with fresh heartbeat.",
        "# TYPE secflow_dfa_cluster_heartbeat_live_tasks gauge",
        f"secflow_dfa_cluster_heartbeat_live_tasks {heartbeat_live}",
        "# HELP secflow_dfa_cluster_heartbeat_stale_tasks Running tasks with stale heartbeat.",
        "# TYPE secflow_dfa_cluster_heartbeat_stale_tasks gauge",
        f"secflow_dfa_cluster_heartbeat_stale_tasks {heartbeat_stale}",
        "# HELP secflow_dfa_cluster_heartbeat_age_seconds_max Max heartbeat age in seconds.",
        "# TYPE secflow_dfa_cluster_heartbeat_age_seconds_max gauge",
        f"secflow_dfa_cluster_heartbeat_age_seconds_max {_fmt(heartbeat_age_max)}",
        "# HELP secflow_dfa_cluster_retry_count Aggregated retry count derived from extra rounds.",
        "# TYPE secflow_dfa_cluster_retry_count gauge",
        f"secflow_dfa_cluster_retry_count {retry_total}",
        "# HELP secflow_dfa_cluster_timeout_count Timeout-classified terminal tasks.",
        "# TYPE secflow_dfa_cluster_timeout_count gauge",
        f"secflow_dfa_cluster_timeout_count {timeout_total}",
        "# HELP secflow_dfa_cluster_cancel_count Cancelled tasks.",
        "# TYPE secflow_dfa_cluster_cancel_count gauge",
        f"secflow_dfa_cluster_cancel_count {cancel_total}",
        "# HELP secflow_dfa_cluster_failure_category Failure distribution by category.",
        "# TYPE secflow_dfa_cluster_failure_category gauge",
    ])
    for category in sorted(failure_category_counts):
        lines.append(f"secflow_dfa_cluster_failure_category{_labels(category=category)} {failure_category_counts[category]}")
    lines.extend([
        "# HELP secflow_dfa_cluster_token_usage Aggregated token usage snapshot.",
        "# TYPE secflow_dfa_cluster_token_usage gauge",
        f'secflow_dfa_cluster_token_usage{{type="input"}} {token_input_total}',
        f'secflow_dfa_cluster_token_usage{{type="output"}} {token_output_total}',
        f'secflow_dfa_cluster_token_usage{{type="cache_read"}} {token_cache_read_total}',
        f'secflow_dfa_cluster_token_usage{{type="cache_write"}} {token_cache_write_total}',
        f'secflow_dfa_cluster_token_usage{{type="total"}} {token_input_total + token_output_total + token_cache_read_total + token_cache_write_total}',
        "# HELP secflow_dfa_cluster_token_cost Aggregated token cost snapshot.",
        "# TYPE secflow_dfa_cluster_token_cost gauge",
        f"secflow_dfa_cluster_token_cost {_fmt(token_cost_total)}",
        "# HELP secflow_dfa_cluster_running_token_usage Running-task token usage snapshot.",
        "# TYPE secflow_dfa_cluster_running_token_usage gauge",
        f'secflow_dfa_cluster_running_token_usage{{type="input"}} {token_input_running}',
        f'secflow_dfa_cluster_running_token_usage{{type="output"}} {token_output_running}',
        f'secflow_dfa_cluster_running_token_usage{{type="total"}} {token_input_running + token_output_running}',
        "# HELP secflow_dfa_cluster_running_token_cost Running-task token cost snapshot.",
        "# TYPE secflow_dfa_cluster_running_token_cost gauge",
        f"secflow_dfa_cluster_running_token_cost {_fmt(token_cost_running)}",
        "# HELP secflow_dfa_cluster_round_duration_seconds Aggregated round duration.",
        "# TYPE secflow_dfa_cluster_round_duration_seconds summary",
        f"secflow_dfa_cluster_round_duration_seconds_count {round_total}",
        f"secflow_dfa_cluster_round_duration_seconds_sum {_fmt(round_duration_sum)}",
        "# HELP secflow_dfa_cluster_judge_duration_seconds Aggregated judge duration.",
        "# TYPE secflow_dfa_cluster_judge_duration_seconds summary",
        f"secflow_dfa_cluster_judge_duration_seconds_count {judge_total}",
        f"secflow_dfa_cluster_judge_duration_seconds_sum {_fmt(judge_duration_sum)}",
        "# HELP secflow_dfa_cluster_functions Aggregated function analysis count snapshot.",
        "# TYPE secflow_dfa_cluster_functions gauge",
        f"secflow_dfa_cluster_functions {function_total}",
        "# HELP secflow_dfa_cluster_total_duration_accumulated_seconds Aggregated cumulative total_duration_ms converted to seconds.",
        "# TYPE secflow_dfa_cluster_total_duration_accumulated_seconds gauge",
        f"secflow_dfa_cluster_total_duration_accumulated_seconds {_fmt(cumulative_duration_total)}",
        "# HELP secflow_dfa_cluster_wall_clock_duration_seconds Aggregated wall-clock task duration in seconds.",
        "# TYPE secflow_dfa_cluster_wall_clock_duration_seconds gauge",
        f"secflow_dfa_cluster_wall_clock_duration_seconds {_fmt(wall_clock_duration_total)}",
        "# HELP secflow_dfa_cluster_trace_depth_max Maximum trace depth observed from stage events.",
        "# TYPE secflow_dfa_cluster_trace_depth_max gauge",
        f"secflow_dfa_cluster_trace_depth_max {trace_depth_max}",
        "# HELP secflow_dfa_cluster_trace_callees Aggregated trace callee count observed from stage events.",
        "# TYPE secflow_dfa_cluster_trace_callees gauge",
        f"secflow_dfa_cluster_trace_callees {trace_callee_total}",
        "# HELP secflow_dfa_cluster_dispatch_status Aggregated dispatch status count.",
        "# TYPE secflow_dfa_cluster_dispatch_status gauge",
    ])
    for dispatch_status in sorted(dispatch_counts):
        lines.append(f"secflow_dfa_cluster_dispatch_status{_labels(status=dispatch_status)} {dispatch_counts[dispatch_status]}")
    lines.extend(_render_cluster_worker_detail_metrics(worker_snapshot))
    _append_ai_alias_metrics(
        lines,
        prefix="secflow_dfa_cluster",
        worker_count=status_counts.get("running", 0),
        judge_count=judge_total,
        session_total=session_gauge,
        round_total=round_total,
        retry_total=retry_total,
        timeout_total=timeout_total,
        cancel_total=cancel_total,
        failure_category_counts=failure_category_counts,
        token_input_total=token_input_total,
        token_output_total=token_output_total,
        token_cache_read_total=token_cache_read_total,
        token_cache_write_total=token_cache_write_total,
        token_cost_total=token_cost_total,
        review_pass_total=status_counts.get("passed", 0),
        review_fail_total=sum(
            count for key, count in status_counts.items() if key not in {"passed", "success", "completed"}
        ),
        worker_duration_seconds=execution_sum,
        judge_duration_seconds=judge_duration_sum,
    )
    return lines


def _render_cluster_worker_detail_metrics(worker_snapshot) -> list[str]:
    if worker_snapshot is None:
        return []
    lines = [
        "# HELP secflow_dfa_cluster_worker_runtime Per-worker runtime snapshot derived from lease registry.",
        "# TYPE secflow_dfa_cluster_worker_runtime gauge",
        "# HELP secflow_dfa_cluster_worker_active_jobs Per-worker active job counts by task status.",
        "# TYPE secflow_dfa_cluster_worker_active_jobs gauge",
        "# HELP secflow_dfa_cluster_worker_last_heartbeat_timestamp_seconds Per-worker last heartbeat UNIX timestamp.",
        "# TYPE secflow_dfa_cluster_worker_last_heartbeat_timestamp_seconds gauge",
    ]
    for worker in worker_snapshot.workers:
        base_labels = {
            "worker_id": worker.worker_id,
            "host_name": worker.host_name or worker.worker_id,
            "healthy": "true" if worker.healthy else "false",
            "source": worker.source or "lease_registry",
        }
        lines.append(
            f"secflow_dfa_cluster_worker_runtime{_labels(**base_labels, kind='capacity')} {worker.max_concurrent_jobs}"
        )
        lines.append(
            f"secflow_dfa_cluster_worker_runtime{_labels(**base_labels, kind='running_jobs')} {worker.running_jobs}"
        )
        lines.append(
            f"secflow_dfa_cluster_worker_runtime{_labels(**base_labels, kind='available_slots')} {worker.available_slots}"
        )
        heartbeat_ts = worker.last_heartbeat_at.timestamp() if worker.last_heartbeat_at else 0.0
        lines.append(
            f"secflow_dfa_cluster_worker_last_heartbeat_timestamp_seconds{_labels(worker_id=worker.worker_id, host_name=worker.host_name or worker.worker_id)} {_fmt(heartbeat_ts)}"
        )
        status_counts: dict[str, int] = defaultdict(int)
        for job in worker.active_jobs:
            status_counts[str(job.status or "unknown")] += 1
        if not status_counts:
            lines.append(
                f"secflow_dfa_cluster_worker_active_jobs{_labels(worker_id=worker.worker_id, host_name=worker.host_name or worker.worker_id, status='none')} 0"
            )
        else:
            for status, count in sorted(status_counts.items()):
                lines.append(
                    f"secflow_dfa_cluster_worker_active_jobs{_labels(worker_id=worker.worker_id, host_name=worker.host_name or worker.worker_id, status=status)} {count}"
                )
    return lines


def _task_run_root(row: AppDfaTask) -> Path | None:
    if not row.output_path:
        return None
    root = Path(row.output_path) / row.task_id / "run"
    epochs_root = root / "epochs"
    if epochs_root.is_dir():
        candidates = sorted([path for path in epochs_root.iterdir() if path.is_dir()], key=lambda path: path.name)
        return candidates[-1] if candidates else root
    return root


def _trace_stats(row: AppDfaTask, run_root: Path | None) -> tuple[int, int]:
    max_depth = 0
    callee_total = 0
    stages = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = stages.get("events") if isinstance(stages.get("events"), list) else []
    for event in events:
        if not isinstance(event, dict):
            continue
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        max_depth = max(max_depth, int(data.get("depth") or 0))
        if event.get("type") == "trace_callees":
            callees = data.get("callees") if isinstance(data.get("callees"), list) else []
            callee_total += len(callees)
    if run_root and run_root.is_dir():
        for path in run_root.rglob("tainted.list"):
            try:
                callee_total += len([line for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.strip().startswith("#")])
            except Exception:
                continue
    return max_depth, callee_total


def _count_session_files(path: Path | None) -> int:
    if path is None or not path.is_dir():
        return 0
    return sum(1 for item in path.rglob("*.jsonl") if item.is_file())


def _classify_failure(error: Any, result_json: dict[str, Any]) -> str:
    status = str(result_json.get("status") or result_json.get("analysis_status") or "").lower()
    reason = str(result_json.get("completion_reason") or error or "").lower()
    text = f"{status} {reason}"
    if "cancel" in text:
        return "cancel"
    if "timeout" in text or "timed out" in text or "deadline" in text:
        return "timeout"
    if "lease" in text:
        return "lease_lost"
    if "invalid" in text or "validation" in text:
        return "validation"
    if "error" in text:
        return "error"
    if "failed" in text:
        return "failed"
    return "none"


def _token_usage(value: dict[str, Any] | None) -> dict[str, int | float]:
    usage = value if isinstance(value, dict) else {}
    return {
        "input": int(usage.get("input", 0) or usage.get("prompt_tokens", 0) or 0),
        "output": int(usage.get("output", 0) or usage.get("completion_tokens", 0) or 0),
        "cache_read": int(usage.get("cache_read", 0) or 0),
        "cache_write": int(usage.get("cache_write", 0) or 0),
        "cost": float(usage.get("cost", 0.0) or 0.0),
    }


def _seconds_between(start: datetime | None, end: datetime | None) -> float:
    if not start or not end:
        return 0.0
    return max(0.0, (end - start).total_seconds())


def _labels(**labels: Any) -> str:
    parts = []
    for key, value in labels.items():
        safe = str(value).replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")
        parts.append(f'{key}="{safe}"')
    return "{" + ",".join(parts) + "}" if parts else ""


def _fmt(value: float) -> str:
    return f"{float(value):.6f}"


def _render_agent_observability_metrics() -> list[str]:
    from .db import get_db
    from .service.agent_observability import get_agent_observability_service

    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            snapshot = get_agent_observability_service().build_snapshot(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        return []

    processes = list(snapshot.get("processes") or [])
    sessions = list(snapshot.get("sessions") or [])
    tasks = list(snapshot.get("tasks") or [])
    lines = [
        "# HELP secflow_dfa_agent_process_total Agent process total grouped by owner state, pod and role.",
        "# TYPE secflow_dfa_agent_process_total gauge",
        "# HELP secflow_dfa_agent_orphan_process_total Confirmed orphan agent process total by pod.",
        "# TYPE secflow_dfa_agent_orphan_process_total gauge",
        "# HELP secflow_dfa_agent_suspected_orphan_process_total Suspected orphan agent process total by pod.",
        "# TYPE secflow_dfa_agent_suspected_orphan_process_total gauge",
        "# HELP secflow_dfa_agent_killable_orphan_process_total Killable orphan agent process total by pod.",
        "# TYPE secflow_dfa_agent_killable_orphan_process_total gauge",
        "# HELP secflow_dfa_agent_killable_suspected_orphan_process_total Killable suspected orphan agent process total by pod.",
        "# TYPE secflow_dfa_agent_killable_suspected_orphan_process_total gauge",
        "# HELP secflow_dfa_agent_session_total Agent session total grouped by state, pod and role.",
        "# TYPE secflow_dfa_agent_session_total gauge",
        "# HELP secflow_dfa_agent_orphan_session_total Orphan agent session total by pod.",
        "# TYPE secflow_dfa_agent_orphan_session_total gauge",
        "# HELP secflow_dfa_agent_task_ownership_total Agent task ownership total by status.",
        "# TYPE secflow_dfa_agent_task_ownership_total gauge",
    ]
    process_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    session_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    orphan_by_pod: dict[str, int] = defaultdict(int)
    suspected_by_pod: dict[str, int] = defaultdict(int)
    killable_by_pod: dict[str, int] = defaultdict(int)
    killable_suspected_by_pod: dict[str, int] = defaultdict(int)
    orphan_sessions_by_pod: dict[str, int] = defaultdict(int)
    ownership_counts: dict[str, int] = defaultdict(int)
    for item in processes:
        key = (str(item.get("owner_kind") or "unknown"), str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        process_counts[key] += 1
        if str(item.get("owner_kind") or "") == "orphan":
            orphan_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_by_pod[str(item.get("pod_name") or "unknown")] += 1
        if str(item.get("owner_kind") or "") == "unknown":
            suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in sessions:
        session_state = "orphan" if bool(item.get("orphan_session")) else ("live" if bool(item.get("live")) else "history")
        key = (session_state, str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        session_counts[key] += 1
        if bool(item.get("orphan_session")):
            orphan_sessions_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in tasks:
        ownership_counts[str(item.get("ownership_status") or "unknown")] += 1
    for (state, pod, role_kind), value in sorted(process_counts.items()):
        lines.append(f"secflow_dfa_agent_process_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_by_pod.items()):
        lines.append(f"secflow_dfa_agent_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(suspected_by_pod.items()):
        lines.append(f"secflow_dfa_agent_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_by_pod.items()):
        lines.append(f"secflow_dfa_agent_killable_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_suspected_by_pod.items()):
        lines.append(f"secflow_dfa_agent_killable_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for (state, pod, role_kind), value in sorted(session_counts.items()):
        lines.append(f"secflow_dfa_agent_session_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_sessions_by_pod.items()):
        lines.append(f"secflow_dfa_agent_orphan_session_total{_labels(pod=pod)} {value}")
    for ownership_status, value in sorted(ownership_counts.items()):
        lines.append(f"secflow_dfa_agent_task_ownership_total{_labels(ownership_status=ownership_status)} {value}")
    return lines


def _append_ai_alias_metrics(
    lines: list[str],
    *,
    prefix: str,
    worker_count: int,
    judge_count: int,
    session_total: int,
    round_total: int,
    retry_total: int,
    timeout_total: int,
    cancel_total: int,
    failure_category_counts: dict[str, int],
    token_input_total: int,
    token_output_total: int,
    token_cache_read_total: int,
    token_cache_write_total: int,
    token_cost_total: float,
    review_pass_total: int,
    review_fail_total: int,
    worker_duration_seconds: float,
    judge_duration_seconds: float,
) -> None:
    lines.extend([
        f"# HELP {prefix}_ai_role_count Aggregated AI role counts for this service.",
        f"# TYPE {prefix}_ai_role_count gauge",
        f"# HELP {prefix}_ai_role_duration_seconds Aggregated AI role duration in seconds.",
        f"# TYPE {prefix}_ai_role_duration_seconds gauge",
        f"# HELP {prefix}_ai_session_total Aggregated AI session count by role.",
        f"# TYPE {prefix}_ai_session_total counter",
        f"# HELP {prefix}_ai_round_total Aggregated AI round counts by kind.",
        f"# TYPE {prefix}_ai_round_total counter",
        f"# HELP {prefix}_ai_retry_total Aggregated AI retry counts by reason.",
        f"# TYPE {prefix}_ai_retry_total counter",
        f"# HELP {prefix}_ai_timeout_total Aggregated AI timeout counts by scope.",
        f"# TYPE {prefix}_ai_timeout_total counter",
        f"# HELP {prefix}_ai_failure_total Aggregated AI failures by category.",
        f"# TYPE {prefix}_ai_failure_total counter",
        f"# HELP {prefix}_ai_token_usage_total Aggregated AI token usage by type.",
        f"# TYPE {prefix}_ai_token_usage_total counter",
        f"# HELP {prefix}_ai_token_cost_total Aggregated AI token cost.",
        f"# TYPE {prefix}_ai_token_cost_total counter",
        f"# HELP {prefix}_ai_review_total Aggregated AI review outcomes.",
        f"# TYPE {prefix}_ai_review_total counter",
    ])
    lines.append(f'{prefix}_ai_role_count{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_role_count{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="worker"}} {_fmt(worker_duration_seconds)}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="judge"}} {_fmt(judge_duration_seconds)}')
    lines.append(f'{prefix}_ai_session_total{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="agent"}} {max(0, int(session_total))}')
    lines.append(f'{prefix}_ai_round_total{{kind="round"}} {max(0, int(round_total))}')
    lines.append(f'{prefix}_ai_retry_total{{reason="reflection"}} {max(0, int(retry_total))}')
    lines.append(f'{prefix}_ai_timeout_total{{scope="task"}} {max(0, int(timeout_total))}')
    lines.append(f'{prefix}_ai_failure_total{{category="cancel"}} {max(0, int(cancel_total))}')
    for category in sorted(failure_category_counts):
        lines.append(f'{prefix}_ai_failure_total{{category="{category}"}} {max(0, int(failure_category_counts[category]))}')
    total_tokens = token_input_total + token_output_total + token_cache_read_total + token_cache_write_total
    lines.append(f'{prefix}_ai_token_usage_total{{type="input"}} {max(0, int(token_input_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="output"}} {max(0, int(token_output_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_read"}} {max(0, int(token_cache_read_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_write"}} {max(0, int(token_cache_write_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="total"}} {max(0, int(total_tokens))}')
    lines.append(f"{prefix}_ai_token_cost_total {_fmt(token_cost_total)}")
    lines.append(f'{prefix}_ai_review_total{{result="pass"}} {max(0, int(review_pass_total))}')
    lines.append(f'{prefix}_ai_review_total{{result="fail"}} {max(0, int(review_fail_total))}')
