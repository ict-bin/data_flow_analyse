from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from .db.models import AppDfaTask
from .runtime_context import DISPATCHER_ENABLED, EXECUTOR_ENABLED, HEARTBEAT_INTERVAL_SECONDS
from .service.task_service import get_task_service

_REQUEST_LOCK = threading.Lock()
_REQUEST_TOTAL = defaultdict(int)
_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0})
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}


def observe_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    key = (method.upper(), path or "/", str(int(status_code)))
    with _REQUEST_LOCK:
        _REQUEST_TOTAL[key] += 1
        bucket = _REQUEST_DURATION[key]
        bucket["count"] += 1
        bucket["sum"] += max(0.0, float(duration_seconds))


def render_metrics() -> str:
    lines = ["# HELP secflow_dfa_up Service metrics scrape succeeded.", "# TYPE secflow_dfa_up gauge"]
    try:
        lines.append("secflow_dfa_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_task_metrics())
    except Exception:
        lines.append("secflow_dfa_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_dfa_api_requests_total Total API requests observed by this process.",
        "# TYPE secflow_dfa_api_requests_total counter",
        "# HELP secflow_dfa_api_request_duration_seconds API request duration in seconds.",
        "# TYPE secflow_dfa_api_request_duration_seconds summary",
    ]
    with _REQUEST_LOCK:
        totals = dict(_REQUEST_TOTAL)
        durations = {key: dict(value) for key, value in _REQUEST_DURATION.items()}
    for key in sorted(set(totals) | set(durations)):
        method, path, status = key
        labels = _labels(method=method, path=path, status=status)
        lines.append(f"secflow_dfa_api_requests_total{labels} {totals.get(key, 0)}")
        duration = durations.get(key, {"count": 0, "sum": 0.0})
        lines.append(f"secflow_dfa_api_request_duration_seconds_count{labels} {int(duration['count'])}")
        lines.append(f"secflow_dfa_api_request_duration_seconds_sum{labels} {_fmt(duration['sum'])}")
    return lines


def _render_task_metrics() -> list[str]:
    from .db import get_db

    db_up = 0
    rows: list[AppDfaTask] = []
    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = db.query(AppDfaTask).filter(AppDfaTask.is_deleted.is_(False)).all()
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
    heartbeat_age_max = 0.0
    heartbeat_live = 0
    session_gauge = 0

    now = datetime.now(timezone.utc).timestamp()
    for row in rows:
        status = str(row.status or "unknown")
        status_counts[status] += 1
        dispatch_counts[str(row.dispatch_status or "unknown")] += 1
        if row.execution_owner_id and row.execution_lease_until and row.execution_lease_until.timestamp() >= now:
            leased_tasks += 1
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

        classification = _classify_failure(row.error, result_json)
        if classification == "timeout":
            timeout_total += 1
        if classification == "cancel":
            cancel_total += 1
        if classification != "none":
            failure_category_counts[classification] += 1

    task_service = get_task_service()
    local_running = int(task_service.local_running_task_count())
    dispatcher_running = 1 if DISPATCHER_ENABLED else 0
    executor_running = 1 if EXECUTOR_ENABLED else 0
    lines = [
        "# HELP secflow_dfa_db_up Database query path for metrics is available.",
        "# TYPE secflow_dfa_db_up gauge",
        f"secflow_dfa_db_up {db_up}",
        "# HELP secflow_dfa_tasks_status Number of tasks by status.",
        "# TYPE secflow_dfa_tasks_status gauge",
    ]
    for status in sorted(status_counts):
        lines.append(f"secflow_dfa_tasks_status{_labels(status=status)} {status_counts[status]}")
    finished_count = sum(count for status, count in status_counts.items() if status in _TERMINAL_STATUSES)
    lines.extend([
        "# HELP secflow_dfa_tasks_pending Pending tasks.",
        "# TYPE secflow_dfa_tasks_pending gauge",
        f"secflow_dfa_tasks_pending {status_counts.get('pending', 0)}",
        "# HELP secflow_dfa_tasks_running Running tasks.",
        "# TYPE secflow_dfa_tasks_running gauge",
        f"secflow_dfa_tasks_running {status_counts.get('running', 0)}",
        "# HELP secflow_dfa_tasks_finished Finished tasks.",
        "# TYPE secflow_dfa_tasks_finished gauge",
        f"secflow_dfa_tasks_finished {finished_count}",
        "# HELP secflow_dfa_queue_wait_seconds Queue wait duration aggregated over tasks.",
        "# TYPE secflow_dfa_queue_wait_seconds summary",
        f"secflow_dfa_queue_wait_seconds_count {queue_count}",
        f"secflow_dfa_queue_wait_seconds_sum {_fmt(queue_sum)}",
        "# HELP secflow_dfa_execution_seconds Execution duration aggregated over tasks.",
        "# TYPE secflow_dfa_execution_seconds summary",
        f"secflow_dfa_execution_seconds_count {execution_count}",
        f"secflow_dfa_execution_seconds_sum {_fmt(execution_sum)}",
        "# HELP secflow_dfa_turnaround_seconds End-to-end turnaround duration aggregated over tasks.",
        "# TYPE secflow_dfa_turnaround_seconds summary",
        f"secflow_dfa_turnaround_seconds_count {turnaround_count}",
        f"secflow_dfa_turnaround_seconds_sum {_fmt(turnaround_sum)}",
        "# HELP secflow_dfa_workers Local running task count.",
        "# TYPE secflow_dfa_workers gauge",
        f"secflow_dfa_workers {local_running}",
        "# HELP secflow_dfa_judges Aggregated judge count.",
        "# TYPE secflow_dfa_judges gauge",
        f"secflow_dfa_judges {judge_total}",
        "# HELP secflow_dfa_sessions Aggregated session file count.",
        "# TYPE secflow_dfa_sessions gauge",
        f"secflow_dfa_sessions {session_gauge}",
        "# HELP secflow_dfa_leased_tasks Active leased task count.",
        "# TYPE secflow_dfa_leased_tasks gauge",
        f"secflow_dfa_leased_tasks {leased_tasks}",
        "# HELP secflow_dfa_dispatcher_running Dispatcher role enabled flag.",
        "# TYPE secflow_dfa_dispatcher_running gauge",
        f"secflow_dfa_dispatcher_running {dispatcher_running}",
        "# HELP secflow_dfa_executor_running Executor role enabled flag.",
        "# TYPE secflow_dfa_executor_running gauge",
        f"secflow_dfa_executor_running {executor_running}",
        "# HELP secflow_dfa_heartbeat_live Current tasks with fresh heartbeat.",
        "# TYPE secflow_dfa_heartbeat_live gauge",
        f"secflow_dfa_heartbeat_live {heartbeat_live}",
        "# HELP secflow_dfa_heartbeat_age_seconds_max Max heartbeat age in seconds.",
        "# TYPE secflow_dfa_heartbeat_age_seconds_max gauge",
        f"secflow_dfa_heartbeat_age_seconds_max {_fmt(heartbeat_age_max)}",
        "# HELP secflow_dfa_retry_total Aggregated retry count derived from extra rounds.",
        "# TYPE secflow_dfa_retry_total counter",
        f"secflow_dfa_retry_total {retry_total}",
        "# HELP secflow_dfa_timeout_total Timeout-classified terminal tasks.",
        "# TYPE secflow_dfa_timeout_total counter",
        f"secflow_dfa_timeout_total {timeout_total}",
        "# HELP secflow_dfa_cancel_total Cancelled tasks.",
        "# TYPE secflow_dfa_cancel_total counter",
        f"secflow_dfa_cancel_total {cancel_total}",
        "# HELP secflow_dfa_failure_category_total Terminal tasks classified by failure category.",
        "# TYPE secflow_dfa_failure_category_total counter",
    ])
    for category in sorted(failure_category_counts):
        lines.append(f"secflow_dfa_failure_category_total{_labels(category=category)} {failure_category_counts[category]}")
    lines.extend([
        "# HELP secflow_dfa_token_input_total Aggregated input tokens.",
        "# TYPE secflow_dfa_token_input_total counter",
        f"secflow_dfa_token_input_total {token_input_total}",
        "# HELP secflow_dfa_token_output_total Aggregated output tokens.",
        "# TYPE secflow_dfa_token_output_total counter",
        f"secflow_dfa_token_output_total {token_output_total}",
        "# HELP secflow_dfa_token_cost_total Aggregated token cost.",
        "# TYPE secflow_dfa_token_cost_total counter",
        f"secflow_dfa_token_cost_total {_fmt(token_cost_total)}",
        "# HELP secflow_dfa_token_input_running Current running-task input tokens snapshot.",
        "# TYPE secflow_dfa_token_input_running gauge",
        f"secflow_dfa_token_input_running {token_input_running}",
        "# HELP secflow_dfa_token_output_running Current running-task output tokens snapshot.",
        "# TYPE secflow_dfa_token_output_running gauge",
        f"secflow_dfa_token_output_running {token_output_running}",
        "# HELP secflow_dfa_token_cost_running Current running-task token cost snapshot.",
        "# TYPE secflow_dfa_token_cost_running gauge",
        f"secflow_dfa_token_cost_running {_fmt(token_cost_running)}",
        "# HELP secflow_dfa_round_duration_seconds Aggregated round duration.",
        "# TYPE secflow_dfa_round_duration_seconds summary",
        f"secflow_dfa_round_duration_seconds_count {round_total}",
        f"secflow_dfa_round_duration_seconds_sum {_fmt(round_duration_sum)}",
        "# HELP secflow_dfa_judge_duration_seconds Aggregated judge duration.",
        "# TYPE secflow_dfa_judge_duration_seconds summary",
        f"secflow_dfa_judge_duration_seconds_count {judge_total}",
        f"secflow_dfa_judge_duration_seconds_sum {_fmt(judge_duration_sum)}",
        "# HELP secflow_dfa_function_total Aggregated function analysis count.",
        "# TYPE secflow_dfa_function_total counter",
        f"secflow_dfa_function_total {function_total}",
        "# HELP secflow_dfa_total_duration_accumulated_seconds Aggregated cumulative total_duration_ms converted to seconds.",
        "# TYPE secflow_dfa_total_duration_accumulated_seconds counter",
        f"secflow_dfa_total_duration_accumulated_seconds {_fmt(cumulative_duration_total)}",
        "# HELP secflow_dfa_wall_clock_duration_seconds Aggregated wall-clock task duration in seconds.",
        "# TYPE secflow_dfa_wall_clock_duration_seconds counter",
        f"secflow_dfa_wall_clock_duration_seconds {_fmt(wall_clock_duration_total)}",
        "# HELP secflow_dfa_trace_depth_max Maximum trace depth observed from stage events.",
        "# TYPE secflow_dfa_trace_depth_max gauge",
        f"secflow_dfa_trace_depth_max {trace_depth_max}",
        "# HELP secflow_dfa_trace_callee_total Aggregated trace callee count observed from stage events.",
        "# TYPE secflow_dfa_trace_callee_total counter",
        f"secflow_dfa_trace_callee_total {trace_callee_total}",
        "# HELP secflow_dfa_dispatch_status Aggregated dispatch status count.",
        "# TYPE secflow_dfa_dispatch_status gauge",
    ])
    for dispatch_status in sorted(dispatch_counts):
        lines.append(f"secflow_dfa_dispatch_status{_labels(status=dispatch_status)} {dispatch_counts[dispatch_status]}")
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
