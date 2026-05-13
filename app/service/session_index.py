from __future__ import annotations

import json
import re
import time as _time
from pathlib import Path


_STAGE_ORDER = {
    "worker": 10,
    "judge": 20,
    "report": 30,
}

_STAGE_LABEL = {
    "worker": "数据流分析",
    "judge": "Judge 评审",
    "report": "综合报告",
}


def _normalize_relative_path(path: str) -> str:
    return str(path or "").replace("\\", "/").strip().lstrip("/")


def _parse_iso_timestamp(value: object) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        from datetime import datetime

        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return None


def _extract_session_timestamps(session_meta: dict, events: list[dict], stat_mtime: float) -> tuple[float | None, float | None]:
    started_ts = _parse_iso_timestamp(session_meta.get("timestamp"))
    event_timestamps = [
        ts
        for ts in (_parse_iso_timestamp(evt.get("timestamp") or evt.get("display_timestamp")) for evt in events)
        if ts is not None
    ]
    if started_ts is None and event_timestamps:
        started_ts = event_timestamps[0]
    last_ts = event_timestamps[-1] if event_timestamps else started_ts
    if started_ts is None:
        started_ts = stat_mtime
    if last_ts is None:
        last_ts = stat_mtime
    return started_ts, last_ts


def _round_status_to_session_status(status: str, is_active: bool) -> str:
    if is_active:
        return "running"
    normalized = str(status or "").strip().lower()
    if normalized in {"passed", "completed", "success", "skipped"}:
        return "completed"
    if normalized in {"failed", "error", "cancelled", "invalid_input", "completed_limited"}:
        return "blocked"
    return "completed"


def _result_session_relpath(raw_path: object) -> str:
    normalized = _normalize_relative_path(str(raw_path or ""))
    if not normalized:
        return ""
    marker = "/run/"
    if marker in normalized:
        normalized = normalized.split(marker, 1)[1]
    return normalized


def _load_round_refs(result_json: dict | None) -> dict[str, list[dict]]:
    refs: dict[str, list[dict]] = {}
    rounds = result_json.get("rounds") if isinstance(result_json, dict) and isinstance(result_json.get("rounds"), list) else []
    for item in rounds:
        if not isinstance(item, dict):
            continue
        base_ref = {
            "round": item.get("round"),
            "stage_round": item.get("stage_round") or item.get("round"),
            "stage": item.get("stage") or "worker",
            "module_name": item.get("function_name") or item.get("function") or item.get("func") or item.get("entry"),
            "status": item.get("status") or ("passed" if item.get("passed") else "failed"),
            "started_at": item.get("started_at"),
            "ended_at": item.get("ended_at"),
            "completion_reason": item.get("completion_reason"),
        }
        for worker in item.get("worker_results") or []:
            if not isinstance(worker, dict):
                continue
            session_file = _result_session_relpath(worker.get("session_file"))
            if session_file:
                refs.setdefault(session_file, []).append({
                    **base_ref,
                    "kind": "worker",
                    "model": worker.get("model"),
                    "worker_id": worker.get("worker_id"),
                })
        for judge in item.get("judge_results") or []:
            if not isinstance(judge, dict):
                continue
            session_file = _result_session_relpath(judge.get("session_file"))
            if session_file:
                refs.setdefault(session_file, []).append({
                    **base_ref,
                    "kind": "judge",
                    "model": judge.get("model"),
                    "judge_id": judge.get("judge_id"),
                })
    return refs


def _infer_path_descriptor(relative_path: str) -> dict:
    normalized = _normalize_relative_path(relative_path)
    parts = normalized.split("/")
    stem = Path(normalized).stem
    desc = {
        "role": "worker",
        "role_label": "Worker",
        "stage_key": "worker",
        "stage_label": _STAGE_LABEL["worker"],
        "stage_order": _STAGE_ORDER["worker"],
        "module_name": None,
        "attempt": None,
        "judge_index": None,
        "batch_index": None,
        "parent_relative_path": None,
        "parallel_group": None,
        "family_key": None,
        "flow_kind": "worker",
    }
    if normalized.startswith("sessions/"):
        if stem.endswith("-base"):
            desc["family_key"] = stem
            return desc
        taint_match = re.fullmatch(r"(worker-\d+)-taint-(.+)", stem)
        if taint_match:
            desc.update({
                "role": "sub_worker",
                "role_label": "Sub Worker",
                "module_name": taint_match.group(2),
                "parent_relative_path": f"sessions/{taint_match.group(1)}-base.jsonl",
                "parallel_group": f"taint::{taint_match.group(1)}",
                "family_key": f"taint::{taint_match.group(1)}",
                "flow_kind": "parallel",
            })
            return desc
        if stem.endswith("-summary"):
            worker_prefix = stem[:-len("-summary")]
            desc.update({
                "parent_relative_path": f"sessions/{worker_prefix}-base.jsonl",
                "family_key": f"summary::{worker_prefix}",
            })
            return desc
        if stem.startswith("merge-") or stem == "merge":
            desc.update({
                "stage_key": "report",
                "stage_label": _STAGE_LABEL["report"],
                "stage_order": _STAGE_ORDER["report"],
                "family_key": "merge",
            })
            return desc
        return desc
    if "judges" in parts:
        round_match = re.search(r"round_(\d+)", normalized)
        judge_dir_index = parts.index("judges")
        judge_name = parts[judge_dir_index + 1] if judge_dir_index + 1 < len(parts) else "judge"
        judge_match = re.search(r"judge-(\d+)", judge_name)
        desc.update({
            "role": "judge",
            "role_label": "Judge",
            "stage_key": "judge",
            "stage_label": _STAGE_LABEL["judge"],
            "stage_order": _STAGE_ORDER["judge"],
            "attempt": int(round_match.group(1)) if round_match else None,
            "judge_index": int(judge_match.group(1)) if judge_match else None,
            "parent_relative_path": "sessions/worker-0-summary.jsonl" if normalized.startswith("round_") else None,
            "parallel_group": f"judge::{round_match.group(1)}" if round_match else "judge",
            "family_key": f"judge::{round_match.group(1)}" if round_match else "judge",
            "flow_kind": "parallel",
        })
        return desc
    return desc


def _parse_session_file(path: Path) -> tuple[dict, list[dict], list[str], int]:
    session_meta: dict = {}
    events: list[dict] = []
    warnings: list[str] = []
    line_count = 0
    for index, raw in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        line_count += 1
        try:
            obj = json.loads(line)
        except Exception:
            warnings.append(f"第 {index} 行 JSON 解析失败")
            events.append({"type": "raw", "line": index, "event_index": index, "raw_line": line[:200], "summary": line[:200]})
            continue
        if isinstance(obj, dict) and obj.get("type") == "session":
            session_meta = {
                "id": obj.get("id", ""),
                "version": obj.get("version", ""),
                "timestamp": obj.get("timestamp", ""),
                "cwd": obj.get("cwd", ""),
            }
            continue
        if not isinstance(obj, dict):
            events.append({"type": "raw", "line": index, "event_index": index, "raw_line": line[:200], "summary": line[:200]})
            continue
        events.append({
            "type": obj.get("type", "unknown_event"),
            "line": index,
            "event_index": index,
            "timestamp": obj.get("timestamp", ""),
            "display_timestamp": obj.get("timestamp", ""),
            "raw_line": line[:200],
            "summary": str(obj)[:200],
        })
    return session_meta, events, warnings, line_count


def build_session_catalog(*, task_id: str, row_status: str, run_root: Path, result_json: dict | None, write_json_atomic) -> dict:
    refs_by_path = _load_round_refs(result_json)
    now_ts = _time.time()
    warnings: list[str] = []
    items: list[dict] = []
    nodes: list[dict] = []
    node_map: dict[str, dict] = {}

    for session_file in sorted(run_root.glob("**/*.jsonl")):
        if not session_file.is_file():
            continue
        try:
            relative_path = _normalize_relative_path(str(session_file.relative_to(run_root)))
            if relative_path.endswith("index.jsonl"):
                continue
            stage_group = relative_path.split("/")[0] if "/" in relative_path else "root"
            session_name = session_file.stem
            session_meta, events, session_warnings, line_count = _parse_session_file(session_file)
            stat = session_file.stat()
            is_active = row_status in {"pending", "running"} and (now_ts - stat.st_mtime) <= 120
            display_name = session_name if stage_group == "root" else f"{stage_group} / {session_name}"
            desc = _infer_path_descriptor(relative_path)
            round_refs = refs_by_path.get(relative_path, [])
            latest_ref = round_refs[-1] if round_refs else {}
            started_ts, last_event_ts = _extract_session_timestamps(session_meta, events, stat.st_mtime)
            status = _round_status_to_session_status(str(latest_ref.get("status") or ""), is_active)
            items.append({
                "session_id": relative_path,
                "session_name": session_name,
                "relative_path": relative_path,
                "stage_group": stage_group,
                "role_name": desc["role"],
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "event_count": len(events),
                "line_count": line_count,
                "is_active": is_active,
                "display_name": display_name,
                "warnings": session_warnings,
                "epoch": None,
                "is_latest_epoch": True,
            })
            node = {
                "node_id": relative_path,
                "relative_path": relative_path,
                "session_name": session_name,
                "display_name": display_name,
                "role": desc["role"],
                "role_label": desc["role_label"],
                "status": status,
                "is_active": is_active,
                "stage_key": desc["stage_key"],
                "stage_label": desc["stage_label"],
                "stage_order": desc["stage_order"],
                "stage_group": stage_group,
                "module_name": desc["module_name"],
                "attempt": desc["attempt"],
                "judge_index": desc["judge_index"],
                "batch_index": desc["batch_index"],
                "parent_relative_path": desc["parent_relative_path"],
                "parallel_group": desc["parallel_group"],
                "family_key": desc["family_key"],
                "flow_kind": desc["flow_kind"],
                "started_at": latest_ref.get("started_at") or session_meta.get("timestamp"),
                "ended_at": latest_ref.get("ended_at"),
                "started_ts": started_ts,
                "last_event_at": latest_ref.get("ended_at") or latest_ref.get("started_at") or session_meta.get("timestamp"),
                "last_event_ts": last_event_ts,
                "mtime": stat.st_mtime,
                "size": stat.st_size,
                "event_count": len(events),
                "line_count": line_count,
                "warnings": session_warnings,
                "session_header": session_meta,
                "cwd": session_meta.get("cwd"),
                "model": latest_ref.get("model"),
                "latest_round_ref": latest_ref or None,
                "round_refs": round_refs,
                "attempts_seen": sorted({int(ref.get("stage_round")) for ref in round_refs if str(ref.get("stage_round") or "").isdigit()}),
            }
            nodes.append(node)
            node_map[relative_path] = node
        except Exception as exc:
            warnings.append(f"{session_file.name} 解析失败: {exc}")

    edges: list[dict] = []
    seen_edges: set[tuple[str, str, str]] = set()

    def add_edge(source: str | None, target: str | None, kind: str, label: str) -> None:
        if not source or not target or source == target or source not in node_map or target not in node_map:
            return
        key = (source, target, kind)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append({
            "edge_id": f"{kind}:{source}->{target}",
            "source_node_id": source,
            "target_node_id": target,
            "kind": kind,
            "label": label,
        })

    for node in nodes:
        add_edge(node.get("parent_relative_path"), node["relative_path"], "spawn", "派生")

    groups: list[dict] = []
    groups_by_key: dict[str, list[str]] = {}
    for node in nodes:
        key = str(node.get("parallel_group") or "").strip()
        if key:
            groups_by_key.setdefault(key, []).append(node["node_id"])
    for group_key, node_ids in sorted(groups_by_key.items()):
        node_ids.sort(key=lambda value: (float(node_map[value].get("started_ts") or node_map[value].get("mtime") or 0.0), value))
        groups.append({
            "group_id": group_key,
            "kind": "parallel",
            "label": "并列 Judge" if node_map[node_ids[0]].get("role") == "judge" else "并列 Worker",
            "stage_key": node_map[node_ids[0]].get("stage_key"),
            "module_name": node_map[node_ids[0]].get("module_name"),
            "node_ids": node_ids,
        })
        if len(node_ids) >= 2:
            for left, right in zip(node_ids, node_ids[1:]):
                add_edge(left, right, "parallel", "并列")

    nodes.sort(key=lambda item: (int(item.get("stage_order") or 999), float(item.get("started_ts") or item.get("mtime") or 0.0), str(item.get("relative_path") or "")))
    items.sort(key=lambda item: (item["stage_group"], -item["mtime"], item["relative_path"]))
    payload = {
        "version": 1,
        "generated_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(now_ts)),
        "task_id": task_id,
        "task_status": row_status,
        "sessions_root": str(run_root / "sessions"),
        "summary": {
            "session_count": len(nodes),
            "active_session_count": sum(1 for node in nodes if node.get("is_active")),
            "worker_count": sum(1 for node in nodes if node.get("role") == "worker"),
            "judge_count": sum(1 for node in nodes if node.get("role") == "judge"),
            "sub_worker_count": sum(1 for node in nodes if node.get("role") == "sub_worker"),
            "edge_count": len(edges),
            "parallel_group_count": len(groups),
            "stage_count": len({str(node.get("stage_key") or "") for node in nodes}),
        },
        "nodes": nodes,
        "edges": edges,
        "groups": groups,
        "warnings": warnings,
    }
    index_path = run_root / "sessions" / "index.json"
    write_json_atomic(index_path, payload)
    return {
        "task_id": task_id,
        "status": row_status,
        "sessions_root": str(run_root / "sessions"),
        "index_path": str(index_path),
        "generated_at": payload["generated_at"],
        "items": items,
        "index": payload,
        "warnings": warnings,
    }
