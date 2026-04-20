#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

APP_ROOT = Path(os.environ.get("APP_ROOT", "/app")).resolve()
RUN_ROOT = APP_ROOT / ".run"
STAGE = "04-dataflow"
PREV_STAGE = "03-entry"
NEXT_STAGE = "05-vuln"
DATAFLOW_TIMEOUT_SEC = int(os.environ.get("DATAFLOW_ANALYSE_TIMEOUT_SEC", "45"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def update_status(status: str, message: str = "") -> None:
    save_json(RUN_ROOT / STAGE / "status.json", {
        "stage": STAGE,
        "status": status,
        "message": message,
        "updated_at": now_iso(),
    })


def update_pipeline(current_stage: str, status: str, message: str = "", mode: str = "real") -> None:
    save_json(RUN_ROOT / "meta" / "pipeline.json", {
        "current_stage": current_stage,
        "status": status,
        "mode": mode,
        "message": message,
        "updated_at": now_iso(),
    })


def require_previous_passed() -> None:
    status = load_json(RUN_ROOT / PREV_STAGE / "status.json").get("status")
    if status != "passed":
        raise RuntimeError(f"upstream stage not passed: {PREV_STAGE}={status!r}")


def create_request_for_next(payload: dict) -> None:
    save_json(RUN_ROOT / NEXT_STAGE / "request.json", payload)


def has_any_file(root: Path) -> bool:
    return root.is_dir() and any(p.is_file() for p in root.rglob("*"))


def downstream_source_root() -> Path:
    recovered = RUN_ROOT / "02-re" / "output" / "recovered"
    if has_any_file(recovered):
        return recovered
    unpacked = RUN_ROOT / "00-unpack" / "output" / "unpacked"
    if has_any_file(unpacked):
        return unpacked
    return APP_ROOT


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def sync_tree(src: Path, dst: Path, *, exclude_run: bool = False) -> None:
    reset_dir(dst)
    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)
        if exclude_run and rel.parts[:1] == (".run",):
            dirs[:] = []
            continue
        target_root = dst / rel
        target_root.mkdir(parents=True, exist_ok=True)
        for name in files:
            src_file = root_path / name
            if exclude_run and RUN_ROOT in src_file.parents:
                continue
            shutil.copy2(src_file, target_root / name)


def prepare_input() -> Path:
    input_dir = RUN_ROOT / STAGE / "input"
    src_root = downstream_source_root()
    sync_tree(src_root, input_dir, exclude_run=(src_root == APP_ROOT))
    return input_dir


def force_symlink(link: Path, target: Path) -> None:
    if link.exists() or link.is_symlink():
        if link.is_dir() and not link.is_symlink():
            shutil.rmtree(link)
        else:
            link.unlink()
    link.symlink_to(target)


def setup_data_links(input_dir: Path, config_dir: Path, output_dir: Path) -> None:
    Path("/data").mkdir(parents=True, exist_ok=True)
    force_symlink(Path("/data/target"), input_dir)
    force_symlink(Path("/data/config"), config_dir)
    force_symlink(Path("/data/output"), output_dir)
    models = config_dir / "models.json"
    if models.is_file():
        pi_dir = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))
        pi_dir.mkdir(parents=True, exist_ok=True)
        force_symlink(pi_dir / "models.json", models)


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"required file not found: {path}")


def copy_if_missing(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or not src.is_file():
        return
    shutil.copy2(src, dst)


def ensure_default_config(config_dir: Path) -> None:
    copy_if_missing(Path("/opt/data_flow_analyse/config.example.json"), config_dir / "config.json")
    copy_if_missing(Path("/root/.pi/agent/models.json"), config_dir / "models.json")


def pick_smoke_input() -> Path:
    preferred = APP_ROOT / "hello"
    if preferred.is_file():
        return preferred
    for path in APP_ROOT.rglob("*"):
        if path.is_file() and RUN_ROOT not in path.parents:
            return path
    raise FileNotFoundError("no analysis input found under /app (excluding /app/.run)")


def run_smoke() -> None:
    target_file = pick_smoke_input()
    output_dir = RUN_ROOT / STAGE / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(target_file.read_bytes()).hexdigest()
    stage_file = output_dir / f"{STAGE}.txt"
    stage_file.write_text(f"stage={STAGE}\ninput={target_file}\nsha256={digest}\n", encoding="utf-8")
    save_json(output_dir / "summary.json", {
        "stage": STAGE,
        "mode": "smoke",
        "selected_input": str(target_file),
        "selected_input_sha256": digest,
        "final_output": str(stage_file),
        "updated_at": now_iso(),
    })
    create_request_for_next({"from_stage": STAGE, "input_file": str(stage_file), "mode": "smoke"})


def run_real() -> None:
    input_dir = prepare_input()
    output_dir = RUN_ROOT / STAGE / "output"
    config_dir = RUN_ROOT / "config" / STAGE
    output_dir.mkdir(parents=True, exist_ok=True)
    ensure_default_config(config_dir)
    require_file(config_dir / "config.json")
    require_file(config_dir / "models.json")

    entrypoints = load_json(RUN_ROOT / "03-entry" / "output" / "entrypoints.json").get("entries", [])
    tasks: list[dict] = []
    skipped_tasks: list[dict] = []
    for entry in entrypoints:
        file_name = entry.get("file", "")
        function_name = entry.get("function", "")
        module_name = entry.get("module", "unknown")
        if not file_name or not function_name:
            continue
        task_name = f"{module_name}__{Path(file_name).name}__{function_name}"
        task_dir = output_dir / "tasks" / task_name
        task_dir.mkdir(parents=True, exist_ok=True)
        setup_data_links(input_dir, config_dir, task_dir)
        try:
            subprocess.run([
                "python3", "cli.py",
                f"对 {file_name} 的 {function_name} 函数完成数据流分析",
                "--config", "/data/config/config.json",
                "--cwd", "/data/target",
                "--quiet",
            ], check=True, timeout=DATAFLOW_TIMEOUT_SEC)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            skipped_tasks.append({
                "module": module_name,
                "file": file_name,
                "function": function_name,
                "reason": type(exc).__name__,
                "message": str(exc),
            })
            continue
        tasks.append({
            "module": module_name,
            "file": file_name,
            "function": function_name,
            "task_dir": str(task_dir),
        })

    save_json(output_dir / "summary.json", {
        "stage": STAGE,
        "mode": "real",
        "status": "passed",
        "task_count": len(tasks),
        "tasks": tasks,
        "skipped_tasks": skipped_tasks,
        "updated_at": now_iso(),
    })
    create_request_for_next({
        "from_stage": STAGE,
        "tasks": tasks,
        "skipped_tasks": skipped_tasks,
        "mode": "real",
    })


def main() -> int:
    require_previous_passed()
    mode = os.environ.get("CHAINED_MODE") or load_json(RUN_ROOT / STAGE / "request.json").get("mode") or "real"
    update_pipeline(STAGE, "running", mode=mode)
    update_status("running")
    try:
        if mode == "smoke":
            run_smoke()
        else:
            run_real()
    except Exception as exc:
        update_status("failed", str(exc))
        update_pipeline(STAGE, "failed", str(exc), mode=mode)
        raise
    update_status("passed")
    update_pipeline(NEXT_STAGE, "running", mode=mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
