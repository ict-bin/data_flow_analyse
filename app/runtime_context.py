from __future__ import annotations

import os
import uuid


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


POD_NAME = str(os.environ.get("DFA_POD_NAME") or os.environ.get("HOSTNAME") or "local").strip() or "local"
POD_IP = str(os.environ.get("DFA_POD_IP") or "").strip()
WORKER_ID = POD_NAME
INSTANCE_ID = f"{POD_NAME}:{uuid.uuid4().hex[:8]}"
LEASE_TTL_SECONDS = int(os.environ.get("DFA_LEASE_TTL_SECONDS", "90"))
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("DFA_HEARTBEAT_INTERVAL_SECONDS", "15"))
DISPATCH_POLL_INTERVAL_SECONDS = float(os.environ.get("DFA_DISPATCH_POLL_INTERVAL_SECONDS", "2"))
MAX_LOCAL_RUNNING_TASKS = int(os.environ.get("DFA_MAX_LOCAL_RUNNING_TASKS", "2"))
WORKER_SLOT_HEARTBEAT_SECONDS = int(os.environ.get("DFA_WORKER_SLOT_HEARTBEAT_SECONDS", "30"))
WORKER_SLOT_STALE_AFTER_SECONDS = int(
    os.environ.get("DFA_WORKER_SLOT_STALE_AFTER_SECONDS", str(max(30, WORKER_SLOT_HEARTBEAT_SECONDS * 3)))
)
WORKER_SLOT_RETENTION_SECONDS = int(
    os.environ.get("DFA_WORKER_SLOT_RETENTION_SECONDS", str(max(WORKER_SLOT_STALE_AFTER_SECONDS, WORKER_SLOT_STALE_AFTER_SECONDS * 10)))
)
CLUSTER_EXPECTED_WORKERS = int(os.environ.get("DFA_CLUSTER_EXPECTED_WORKERS", "0"))
CLUSTER_EXPECTED_WORKER_CAPACITY = int(os.environ.get("DFA_CLUSTER_EXPECTED_WORKER_CAPACITY", str(MAX_LOCAL_RUNNING_TASKS)))
ROLE = str(os.environ.get("DFA_ROLE", "all")).strip().lower() or "all"
PUBLIC_API_ENABLED = _env_bool("DFA_ENABLE_PUBLIC_API", ROLE in {"all", "api"})
DISPATCHER_ENABLED = _env_bool("DFA_ENABLE_DISPATCHER", ROLE in {"all", "worker"})
EXECUTOR_ENABLED = _env_bool("DFA_ENABLE_EXECUTOR", ROLE in {"all", "worker"})
REGISTRY_ENABLED = _env_bool("DFA_ENABLE_REGISTRY", ROLE in {"all", "api"})
WORKER_SLOT_REGISTRY_ENABLED = _env_bool("DFA_ENABLE_WORKER_SLOT_REGISTRY", ROLE in {"all", "worker"})
