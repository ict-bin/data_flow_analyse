from __future__ import annotations

import os
import uuid


INSTANCE_ID = f"{os.environ.get('HOSTNAME', 'local')}:{uuid.uuid4().hex[:8]}"
LEASE_TTL_SECONDS = int(os.environ.get("DFA_LEASE_TTL_SECONDS", "90"))
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("DFA_HEARTBEAT_INTERVAL_SECONDS", "15"))
DISPATCH_POLL_INTERVAL_SECONDS = float(os.environ.get("DFA_DISPATCH_POLL_INTERVAL_SECONDS", "2"))
MAX_LOCAL_RUNNING_TASKS = int(os.environ.get("DFA_MAX_LOCAL_RUNNING_TASKS", "2"))
