# DFA Aggregate Metrics Validation

## Scope

This checklist validates the Dataflow Analysis aggregate metrics rollout:

- local metrics vs aggregate metrics split
- cluster task / lease / heartbeat snapshots
- configured worker capacity view
- observed active owner view
- derived alert metrics on the dashboard

The current design intentionally distinguishes:

- configured worker capacity
- observed active owners from DB lease ownership
- observed live heartbeat owners

It does not yet claim full worker scrape coverage.

## Endpoints

- Local pod metrics:
  - `/api/app/dataflow-analyse/metrics`
- Aggregate cluster metrics:
  - `/api/app/dataflow-analyse/metrics/aggregate`
- Health:
  - `/api/app/dataflow-analyse/health`

## Key Metrics

### Local Metrics

- `secflow_dfa_local_role_info`
- `secflow_dfa_local_running_tasks`
- `secflow_dfa_local_running_capacity`
- `secflow_dfa_local_events_total{event,result}`

### Aggregate Metrics

- `secflow_dfa_cluster_tasks_pending`
- `secflow_dfa_cluster_tasks_running`
- `secflow_dfa_cluster_tasks_terminal`
- `secflow_dfa_cluster_leased_tasks`
- `secflow_dfa_cluster_stale_leases`
- `secflow_dfa_cluster_heartbeat_live_tasks`
- `secflow_dfa_cluster_heartbeat_stale_tasks`
- `secflow_dfa_cluster_workers{state="configured"}`
- `secflow_dfa_cluster_workers{state="observed_active_owner"}`
- `secflow_dfa_cluster_workers{state="observed_live_heartbeat_owner"}`
- `secflow_dfa_cluster_worker_slots{kind="capacity|busy|free"}`
- `secflow_dfa_cluster_worker_slot_utilization_ratio`
- `secflow_dfa_cluster_worker_observed_coverage_ratio`
- `secflow_dfa_cluster_queue_pressure_ratio`
- `secflow_dfa_cluster_timeout_count`
- `secflow_dfa_cluster_failure_category{category=...}`

## Test Matrix

### 1. Aggregate Endpoint Smoke

Goal:

- confirm the aggregate endpoint is reachable
- confirm local and aggregate outputs are different by design

Steps:

1. Request local metrics from an API pod.
2. Request aggregate metrics from the service endpoint.
3. Confirm local output contains `secflow_dfa_local_*`.
4. Confirm aggregate output contains `secflow_dfa_cluster_*`.
5. Confirm aggregate output contains `secflow_dfa_metrics_aggregate_up 1`.

Expected:

- local metrics do not contain cluster-only KPI families
- aggregate metrics do not rely on API-local running count

### 2. Idle Cluster Baseline

Goal:

- establish the expected zero-load baseline

Steps:

1. Ensure no DFA tasks are pending or running.
2. Open aggregate metrics and dashboard.

Expected:

- `cluster_tasks_pending = 0`
- `cluster_tasks_running = 0`
- `cluster_leased_tasks = 0`
- `cluster_stale_leases = 0`
- `cluster_heartbeat_live_tasks = 0`
- `cluster_heartbeat_stale_tasks = 0`
- `cluster_worker_slots{kind="busy"} = 0`
- `cluster_worker_slots{kind="free"} = capacity`
- dashboard shows either no alert or `聚合视图平稳`

### 3. Single Task Execution

Goal:

- verify a simple end-to-end task changes metrics as expected

Steps:

1. Submit one DFA task.
2. Observe aggregate metrics every 3 to 5 seconds.
3. Wait for terminal completion.

Expected during run:

- `pending` may briefly rise then fall
- `running >= 1`
- `leased_tasks >= 1`
- `observed_active_owner >= 1`
- `observed_live_heartbeat_owner >= 1`
- `worker_slots{busy} >= 1`
- `worker_slot_utilization_ratio > 0`

Expected after completion:

- `running = 0`
- `leased_tasks = 0`
- `worker_slots{busy} = 0`
- `cluster_tasks_terminal` increases relative to baseline
- one terminal status bucket rises in `secflow_dfa_cluster_tasks{status=...}`

### 4. Multi-Task Concurrency

Goal:

- validate pending/running/slot pressure under concurrent submissions

Suggested load:

- submit 20 tasks quickly

Steps:

1. Submit tasks in a burst.
2. Watch aggregate metrics and dashboard cards.

Expected:

- `pending` rises first
- `running` rises until cluster capacity pressure appears
- `worker_slots{busy}` approaches configured capacity
- `worker_slots{free}` approaches 0
- `worker_slot_utilization_ratio` approaches 1.0
- dashboard may show `执行槽位逼近打满`

### 5. Queue Pressure Validation

Goal:

- verify backlog alert logic

Steps:

1. Keep submitting tasks until `pending > free slots`.
2. Refresh dashboard.

Expected:

- `queue_pressure_ratio >= 1.0` when backlog exceeds configured slot count
- dashboard shows `队列压力偏高`
- `avg queue wait` starts increasing over time

### 6. Observed Owner Coverage Validation

Goal:

- confirm configured vs observed owner distinction works

Precondition:

- configured workers and per-pod capacity env vars are set in both API and worker deployments

Steps:

1. Verify:
   - `DFA_CLUSTER_EXPECTED_WORKERS`
   - `DFA_CLUSTER_EXPECTED_WORKER_CAPACITY`
2. Scale worker deployment lower than configured value, or temporarily keep some workers unavailable.
3. Start several tasks.

Expected:

- `cluster_workers{state="configured"}` remains fixed
- `cluster_workers{state="observed_active_owner"}` reflects only owners actually holding leases
- `cluster_workers{state="observed_live_heartbeat_owner"}` reflects owners with fresh heartbeat
- dashboard may show `观测 Owner 偏少`

### 7. Heartbeat Timeout / Lease Stale Validation

Goal:

- verify stale heartbeat and lease-derived alerts

Safer staging method:

1. Start a running task.
2. Kill one worker pod forcefully during execution, or block it from renewing lease long enough to expire.

Expected:

- `cluster_heartbeat_stale_tasks > 0`
- `cluster_heartbeat_age_seconds_max` grows
- `cluster_stale_leases` may rise depending on ownership state
- dashboard shows `存在心跳超时任务`

Notes:

- run only in test or staging
- observe how quickly the task is recovered or fails

### 8. Timeout Failure Validation

Goal:

- verify timeout-based failure counters and alerts

Options:

1. submit tasks known to exceed time budget
2. temporarily reduce agent timeout in test env

Expected:

- `cluster_timeout_count` rises
- `cluster_failure_category{category="timeout"}` rises
- dashboard may show `超时失败偏高`

### 9. Failure Category Validation

Goal:

- verify failure grouping is meaningful

Steps:

1. Create separate tasks that trigger:
   - timeout
   - cancel
   - lease-lost-like interruption
   - generic execution error
2. Compare task terminal result with aggregate category counts.

Expected:

- aggregate category distribution is directionally correct
- category ranking on the dashboard matches actual dominant failure mode

## Dashboard Validation

## KPI Cards

Validate these cards against aggregate output:

- 排队任务
- 运行中任务
- 有效租约
- 陈旧租约
- 心跳正常/超时
- Worker 配置/观测

## Load Cards

Validate:

- Busy / Free Slots
- 平均排队
- 平均执行
- 平均周转
- 平均轮次 / Judge
- Token / 成本

## Alerts

Expected trigger rules:

- `观测 Owner 偏少`
  - observed coverage ratio below threshold
- `执行槽位逼近打满`
  - slot utilization high
- `存在心跳超时任务`
  - stale heartbeat tasks greater than zero
- `队列压力偏高`
  - pending too high relative to configured slots
- `超时失败偏高`
  - timeout count high in absolute or relative terms

## Suggested Commands

### Port-forward API service

```bash
kubectl -n secflow-ns port-forward svc/secflow-app-dataflow-analyse 18080:80
```

### Fetch aggregate metrics

```bash
curl -s http://127.0.0.1:18080/api/app/dataflow-analyse/metrics/aggregate | grep secflow_dfa_cluster
```

### Fetch local metrics from one pod

```bash
kubectl -n secflow-ns port-forward pod/<dfa-api-pod> 18081:8080
curl -s http://127.0.0.1:18081/api/app/dataflow-analyse/metrics | grep secflow_dfa_local
```

### Watch workers

```bash
kubectl -n secflow-ns get pods -l name=secflow-app-dataflow-analyse -L role -w
```

## Go/No-Go Checks

Ship this rollout only if:

1. aggregate endpoint is stable and returns quickly
2. dashboard cards match aggregate metric values
3. single-task and burst-task behavior matches expectation
4. configured vs observed worker distinction is understandable to operators
5. stale heartbeat and timeout alerts can be reproduced in staging

## Known Gaps

Current design still does not provide:

- true per-pod scrape coverage of all worker local metrics
- cluster-wide aggregation of `secflow_dfa_local_events_total`
- direct K8S-native worker discovery

Those belong to the next stage if deeper runtime observability is needed.
