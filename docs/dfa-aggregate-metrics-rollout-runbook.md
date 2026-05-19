# DFA Aggregate Metrics Rollout Runbook

## Goal

Roll out the Dataflow Analysis aggregate metrics/dashboard changes safely and validate:

- local vs aggregate metrics split
- configured worker capacity view
- observed active owner / heartbeat owner view
- derived dashboard alerts

This runbook is intended for test, staging, or production rollout.

## Changed Areas

Backend:

- local metrics endpoint
- aggregate metrics endpoint
- cluster task / lease / heartbeat snapshots
- configured capacity and observed owner metrics
- local execution event counters

Frontend:

- DFA metrics source switched to `/metrics/aggregate`
- DFA-specific observability cards and alerts

K8S:

- new env vars:
  - `DFA_CLUSTER_EXPECTED_WORKERS`
  - `DFA_CLUSTER_EXPECTED_WORKER_CAPACITY`

## Files

- [metrics.py](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-app-dataflow-analyse/app/metrics.py)
- [server.py](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-app-dataflow-analyse/app/server.py)
- [runtime_context.py](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-app-dataflow-analyse/app/runtime_context.py)
- [task_service.py](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-app-dataflow-analyse/app/service/task_service.py)
- [binarySecurityMetrics.ts](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-frontend/clients/binarySecurityMetrics.ts)
- [BinarySecurityMetricsDashboardPage.tsx](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-frontend/pages/execution/BinarySecurityMetricsDashboardPage.tsx)
- [00-secflow-105-01-app-dataflow-analyse-deployment.yaml](/home/runshine/CLionProjects/sothoth/13-secflow-service/00-secflow-105-01-app-dataflow-analyse-deployment.yaml)
- [00-secflow-105-03-app-dataflow-analyse-worker-deployment.yaml](/home/runshine/CLionProjects/sothoth/13-secflow-service/00-secflow-105-03-app-dataflow-analyse-worker-deployment.yaml)

## Pre-Checks

1. Confirm image tag to deploy.
2. Confirm worker replica target and slot capacity target.
3. Confirm these env values match real deployment intent:
   - `DFA_CLUSTER_EXPECTED_WORKERS`
   - `DFA_CLUSTER_EXPECTED_WORKER_CAPACITY`
4. Confirm the frontend image includes the DFA dashboard updates.
5. Confirm no unrelated rollout is happening for the same service.

## Recommended Rollout Order

1. Deploy backend image for DFA API and worker.
2. Wait for API and worker pods to become ready.
3. Verify aggregate and local metrics endpoints manually.
4. Deploy frontend image.
5. Open the performance dashboard and validate DFA cards/alerts.
6. Run the validation checklist from:
   - [dfa-aggregate-metrics-validation.md](/home/runshine/CLionProjects/sothoth/13-secflow-service/image_build/secflow-app-dataflow-analyse/docs/dfa-aggregate-metrics-validation.md)

## K8S Commands

## Check Current State

```bash
kubectl -n secflow-ns get deploy secflow-app-dataflow-analyse secflow-app-dataflow-analyse-worker
kubectl -n secflow-ns get pods -l name=secflow-app-dataflow-analyse -L role
kubectl -n secflow-ns get svc secflow-app-dataflow-analyse
```

## Apply Manifest Changes

```bash
kubectl apply -f 13-secflow-service/00-secflow-105-01-app-dataflow-analyse-deployment.yaml
kubectl apply -f 13-secflow-service/00-secflow-105-02-app-dataflow-analyse-service.yaml
kubectl apply -f 13-secflow-service/00-secflow-105-03-app-dataflow-analyse-worker-deployment.yaml
```

## Watch Rollout

```bash
kubectl -n secflow-ns rollout status deploy/secflow-app-dataflow-analyse --timeout=10m
kubectl -n secflow-ns rollout status deploy/secflow-app-dataflow-analyse-worker --timeout=20m
kubectl -n secflow-ns get pods -l name=secflow-app-dataflow-analyse -L role -w
```

## Confirm Env Values

```bash
kubectl -n secflow-ns get deploy secflow-app-dataflow-analyse -o yaml | grep -A2 DFA_CLUSTER_EXPECTED
kubectl -n secflow-ns get deploy secflow-app-dataflow-analyse-worker -o yaml | grep -A2 DFA_CLUSTER_EXPECTED
```

## Manual Endpoint Verification

## Aggregate Endpoint

```bash
kubectl -n secflow-ns port-forward svc/secflow-app-dataflow-analyse 18080:80
curl -s http://127.0.0.1:18080/api/app/dataflow-analyse/metrics/aggregate | grep secflow_dfa_cluster
```

Key checks:

- `secflow_dfa_metrics_aggregate_up 1`
- `secflow_dfa_cluster_workers{state="configured"}`
- `secflow_dfa_cluster_worker_slots{kind="capacity"}`
- `secflow_dfa_cluster_worker_slot_utilization_ratio`
- `secflow_dfa_cluster_worker_observed_coverage_ratio`
- `secflow_dfa_cluster_queue_pressure_ratio`

## Local Metrics From API Pod

```bash
kubectl -n secflow-ns get pods -l name=secflow-app-dataflow-analyse,role=api
kubectl -n secflow-ns port-forward pod/<dfa-api-pod> 18081:8080
curl -s http://127.0.0.1:18081/api/app/dataflow-analyse/metrics | grep secflow_dfa_local
```

Key checks:

- `secflow_dfa_local_role_info{role="api"}`
- `secflow_dfa_local_running_capacity`
- `secflow_dfa_local_events_total`

## Local Metrics From Worker Pod

```bash
kubectl -n secflow-ns get pods -l name=secflow-app-dataflow-analyse,role=worker
kubectl -n secflow-ns port-forward pod/<dfa-worker-pod> 18082:8080
curl -s http://127.0.0.1:18082/api/app/dataflow-analyse/metrics | grep secflow_dfa_local
```

Key checks:

- `secflow_dfa_local_role_info{role="worker"}`
- `secflow_dfa_local_running_tasks`
- `secflow_dfa_local_events_total{event="dispatch_claim",...}`

## Frontend Validation

After frontend rollout:

1. Open performance dashboard.
2. Select `数据流分析`.
3. Confirm page is using aggregate view rather than API-only view.
4. Confirm cards render:
   - 排队任务
   - 运行中任务
   - 有效租约
   - 陈旧租约
   - 心跳正常/超时
   - Worker 配置/观测
5. Confirm load cards render:
   - Busy / Free Slots
   - 平均排队
   - 平均执行
   - 平均周转
6. Confirm alert strip renders at least one stable message.

## Live Validation Scenarios

### Scenario A: Idle Baseline

Expected:

- pending/running/leased all near zero
- busy slots = 0
- free slots = configured capacity
- no severe alert

### Scenario B: Single Task

Expected:

- running rises
- observed active owner rises
- busy slots rises
- after completion terminal count rises

### Scenario C: Burst Tasks

Expected:

- pending rises
- slot utilization rises
- queue pressure may trigger alert

### Scenario D: Worker Loss

Expected:

- observed heartbeat owners may drop
- heartbeat stale may rise
- owner coverage ratio may fall
- alert strip may show heartbeat or owner coverage issue

## Suggested Observability Commands

```bash
kubectl -n secflow-ns logs deploy/secflow-app-dataflow-analyse --tail=200
kubectl -n secflow-ns logs deploy/secflow-app-dataflow-analyse-worker --tail=200
kubectl -n secflow-ns top pods -l name=secflow-app-dataflow-analyse
```

Useful greps:

```bash
kubectl -n secflow-ns logs deploy/secflow-app-dataflow-analyse-worker --tail=500 | grep -E "task_leased|task_execution_started|task_terminal_committed|task_lease_lost|task_error"
```

## Rollback Triggers

Rollback if any of these occur:

1. aggregate endpoint causes repeated API pod instability
2. aggregate metrics latency becomes too high
3. dashboard fails to render DFA page
4. worker rollout causes execution regression
5. configured capacity values are misleading enough to confuse operations

## Rollback Steps

### Rollback Deployment

```bash
kubectl -n secflow-ns rollout undo deploy/secflow-app-dataflow-analyse
kubectl -n secflow-ns rollout undo deploy/secflow-app-dataflow-analyse-worker
kubectl -n secflow-ns rollout status deploy/secflow-app-dataflow-analyse --timeout=10m
kubectl -n secflow-ns rollout status deploy/secflow-app-dataflow-analyse-worker --timeout=20m
```

### Rollback Frontend

Roll back the frontend deployment/image to the previous known-good tag.

### Verify Rollback

1. health endpoint returns normal
2. task submission and execution still work
3. old dashboard behavior is restored

## Post-Rollout Decision

If rollout is stable and dashboard values are useful, the next stage should be one of:

1. add true worker scrape aggregation
2. aggregate `secflow_dfa_local_events_total` across workers
3. define Prometheus/Grafana alert rules using the new derived DFA metrics

The current rollout should be considered a success if it improves operational clarity without destabilizing task execution.
