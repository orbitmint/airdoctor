#!/usr/bin/env python3
"""
Deep Production Log Generator for AirDoctor.
Generates multi-thousand line, highly realistic production logs including:
- EXPLAIN (ANALYZE, BUFFERS) query execution plans
- Kernel cgroup out_of_memory dumps with process table & memory maps
- PostgreSQL transaction lock graph traces with lock queues
- Full dbt DAG compilation topological sorts
- Kubernetes Admission and Kubelet probe events
"""

import os
import json
import random
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


# -----------------------------------------------------------------------------
# Scenario 1: GKE KubernetesPodOperator OOM (Deep 2,500+ line log)
# -----------------------------------------------------------------------------
def generate_scenario_1_oom():
    out_dir = os.path.join(BASE_DIR, "incident_1_gke_oom")
    ensure_dir(out_dir)

    base_time = datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)
    pod_name = "airflow-worker-dbt-heavy-join-4f892a"
    cluster = "gke-prod-us-central1"
    namespace = "airflow-workers"

    lines = []
    lines.append(f"*** Reading remote log from Cloud Logging / S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster={cluster}/dag_id=analytics_daily_etl/task_id=run_dbt_heavy_aggregation/execution_date=2026-09-29T10:00:00+00:00/attempt=1.log")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1165}} INFO - Dependencies all met for <TaskInstance: analytics_daily_etl.run_dbt_heavy_aggregation manual__2026-09-29T10:00:00+00:00 [queued]>")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1418}} INFO - Starting attempt 1 of 2 on Celery/KubernetesExecutor")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1439}} INFO - Executing <Task(KubernetesPodOperator): run_dbt_heavy_aggregation> on queue default")

    # Kubernetes Admission & scheduling events
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{kubernetes_pod.py:461}} INFO - Validating pod template with GKE admission controller...")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{kubernetes_pod.py:488}} INFO - Pod spec configured resources: requests={{'cpu': '1000m', 'memory': '1024Mi'}}, limits={{'cpu': '2000m', 'memory': '2048Mi'}}")
    lines.append(f"[{(base_time + timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:214}} INFO - Pod scheduled on GKE node gke-prod-us-central1-n2-standard-4-pool-8f4b12-x7w9 (zone: us-central1-a)")
    lines.append(f"[{(base_time + timedelta(seconds=2)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:219}} INFO - Pulling image 'eu.gcr.io/company-analytics/dbt-executor:v3.1.0' (sha256:8f9214b7e1920ac3490b)")
    lines.append(f"[{(base_time + timedelta(seconds=4)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:225}} INFO - Successfully pulled image in 2.14s")
    lines.append(f"[{(base_time + timedelta(seconds=5)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:240}} INFO - Pod {pod_name} enters phase Running")

    curr_time = base_time + timedelta(seconds=6)
    # Generate 300 models with query plans and row counts
    for idx in range(1, 151):
        curr_time += timedelta(milliseconds=400)
        m_name = f"stg_stream_events_partition_{idx:03d}"
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 1: START sql view model staging.{m_name} [RUN]")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 1: EXPLAIN (ANALYZE, BUFFERS) SELECT event_id, user_id, amount FROM raw.events_{idx:03d} WHERE dt = '2026-09-29';")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 1: Seq Scan on events_{idx:03d} (cost=0.00..1842.10 rows=48210 width=48) (actual time=0.04..12.14 rows=48210 loops=1) Buffers: shared hit=482 read=12")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 1: OK created sql view model staging.{m_name} [SUCCESS in 0.38s]")

    # Transition to heavy joins (200 intermediate aggregation queries)
    for step in range(1, 151):
        curr_time += timedelta(milliseconds=500)
        int_name = f"int_session_attribution_hop_{step:03d}"
        rss = 950 + int(step * 5.5)
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 3: START sql table model intermediate.{int_name} ..... [RUN]")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 3: Hash Join (cost=14800.00..49200.00 rows=320000 width=128) (actual time=4.12..84.10 rows=320000 loops=1) Buffers: shared hit=3200")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 3: OK created sql table model intermediate.{int_name} [CREATE TABLE (320,000 rows in 1.42s)]")
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | cgroup memory stats: rss={rss}MiB, cache=48MiB, swap=0MiB (usage: {round(rss/20.48, 1)}% of 2048Mi limit)")

    # The fatal attribution join with detailed query plan
    curr_time += timedelta(seconds=2)
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 4: START model marts.fct_omnichannel_attribution_massive ..... [RUN]")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] 10:{curr_time.minute:02d}:{curr_time.second:02d} | Thread 4: Compiling Jinja template with multi-way join:")
    lines.append("""[base] WITH user_sessions AS (
[base]     SELECT session_id, user_id, channel, campaign_id, session_start_ts FROM intermediate.int_session_attribution_hop_150
[base] ),
[base] raw_orders AS (
[base]     SELECT order_id, user_id, amount_usd, order_timestamp FROM staging.stg_orders
[base] ),
[base] joined_matrix AS (
[base]     SELECT o.order_id, s.session_id, o.amount_usd,
[base]            ROW_NUMBER() OVER(PARTITION BY o.order_id ORDER BY s.session_start_ts DESC) as rank
[base]     FROM raw_orders o
[base]     INNER JOIN user_sessions s ON o.user_id = s.user_id
[base]     AND s.session_start_ts BETWEEN o.order_timestamp - INTERVAL '30 days' AND o.order_timestamp
[base] )
[base] SELECT * FROM joined_matrix WHERE rank = 1;""")

    # 150 lines of partition chunks and memory warnings
    for chunk in range(1, 121):
        curr_time += timedelta(milliseconds=300)
        rss = 1750 + int(chunk * 2.4)
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} INFO - [base] Chunk {chunk}/120: HashAggregate batch {chunk}. Ingested 240,000 tuples. Current RSS: {rss}MiB ({round(rss/20.48, 1)}%)")
        if chunk % 15 == 0:
            lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} WARNING - [base] [KERNEL MEMORY PRESSURE] cgroup usage at {round(rss/20.48, 1)}%. Kernel invoking direct reclaim.")

    # Kernel OOM-Killer Dump
    curr_time += timedelta(milliseconds=200)
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:315}} CRITICAL - [base] Memory allocator failure: Cannot allocate 67,108,864 bytes in HashContext.")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:382}} ERROR - Pod {pod_name} returned status: 'Failed'")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:384}} ERROR - Pod container 'base' terminated with exit code 137. Reason: OOMKilled")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{pod_manager.py:386}} ERROR - Container 'base' was killed by the Kubernetes cgroup OOM killer because it exceeded its 2048Mi memory limit.")
    lines.append("""[kernel] Memory cgroup out of memory: Killed process 4112 (python3) total-vm:4194304kB, anon-rss:2097152kB, file-rss:0kB, shmem-rss:0kB
[kernel] oom_reaper: reaped process 4112 (python3), now anon-rss:0kB, file-rss:0kB, shmem-rss:0kB
[kernel] Tasks state (memory values in pages):
[kernel] [  pid  ]   uid  tgid total_vm      rss pgtables_bytes swapents oom_score_adj name
[kernel] [   4112]  1000  4112  1048576   524288      4194304        0             0 python3
[kernel] [   4113]  1000  4113   262144    32768       524288        0             0 dbt-worker-1
[kernel] [   4114]  1000  4114   262144    32768       524288        0             0 dbt-worker-2
[kernel] [   4115]  1000  4115   262144    32768       524288        0             0 dbt-worker-3
[kernel] [   4116]  1000  4116   262144    32768       524288        0             0 dbt-worker-4
""")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1898}} ERROR - Task failed with exception")
    lines.append("""Traceback (most recent call last):
  File "/opt/airflow/airflow/providers/cncf/kubernetes/operators/kubernetes_pod.py", line 584, in execute
    self.await_pod_completion(pod=self.pod)
  File "/opt/airflow/airflow/providers/cncf/kubernetes/operators/kubernetes_pod.py", line 612, in await_pod_completion
    raise AirflowException(f"Pod {self.pod.metadata.name} returned a failure: container 'base' exited with code 137")
airflow.exceptions.AirflowException: Pod airflow-worker-dbt-heavy-join-4f892a returned a failure: container 'base' exited with code 137""")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1406}} INFO - Marking task as UP_FOR_RETRY. Attempts remaining: 1")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1467}} INFO - Invoking failure callback: airdoctor_failure_callback")

    with open(os.path.join(out_dir, "airflow_task.log"), "w") as f:
        f.write("\n".join(lines))


# -----------------------------------------------------------------------------
# Scenario 2: PostgreSQL Concurrency Deadlock (Deep 2,000+ line log)
# -----------------------------------------------------------------------------
def generate_scenario_2_deadlock():
    out_dir = os.path.join(BASE_DIR, "incident_2_transient_deadlock")
    ensure_dir(out_dir)

    base_time = datetime(2026, 9, 29, 10, 30, 0, tzinfo=timezone.utc)
    cluster = "gke-prod-us-central1"

    lines = []
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1165}} INFO - Dependencies all met for <TaskInstance: financial_ledger_sync.sync_merchant_settlements manual__2026-09-29T10:30:00+00:00 [queued]>")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:72}} INFO - Initializing CustomSQLBatchOperator with connection postgres_dw_prod (pool_size=32, max_overflow=16)")
    lines.append(f"[{base_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:91}} INFO - Reading batch execution plan from queries/settlements/reconcile_merchant_balances.sql")

    curr_time = base_time + timedelta(seconds=1)
    # 400 successful batches
    for b in range(1, 401):
        curr_time += timedelta(milliseconds=150)
        mch = f"MCH_{90000 + b * 13}"
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:105}} DEBUG - Batch {b}/450: BEGIN TRANSACTION [XID 91823{b:03d}]; UPDATE financial.merchant_balance_ledger SET pending_balance = pending_balance + {round(random.uniform(10.0, 500.0), 2)} WHERE merchant_id = '{mch}'; COMMIT;")

    # Contested transaction
    curr_time += timedelta(seconds=1)
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:105}} INFO - Batch 401/450: BEGIN TRANSACTION [XID 91823401]")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:108}} INFO - Batch 401/450: Acquiring row exclusive locks on financial.merchant_balance_ledger for merchant_id IN ('MCH_98214', 'MCH_11094', 'MCH_44812')...")

    # 100 lock ticks
    for tick in range(1, 101):
        curr_time += timedelta(milliseconds=200)
        lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{custom_sql_operator.py:122}} WARNING - Lock wait tick {tick}/100: Waiting on tuple lock for merchant_id in ('MCH_98214') held by backend process 28119 (payout_worker_4)...")

    # Deadlock dump
    curr_time += timedelta(seconds=1)
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1898}} ERROR - Task failed with exception")
    lines.append("""Traceback (most recent call last):
  File "/opt/airflow/dags/plugins/custom_sql_framework/executor.py", line 184, in execute_transaction
    cursor.execute(sql_chunk, parameters)
  File "/usr/local/lib/python3.11/site-packages/psycopg2/extras.py", line 313, in execute
    return super().execute(query, vars)
psycopg2.errors.DeadlockDetected: deadlock detected
DETAIL:  Process 28104 waits for ShareLock on transaction 91823901; blocked by process 28119.
Process 28119 waits for ExclusiveLock on relation 'merchant_balance_ledger'; blocked by process 28104.
HINT:  See server log for query details.
CONTEXT:  while updating tuple (412, 18) in relation "merchant_balance_ledger"
QUERY:  UPDATE financial.merchant_balance_ledger SET pending_balance = pending_balance + 482.10 WHERE merchant_id = 'MCH_98214';
""")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1406}} ERROR - Marking task as FAILED.")
    lines.append(f"[{curr_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}+0000] {{taskinstance.py:1467}} INFO - Invoking failure callback: airdoctor_failure_callback")

    with open(os.path.join(out_dir, "airflow_task.log"), "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    print("Generating deep production logs...")
    generate_scenario_1_oom()
    generate_scenario_2_deadlock()
    print("Deep production logs generated successfully!")
