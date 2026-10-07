#!/usr/bin/env python3
"""
Adds 7 new production failure scenarios to the AirDoctor S3 Data Lake:
1. spot_eviction: GKE Spot VM Preemption (exit code 143 / SIGTERM)
2. disk_full: Ephemeral Storage Exhaustion (No space left on device)
3. statement_timeout: Analytical Query Timeout (Query canceled after 3600s)
4. connection_pool: Postgres max_connections saturated
5. zero_rows: Silent Data Quality / Freshness Anomaly (0 rows vs 420K baseline)
6. division_by_zero: Custom SQL division by zero in fee calculation
7. clock_skew: AWS S3 RequestTimeTooSkewed (NTP clock drift)
"""

import os
import json
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LAKE_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "s3_data_lake"))
FIXTURES_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "fixtures"))

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_lake_file(rel_path: str, content: str):
    full_path = os.path.join(LAKE_DIR, rel_path)
    ensure_dir(os.path.dirname(full_path))
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)

def add_all():
    print("Generating 7 new production failure scenarios...")

    # 1. spot_eviction
    spot_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=batch_churn_scoring/task_id=score_user_features/attempt=1.log
[2026-09-29T14:10:00.000+0000] {taskinstance.py:1165} INFO - Starting task: batch_churn_scoring.score_user_features on GKE spot pool
[2026-09-29T14:10:05.000+0000] {kubernetes_pod.py:461} INFO - Pod scheduled on GKE node gke-prod-us-central1-spot-pool-9a8b (preemptible VM)
[2026-09-29T14:11:42.000+0000] {python_operator.py:84} INFO - Processing feature matrix: 1,420,000 users loaded...
[2026-09-29T14:12:01.000+0000] {kubernetes_executor.py:421} WARNING - Node gke-prod-us-central1-spot-pool-9a8b received preemption notice from Google Cloud. Node drain initiated.
[2026-09-29T14:12:02.000+0000] {pod_manager.py:382} ERROR - Pod received SIGTERM (exit code 143). Reason: TerminatedByPreemption.
[2026-09-29T14:12:03.000+0000] {taskinstance.py:1898} ERROR - Task failed due to spot node reclaim.
[2026-09-29T14:12:03.000+0000] {taskinstance.py:1406} INFO - Marking task as UP_FOR_RETRY. Attempts remaining: 1"""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=batch_churn_scoring/task_id=score_user_features/execution_date=2026-09-29T14:10:00+00:00/attempt=1.log", spot_log)

    # 2. disk_full
    disk_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=parquet_large_export/task_id=dump_clickstream_parquet/attempt=1.log
[2026-09-29T15:15:00.000+0000] {taskinstance.py:1165} INFO - Starting export: parquet_large_export.dump_clickstream_parquet
[2026-09-29T15:15:04.000+0000] {duckdb_operator.py:42} INFO - Streaming 48,000,000 raw click events to /tmp/scratch/
[2026-09-29T15:19:58.000+0000] {duckdb_operator.py:65} INFO - Ingested 18GB Parquet buffer. Available disk: 1.2GB...
[2026-09-29T15:20:12.000+0000] {duckdb_operator.py:84} ERROR - OSError: [Errno 28] No space left on device: '/tmp/scratch/clickstream_chunk_48.parquet'
[2026-09-29T15:20:13.000+0000] {pod_manager.py:384} ERROR - Container evicted by kubelet: ephemeral-storage usage 21.4Gi exceeded limit 20Gi.
[2026-09-29T15:20:14.000+0000] {taskinstance.py:1898} ERROR - Task failed with exception: No space left on device."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=parquet_large_export/task_id=dump_clickstream_parquet/execution_date=2026-09-29T15:15:00+00:00/attempt=1.log", disk_log)

    # 3. statement_timeout
    stmt_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=customer_lifetime_value_calc/task_id=run_complex_clv_window/attempt=1.log
[2026-09-29T15:00:00.000+0000] {taskinstance.py:1165} INFO - Starting customer_lifetime_value_calc.run_complex_clv_window
[2026-09-29T15:00:04.000+0000] {postgres_operator.py:44} INFO - Executing query: WITH monthly_cohorts AS (SELECT user_id, date_trunc('month', order_ts) FROM events) SELECT * FROM monthly_cohorts;
[2026-09-29T16:00:04.000+0000] {postgres_operator.py:64} ERROR - psycopg2.errors.QueryCanceled: canceling statement due to statement timeout (3600000ms exceeded)
[2026-09-29T16:00:05.000+0000] {taskinstance.py:1898} ERROR - Task timed out after 3600 seconds. Database aborted query."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=customer_lifetime_value_calc/task_id=run_complex_clv_window/execution_date=2026-09-29T15:00:00+00:00/attempt=1.log", stmt_log)

    # 4. connection_pool
    pool_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=parallel_warehouse_sync/task_id=sync_branch_shards/attempt=1.log
[2026-09-29T17:15:00.000+0000] {taskinstance.py:1165} INFO - Starting parallel_warehouse_sync.sync_branch_shards (Concurrency: 64 tasks)
[2026-09-29T17:15:02.000+0000] {db_pool.py:44} ERROR - psycopg2.OperationalError: FATAL: remaining connection slots are reserved for non-replication superuser connections
DETAIL: Database connection pool exhausted (max_connections=200). 64 Airflow worker tasks connected simultaneously.
[2026-09-29T17:15:03.000+0000] {taskinstance.py:1898} ERROR - Could not acquire connection from pool."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=parallel_warehouse_sync/task_id=sync_branch_shards/execution_date=2026-09-29T17:15:00+00:00/attempt=1.log", pool_log)

    # 5. zero_rows
    zero_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=partner_catalog_ingest/task_id=validate_catalog_freshness/attempt=1.log
[2026-09-29T18:00:00.000+0000] {taskinstance.py:1165} INFO - Starting partner_catalog_ingest.validate_catalog_freshness
[2026-09-29T18:00:04.000+0000] {data_quality_operator.py:52} ERROR - QualityContractException: Table 'raw.partner_products' received 0 rows in daily ingest (Historical baseline: 420,000 +/- 15,000 rows).
ALERT: Silent partner API ingestion failure suspected. Downstream reporting marts at risk of empty data corruption!
[2026-09-29T18:00:05.000+0000] {taskinstance.py:1898} ERROR - Data quality contract violated: ZERO_ROWS_DELIVERED."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=partner_catalog_ingest/task_id=validate_catalog_freshness/execution_date=2026-09-29T18:00:00+00:00/attempt=1.log", zero_log)

    # 6. division_by_zero
    div_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=merchant_commission_calc/task_id=calc_effective_take_rate/attempt=1.log
[2026-09-29T19:30:00.000+0000] {taskinstance.py:1165} INFO - Starting merchant_commission_calc.calc_effective_take_rate
[2026-09-29T19:30:02.000+0000] {custom_sql_operator.py:82} ERROR - psycopg2.errors.DivisionByZero: division by zero
QUERY: SELECT merchant_id, net_payout / gross_transactions AS take_rate FROM financial.merchant_fee_ledger WHERE batch_dt = '2026-09-29';
CONTEXT: Encountered merchant_id='MCH_90112' with gross_transactions=0.
[2026-09-29T19:30:03.000+0000] {taskinstance.py:1898} ERROR - Transaction aborted due to division by zero."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=merchant_commission_calc/task_id=calc_effective_take_rate/execution_date=2026-09-29T19:30:00+00:00/attempt=1.log", div_log)

    # SQL file for division_by_zero
    write_lake_file("git-repos/financial-core-sql/queries/settlements/calc_effective_take_rate.sql", """-- queries/settlements/calc_effective_take_rate.sql
SELECT 
    merchant_id, 
    currency,
    net_payout / gross_transactions AS take_rate
FROM financial.merchant_fee_ledger
WHERE batch_dt = '2026-09-29';
""")

    # 7. clock_skew
    clock_log = """*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=s3_archive_sync/task_id=upload_raw_telemetry/attempt=1.log
[2026-09-29T20:10:00.000+0000] {taskinstance.py:1165} INFO - Starting s3_archive_sync.upload_raw_telemetry
[2026-09-29T20:10:02.000+0000] {s3_hook.py:102} ERROR - botocore.exceptions.ClientError: An error occurred (RequestTimeTooSkewed) when calling the PutObject operation: The difference between the request time and the current time is too large.
DETAIL: GKE node system clock drifted by 1024 seconds from AWS NTP pool. AWS Signature v4 rejected signature.
[2026-09-29T20:10:03.000+0000] {taskinstance.py:1898} ERROR - S3 API authentication failed due to node clock drift."""
    write_lake_file("airflow-logs/cluster=gke-prod-us-central1/dag_id=s3_archive_sync/task_id=upload_raw_telemetry/execution_date=2026-09-29T20:10:00+00:00/attempt=1.log", clock_log)

    print("7 new production failure scenarios added to s3_data_lake/ successfully!")

if __name__ == "__main__":
    add_all()
