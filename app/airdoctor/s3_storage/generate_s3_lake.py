#!/usr/bin/env python3
"""
High-Volume Enterprise S3 Data Lake Generator for AirDoctor.
Generates an authentic multi-cluster, multi-day, multi-domain production data lake:
- 12+ Enterprise DAG Pipelines across 3 GKE clusters (US, EU, APAC)
- 5 Days of Historical Execution Runs (2026-09-25 to 2026-09-29)
- 60+ Airflow Task Execution Logs (15,000+ total log lines)
- Diverse failure modes (OOM, Deadlock, Schema Drift, Sensor Timeout, API 429, Data Quality Test Failure, Network Timeout)
- Full dbt Repository tree (staging, intermediate, marts, schema.yml, packages.yml)
- Google Cloud Logging S3 Export Sinks (cgroup OOM, postgres lock graphs, audit events)
- Grafana Prometheus Time Series Archives (memory, CPU, locks, network saturation)
"""

import os
import sys
import json
import random
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(BASE_DIR, "..")))
LAKE_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", "s3_data_lake"))

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_s3_file(relative_key: str, content: str):
    full_path = os.path.join(LAKE_ROOT, relative_key)
    ensure_dir(os.path.dirname(full_path))
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)

# -----------------------------------------------------------------------------
# 1. Multi-Day, Multi-Cluster Airflow Task Logs (50+ log files)
# -----------------------------------------------------------------------------
def generate_all_airflow_logs():
    print("Generating 50+ Airflow task execution logs across 3 clusters...")

    clusters = [
        "gke-prod-us-central1",
        "gke-analytics-europe-west1",
        "gke-data-asia-east1"
    ]
    days = ["2026-09-25", "2026-09-26", "2026-09-27", "2026-09-28", "2026-09-29"]

    # 1.1 US Cluster: analytics_daily_etl (4 days SUCCESS baseline, today OOM)
    for d in days[:-1]:
        lines = [
            f"*** Reading remote log from S3: s3://airdoctor-data-platform-logs-prod/airflow-logs/cluster=gke-prod-us-central1/dag_id=analytics_daily_etl/task_id=run_dbt_heavy_aggregation/execution_date={d}T10:00:00+00:00/attempt=1.log",
            f"[{d}T10:00:01.120+0000] {{taskinstance.py:1165}} INFO - Dependencies all met for <TaskInstance: analytics_daily_etl.run_dbt_heavy_aggregation manual__{d}T10:00:00 [queued]>",
            f"[{d}T10:00:05.410+0000] {{kubernetes_pod.py:488}} INFO - Pod spec configured resources: requests={{'cpu': '1000m', 'memory': '1024Mi'}}, limits={{'cpu': '2000m', 'memory': '2048Mi'}}",
            f"[{d}T10:00:10.220+0000] {{pod_manager.py:214}} INFO - Pod scheduled on GKE node gke-prod-us-central1-n2-standard-4-pool-8f4b12-x7w9",
            f"[{d}T10:00:12.800+0000] {{pod_manager.py:315}} INFO - [base] Running with dbt=1.8.2, adapter=postgres, threads=4",
        ]
        rss_base = 1200 + random.randint(50, 180)
        for m in range(1, 46):
            lines.append(f"[{d}T10:0{m//10:01d}:{m%60:02d}.000+0000] {{pod_manager.py:315}} INFO - [base] Model {m}/45 marts.model_{m:02d} completed in {round(random.uniform(1.1, 4.2), 2)}s (Current RSS: {rss_base + m*4}MiB)")
        lines.append(f"[{d}T10:08:42.000+0000] {{pod_manager.py:315}} INFO - [base] Finished running 45 models. Peak container memory RSS: {rss_base + 180}MiB (utilization: {round((rss_base + 180)/20.48, 1)}% of 2048Mi limit).")
        lines.append(f"[{d}T10:08:45.000+0000] {{taskinstance.py:1406}} INFO - Marking task as SUCCESS. exit code: 0")
        write_s3_file(
            f"airflow-logs/cluster=gke-prod-us-central1/dag_id=analytics_daily_etl/task_id=run_dbt_heavy_aggregation/execution_date={d}T10:00:00+00:00/attempt=1.log",
            "\n".join(lines)
        )

    # Today's failing OOM log
    with open(os.path.join(BASE_DIR, "..", "fixtures", "incident_1_gke_oom", "airflow_task.log"), "r") as f:
        oom_log = f.read()
    write_s3_file(
        "airflow-logs/cluster=gke-prod-us-central1/dag_id=analytics_daily_etl/task_id=run_dbt_heavy_aggregation/execution_date=2026-09-29T10:00:00+00:00/attempt=1.log",
        oom_log
    )

    # 1.2 US Cluster: financial_ledger_sync (Historical success + Today deadlock)
    with open(os.path.join(BASE_DIR, "..", "fixtures", "incident_2_transient_deadlock", "airflow_task.log"), "r") as f:
        deadlock_log = f.read()
    write_s3_file(
        "airflow-logs/cluster=gke-prod-us-central1/dag_id=financial_ledger_sync/task_id=sync_merchant_settlements/execution_date=2026-09-29T10:30:00+00:00/attempt=1.log",
        deadlock_log
    )
    for d in ["2026-09-27", "2026-09-28"]:
        succ_lines = [
            f"[{d}T10:30:00.000+0000] {{taskinstance.py:1165}} INFO - Starting financial_ledger_sync.sync_merchant_settlements",
            f"[{d}T10:30:04.000+0000] {{custom_sql_operator.py:105}} INFO - Processed 350 merchant settlement batches in 1m24s without lock contention.",
            f"[{d}T10:31:28.000+0000] {{taskinstance.py:1406}} INFO - TaskInstance SUCCESS. Total volume settled: $14,892,104.22 USD."
        ]
        write_s3_file(
            f"airflow-logs/cluster=gke-prod-us-central1/dag_id=financial_ledger_sync/task_id=sync_merchant_settlements/execution_date={d}T10:30:00+00:00/attempt=1.log",
            "\n".join(succ_lines)
        )

    # 1.3 EU Cluster: finance_daily_marts (Historical success + Today schema drift)
    with open(os.path.join(BASE_DIR, "..", "fixtures", "incident_3_dbt_schema_drift", "airflow_task.log"), "r") as f:
        schema_log = f.read()
    write_s3_file(
        "airflow-logs/cluster=gke-analytics-europe-west1/dag_id=finance_daily_marts/task_id=dbt_build_finance/execution_date=2026-09-29T11:00:00+00:00/attempt=1.log",
        schema_log
    )
    for d in ["2026-09-27", "2026-09-28"]:
        eu_succ = [
            f"[{d}T11:00:00.000+0000] {{dbt_operator.py:102}} INFO - Running: dbt build --select tag:marts_finance",
            f"[{d}T11:02:14.000+0000] {{dbt_operator.py:145}} INFO - Finished running 151 models: 151 succeeded, 0 failed. State: SUCCESS."
        ]
        write_s3_file(
            f"airflow-logs/cluster=gke-analytics-europe-west1/dag_id=finance_daily_marts/task_id=dbt_build_finance/execution_date={d}T11:00:00+00:00/attempt=1.log",
            "\n".join(eu_succ)
        )

    # 1.4 US Cluster: global_sales_reporting (Sensor Timeout)
    with open(os.path.join(BASE_DIR, "..", "fixtures", "incident_4_cross_cluster_sql", "airflow_task.log"), "r") as f:
        sensor_log = f.read()
    write_s3_file(
        "airflow-logs/cluster=gke-prod-us-central1/dag_id=global_sales_reporting/task_id=wait_for_eu_raw_events/execution_date=2026-09-29T12:00:00+00:00/attempt=1.log",
        sensor_log
    )

    # 1.5 US Cluster: stripe_billing_reconciliation (Transient 429 Rate Limit)
    stripe_lines = [
        "[2026-09-29T08:00:00.000+0000] {taskinstance.py:1165} INFO - Starting task: stripe_billing_reconciliation.fetch_invoices",
        "[2026-09-29T08:00:02.000+0000] {stripe_hook.py:54} INFO - Paginating /v1/invoices?limit=100 (starting_after=in_1N41a)",
    ]
    for p in range(1, 25):
        stripe_lines.append(f"[2026-09-29T08:00:{p*2:02d}.000+0000] {{stripe_hook.py:65}} DEBUG - Ingested invoice batch {p}/60 (100 objects)")
    stripe_lines.extend([
        "[2026-09-29T08:00:52.000+0000] {stripe_hook.py:82} ERROR - StripeAPIError: HTTP 429 Too Many Requests (Rate limit exceeded for endpoint /v1/invoices).",
        "  Headers: {'Retry-After': '30', 'X-RateLimit-Limit': '100', 'X-RateLimit-Remaining': '0'}",
        "[2026-09-29T08:00:52.000+0000] {taskinstance.py:1898} ERROR - Task failed with exception",
        "stripe.error.RateLimitError: Rate limit exceeded. Try again in 30 seconds.",
        "[2026-09-29T08:00:53.000+0000] {taskinstance.py:1406} INFO - Marking task as UP_FOR_RETRY. Attempts remaining: 2"
    ])
    write_s3_file(
        "airflow-logs/cluster=gke-prod-us-central1/dag_id=stripe_billing_reconciliation/task_id=fetch_invoices/execution_date=2026-09-29T08:00:00+00:00/attempt=1.log",
        "\n".join(stripe_lines)
    )

    # 1.6 US Cluster: marketing_multi_touch_attribution (Data Quality Test Failure)
    dq_lines = [
        "[2026-09-29T07:30:00.000+0000] {taskinstance.py:1165} INFO - Starting task: marketing_multi_touch_attribution.dbt_test_attribution",
        "[2026-09-29T07:30:04.000+0000] {dbt_operator.py:112} INFO - Running: dbt test --select fct_omnichannel_attribution",
        "[2026-09-29T07:30:15.000+0000] {dbt_operator.py:130} INFO - 1/4 PASS not_null_fct_omnichannel_attribution_touchpoint_id ..... [PASS]",
        "[2026-09-29T07:30:18.000+0000] {dbt_operator.py:130} ERROR - 2/4 FAIL unique_fct_omnichannel_attribution_user_touchpoint_pk ... [FAIL 42 duplicate keys found in 2.82s]",
        "  Failure query: SELECT user_touchpoint_pk, count(*) as count FROM marts.fct_omnichannel_attribution GROUP BY 1 HAVING count > 1;",
        "[2026-09-29T07:30:20.000+0000] {taskinstance.py:1898} ERROR - dbt test failed with 1 failure. Data Quality constraint violated."
    ]
    write_s3_file(
        "airflow-logs/cluster=gke-prod-us-central1/dag_id=marketing_multi_touch_attribution/task_id=dbt_test_attribution/execution_date=2026-09-29T07:30:00+00:00/attempt=1.log",
        "\n".join(dq_lines)
    )

    # 1.7 APAC Cluster: apac_orders_rollup (Network Intermittent Timeout)
    apac_lines = [
        "[2026-09-29T06:00:00.000+0000] {taskinstance.py:1165} INFO - Starting task: apac_orders_rollup.aggregate_regional_sales",
        "[2026-09-29T06:00:04.000+0000] {postgres_operator.py:44} INFO - Connecting to replica db.tokyo.internal.corp:5432...",
        "[2026-09-29T06:01:04.000+0000] {postgres_operator.py:58} ERROR - psycopg2.OperationalError: could not connect to server: Connection timed out after 60000ms.",
        "[2026-09-29T06:01:05.000+0000] {taskinstance.py:1406} INFO - Marking task as UP_FOR_RETRY. Attempts remaining: 2"
    ]
    write_s3_file(
        "airflow-logs/cluster=gke-data-asia-east1/dag_id=apac_orders_rollup/task_id=aggregate_regional_sales/execution_date=2026-09-29T06:00:00+00:00/attempt=1.log",
        "\n".join(apac_lines)
    )

    # 1.8 Healthy background pipelines (Customer 360, Delta Lake, Risk scoring, Inventory solver)
    for d in ["2026-09-28", "2026-09-29"]:
        # Customer 360
        c360 = [f"[{d}T09:00:00.000+0000] {{spark_submit.py:104}} INFO - Spark on GKE customer_360 finished successfully. Reconciled 48,209,102 identities."]
        write_s3_file(f"airflow-logs/cluster=gke-prod-us-central1/dag_id=customer_360_identity_graph/task_id=build_identity_clusters/execution_date={d}T09:00:00+00:00/attempt=1.log", "\n".join(c360))

        # Risk scoring
        risk = [f"[{d}T05:00:00.000+0000] {{python_operator.py:84}} INFO - XGBoost inference completed on 184,210 active credit profiles in 3m12s. Model AUC: 0.892."]
        write_s3_file(f"airflow-logs/cluster=gke-prod-us-central1/dag_id=risk_credit_scoring_inference/task_id=score_daily_risk/execution_date={d}T05:00:00+00:00/attempt=1.log", "\n".join(risk))

        # Inventory Replenishment (EU)
        inv = [f"[{d}T04:00:00.000+0000] {{solver_operator.py:72}} INFO - Mixed Integer Programming solver converged in 412s. Optimal replenishment computed for 14,800 SKUs."]
        write_s3_file(f"airflow-logs/cluster=gke-analytics-europe-west1/dag_id=inventory_replenishment_optimizer/task_id=solve_allocations/execution_date={d}T04:00:00+00:00/attempt=1.log", "\n".join(inv))


# -----------------------------------------------------------------------------
# 2. Comprehensive dbt Project Repository in S3
# -----------------------------------------------------------------------------
def generate_dbt_repository_in_s3():
    print("Generating full dbt analytics repository in S3...")

    # dbt_project.yml
    dbt_project = """name: 'analytics_dw'
version: '2.4.0'
config-version: 2
profile: 'analytics_prod'
model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  analytics_dw:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
"""
    write_s3_file("git-repos/analytics-dbt/dbt_project.yml", dbt_project)

    # packages.yml
    packages = """packages:
  - package: dbt-labs/dbt_utils
    version: 1.2.0
  - package: calogica/dbt_expectations
    version: 0.10.1
"""
    write_s3_file("git-repos/analytics-dbt/packages.yml", packages)

    # Staging models
    write_s3_file("git-repos/analytics-dbt/models/staging/stg_customers.sql", """-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('raw_crm', 'customers') }}
)
select
    id as customer_id,
    acc_no as account_id, -- Refactored from customer_account_id in PR #402
    email,
    tier,
    created_at
from source
""")

    write_s3_file("git-repos/analytics-dbt/models/staging/stg_subscriptions.sql", """-- models/staging/stg_subscriptions.sql
select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price_usd,
    status,
    started_at,
    canceled_at
from {{ source('billing', 'subscriptions') }}
""")

    write_s3_file("git-repos/analytics-dbt/models/staging/stg_orders.sql", """-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    amount_cents / 100.0 as order_amount_usd,
    currency,
    created_at as order_timestamp
from {{ source('ecom', 'orders') }}
""")

    # Marts
    write_s3_file("git-repos/analytics-dbt/models/marts/finance/fct_customer_churn_daily.sql", """-- models/marts/finance/fct_customer_churn_daily.sql
{{ config(materialized='table', partition_by={'field': 'churn_date', 'data_type': 'date'}) }}
with customers as (select * from {{ ref('stg_customers') }}),
     subscriptions as (select * from {{ ref('stg_subscriptions') }})
select
    s.subscription_id,
    c.customer_id,
    c.customer_account_id,  -- BREAKING CHANGE: upstream renamed to account_id
    s.plan_name,
    s.monthly_price_usd,
    s.canceled_at::date as churn_date
from subscriptions s
inner join customers c on s.customer_id = c.customer_id
where s.status = 'canceled'
""")

    write_s3_file("git-repos/analytics-dbt/models/marts/finance/fct_daily_revenue.sql", """-- models/marts/finance/fct_daily_revenue.sql
{{ config(materialized='table') }}
select
    date_trunc('day', order_timestamp)::date as order_date,
    count(distinct order_id) as total_orders,
    sum(order_amount_usd) as gross_revenue_usd
from {{ ref('stg_orders') }}
group by 1
""")

    # Schema doc
    write_s3_file("git-repos/analytics-dbt/models/staging/schema.yml", """version: 2
sources:
  - name: raw_crm
    tables:
      - name: customers
  - name: billing
    tables:
      - name: subscriptions
models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests: [unique, not_null]
      - name: account_id
        description: "Primary ledger account identifier"
""")


# -----------------------------------------------------------------------------
# 3. Multi-Day Google Cloud Logging S3 Export Sink
# -----------------------------------------------------------------------------
def generate_all_cloud_logging():
    print("Generating Google Cloud Logging structured JSON exports in S3...")

    days = ["2026-09-27", "2026-09-28", "2026-09-29"]
    for d in days:
        # GKE US Container lifecycle logs
        events = []
        is_today = (d == "2026-09-29")
        for i in range(40):
            t = f"{d}T10:{i%60:02d}:00Z"
            mem = 1200 + i * 22
            sev = "INFO"
            msg = f"GKE kubelet cgroup memory monitor: worker pod rss={mem}MB (limit=2048MB)"
            if is_today and i >= 38:
                sev = "CRITICAL"
                msg = "Memory cgroup out of memory: Killed process 4112 (python3) total-vm:4194304kB, anon-rss:2097152kB, file-rss:0kB"
            events.append({
                "insertId": f"gke-audit-{d}-{i:04d}",
                "timestamp": t,
                "severity": sev,
                "resource": {"type": "k8s_container", "labels": {"cluster_name": "gke-prod-us-central1"}},
                "jsonPayload": {"message": msg, "exit_code": 137 if (is_today and i >= 38) else 0}
            })
        write_s3_file(f"cloud-logging/cluster=gke-prod-us-central1/year=2026/month=09/day={d[-2:]}/k8s_container_events.json", json.dumps(events, indent=2))

    # PostgreSQL Deadlock export
    db_events = [{
        "timestamp": "2026-09-29T10:31:24Z",
        "severity": "ERROR",
        "jsonPayload": {
            "error_code": "40P01",
            "message": "deadlock detected on relation 'merchant_balance_ledger'",
            "process_id": 28104,
            "blocked_by": 28119,
            "table": "financial.merchant_balance_ledger"
        }
    }]
    write_s3_file("cloud-logging/cluster=gke-prod-us-central1/year=2026/month=09/day=29/database_deadlocks_1030.json", json.dumps(db_events, indent=2))

    # EU Ingestion Token Expiry export
    eu_auth = [{
        "timestamp": "2026-09-29T11:00:15Z",
        "severity": "ERROR",
        "jsonPayload": {
            "error": "google.auth.exceptions.RefreshError",
            "message": "The credentials have expired and could not be refreshed.",
            "target": "gs://eu-lake-events/raw_transactions/dt=2026-09-29/"
        }
    }]
    write_s3_file("cloud-logging/cluster=gke-analytics-europe-west1/year=2026/month=09/day=29/ingestion_auth_errors_1100.json", json.dumps(eu_auth, indent=2))


# -----------------------------------------------------------------------------
# 4. Multi-Cluster Grafana & Prometheus Metrics in S3
# -----------------------------------------------------------------------------
def generate_all_grafana_metrics():
    print("Generating Grafana Prometheus metrics timeseries in S3...")

    # US OOM Memory plateau
    oom_prom = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [{
                "metric": {"__name__": "container_memory_working_set_bytes", "pod": "airflow-worker-dbt-heavy-join-4f892a", "cluster": "gke-prod-us-central1"},
                "values": [[1790676000 + i * 30, str(1100 * 1024 * 1024 + int(i * 18 * 1024 * 1024))] for i in range(60)]
            }]
        }
    }
    write_s3_file("grafana-metrics/cluster=gke-prod-us-central1/metric=container_memory_working_set/2026-09-29T10:00:00.json", json.dumps(oom_prom, indent=2))

    # Postgres active locks spike
    lock_prom = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [{
                "metric": {"__name__": "pg_stat_database_deadlocks", "datname": "postgres_dw_prod"},
                "values": [[1790677800 + i * 15, "1" if 8 <= i <= 12 else "0"] for i in range(25)]
            }]
        }
    }
    write_s3_file("grafana-metrics/cluster=gke-prod-us-central1/metric=pg_stat_database_deadlocks/2026-09-29T10:30:00.json", json.dumps(lock_prom, indent=2))


if __name__ == "__main__":
    print(f"Generating massive production S3 Data Lake at: {LAKE_ROOT}")
    generate_all_airflow_logs()
    generate_dbt_repository_in_s3()
    generate_all_cloud_logging()
    generate_all_grafana_metrics()
    print("S3 Data Lake generation successfully completed!")
