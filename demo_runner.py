#!/usr/bin/env python3
"""
AirDoctor Live Hackathon Demo Runner & Incident Command Center.
Demonstrates enterprise-grade autonomous self-healing for multi-cluster Airflow:
- Multi-layer observability (Airflow + Cloud Logging + Grafana)
- Idempotency & Safety Guardrails evaluation
- Autonomous remediation (memory scaling, retry backoff, cross-cluster orchestration)
- Automated Git Diff & PR synthesis for schema drift
- Executive Slack Incident Reports
"""

import sys
import os

# Auto-reexec under project virtualenv if running in system Python without boto3/dependencies
VENV_PYTHON = os.path.abspath(os.path.join(os.path.dirname(__file__), "app", "airdoctor", ".venv", "bin", "python"))
if os.path.exists(VENV_PYTHON) and sys.executable != VENV_PYTHON:
    try:
        import boto3
        import mcp
    except ImportError:
        os.execv(VENV_PYTHON, [VENV_PYTHON] + sys.argv)

import json
import argparse
import asyncio
import time

# Setup python search paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, "app", "airdoctor"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "mcp_server"))

from core.guardrails import AirDoctorGuardrails, FailureCategory, IdempotencyLevel
from core.patcher import AirDoctorPatcher
from core.repo_resolver import AirDoctorRepoResolver
from core.slack_approval import AirDoctorSlackApproval
from mcp_server.mock_server import (
    get_airflow_task_details,
    fetch_airflow_task_logs,
    query_gcp_cloud_logging,
    query_grafana_metrics,
    read_dbt_artifacts,
    execute_airflow_remediation
)

repo_resolver = AirDoctorRepoResolver()

SCENARIOS = {
    "oom": {
        "title": "GKE KubernetesPodOperator OOMKilled (Exit Code 137)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "analytics_daily_etl",
        "task_id": "run_dbt_heavy_aggregation",
        "run_id": "manual__2026-09-29T10:00:00",
        "sla_tier": "TIER-1 (Finance Core Daily)",
        "operator": "KubernetesPodOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'analytics_daily_etl' task 'run_dbt_heavy_aggregation' run 'manual__2026-09-29T10:00:00'."
    },
    "deadlock": {
        "title": "Transient PostgreSQL Deadlock (Custom SQL Batch Framework)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "financial_ledger_sync",
        "task_id": "sync_merchant_settlements",
        "run_id": "manual__2026-09-29T10:30:00",
        "sla_tier": "TIER-2 (Merchant Payouts)",
        "operator": "CustomSQLBatchOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'financial_ledger_sync' task 'sync_merchant_settlements' run 'manual__2026-09-29T10:30:00'."
    },
    "schema_drift": {
        "title": "dbt Schema Drift: Missing Renamed Column in Mart",
        "cluster": "gke-analytics-europe-west1",
        "dag_id": "finance_daily_marts",
        "task_id": "dbt_build_finance",
        "run_id": "scheduled__2026-09-29T11:00:00",
        "sla_tier": "TIER-1 (Executive KPI Marts)",
        "operator": "DbtRunOperator",
        "prompt": "Airflow task failed on cluster 'gke-analytics-europe-west1' in DAG 'finance_daily_marts' task 'dbt_build_finance' run 'scheduled__2026-09-29T11:00:00'."
    },
    "cross_cluster": {
        "title": "Cross-Cluster Upstream Missing Partition / Sensor Timeout",
        "cluster": "gke-prod-us-central1",
        "dag_id": "global_sales_reporting",
        "task_id": "wait_for_eu_raw_events",
        "run_id": "scheduled__2026-09-29T12:00:00",
        "sla_tier": "TIER-2 (Global Aggregations)",
        "operator": "CustomGCSKeySensor",
        "prompt": "Airflow sensor timed out on cluster 'gke-prod-us-central1' in DAG 'global_sales_reporting' task 'wait_for_eu_raw_events' run 'scheduled__2026-09-29T12:00:00'."
    },
    "rate_limit": {
        "title": "External SaaS API Rate Limit Exceeded (HTTP 429)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "stripe_billing_reconciliation",
        "task_id": "fetch_invoices",
        "run_id": "scheduled__2026-09-29T08:00:00",
        "sla_tier": "TIER-2 (Billing Ingestion)",
        "operator": "StripeBillingIngestOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'stripe_billing_reconciliation' task 'fetch_invoices' run 'scheduled__2026-09-29T08:00:00'."
    },
    "data_quality": {
        "title": "dbt Test Failure: Unique Key Constraint Violation",
        "cluster": "gke-prod-us-central1",
        "dag_id": "marketing_multi_touch_attribution",
        "task_id": "dbt_test_attribution",
        "run_id": "scheduled__2026-09-29T07:30:00",
        "sla_tier": "TIER-2 (Marketing Attribution)",
        "operator": "DbtTestOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'marketing_multi_touch_attribution' task 'dbt_test_attribution' run 'scheduled__2026-09-29T07:30:00'."
    },
    "spot_eviction": {
        "title": "GKE Spot VM Preemption (Exit Code 143 / SIGTERM)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "batch_churn_scoring",
        "task_id": "score_user_features",
        "run_id": "scheduled__2026-09-29T14:10:00",
        "sla_tier": "TIER-2 (ML Feature Store)",
        "operator": "KubernetesPodOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'batch_churn_scoring' task 'score_user_features' run 'scheduled__2026-09-29T14:10:00'."
    },
    "disk_full": {
        "title": "GKE Worker Ephemeral Disk Full (No space left on device)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "parquet_large_export",
        "task_id": "dump_clickstream_parquet",
        "run_id": "scheduled__2026-09-29T15:15:00",
        "sla_tier": "TIER-2 (Clickstream Exports)",
        "operator": "DuckDbOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'parquet_large_export' task 'dump_clickstream_parquet' run 'scheduled__2026-09-29T15:15:00'."
    },
    "statement_timeout": {
        "title": "Analytical Warehouse Query Timeout (Exceeded 3600s)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "customer_lifetime_value_calc",
        "task_id": "run_complex_clv_window",
        "run_id": "scheduled__2026-09-29T15:00:00",
        "sla_tier": "TIER-1 (Executive CLV Marts)",
        "operator": "PostgresOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'customer_lifetime_value_calc' task 'run_complex_clv_window' run 'scheduled__2026-09-29T15:00:00'."
    },
    "connection_pool": {
        "title": "Database Connection Pool Exhaustion (max_connections=200)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "parallel_warehouse_sync",
        "task_id": "sync_branch_shards",
        "run_id": "scheduled__2026-09-29T17:15:00",
        "sla_tier": "TIER-1 (Core Ledger Sync)",
        "operator": "CustomSQLBatchOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'parallel_warehouse_sync' task 'sync_branch_shards' run 'scheduled__2026-09-29T17:15:00'."
    },
    "zero_rows": {
        "title": "Silent Ingestion Anomaly (0 Rows Delivered vs 420K Baseline)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "partner_catalog_ingest",
        "task_id": "validate_catalog_freshness",
        "run_id": "scheduled__2026-09-29T18:00:00",
        "sla_tier": "TIER-1 (Product Catalog)",
        "operator": "DataQualityContractOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'partner_catalog_ingest' task 'validate_catalog_freshness' run 'scheduled__2026-09-29T18:00:00'."
    },
    "division_by_zero": {
        "title": "Custom SQL Framework: Division by Zero in Fee Calculation",
        "cluster": "gke-prod-us-central1",
        "dag_id": "merchant_commission_calc",
        "task_id": "calc_effective_take_rate",
        "run_id": "scheduled__2026-09-29T19:30:00",
        "sla_tier": "TIER-1 (Settlement Calculations)",
        "operator": "CustomSQLBatchOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 'merchant_commission_calc' task 'calc_effective_take_rate' run 'scheduled__2026-09-29T19:30:00'."
    },
    "clock_skew": {
        "title": "AWS S3 SignatureDoesNotMatch (GKE Node NTP Clock Drift)",
        "cluster": "gke-prod-us-central1",
        "dag_id": "s3_archive_sync",
        "task_id": "upload_raw_telemetry",
        "run_id": "scheduled__2026-09-29T20:10:00",
        "sla_tier": "TIER-2 (Data Lake Archival)",
        "operator": "S3HookOperator",
        "prompt": "Airflow task failed on cluster 'gke-prod-us-central1' in DAG 's3_archive_sync' task 'upload_raw_telemetry' run 'scheduled__2026-09-29T20:10:00'."
    }
}

class UI:
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RESET = '\033[0m'

    @staticmethod
    def header(title):
        print(f"\n{UI.BOLD}{UI.CYAN}╔{'═' * 78}╗{UI.RESET}")
        print(f"{UI.BOLD}{UI.CYAN}║  🩺 AIRDOCTOR AUTONOMOUS SRE COMMAND CENTER{' ' * 33}║{UI.RESET}")
        print(f"{UI.BOLD}{UI.CYAN}╠{'═' * 78}╣{UI.RESET}")
        print(f"{UI.BOLD}{UI.CYAN}║  Incident: {title:<66}║{UI.RESET}")
        print(f"{UI.BOLD}{UI.CYAN}╚{'═' * 78}╝{UI.RESET}\n")

    @staticmethod
    def section(title, icon="🔍"):
        print(f"\n{UI.BOLD}{UI.MAGENTA}{icon}  {title.upper()}{UI.RESET}")
        print(f"{UI.DIM}{'─' * 80}{UI.RESET}")

    @staticmethod
    def log_box(lines):
        print(f"{UI.DIM}┌{'─' * 78}┐{UI.RESET}")
        for l in lines:
            print(f"{UI.DIM}│{UI.RESET} {UI.YELLOW}{l[:76]:<76}{UI.RESET} {UI.DIM}│{UI.RESET}")
        print(f"{UI.DIM}└{'─' * 78}┘{UI.RESET}")

    @staticmethod
    def diff_box(diff_text):
        print(f"{UI.DIM}┌{'─' * 78}┐{UI.RESET}")
        for line in diff_text.strip().split("\n"):
            if line.startswith("+"):
                print(f"{UI.DIM}│{UI.RESET} {UI.GREEN}{line[:76]:<76}{UI.RESET} {UI.DIM}│{UI.RESET}")
            elif line.startswith("-"):
                print(f"{UI.DIM}│{UI.RESET} {UI.RED}{line[:76]:<76}{UI.RESET} {UI.DIM}│{UI.RESET}")
            else:
                print(f"{UI.DIM}│{UI.RESET} {UI.DIM}{line[:76]:<76}{UI.RESET} {UI.DIM}│{UI.RESET}")
        print(f"{UI.DIM}└{'─' * 78}┘{UI.RESET}")


async def run_scenario(scenario_key: str):
    sc = SCENARIOS[scenario_key]
    guardrails = AirDoctorGuardrails()

    UI.header(sc["title"])

    # 1. INCIDENT TELEMETRY
    UI.section("1. Incident Interception (Airflow Webhook)", "🚨")
    print(f"  • Cluster:          {UI.BOLD}{sc['cluster']}{UI.RESET}")
    print(f"  • DAG:              {UI.BOLD}{sc['dag_id']}{UI.RESET}")
    print(f"  • Task:             {UI.BOLD}{sc['task_id']}{UI.RESET}")
    print(f"  • Run ID:           {UI.BOLD}{sc['run_id']}{UI.RESET}")
    print(f"  • SLA Priority:     {UI.BOLD}{UI.RED}{sc['sla_tier']}{UI.RESET}")
    await asyncio.sleep(0.5)

    # 2. MULTI-LAYER OBSERVABILITY
    UI.section("2. Cross-System Observability Correlation (Amazon S3 Data Lake)", "🔭")
    print(f"  ➜ {UI.BLUE}Airflow REST API:{UI.RESET} Interrogating worker task instance...")
    details = get_airflow_task_details(sc["cluster"], sc["dag_id"], sc["task_id"], sc["run_id"])
    print(f"     Operator: {UI.BOLD}{details.get('operator')}{UI.RESET} | Pod: {details.get('pod_name')} | Duration: {details.get('duration_seconds')}s")
    print(f"     {UI.CYAN}S3 Remote Log URI:{UI.RESET} {UI.BOLD}{details.get('s3_log_uri')}{UI.RESET}")

    print(f"\n  ➜ {UI.BLUE}Amazon S3 Data Lake:{UI.RESET} Streaming task execution log bytes from S3...")
    full_tail_logs = fetch_airflow_task_logs(sc["cluster"], sc["dag_id"], sc["task_id"], sc["run_id"], tail_lines=40)
    display_lines = [l.strip() for l in full_tail_logs.strip().split("\n") if l.strip()][-6:]
    UI.log_box(display_lines)

    print(f"\n  ➜ {UI.BLUE}Google Cloud Logging (S3 Sink):{UI.RESET} Querying exported GKE cgroup & kernel logs...")
    await asyncio.sleep(0.6)
    if scenario_key == "oom":
        gcp_res = json.loads(query_gcp_cloud_logging(sc["cluster"], details.get("pod_name", "")))
        crit_event = next((e for e in gcp_res if e.get("severity") == "CRITICAL"), gcp_res[-1])
        print(f"     {UI.RED}CRITICAL: {crit_event['jsonPayload']['message']}{UI.RESET}")
        print(f"  ➜ {UI.BLUE}Grafana Metrics:{UI.RESET} Container working set memory hit 2048MiB ceiling (100% saturation).")
    elif scenario_key == "deadlock":
        print(f"     {UI.YELLOW}Cloud Logging DB: Error 40P01 - deadlock detected on table 'merchant_balance_ledger'.{UI.RESET}")
        print(f"  ➜ {UI.BLUE}Grafana Metrics:{UI.RESET} Active locks spike subsided back to 0. Transient conflict confirmed.")
    elif scenario_key == "schema_drift":
        artifacts = read_dbt_artifacts()
        print(f"     {UI.YELLOW}dbt Run Results: Error 42703 in model 'fct_customer_churn_daily'.{UI.RESET}")
        print(f"     Lineage Error: Column 'customer_account_id' missing from upstream source 'stg_customers'.")
    elif scenario_key == "cross_cluster":
        print(f"     {UI.YELLOW}GKE EU Cluster Logs: Upstream DAG 'eu_raw_ingestion_hourly' failed (GCS credential refresh error).{UI.RESET}")
        print(f"  ➜ {UI.BLUE}Grafana Metrics:{UI.RESET} Partition lag = 7,200 seconds (Sensor Timeout).")

    # 3. GIT REPOSITORY & LINEAGE DISCOVERY
    UI.section("3. Git Repository & Lineage Discovery", "🔍")
    await asyncio.sleep(0.4)
    target_hint = "models/marts/finance/fct_customer_churn_daily.sql" if scenario_key == "schema_drift" else None
    repo_res = repo_resolver.resolve(dag_id=sc["dag_id"], task_id=sc["task_id"], failing_file_hint=target_hint)
    print(f"  • Target Git Repository:    {UI.BOLD}{UI.CYAN}{repo_res.repo_url}{UI.RESET} (branch: {repo_res.default_branch})")
    print(f"  • Target File Path:         {UI.BOLD}{repo_res.target_file_path}{UI.RESET}")
    print(f"  • Discovery Mechanism:      {UI.GREEN}{repo_res.discovery_method}{UI.RESET}")
    print(f"  • Code Owner & Channel:     {repo_res.team_owner} ({repo_res.slack_channel})")

    # 4. ENTERPRISE SAFETY GUARDRAILS EVALUATION
    UI.section("4. Enterprise Safety & Idempotency Guardrails", "🛡️")
    await asyncio.sleep(0.6)
    decision = guardrails.evaluate(
        task_metadata=details,
        log_text=full_tail_logs,
        query_or_code="dbt run" if "dbt" in sc["dag_id"] else "MERGE INTO"
    )

    print(f"  • Failure Classification:  {UI.BOLD}{decision.failure_category.value}{UI.RESET}")
    print(f"  • Idempotency Level:       {UI.BOLD}{UI.GREEN}{decision.idempotency_level.value}{UI.RESET}")
    print(f"  • Auto-Retry Authorization:{UI.BOLD} {UI.GREEN if decision.can_auto_remediate else UI.YELLOW}{'APPROVED' if decision.can_auto_remediate else 'REQUIRES_CODE_PATCH'}{UI.RESET}")
    for reason in decision.reasons:
        print(f"    ✔ {reason}")
    for violation in decision.violations:
        print(f"    ✖ {UI.RED}{violation}{UI.RESET}")

    # 5. REMEDIATION & HUMAN-IN-THE-LOOP APPROVAL
    UI.section("5. Autonomous Remediation Execution", "⚡")
    await asyncio.sleep(0.8)

    if scenario_key == "oom":
        # Interactive Slack Approval for Memory Scaling
        appr_req = AirDoctorSlackApproval.create_request(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            action_type="BUMP_POD_MEMORY_AND_CLEAR",
            proposed_change="Scale GKE worker pod limit from 2048Mi ➜ 4096Mi and restart task",
            risk_level="MEDIUM",
            root_cause="TRANSIENT_OOM (GKE Container 2048Mi Limit Exceeded)",
            channel=repo_res.slack_channel
        )
        AirDoctorSlackApproval.dispatch_to_slack(appr_req)
        AirDoctorSlackApproval.render_cli_card(appr_req, status="APPROVED", approver="@daniyar (On-Call SRE)")

        remediation = execute_airflow_remediation(
            action="BUMP_POD_MEMORY_AND_CLEAR",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            parameters={"memory_limit": "4096Mi"}
        )
        AirDoctorSlackApproval.dispatch_approved_recovery_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_OOM (GKE Container 2048Mi Limit Exceeded)",
            action_taken="Scaled pod memory limit to 4096MiB and cleared task in Airflow.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ {remediation['details']}{UI.RESET}")
        print(f"  {UI.GREEN}✔ Dynamic Pod Spec Override Applied: resources.limits.memory: 2048Mi ➜ 4096Mi{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow Task State Reset: [UP_FOR_RETRY ➜ RUNNING]{UI.RESET}")

    elif scenario_key == "deadlock":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            parameters={"backoff_seconds": 15}
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_CONCURRENCY (PostgreSQL 40P01 Deadlock)",
            action_taken=remediation["details"],
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ {remediation['details']}{UI.RESET}")
        print(f"  {UI.GREEN}✔ Jitter backoff elapsed. Airflow task instance cleared.{UI.RESET}")
        print(f"  {UI.GREEN}✔ Concurrency lock released. Pipeline running smoothly!{UI.RESET}")

    elif scenario_key == "schema_drift":
        artifacts = read_dbt_artifacts()
        patch = AirDoctorPatcher.patch_dbt_schema_drift(
            file_path=repo_res.target_file_path,
            original_sql=artifacts["failing_model_sql"],
            old_column="customer_account_id",
            new_column="account_id"
        )
        print(f"  {UI.BOLD}Synthesized Unified Git Diff for {repo_res.repo_url}:{UI.RESET}")
        UI.diff_box(patch.unified_diff)

        # Step A: Open GitHub Draft PR FIRST so CI/CD can run
        draft_pr_url = "https://github.com/company/analytics-dbt/pull/403"
        print(f"\n  {UI.GREEN}✔ GitHub Draft PR #403 Opened:{UI.RESET} {UI.BOLD}{draft_pr_url}{UI.RESET}")
        print(f"  {UI.GREEN}✔ Automated CI/CD Check:{UI.RESET} `dbt compile --select fct_customer_churn_daily` [PASSED in 1.4s]")

        # Step B: Interactive Slack Approval with Clickable Draft PR Link
        appr_req = AirDoctorSlackApproval.create_request(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            action_type="MERGE_PR_AND_RESUME_PIPELINE",
            proposed_change=f"Merge Draft PR #403 into 'main' and clear Airflow task to resume reporting.",
            code_diff=patch.unified_diff,
            risk_level="MEDIUM",
            root_cause="STRUCTURAL_SCHEMA_DRIFT (Missing column 'customer_account_id' in stg_customers)",
            pr_url=draft_pr_url,
            channel=repo_res.slack_channel
        )
        AirDoctorSlackApproval.dispatch_to_slack(appr_req)
        AirDoctorSlackApproval.render_cli_card(appr_req, status="APPROVED", approver="@daniyar (On-Call SRE)")

        # Step C: Upon Human Approval -> Auto-Merge PR & Clear Task in Airflow
        remediation = execute_airflow_remediation(
            action="CREATE_GITHUB_PR",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            parameters={"branch": patch.branch_name}
        )
        print(f"  {UI.GREEN}✔ GitHub PR #403 marked as ready for review and MERGED into 'main'.{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow REST API: Cleared task instance 'dbt_build_finance'. Pipeline resumed!{UI.RESET}")

        # Step D: Dispatch final Approved-Recovery confirmation notification to Slack
        AirDoctorSlackApproval.dispatch_approved_recovery_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="STRUCTURAL_SCHEMA_DRIFT (Fixed via PR #403)",
            action_taken=f"PR #403 merged into 'main'. Airflow task instance cleared and running.",
            pr_url=draft_pr_url,
            s3_log_uri=details.get("s3_log_uri")
        )

    elif scenario_key == "cross_cluster":
        remediation = execute_airflow_remediation(
            action="TRIGGER_UPSTREAM_DAG",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="DEPENDENCY_LAG (EU Ingestion Token Expiry)",
            action_taken=remediation["details"],
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ {remediation['details']}{UI.RESET}")
        print(f"  {UI.GREEN}✔ Downstream sensor reset to wait for refreshed EU partition.{UI.RESET}")

    elif scenario_key == "rate_limit":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            parameters={"backoff_seconds": 30}
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_RATE_LIMIT (Stripe API HTTP 429)",
            action_taken=remediation["details"],
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ {remediation['details']}{UI.RESET}")
        print(f"  {UI.GREEN}✔ S3 Rate Limit Telemetry verified: Retry-After 30s elapsed.{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow Task State Reset: Scheduled retry with exponential jitter backoff.{UI.RESET}")

    elif scenario_key == "data_quality":
        print(f"  {UI.YELLOW}⚠ Blind retry BLOCKED by Guardrails: duplicate rows will re-trigger failure.{UI.RESET}")
        print(f"  {UI.GREEN}✔ Dispatched Alert to Data Governance & Analytics Engineering Stewards.{UI.RESET}")
        print(f"  {UI.GREEN}✔ Generated Root Cause Query: SELECT user_touchpoint_pk, count(*) FROM marts.fct_omnichannel_attribution GROUP BY 1 HAVING count > 1;{UI.RESET}")

    elif scenario_key == "spot_eviction":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_SPOT_PREEMPTION (GKE Node TerminatedByPreemption)",
            action_taken="Injected node-affinity override (schedule on on-demand-pool) and cleared task in Airflow.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ Injected GKE node-affinity override: [nodeSelector: on-demand-pool]{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow REST API: Cleared task instance state. Rescheduled on reliable on-demand node!{UI.RESET}")

    elif scenario_key == "disk_full":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_DISK_FULL (Ephemeral storage 20Gi exceeded)",
            action_taken="Patched pod spec ephemeral-storage from 20Gi -> 50Gi and cleared task in Airflow.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ Dynamic Pod Spec Override Applied: resources.limits.ephemeral-storage: 20Gi ➜ 50Gi{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow Task State Reset: [UP_FOR_RETRY ➜ RUNNING] with 50Gi scratch disk.{UI.RESET}")

    elif scenario_key == "statement_timeout":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_STATEMENT_TIMEOUT (Analytical Query exceeded 3600s)",
            action_taken="Dynamically scaled statement_timeout from 1h -> 2h and cleared task in Airflow.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ Injected Session Parameter: SET statement_timeout = '7200000ms' (2h){UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow Task State Reset: Task retry initiated with extended query cutoff.{UI.RESET}")

    elif scenario_key == "connection_pool":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_CONNECTION_POOL (Database max_connections=200 saturated)",
            action_taken="Throttled Airflow DAG concurrency from 64 to 8 active tasks and queued retry.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ Throttled Airflow DAG Concurrency: [dag.max_active_tasks: 64 ➜ 8]{UI.RESET}")
        print(f"  {UI.GREEN}✔ Database Connection Pressure Relieved. Task instance queued for retry.{UI.RESET}")

    elif scenario_key == "zero_rows":
        print(f"  {UI.YELLOW}⚠ Downstream Pipeline PAUSED by Guardrails: Prevented overwriting dashboards with empty data!{UI.RESET}")
        print(f"  {UI.GREEN}✔ Dispatched Critical Alert to Partner Integration & SRE Teams.{UI.RESET}")

    elif scenario_key == "division_by_zero":
        raw_sql = "SELECT merchant_id, currency, net_payout / gross_transactions AS take_rate FROM financial.merchant_fee_ledger;"
        patch = AirDoctorPatcher.patch_custom_sql_query(
            file_path="queries/settlements/calc_effective_take_rate.sql",
            original_sql=raw_sql,
            error_type="division_by_zero"
        )
        print(f"  {UI.BOLD}Synthesized Custom SQL NULLIF Guard Diff:{UI.RESET}")
        UI.diff_box(patch.unified_diff)

        appr_req = AirDoctorSlackApproval.create_request(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            action_type="MERGE_PR_AND_RESUME_PIPELINE",
            proposed_change="Wrap divisor with NULLIF(gross_transactions, 0) to guard zero-division crashes.",
            code_diff=patch.unified_diff,
            risk_level="MEDIUM",
            root_cause="STRUCTURAL_DIVISION_BY_ZERO in calc_effective_take_rate.sql",
            pr_url="https://github.com/company/financial-core-sql/pull/112",
            channel=repo_res.slack_channel
        )
        AirDoctorSlackApproval.dispatch_to_slack(appr_req)
        AirDoctorSlackApproval.render_cli_card(appr_req, status="APPROVED", approver="@daniyar (On-Call SRE)")
        print(f"  {UI.GREEN}✔ GitHub PR #112 Created & Merged: Wrapped denominator in NULLIF(gross_transactions, 0).{UI.RESET}")
        print(f"  {UI.GREEN}✔ Airflow REST API: Cleared task instance 'calc_effective_take_rate'. Pipeline resumed!{UI.RESET}")

    elif scenario_key == "clock_skew":
        remediation = execute_airflow_remediation(
            action="CLEAR_TASK_INSTANCE",
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"]
        )
        AirDoctorSlackApproval.dispatch_auto_heal_notification(
            cluster=sc["cluster"],
            dag_id=sc["dag_id"],
            task_id=sc["task_id"],
            root_cause="TRANSIENT_CLOCK_SKEW (Node drifted >15m from AWS NTP)",
            action_taken="Rescheduled worker pod on NTP time-synchronized node pool and cleared task.",
            s3_log_uri=details.get("s3_log_uri")
        )
        print(f"  {UI.GREEN}✔ Node Clock Drift Verified: Rescheduled pod on time-synchronized GKE node.{UI.RESET}")
        print(f"  {UI.GREEN}✔ S3 Signature v4 Authentication Restored. Task instance running smoothly.{UI.RESET}")

    # 6. SLACK EXECUTIVE INCIDENT SUMMARY
    UI.section("6. Executive Slack Card (Ready to Share)", "💬")
    print(f"{UI.DIM}┌{'─' * 78}┐{UI.RESET}")
    print(f"{UI.DIM}│{UI.RESET} {UI.BOLD}#data-platform-alerts 🚨 [AirDoctor Incident Report]{UI.RESET}")
    print(f"{UI.DIM}│{UI.RESET} *Status:* {'✅ Auto-Resolved' if decision.can_auto_remediate else '🛠️ Human Action Required'}")
    print(f"{UI.DIM}│{UI.RESET} *Pipeline:* `{sc['dag_id']}.{sc['task_id']}` on `{sc['cluster']}`")
    print(f"{UI.DIM}│{UI.RESET} *Root Cause:* {decision.failure_category.value}")
    if scenario_key == "schema_drift":
        print(f"{UI.DIM}│{UI.RESET} *Action:* PR #403 opened to update `customer_account_id` -> `account_id`")
    elif scenario_key == "oom":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Auto-scaled pod memory limit to 4096MiB & cleared task")
    elif scenario_key == "deadlock":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Verified transient lock resolution & auto-retried after backoff")
    elif scenario_key == "cross_cluster":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Triggered upstream catchup run on EU cluster")
    elif scenario_key == "rate_limit":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Cooldown backoff applied (30s) & cleared task instance")
    elif scenario_key == "data_quality":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Blocked retry to prevent warehouse cost; alerted data stewards")
    elif scenario_key == "spot_eviction":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Auto-rescheduled on reliable On-Demand GKE node pool")
    elif scenario_key == "disk_full":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Dynamically scaled pod ephemeral storage limit to 50GiB")
    elif scenario_key == "statement_timeout":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Scaled query statement_timeout to 2 hours for catchup run")
    elif scenario_key == "connection_pool":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Throttled DAG active tasks from 64 to 8 to release DB connections")
    elif scenario_key == "zero_rows":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Paused downstream reporting DAG to protect production dashboards")
    elif scenario_key == "division_by_zero":
        print(f"{UI.DIM}│{UI.RESET} *Action:* PR #112 created wrapping divisor with NULLIF(gross_transactions, 0)")
    elif scenario_key == "clock_skew":
        print(f"{UI.DIM}│{UI.RESET} *Action:* Re-routed pod to NTP synchronized GKE worker node pool")
    print(f"{UI.DIM}└{'─' * 78}┘{UI.RESET}\n")


def main():
    parser = argparse.ArgumentParser(description="AirDoctor Autonomous SRE Command Center")
    parser.add_argument("--scenario", choices=[
        "oom", "deadlock", "schema_drift", "cross_cluster", "rate_limit", "data_quality",
        "spot_eviction", "disk_full", "statement_timeout", "connection_pool",
        "zero_rows", "division_by_zero", "clock_skew"
    ], default="oom", help="Incident scenario to run (default: oom)")
    parser.add_argument("--list", action="store_true", help="List all available incident scenarios")

    args = parser.parse_args()

    if args.list:
        print("\nAvailable Hackathon Demo Scenarios:")
        for k, v in SCENARIOS.items():
            print(f"  • {k:15}: {v['title']} [{v['sla_tier']}]")
        print()
        return

    asyncio.run(run_scenario(args.scenario))


if __name__ == "__main__":
    main()
