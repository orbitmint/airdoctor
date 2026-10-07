#!/usr/bin/env python3
"""
AirDoctor MCP Server.
Provides MCP tools for Airflow multi-cluster management, S3 Data Lake access,
GKE Cloud Logging, Grafana metrics, dbt artifacts, and self-healing remediations.
"""

import os
import sys
import json
from typing import Dict, Any, Optional, List
from mcp.server.fastmcp import FastMCP

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, PARENT_DIR)

from s3_storage.client import S3LakeClient
from core.repo_resolver import AirDoctorRepoResolver
from core.slack_approval import AirDoctorSlackApproval
from core.patcher import AirDoctorPatcher

FIXTURES_DIR = os.path.join(PARENT_DIR, "fixtures")
s3_lake = S3LakeClient()
repo_resolver = AirDoctorRepoResolver()

# Create FastMCP server
mcp = FastMCP(
    name="AirDoctorMCP",
    instructions="Enterprise SRE tools for investigating Airflow failures in S3 data lake, querying GKE Cloud Logging & Grafana, reading dbt models, and executing self-healing remediations.",
    host="0.0.0.0",
    port=8081,
    streamable_http_path="/"
)

def _find_scenario_dir(cluster: str, dag_id: str) -> str:
    """Helper to locate the fixture scenario directory based on dag_id and cluster."""
    for folder in os.listdir(FIXTURES_DIR):
        meta_path = os.path.join(FIXTURES_DIR, folder, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
                if meta.get("dag_id") == dag_id:
                    return os.path.join(FIXTURES_DIR, folder)

    for folder in os.listdir(FIXTURES_DIR):
        meta_path = os.path.join(FIXTURES_DIR, folder, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
                if meta.get("cluster") == cluster:
                    return os.path.join(FIXTURES_DIR, folder)

    return os.path.join(FIXTURES_DIR, "incident_1_gke_oom")


# -----------------------------------------------------------------------------
# Airflow REST API Tools
# -----------------------------------------------------------------------------
@mcp.tool()
def get_airflow_task_details(cluster: str, dag_id: str, task_id: str, run_id: str) -> Dict[str, Any]:
    """
    Fetch task instance execution metadata from the Airflow REST API on the target cluster.
    Returns task state, try number, operator type, GKE pod name, and remote S3 log URI.
    """
    scenario_dir = _find_scenario_dir(cluster, dag_id)
    meta_path = os.path.join(scenario_dir, "metadata.json")
    operator = "KubernetesPodOperator"
    pod_name = f"airflow-worker-{task_id}-pod"

    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            meta = json.load(f)
            operator = meta.get("operator", operator)
            pod_name = meta.get("pod_name", pod_name)

    # Canonical S3 remote log path
    prefix = f"airflow-logs/cluster={cluster}/dag_id={dag_id}/task_id={task_id}/"
    matching = s3_lake.list_objects(prefix=prefix, max_keys=5)
    if matching:
        matching.sort(reverse=True)
        s3_log_uri = matching[0]
    else:
        s3_log_uri = f"s3://{s3_lake.bucket}/{prefix}execution_date=2026-09-29T10:00:00+00:00/attempt=1.log"

    return {
        "cluster": cluster,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "state": "failed",
        "try_number": 1,
        "max_tries": 2,
        "operator": operator,
        "pod_name": pod_name,
        "s3_log_uri": s3_log_uri,
        "start_date": "2026-09-29T10:00:00+00:00",
        "end_date": "2026-09-29T10:14:02+00:00",
        "duration_seconds": 842.1
    }


# -----------------------------------------------------------------------------
# S3 Data Lake Retrieval Tools
# -----------------------------------------------------------------------------
@mcp.tool()
def s3_fetch_airflow_log(
    cluster: str,
    dag_id: str,
    task_id: str,
    execution_date: Optional[str] = None,
    attempt: int = 1,
    tail_lines: int = 150
) -> str:
    """
    Fetches Airflow remote task execution logs directly from the S3 Data Lake.
    Resolves key: airflow-logs/cluster=<cluster>/dag_id=<dag_id>/task_id=<task_id>/...
    """
    if execution_date:
        s3_key = f"airflow-logs/cluster={cluster}/dag_id={dag_id}/task_id={task_id}/execution_date={execution_date}/attempt={attempt}.log"
        text = s3_lake.get_object_text(f"s3://{s3_lake.bucket}/{s3_key}", max_lines=tail_lines)
        if not text.startswith("S3 Object not found"):
            return text

    # Search prefix for any matching logs
    prefix = f"airflow-logs/cluster={cluster}/dag_id={dag_id}/task_id={task_id}/"
    matching = s3_lake.list_objects(prefix=prefix, max_keys=10)
    if matching:
        # Sort descending to pick latest
        matching.sort(reverse=True)
        return s3_lake.get_object_text(matching[0], max_lines=tail_lines)

    return f"Log not found in S3 for task {dag_id}.{task_id} on cluster {cluster}"


@mcp.tool()
def s3_list_logs_in_prefix(prefix: str = "airflow-logs/", max_keys: int = 25) -> List[str]:
    """
    Lists log files and telemetry objects available in the S3 Data Lake under a given prefix.
    """
    return s3_lake.list_objects(prefix=prefix, max_keys=max_keys)


@mcp.tool()
def s3_query_cloud_logging_sink(cluster: str, date_str: str = "2026-09-29") -> str:
    """
    Reads structured GCP Cloud Logging JSON exports from the S3 sink:
    s3://{bucket}/cloud-logging/cluster=<cluster>/year=.../month=.../day=.../
    """
    prefix = f"cloud-logging/cluster={cluster}/"
    matching = s3_lake.list_objects(prefix)
    if matching:
        return s3_lake.get_object_text(matching[0])
    return "[]"


@mcp.tool()
def s3_query_grafana_metrics(cluster: str, metric_name: str = "container_memory_working_set") -> str:
    """
    Reads Prometheus / Grafana metrics timeseries archived in S3:
    s3://{bucket}/grafana-metrics/cluster=<cluster>/metric=<metric_name>/
    """
    prefix = f"grafana-metrics/cluster={cluster}/"
    matching = s3_lake.list_objects(prefix)
    for uri in matching:
        if metric_name in uri:
            return s3_lake.get_object_text(uri)
    if matching:
        return s3_lake.get_object_text(matching[0])
    return "{}"


@mcp.tool()
def s3_get_dbt_artifact(project: str = "finance", run_id: str = "scheduled__2026-09-29T11:00:00", artifact_type: str = "run_results") -> Dict[str, Any]:
    """
    Reads compiled dbt artifacts (run_results.json, manifest.json) from the S3 data lake:
    s3://{bucket}/dbt-artifacts/project=<project>/run_id=<run_id>/<artifact_type>.json
    """
    s3_key = f"dbt-artifacts/project={project}/run_id={run_id}/{artifact_type}.json"
    s3_uri = f"s3://{s3_lake.bucket}/{s3_key}"
    return s3_lake.get_object_json(s3_uri)


@mcp.tool()
def fetch_airflow_task_logs(cluster: str, dag_id: str, task_id: str, run_id: str, tail_lines: int = 150) -> str:
    """
    Backward-compatible tool: fetches Airflow task log via S3 data lake.
    """
    return s3_fetch_airflow_log(cluster=cluster, dag_id=dag_id, task_id=task_id, tail_lines=tail_lines)


@mcp.tool()
def query_gcp_cloud_logging(cluster: str, pod_name: str, query_filter: str = "") -> str:
    """
    Backward-compatible tool: queries Cloud Logging S3 sink.
    """
    return s3_query_cloud_logging_sink(cluster=cluster)


@mcp.tool()
def query_grafana_metrics(cluster: str, metric_name: str = "container_memory_working_set_bytes") -> str:
    """
    Backward-compatible tool: queries Grafana metrics S3 archive.
    """
    return s3_query_grafana_metrics(cluster=cluster, metric_name=metric_name)


@mcp.tool()
def read_dbt_artifacts(model_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Backward-compatible tool: reads dbt run results and model code from S3 lake.
    """
    res = {}
    res["run_results"] = s3_get_dbt_artifact(project="finance", artifact_type="run_results")

    model_sql_uri = f"s3://{s3_lake.bucket}/git-repos/analytics-dbt/models/marts/finance/fct_customer_churn_daily.sql"
    res["failing_model_sql"] = s3_lake.get_object_text(model_sql_uri)

    staging_sql_uri = f"s3://{s3_lake.bucket}/git-repos/analytics-dbt/models/staging/stg_customers.sql"
    res["upstream_staging_sql"] = s3_lake.get_object_text(staging_sql_uri)
    return res


@mcp.tool()
def resolve_task_git_repository(cluster: str, dag_id: str, task_id: str, failing_file_hint: Optional[str] = None) -> Dict[str, Any]:
    """
    Discovers the exact Git repository, branch, and target file path for an Airflow DAG/task.
    Uses GKE pod annotations, dbt manifest lineage in S3, DAG tags, and the Service Catalog.
    """
    resolved = repo_resolver.resolve(dag_id=dag_id, task_id=task_id, failing_file_hint=failing_file_hint)
    return {
        "repo_name": resolved.repo_name,
        "repo_url": resolved.repo_url,
        "default_branch": resolved.default_branch,
        "target_file_path": resolved.target_file_path,
        "discovery_method": resolved.discovery_method,
        "team_owner": resolved.team_owner,
        "slack_channel": resolved.slack_channel
    }


@mcp.tool()
def send_slack_approval_request(
    cluster: str,
    dag_id: str,
    task_id: str,
    action_type: str,
    proposed_change: str,
    code_diff: Optional[str] = None,
    risk_level: str = "MEDIUM"
) -> Dict[str, Any]:
    """
    Sends an interactive Slack approval card with [Approve] / [Reject] buttons
    for Human-in-the-Loop authorization before executing high-impact self-healing actions.
    """
    req = AirDoctorSlackApproval.create_request(
        cluster=cluster,
        dag_id=dag_id,
        task_id=task_id,
        action_type=action_type,
        proposed_change=proposed_change,
        code_diff=code_diff,
        risk_level=risk_level
    )
    blocks = AirDoctorSlackApproval.format_slack_blocks(req)
    return {
        "status": "APPROVAL_REQUEST_SENT",
        "approval_id": req.approval_id,
        "channel": req.channel,
        "risk_level": req.risk_level,
        "slack_blocks_payload": blocks,
        "instruction": "Waiting for on-call engineer reaction in Slack..."
    }


@mcp.tool()
def synthesize_code_patch(
    framework: str,
    file_path: str,
    original_code: str,
    error_type: str = "schema_drift",
    parameters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Synthesizes code patches and unified git diffs across dbt models, custom SQL queries,
    and Airflow DAG resource specifications.
    Supported frameworks: 'dbt', 'custom-sql', 'airflow-dag'.
    """
    parameters = parameters or {}
    if framework == "dbt":
        res = AirDoctorPatcher.patch_dbt_schema_drift(
            file_path=file_path,
            original_sql=original_code,
            old_column=parameters.get("old_column", "customer_account_id"),
            new_column=parameters.get("new_column", "account_id")
        )
    elif framework == "custom-sql":
        res = AirDoctorPatcher.patch_custom_sql_query(
            file_path=file_path,
            original_sql=original_code,
            error_type=error_type
        )
    elif framework == "airflow-dag":
        res = AirDoctorPatcher.patch_dag_resource_spec(
            file_path=file_path,
            original_py=original_code,
            task_id=parameters.get("task_id", "run_dbt_heavy_aggregation"),
            new_memory=parameters.get("new_memory", "4096Mi")
        )
    else:
        return {"status": "FAILED", "error": f"Unsupported framework: {framework}"}

    return {
        "status": "SUCCESS",
        "framework": res.framework,
        "file_path": res.original_file_path,
        "branch_name": res.branch_name,
        "pr_title": res.pr_title,
        "pr_body": res.pr_body,
        "unified_diff": res.unified_diff
    }


# -----------------------------------------------------------------------------
# Remediation Execution
# -----------------------------------------------------------------------------
@mcp.tool()
def execute_airflow_remediation(
    action: str,
    cluster: str,
    dag_id: str,
    task_id: str,
    parameters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute an automated self-healing remediation action on the Airflow cluster.
    Supported actions:
      - 'CLEAR_TASK_INSTANCE': Clears task state to trigger an immediate safe retry.
      - 'BUMP_POD_MEMORY_AND_CLEAR': Patches KubernetesPodOperator memory limits and triggers retry.
      - 'TRIGGER_UPSTREAM_DAG': Triggers catch-up on an upstream cross-cluster dependency.
      - 'CREATE_GITHUB_PR': Creates an automated PR fixing broken SQL / dbt schema drift.
    """
    parameters = parameters or {}
    if action == "BUMP_POD_MEMORY_AND_CLEAR":
        new_limit = parameters.get("memory_limit", "4096Mi")
        return {
            "status": "SUCCESS",
            "action_taken": "BUMP_POD_MEMORY_AND_CLEAR",
            "cluster": cluster,
            "dag_id": dag_id,
            "task_id": task_id,
            "details": f"Successfully patched KubernetesPodOperator resources (limits.memory={new_limit}) and cleared task instance in Airflow. Worker restarted.",
            "retry_scheduled": True
        }
    elif action == "CLEAR_TASK_INSTANCE":
        backoff = parameters.get("backoff_seconds", 10)
        return {
            "status": "SUCCESS",
            "action_taken": "CLEAR_TASK_INSTANCE",
            "cluster": cluster,
            "dag_id": dag_id,
            "task_id": task_id,
            "details": f"Successfully cleared task instance after {backoff}s backoff. Transient lock conflict resolved.",
            "retry_scheduled": True
        }
    elif action == "TRIGGER_UPSTREAM_DAG":
        upstream_dag = parameters.get("upstream_dag_id", "eu_raw_ingestion_hourly")
        upstream_cluster = parameters.get("upstream_cluster", "gke-analytics-europe-west1")
        return {
            "status": "SUCCESS",
            "action_taken": "TRIGGER_UPSTREAM_DAG",
            "cluster": upstream_cluster,
            "dag_id": upstream_dag,
            "details": f"Triggered rerun on upstream cluster {upstream_cluster} for DAG {upstream_dag}. Reset downstream partition sensor.",
            "retry_scheduled": True
        }
    elif action == "CREATE_GITHUB_PR":
        branch = parameters.get("branch", "fix/dbt-schema-drift-account-id")
        return {
            "status": "SUCCESS",
            "action_taken": "CREATE_GITHUB_PR",
            "pr_url": f"https://github.com/deliveryhero/analytics-dbt/pull/403",
            "branch": branch,
            "summary": "Automated PR created updating column customer_account_id to account_id in fct_customer_churn_daily.sql",
            "slack_alert_sent": True
        }
    return {
        "status": "FAILED",
        "error": f"Unknown action: {action}"
    }


if __name__ == "__main__":
    print(f"Starting AirDoctor MCP server (S3 Data Lake: {s3_lake.bucket}) on http://0.0.0.0:8081 ...")
    mcp.run(transport="streamable-http")
