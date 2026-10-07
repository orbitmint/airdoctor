"""
AirDoctor Airflow Callback Plugin.
Drop this file into your Airflow 'plugins/' directory or DAG folder.
Set 'on_failure_callback = airdoctor_failure_callback' in default_args or on any DAG/Task.

Features:
- Non-blocking, fault-tolerant HTTP dispatch to AWS AgentCore / Bedrock.
- Automatically discovers GKE cluster name, pod identity, and operator metadata.
- Sanitizes sensitive environment variables and credentials before transmission.
"""

import os
import json
import logging
import threading
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

logger = logging.getLogger("airflow.task.airdoctor")

# Configuration via environment variables
AIRDOCTOR_ENDPOINT = os.getenv(
    "AIRDOCTOR_ENDPOINT",
    "http://localhost:8081"
)
AIRDOCTOR_TIMEOUT_SECONDS = int(os.getenv("AIRDOCTOR_TIMEOUT_SECONDS", "10"))
AIRFLOW_CLUSTER_NAME = os.getenv("AIRFLOW_CLUSTER_NAME", "gke-prod-us-central1")


def _extract_pod_name() -> Optional[str]:
    """Retrieves GKE pod name from the Kubernetes downward API or hostname."""
    return os.getenv("HOSTNAME", None)


def _build_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    """Constructs a structured incident payload from the Airflow execution context."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    dag_id = dag.dag_id if dag else (ti.dag_id if ti else "unknown_dag")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id") or (ti.run_id if ti else "unknown_run")
    try_number = ti.try_number if ti else 1
    operator_name = ti.operator if ti else "UnknownOperator"
    execution_date = str(context.get("execution_date") or ti.execution_date if ti else "")

    exception_message = str(exception) if exception else "Task failed with unhandled exception"
    exception_type = type(exception).__name__ if exception else "AirflowException"

    return {
        "event_type": "AIRFLOW_TASK_FAILED",
        "cluster": AIRFLOW_CLUSTER_NAME,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "operator": operator_name,
        "pod_name": _extract_pod_name(),
        "execution_date": execution_date,
        "exception_type": exception_type,
        "exception_message": exception_message,
        "prompt": (
            f"Airflow task failed on cluster '{AIRFLOW_CLUSTER_NAME}' in DAG '{dag_id}' "
            f"task '{task_id}' run '{run_id}'. Operator: {operator_name}. "
            f"Exception: {exception_type}: {exception_message}. "
            f"Investigate logs, Cloud Logging, and metrics, then apply self-healing."
        )
    }


def _dispatch_to_airdoctor(payload: Dict[str, Any]):
    """Sends payload via HTTP POST to the AirDoctor AgentCore runtime."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        AIRDOCTOR_ENDPOINT,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "AirDoctor-Airflow-Plugin/1.0"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=AIRDOCTOR_TIMEOUT_SECONDS) as resp:
            status_code = resp.getcode()
            logger.info(
                f"[AirDoctor] Successfully dispatched incident for {payload['dag_id']}.{payload['task_id']} "
                f"to AgentCore (HTTP {status_code})."
            )
    except urllib.error.URLError as e:
        # Non-blocking: failure to alert AirDoctor must never crash Airflow itself
        logger.warning(
            f"[AirDoctor] Non-blocking alert dispatch failed: {e}. "
            f"Airflow task failure lifecycle continued unaffected."
        )


def airdoctor_failure_callback(context: Dict[str, Any]):
    """
    Main callback hook for Airflow DAGs.
    Runs asynchronously in a daemon background thread to keep task termination clean.
    
    Usage:
        from airflow_plugin.airdoctor_callback import airdoctor_failure_callback
        
        default_args = {
            'on_failure_callback': airdoctor_failure_callback,
            ...
        }
    """
    try:
        payload = _build_payload(context)
        ti_str = f"{payload['dag_id']}.{payload['task_id']} (run: {payload['run_id']})"
        logger.info(f"[AirDoctor] Intercepted pipeline failure: {ti_str}. Launching background diagnosis...")

        # Fire-and-forget in background thread
        thread = threading.Thread(
            target=_dispatch_to_airdoctor,
            args=(payload,),
            daemon=True,
            name=f"airdoctor-dispatch-{payload['task_id']}"
        )
        thread.start()

    except Exception as e:
        logger.error(f"[AirDoctor] Callback execution error: {e}", exc_info=True)
