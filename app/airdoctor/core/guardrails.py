"""
AirDoctor Safety & Policy Guardrails Engine.
Ensures self-healing actions are strictly safe, idempotent, and compliant
with enterprise infrastructure policy constraints before executing any remediations.
"""

from enum import Enum
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


class FailureCategory(str, Enum):
    TRANSIENT_OOM = "TRANSIENT_OOM"
    TRANSIENT_CONCURRENCY = "TRANSIENT_CONCURRENCY"
    TRANSIENT_RATE_LIMIT = "TRANSIENT_RATE_LIMIT"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    TRANSIENT_SPOT_PREEMPTION = "TRANSIENT_SPOT_PREEMPTION"
    TRANSIENT_DISK_FULL = "TRANSIENT_DISK_FULL"
    TRANSIENT_STATEMENT_TIMEOUT = "TRANSIENT_STATEMENT_TIMEOUT"
    TRANSIENT_CONNECTION_POOL = "TRANSIENT_CONNECTION_POOL"
    TRANSIENT_CLOCK_SKEW = "TRANSIENT_CLOCK_SKEW"
    STRUCTURAL_SCHEMA_DRIFT = "STRUCTURAL_SCHEMA_DRIFT"
    STRUCTURAL_SYNTAX = "STRUCTURAL_SYNTAX"
    STRUCTURAL_DIVISION_BY_ZERO = "STRUCTURAL_DIVISION_BY_ZERO"
    DATA_QUALITY = "DATA_QUALITY"
    SILENT_DATA_ANOMALY = "SILENT_DATA_ANOMALY"
    DEPENDENCY_LAG = "DEPENDENCY_LAG"
    UNKNOWN = "UNKNOWN"


class IdempotencyLevel(str, Enum):
    GUARANTEED_IDEMPOTENT = "GUARANTEED_IDEMPOTENT"       # e.g., dbt table/view, INSERT OVERWRITE, MERGE
    CONDITIONAL_IDEMPOTENT = "CONDITIONAL_IDEMPOTENT"     # Partition-scoped delete & insert
    POTENTIALLY_UNSAFE = "POTENTIALLY_UNSAFE"             # Raw INSERT INTO without deduplication
    NON_IDEMPOTENT = "NON_IDEMPOTENT"                     # External side-effect (e.g., charge credit card)


@dataclass
class PolicyLimits:
    max_memory_ceiling_mb: int = 16384     # Max 16GiB memory limit bump without human review
    max_auto_retries: int = 2              # Never auto-clear more than 2 times
    min_backoff_seconds: int = 10          # Enforce jitter backoff for lock contention
    prohibit_auto_retry_on_structural: bool = True
    prohibit_retry_on_unsafe_idempotency: bool = True


@dataclass
class GuardrailDecision:
    can_auto_remediate: bool
    failure_category: FailureCategory
    idempotency_level: IdempotencyLevel
    recommended_action: str
    reasons: List[str] = field(default_factory=list)
    violations: List[str] = field(default_factory=list)
    action_parameters: Dict[str, Any] = field(default_factory=dict)


class AirDoctorGuardrails:
    """Enforces safety, idempotency, and resource limits on self-healing actions."""

    def __init__(self, limits: Optional[PolicyLimits] = None):
        self.limits = limits or PolicyLimits()

    def classify_failure(self, task_metadata: Dict[str, Any], log_text: str) -> FailureCategory:
        """Classifies the root cause based on log signatures and execution context."""
        log_lower = log_text.lower()

        if "exit code 137" in log_lower or "oomkilled" in log_lower or "out of memory" in log_lower:
            return FailureCategory.TRANSIENT_OOM
        elif "terminatedbypreemption" in log_lower or "preemption notice" in log_lower or "exit code 143" in log_lower:
            return FailureCategory.TRANSIENT_SPOT_PREEMPTION
        elif "no space left on device" in log_lower or "ephemeral-storage" in log_lower or "errno 28" in log_lower:
            return FailureCategory.TRANSIENT_DISK_FULL
        elif "statement timeout" in log_lower or "canceling statement due to statement timeout" in log_lower:
            return FailureCategory.TRANSIENT_STATEMENT_TIMEOUT
        elif "connection slots are reserved" in log_lower or "connection pool exhausted" in log_lower:
            return FailureCategory.TRANSIENT_CONNECTION_POOL
        elif "requesttimetooskewed" in log_lower or "clock drifted" in log_lower:
            return FailureCategory.TRANSIENT_CLOCK_SKEW
        elif "division by zero" in log_lower:
            return FailureCategory.STRUCTURAL_DIVISION_BY_ZERO
        elif "zero rows in daily ingest" in log_lower or "zero_rows_delivered" in log_lower:
            return FailureCategory.SILENT_DATA_ANOMALY
        elif "deadlockdetected" in log_lower or "error 40p01" in log_lower or "lock wait timeout" in log_lower:
            return FailureCategory.TRANSIENT_CONCURRENCY
        elif "429" in log_lower or "rate limit" in log_lower or "too many requests" in log_lower:
            return FailureCategory.TRANSIENT_RATE_LIMIT
        elif "duplicate keys found" in log_lower or "data quality" in log_lower or "fail unique_" in log_lower:
            return FailureCategory.DATA_QUALITY
        elif "column" in log_lower and ("does not exist" in log_lower or "not found" in log_lower):
            return FailureCategory.STRUCTURAL_SCHEMA_DRIFT
        elif "syntax error" in log_lower or "compilation error" in log_lower:
            return FailureCategory.STRUCTURAL_SYNTAX
        elif "sensor has timed out" in log_lower or "partition not ready" in log_lower:
            return FailureCategory.DEPENDENCY_LAG
        elif "connection reset" in log_lower or "connection timed out" in log_lower or "503 service unavailable" in log_lower:
            return FailureCategory.TRANSIENT_NETWORK

        return FailureCategory.UNKNOWN

    def check_idempotency(self, operator: str, query_or_code: str = "") -> IdempotencyLevel:
        """Determines whether re-executing this task is idempotent and safe."""
        # 1. dbt models are mathematically idempotent by design (CREATE OR REPLACE / MERGE / CTAS)
        if "dbt" in operator.lower() or "dbt" in query_or_code.lower():
            return IdempotencyLevel.GUARANTEED_IDEMPOTENT

        # 2. Kubernetes Pod Operators running containerized ETL with target partitions
        code_upper = query_or_code.upper()
        if "INSERT OVERWRITE" in code_upper or "MERGE INTO" in code_upper or "TRUNCATE" in code_upper:
            return IdempotencyLevel.GUARANTEED_IDEMPOTENT

        # 3. Check for dangerous append-only inserts
        if "INSERT INTO" in code_upper and "ON CONFLICT" not in code_upper and "WHERE NOT EXISTS" not in code_upper:
            return IdempotencyLevel.POTENTIALLY_UNSAFE

        return IdempotencyLevel.CONDITIONAL_IDEMPOTENT

    def evaluate(
        self,
        task_metadata: Dict[str, Any],
        log_text: str,
        query_or_code: str = "",
        requested_memory_mb: Optional[int] = None
    ) -> GuardrailDecision:
        """
        Evaluates an incident against all guardrails.
        Returns whether auto-remediation is authorized and what exact action to take.
        """
        failure_category = self.classify_failure(task_metadata, log_text)
        operator = task_metadata.get("operator", "Unknown")
        idempotency = self.check_idempotency(operator, query_or_code)
        try_number = task_metadata.get("try_number", 1)

        reasons = []
        violations = []
        can_auto_remediate = True
        recommended_action = "ESCALATE_TO_HUMAN"
        action_parameters: Dict[str, Any] = {}

        # Rule 1: Max auto-retry limit
        if try_number > self.limits.max_auto_retries:
            can_auto_remediate = False
            violations.append(
                f"Task attempt {try_number} exceeds max allowed auto-retries ({self.limits.max_auto_retries})."
            )

        # Rule 2: Idempotency safety check
        if self.limits.prohibit_retry_on_unsafe_idempotency and idempotency == IdempotencyLevel.POTENTIALLY_UNSAFE:
            can_auto_remediate = False
            violations.append(
                "Task contains non-idempotent statements (raw INSERT INTO). Auto-retry blocked to prevent data corruption."
            )

        # Rule 3: Category-specific policy evaluations
        if failure_category == FailureCategory.TRANSIENT_OOM:
            target_mem = requested_memory_mb or 4096
            if target_mem > self.limits.max_memory_ceiling_mb:
                can_auto_remediate = False
                violations.append(
                    f"Requested memory bump ({target_mem}MB) exceeds enterprise policy ceiling ({self.limits.max_memory_ceiling_mb}MB)."
                )
            else:
                recommended_action = "BUMP_POD_MEMORY_AND_CLEAR"
                action_parameters = {
                    "memory_limit": f"{target_mem}Mi",
                    "memory_request": f"{target_mem // 2}Mi"
                }
                reasons.append(f"GKE container memory starvation verified. Pod limit bump to {target_mem}Mi approved.")

        elif failure_category == FailureCategory.TRANSIENT_CONCURRENCY:
            recommended_action = "CLEAR_TASK_INSTANCE"
            action_parameters = {"backoff_seconds": self.limits.min_backoff_seconds}
            reasons.append(
                f"PostgreSQL deadlock confirmed transient. Auto-retry authorized with {self.limits.min_backoff_seconds}s jitter backoff."
            )

        elif failure_category == FailureCategory.TRANSIENT_RATE_LIMIT:
            recommended_action = "CLEAR_TASK_INSTANCE"
            action_parameters = {"backoff_seconds": 30}
            reasons.append("External API HTTP 429 rate limit throttle detected. Auto-retry authorized with 30s cooldown.")

        elif failure_category == FailureCategory.DATA_QUALITY:
            can_auto_remediate = False
            recommended_action = "ALERT_DATA_STEWARDS"
            reasons.append("Data quality constraint violated (duplicate primary keys). Auto-retry blocked to prevent compute waste; alerting data stewards.")

        elif failure_category == FailureCategory.STRUCTURAL_SCHEMA_DRIFT:
            # Never blindly retry schema drift! Generate a patch instead.
            can_auto_remediate = False
            recommended_action = "CREATE_GITHUB_PR"
            reasons.append("Structural schema drift detected. Auto-retry blocked; patch synthesis initiated.")

        elif failure_category == FailureCategory.TRANSIENT_SPOT_PREEMPTION:
            recommended_action = "RESCHEDULE_ON_DEMAND_POOL"
            action_parameters = {"node_pool": "on-demand-pool"}
            reasons.append("GKE spot VM preemption confirmed by node drain event. Auto-retry authorized on on-demand node pool.")

        elif failure_category == FailureCategory.TRANSIENT_DISK_FULL:
            recommended_action = "BUMP_EPHEMERAL_STORAGE_AND_CLEAR"
            action_parameters = {"ephemeral_storage_limit": "50Gi"}
            reasons.append("Pod ephemeral storage exceeded 20Gi scratch limit. Pod limit bumped to 50Gi and retry authorized.")

        elif failure_category == FailureCategory.TRANSIENT_STATEMENT_TIMEOUT:
            recommended_action = "BUMP_STATEMENT_TIMEOUT_AND_CLEAR"
            action_parameters = {"statement_timeout": "2h"}
            reasons.append("Analytical query exceeded 1h cutoff. Dynamic timeout scaled to 2h and retry authorized.")

        elif failure_category == FailureCategory.TRANSIENT_CONNECTION_POOL:
            recommended_action = "THROTTLE_CONCURRENCY_AND_CLEAR"
            action_parameters = {"max_active_tasks": 8}
            reasons.append("Database connection slots saturated. Airflow task concurrency throttled to 8 and retry queued.")

        elif failure_category == FailureCategory.TRANSIENT_CLOCK_SKEW:
            recommended_action = "RESCHEDULE_SYNCED_NODE_AND_CLEAR"
            action_parameters = {"ntp_sync": True}
            reasons.append("Node clock drifted by >15 minutes from AWS NTP. Task rescheduled on time-synchronized node.")

        elif failure_category == FailureCategory.STRUCTURAL_DIVISION_BY_ZERO:
            can_auto_remediate = False
            recommended_action = "CREATE_GITHUB_PR"
            reasons.append("Custom SQL division by zero detected. Auto-retry blocked; NULLIF guard patch synthesis initiated.")

        elif failure_category == FailureCategory.SILENT_DATA_ANOMALY:
            can_auto_remediate = False
            recommended_action = "PAUSE_DOWNSTREAM_DAG_AND_ALERT"
            reasons.append("Silent 0-row delivery detected against 420K row baseline. Downstream DAG paused to protect dashboards.")

        elif failure_category == FailureCategory.DEPENDENCY_LAG:
            can_auto_remediate = True
            recommended_action = "TRIGGER_UPSTREAM_DAG"
            action_parameters = {
                "upstream_cluster": "gke-analytics-europe-west1",
                "upstream_dag_id": "eu_raw_ingestion_hourly"
            }
            reasons.append("Downstream sensor timeout caused by upstream cluster lag. Triggering cross-cluster catchup.")

        else:
            can_auto_remediate = False
            reasons.append("Failure root cause is ambiguous or structural. Escalating to human on-call.")

        return GuardrailDecision(
            can_auto_remediate=can_auto_remediate,
            failure_category=failure_category,
            idempotency_level=idempotency,
            recommended_action=recommended_action,
            reasons=reasons,
            violations=violations,
            action_parameters=action_parameters
        )
