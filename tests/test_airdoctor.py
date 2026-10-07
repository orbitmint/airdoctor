"""
Unit and Integration Tests for AirDoctor Core SRE Framework.
Tests failure classification, idempotency gates, automated code patching,
multi-cloud model loading, and Airflow 3 client logic.
"""

import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.insert(0, ROOT_DIR)
sys.path.insert(0, os.path.join(ROOT_DIR, "app", "airdoctor"))

from core.guardrails import (
    AirDoctorGuardrails,
    FailureCategory,
    IdempotencyLevel,
    PolicyLimits
)
from core.patcher import AirDoctorPatcher
from core.repo_resolver import AirDoctorRepoResolver
from model.load import load_model


class TestGuardrailsEngine(unittest.TestCase):
    """Tests the failure classifier and idempotency safety gates."""

    def setUp(self):
        self.guardrails = AirDoctorGuardrails()

    def test_classify_oom_failure(self):
        meta = {"operator": "KubernetesPodOperator"}
        log_text = "Task failed with return code 137. Container was OOMKilled by Linux cgroup."
        category = self.guardrails.classify_failure(meta, log_text)
        self.assertEqual(category, FailureCategory.TRANSIENT_OOM)

    def test_classify_deadlock_concurrency(self):
        meta = {"operator": "CustomSQLBatchOperator"}
        log_text = "psycopg2.errors.DeadlockDetected: deadlock detected (ERROR: 40P01)"
        category = self.guardrails.classify_failure(meta, log_text)
        self.assertEqual(category, FailureCategory.TRANSIENT_CONCURRENCY)

    def test_classify_schema_drift(self):
        meta = {"operator": "DbtRunOperator"}
        log_text = "Database error: column 'customer_account_id' does not exist in relation stg_customers"
        category = self.guardrails.classify_failure(meta, log_text)
        self.assertEqual(category, FailureCategory.STRUCTURAL_SCHEMA_DRIFT)

    def test_classify_rate_limit(self):
        meta = {"operator": "StripeBillingIngestOperator"}
        log_text = "HTTP 429 Too Many Requests: Rate limit exceeded. Retry-After: 30"
        category = self.guardrails.classify_failure(meta, log_text)
        self.assertEqual(category, FailureCategory.TRANSIENT_RATE_LIMIT)

    def test_classify_division_by_zero(self):
        meta = {"operator": "PostgresOperator"}
        log_text = "ERROR: division by zero in statement: SELECT net_payout / gross_transactions"
        category = self.guardrails.classify_failure(meta, log_text)
        self.assertEqual(category, FailureCategory.STRUCTURAL_DIVISION_BY_ZERO)

    def test_idempotency_verification(self):
        # Guaranteed idempotent queries
        self.assertEqual(
            self.guardrails.check_idempotency("KubernetesPodOperator", "MERGE INTO target USING source"),
            IdempotencyLevel.GUARANTEED_IDEMPOTENT
        )
        self.assertEqual(
            self.guardrails.check_idempotency("KubernetesPodOperator", "INSERT OVERWRITE TABLE events"),
            IdempotencyLevel.GUARANTEED_IDEMPOTENT
        )
        self.assertEqual(
            self.guardrails.check_idempotency("DbtRunOperator", "dbt run --select marts"),
            IdempotencyLevel.GUARANTEED_IDEMPOTENT
        )

        # Potentially unsafe query
        self.assertEqual(
            self.guardrails.check_idempotency("CustomSQLBatchOperator", "INSERT INTO ledger VALUES (1, 100)"),
            IdempotencyLevel.POTENTIALLY_UNSAFE
        )

    def test_policy_limits_enforced(self):
        task_meta = {"cluster": "gke-prod", "dag_id": "test_dag", "task_id": "t1", "try_number": 3}
        decision = self.guardrails.evaluate(task_meta, "Exit code 137 OOMKilled", "MERGE INTO")
        # Try number 3 exceeds max_auto_retries=2
        self.assertFalse(decision.can_auto_remediate)
        self.assertTrue(any("auto-retries" in v.lower() for v in decision.violations))


class TestCodePatcher(unittest.TestCase):
    """Tests automated code patch synthesis and diff generation."""

    def test_patch_dbt_schema_drift(self):
        original_sql = """
SELECT
    c.customer_account_id,
    c.email,
    sum(s.amount) as total_spend
FROM stg_customers c
JOIN stg_sales s ON c.customer_account_id = s.customer_account_id
GROUP BY 1, 2;
"""
        patch = AirDoctorPatcher.patch_dbt_schema_drift(
            file_path="models/marts/fct_customers.sql",
            original_sql=original_sql,
            old_column="customer_account_id",
            new_column="account_id"
        )

        self.assertIn("account_id", patch.patched_code)
        self.assertNotIn("customer_account_id", patch.patched_code)
        self.assertIn("--- a/models/marts/fct_customers.sql", patch.unified_diff)
        self.assertIn("+    c.account_id,", patch.unified_diff)

    def test_patch_sql_zero_division(self):
        original_sql = "SELECT fee_id, net_amount / total_transactions as effective_rate FROM settlements;"
        patch = AirDoctorPatcher.patch_custom_sql_query(
            file_path="queries/fees.sql",
            original_sql=original_sql,
            error_type="division_by_zero"
        )
        self.assertIn("NULLIF(total_transactions, 0)", patch.patched_code)
        self.assertIn("--- a/queries/fees.sql", patch.unified_diff)


class TestModelLoader(unittest.TestCase):
    """Tests multi-cloud model loader configuration."""

    def test_unsupported_provider_raises(self):
        os.environ["LLM_PROVIDER"] = "invalid_provider"
        with self.assertRaises(ValueError):
            load_model()

    def test_vertexai_missing_package_error(self):
        os.environ["LLM_PROVIDER"] = "vertexai"
        with self.assertRaises(ImportError):
            load_model()

    def test_bedrock_provider_loads(self):
        os.environ["LLM_PROVIDER"] = "bedrock"
        model = load_model()
        self.assertIsNotNone(model)
        self.assertEqual(type(model).__name__, "ChatBedrock")


class TestAirflow3ClientHeaders(unittest.TestCase):
    """Tests Airflow 3 REST API header generation."""

    def test_auth_headers_token(self):
        from mcp_server.real_server import _get_auth_headers
        os.environ["AIRFLOW_API_TOKEN"] = "test-jwt-bearer-token"
        headers = _get_auth_headers()
        self.assertEqual(headers["Authorization"], "Bearer test-jwt-bearer-token")
        self.assertEqual(headers["User-Agent"], "AirDoctor-Airflow3-Client/1.0")
        del os.environ["AIRFLOW_API_TOKEN"]


if __name__ == "__main__":
    unittest.main()
