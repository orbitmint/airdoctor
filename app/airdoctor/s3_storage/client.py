"""
AirDoctor S3 Data Lake Client.
Provides unified S3 access for the Bedrock agent to query Airflow remote logs,
Cloud Logging sinks, Grafana metric archives, and dbt artifacts.

Supports:
1. Real AWS S3 via boto3 (when AIRDOCTOR_S3_BUCKET is configured and AWS credentials present).
2. Local S3 Data Lake Mirror (airdoctor/s3_data_lake/) for offline development, tests, and mock testing.
"""

import os
import io
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger("airdoctor.s3")

# Optional boto3 import with graceful fallback
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception
    HAS_BOTO3 = False

DEFAULT_BUCKET = os.getenv("AIRDOCTOR_S3_BUCKET", "airdoctor-data-platform-logs-prod")
LOCAL_LAKE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "s3_data_lake"))


class S3LakeClient:
    """Enterprise S3 Data Lake client for Airflow logs and telemetry."""

    def __init__(self, bucket: Optional[str] = None, local_dir: Optional[str] = None):
        self.bucket = bucket or DEFAULT_BUCKET
        self.local_dir = local_dir or LOCAL_LAKE_DIR
        self._s3 = None
        self._is_live_s3 = False

        # Attempt to initialize boto3 S3 client if available
        if HAS_BOTO3 and boto3 is not None:
            try:
                self._s3 = boto3.client("s3")
                if os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_PROFILE") or os.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
                    self._is_live_s3 = True
            except Exception:
                self._is_live_s3 = False

    def is_live_s3(self) -> bool:
        return self._is_live_s3

    def parse_s3_uri(self, uri: str) -> tuple[str, str]:
        """Parses s3://bucket/key into (bucket, key)."""
        clean = uri.replace("s3://", "")
        parts = clean.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    def get_object_text(self, s3_uri: str, max_lines: Optional[int] = None) -> str:
        """Retrieves raw text content of an S3 object."""
        bucket, key = self.parse_s3_uri(s3_uri)

        # Try Live S3 first if active
        if self._is_live_s3:
            try:
                resp = self._s3.get_object(Bucket=bucket, Key=key)
                body = resp["Body"].read().decode("utf-8", errors="replace")
                if max_lines:
                    lines = body.splitlines(keepends=True)
                    return "".join(lines[-max_lines:])
                return body
            except (ClientError, NoCredentialsError) as e:
                logger.warning(f"Live S3 get_object failed for {s3_uri}: {e}. Falling back to local lake mirror.")

        # Local Mirror Fallback
        local_path = os.path.join(self.local_dir, key)
        if os.path.exists(local_path):
            with open(local_path, "r", encoding="utf-8", errors="replace") as f:
                if max_lines:
                    lines = f.readlines()
                    return "".join(lines[-max_lines:])
                return f.read()

        return f"S3 Object not found: {s3_uri}"

    def get_object_json(self, s3_uri: str) -> Dict[str, Any]:
        """Retrieves and parses JSON object from S3."""
        text = self.get_object_text(s3_uri)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"error": "Invalid JSON", "raw": text}

    def list_objects(self, prefix: str, max_keys: int = 50) -> List[str]:
        """Lists S3 URIs matching a given prefix."""
        results = []

        if self._is_live_s3:
            try:
                resp = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=max_keys)
                for item in resp.get("Contents", []):
                    results.append(f"s3://{self.bucket}/{item['Key']}")
                if results:
                    return results
            except Exception as e:
                logger.warning(f"Live S3 list_objects failed: {e}. Falling back to local lake.")

        # Local mirror search
        search_dir = os.path.join(self.local_dir, prefix)
        if os.path.isdir(search_dir):
            for root, _, files in os.walk(search_dir):
                for fname in files:
                    rel = os.path.relpath(os.path.join(root, fname), self.local_dir)
                    results.append(f"s3://{self.bucket}/{rel}")
                    if len(results) >= max_keys:
                        break
        return results

    def put_object_text(self, s3_uri: str, content: str):
        """Writes text content to S3 and local mirror."""
        bucket, key = self.parse_s3_uri(s3_uri)

        # Write to local mirror
        local_path = os.path.join(self.local_dir, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(content)

        # Upload to live S3 if connected
        if self._is_live_s3:
            try:
                self._s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
            except Exception as e:
                logger.warning(f"Could not push to live S3 bucket {bucket}: {e}")
