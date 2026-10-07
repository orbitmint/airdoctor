#!/usr/bin/env python3
"""
AirDoctor S3 Lake Sync Utility.
Syncs the generated multi-cluster Airflow log lake and telemetry
from local disk ('s3_data_lake/') to a real Amazon S3 bucket for AWS Bedrock agent access.

Usage:
    python sync_to_s3.py --bucket <your-s3-bucket-name> [--region us-east-1]
"""

import os
import sys
import argparse
import mimetypes

# Auto-reexec under project virtualenv if boto3 is missing
VENV_PYTHON = os.path.abspath(os.path.join(os.path.dirname(__file__), "app", "airdoctor", ".venv", "bin", "python"))
if os.path.exists(VENV_PYTHON) and sys.executable != VENV_PYTHON:
    try:
        import boto3
    except ImportError:
        os.execv(VENV_PYTHON, [VENV_PYTHON] + sys.argv)

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("❌ Error: 'boto3' is not installed in the current Python environment.")
    print("Run with the project virtual environment:")
    print("  ./app/airdoctor/.venv/bin/python sync_to_s3.py")
    print("Or install it via:")
    print("  pip install boto3")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LAKE_DIR = os.path.join(SCRIPT_DIR, "s3_data_lake")


def ensure_bucket_exists(s3_client, bucket_name: str, region: str):
    """Verifies or creates the target S3 bucket."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✔ Found existing S3 bucket: {bucket_name}")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ["404", "NoSuchBucket"]:
            print(f"Bucket '{bucket_name}' not found. Creating in region {region}...")
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
            print(f"✔ Successfully created bucket: {bucket_name}")
        else:
            print(f"⚠ S3 Bucket check failed: {e}")
            raise


def sync_lake_to_s3(bucket_name: str, region: str = "us-east-1"):
    """Walks the local lake directory and uploads all artifacts to S3."""
    if not os.path.isdir(LAKE_DIR):
        print(f"Error: Lake directory not found at {LAKE_DIR}. Run s3_storage/generate_s3_lake.py first.")
        sys.exit(1)

    s3 = boto3.client("s3", region_name=region)
    ensure_bucket_exists(s3, bucket_name, region)

    total_files = 0
    uploaded_bytes = 0

    print(f"\nStarting S3 synchronization to s3://{bucket_name}/ ...")
    for root, _, files in os.walk(LAKE_DIR):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, LAKE_DIR)
            s3_key = rel_path.replace("\\", "/")

            content_type, _ = mimetypes.guess_type(local_path)
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type

            file_size = os.path.getsize(local_path)
            s3.upload_file(local_path, bucket_name, s3_key, ExtraArgs=extra_args)
            total_files += 1
            uploaded_bytes += file_size
            print(f"  ➜ Uploaded s3://{bucket_name}/{s3_key} ({file_size:,} bytes)")

    print(f"\n🎉 Successfully synced {total_files} files ({uploaded_bytes / 1024:.1f} KB) to s3://{bucket_name}/")
    print(f"Set environment variable:\n  export AIRDOCTOR_S3_BUCKET={bucket_name}")


def main():
    parser = argparse.ArgumentParser(description="Sync AirDoctor Data Lake to AWS S3")
    parser.add_argument("--bucket", default=os.getenv("AIRDOCTOR_S3_BUCKET", "airdoctor-data-platform-logs-prod"),
                        help="Target AWS S3 bucket name")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"),
                        help="AWS region (default: us-east-1)")

    args = parser.parse_args()
    sync_lake_to_s3(args.bucket, args.region)


if __name__ == "__main__":
    main()
