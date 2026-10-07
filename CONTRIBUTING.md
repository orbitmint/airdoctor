# Contributing to AirDoctor

We welcome contributions! Whether it's adding new failure scenarios, expanding guardrails, or supporting new data platforms (e.g. Snowflake, Databricks), we'd love your help.

## Development Setup

1. Fork and clone the repository.
2. Ensure you have Python 3.10+ installed.
3. Install dependencies using `uv` or `pip`:
   ```bash
   pip install -r app/airdoctor/requirements.txt
   ```
   *(Note: if `requirements.txt` does not exist, use `uv sync` or standard `pip install langchain-core langgraph boto3 mcp fastmcp`).*

## Adding New Failure Scenarios

AirDoctor uses an offline-first "Fixture" architecture to guarantee deterministic demos and testing.
To add a new scenario:
1. Create a new folder in `fixtures/incident_X_my_new_failure`.
2. Provide `metadata.json` with the cluster, dag_id, and task_id.
3. Provide realistic `cloud_logging_events.json`, `grafana_metrics.json`, and mock Airflow tracebacks.
4. Update `app/airdoctor/core/guardrails.py` to identify the new failure mode.
5. Update `demo_runner.py` with the new scenario selector.

## Real vs Mock MCP Server

If you are developing against *real* APIs:
1. Check out `mcp_server/real_mcp_server.py`.
2. Connect it to your Airflow REST API, GCP Cloud Logging client, and GitHub API.
3. Start the real server instead of the mock when running the agent.

## Pull Requests

1. Create a feature branch.
2. Ensure your code is well-typed and runs cleanly.
3. Open a Pull Request describing your changes.
