# 🏛️ AirDoctor: Architecture & Agent Topology

This document details the architectural design of **AirDoctor**, comparing the **v1 Unified SRE Agent** (current deployment) with the **v2 Multi-Agent Supervisor Roadmap**.

---

## 1. Single Agent (v1 MVP) vs. Multi-Agent Team (v2 Enterprise)

```mermaid
flowchart TB
    subgraph V1["⚡ v1: Unified SRE Agent (Current Deployment)"]
        direction TB
        A1["🧠 AirDoctor Unified Agent<br/>(Claude 3.5 Sonnet on Bedrock)"]
        A1 <--> G1["🛡️ Guardrails & Policy Engine"]
        A1 <--> T1["🔌 Multi-Tool MCP Server<br/>• S3 Log Fetcher<br/>• Cloud Logging Query<br/>• Multi-Framework Patcher<br/>• Airflow REST API"]
    end

    subgraph V2["🌐 v2: Multi-Agent Team (Enterprise Roadmap)"]
        direction TB
        Sup["👑 AirDoctor SRE Supervisor<br/>(Triage, Context Router & Final Approval)"]
        
        subgraph Specialists["Specialized Sub-Agents (Bedrock A2A Protocol)"]
            InfraAgent["🖥️ Infra SRE Agent<br/>• GKE Pod Lifecycle<br/>• cgroup OOMs<br/>• Prometheus Metrics"]
            SqlAgent["📐 dbt & SQL Agent<br/>• dbt Manifest Lineage<br/>• AST Code Patcher<br/>• NULLIF Zero-Div Guards"]
            RemedAgent["⚡ Remediation Agent<br/>• Idempotency Gate<br/>• Airflow API Restarter<br/>• Cross-Cluster DAGs"]
        end
        
        Sup == "A2A Delegation" ==> InfraAgent
        Sup == "A2A Delegation" ==> SqlAgent
        Sup == "A2A Delegation" ==> RemedAgent
    end
```

---

## 2. Complete End-to-End System Architecture (v1)

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  MULTI-CLUSTER GKE AIRFLOW CLUSTERS                                    │
│                                                                                                        │
│   ┌──────────────────────────────┐  ┌──────────────────────────────┐  ┌────────────────────────────┐   │
│   │   [gke-prod-us-central1]     │  │[gke-analytics-europe-west1]  │  │   [gke-data-asia-east1]    │   │
│   │   • analytics_daily_etl      │  │• finance_daily_marts         │  │   • apac_orders_rollup     │   │
│   │   • financial_ledger_sync    │  │• eu_raw_ingestion_hourly     │  │   • wechat_alipay_settle   │   │
│   │   • stripe_reconciliation    │  │• inventory_optimizer         │  │                            │   │
│   └──────────────┬───────────────┘  └──────────────┬───────────────┘  └─────────────┬──────────────┘   │
│                  │                                 │                                │                  │
│                  └─────────────────────────────────┼────────────────────────────────┘                  │
│                                                    │ on_failure_callback                               │
│                                                    ▼                                                   │
│                                 ┌────────────────────────────────────┐                                 │
│                                 │   airdoctor_callback.py (Plugin)   │                                 │
│                                 │  • Non-blocking daemon thread      │                                 │
│                                 │  • Captures cluster/pod/exception  │                                 │
│                                 └──────────────────┬─────────────────┘                                 │
└────────────────────────────────────────────────────┼───────────────────────────────────────────────────┘
                                                     │ HTTP POST Event (Lightweight Metadata)
                                                     ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS AGENTCORE & BEDROCK COGNITIVE RUNTIME                           │
│                                                                                                        │
│  ┌──────────────────────────────────────────────────┐         ┌─────────────────────────────────────┐  │
│  │           Amazon Bedrock (Claude 3.5 Sonnet)     │◄───────►│    LangGraph ReAct Cognitive Loop   │  │
│  │           • SRE Data Platform Persona            │         │    • Plan ➔ Act ➔ Observe           │  │
│  │           • Multi-Cluster Root Cause Diagnosis   │         │    • MemorySaver state tracking     │  │
│  └──────────────────────────────────────────────────┘         └──────────────────┬──────────────────┘  │
│                                                                                  │                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┴──────────────────┐  │
│  │                            🛡️ ENTERPRISE SAFETY & IDEMPOTENCY GUARDRAILS                         │  │
│  │  • Failure Classifier (OOM vs Deadlock vs Schema Drift vs Rate Limit vs Data Quality)            │  │
│  │  • Idempotency Gate (Block retries on raw INSERT INTO; authorize for dbt / MERGE / OVERWRITE)   │  │
│  │  • Policy Limits (Max Memory Ceiling: 16GiB | Max Auto-Retries: 2 | Jitter Backoff: 10s-30s)    │  │
│  │  • Repo Resolver (Discovers target Git repo via Pod env, dbt manifest, DAG tags, and Catalog)   │  │
│  └───────────────────────────────────────────────┬──────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┼─────────────────────────────────────────────────────┘
                                                   │ MCP Protocol (Streamable HTTP :8081)
                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       AIRDOCTOR MCP SERVER (:8081)                                     │
│                                                                                                        │
│   ┌───────────────────────────┐  ┌───────────────────────────┐  ┌──────────────────────────────────┐   │
│   │   Airflow REST API Tools  │  │   S3 Data Lake Tools      │  │   Self-Healing Remediation Tools │   │
│   │   • get_task_details      │  │   • s3_fetch_airflow_log  │  │   • BUMP_POD_MEMORY_AND_CLEAR    │   │
│   │   • list_dag_runs         │  │   • s3_query_cloud_logs   │  │   • CLEAR_TASK_INSTANCE          │   │
│   │   • clear_task_instance   │  │   • s3_query_grafana      │  │   • TRIGGER_UPSTREAM_DAG         │   │
│   │   • patch_pod_spec        │  │   • s3_get_dbt_artifact   │  │   • synthesize_code_patch        │   │
│   │   • resolve_git_repo      │  │   • s3_list_dag_runs      │  │   • send_slack_approval_request  │   │
│   └─────────────┬─────────────┘  └─────────────┬─────────────┘  └─────────────────┬────────────────┘   │
└─────────────────┼──────────────────────────────┼──────────────────────────────────┼────────────────────┘
                  │                              │                                  │
                  ▼                              ▼                                  ▼
┌───────────────────────────────────┐ ┌────────────────────────────────────┐ ┌───────────────────────────┐
│         AIRFLOW REST API          │ │     AMAZON S3 DATA LAKE            │ │   REMEDIATION TARGETS     │
│   • Clear Task State              │ │  s3://airdoctor-logs-prod/         │ │  • Dynamic Pod Spec Bump  │
│   • Trigger Upstream DAG          │ │   ├── airflow-logs/ (41+ files)    │ │  • GitHub PR Synthesis    │
│   • Reset Partition Sensor        │ │   ├── cloud-logging/ (S3 sink)     │ │  • Slack Interactive HITL│
│   • Update Pod Resource Limits    │ │   ├── grafana-metrics/ (Prometheus)│ │  • Data Steward Escalation│
│                                   │ │   └── dbt-artifacts/ (Manifests)   │ │                           │
└───────────────────────────────────┘ └────────────────────────────────────┘ └───────────────────────────┘
```

---

## 3. Incident Triage & Telemetry Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor GKE as GKE Airflow Worker
    participant Callback as airdoctor_callback.py
    participant AgentCore as Bedrock AgentCore
    participant MCP as AirDoctor MCP Server
    participant S3 as Amazon S3 Data Lake
    participant Slack as Slack Channel
    participant GitHub as GitHub Repository

    GKE->>GKE: Task fails (e.g. OOM or schema drift)
    GKE->>Callback: on_failure_callback triggered
    Callback->>AgentCore: POST incident event (cluster, dag_id, task_id)
    
    activate AgentCore
    AgentCore->>MCP: get_airflow_task_details()
    MCP-->>AgentCore: Returns pod_name, operator, S3 URI
    
    AgentCore->>MCP: s3_fetch_airflow_log(tail_lines=150)
    MCP->>S3: Read byte range from s3://.../attempt=1.log
    S3-->>MCP: Log text
    MCP-->>AgentCore: Return log traceback
    
    alt Incident is Infrastructure OOM
        AgentCore->>MCP: s3_query_cloud_logging_sink()
        MCP->>S3: Read cgroup OOM events
        S3-->>MCP: Kernel out_of_memory record
        MCP-->>AgentCore: Confirm exit code 137
        
        AgentCore->>AgentCore: Guardrails check (idempotency: OK, memory: 4096Mi < 16Gi limit)
        AgentCore->>MCP: send_slack_approval_request(action="BUMP_POD_MEMORY")
        MCP->>Slack: Post Block Kit approval card with [Approve] button
        Slack-->>AgentCore: Engineer clicks [Approve]
        
        AgentCore->>MCP: execute_airflow_remediation(action="BUMP_POD_MEMORY_AND_CLEAR")
        MCP->>GKE: Airflow REST API: Clear task with memory override
        GKE-->>AgentCore: Task rescheduled with 4096Mi
    else Incident is Code / Schema Drift
        AgentCore->>MCP: resolve_task_git_repository()
        MCP-->>AgentCore: Target repo: https://github.com/deliveryhero/analytics-dbt
        
        AgentCore->>MCP: synthesize_code_patch(framework="dbt")
        MCP-->>AgentCore: Unified diff + branch fix/account_id
        
        AgentCore->>MCP: send_slack_approval_request(action="CREATE_GITHUB_PR")
        MCP->>Slack: Post Block Kit card with attached unified git diff
        Slack-->>AgentCore: Engineer approves
        
        AgentCore->>MCP: execute_airflow_remediation(action="CREATE_GITHUB_PR")
        MCP->>GitHub: Create Pull Request #403
        GitHub-->>AgentCore: PR URL created
    end
    
    AgentCore->>Slack: Post Final Executive Incident Summary Card
    deactivate AgentCore
```

---

## 4. Why Start with One Agent (The Technical Rationale)

| Metric | Single Agent (v1 MVP) | Multi-Agent Team (v2 Roadmap) |
| :--- | :--- | :--- |
| **End-to-End Latency** | **Fast (2–4 seconds)** | Slower (8–15s due to inter-agent routing) |
| **Context Window Loss** | **None** (Root cause & remediation shared in 1 context) | High risk of loss during supervisor-to-subagent serialization |
| **AWS Deployment Surface** | **1 CDK Runtime** (`agentcore.json`) | 4 separate CDK runtimes + IAM A2A trust policies |
| **Failure Surface** | Single point of observation | Complex distributed agent consensus & timeouts |
| **Tool Count** | Optimal for 6–10 tools | Best when tool count exceeds 20+ specialized tools |
