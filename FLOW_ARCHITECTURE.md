# 🌊 AirDoctor: Flow Architecture Specification

This document details the **Control Flow**, **Data Flow**, and **State Machine Transitions** across the AirDoctor autonomous SRE pipeline.

---

## 1. End-to-End System Flow (Mermaid)

```mermaid
flowchart TD
    subgraph GKE["☸️ Google Kubernetes Engine (GKE) Airflow"]
        Crash["💥 Task Crashes on Worker Pod<br/>(OOM, Deadlock, Schema Drift, etc.)"]
        Plugin["🔌 AirDoctor Plugin<br/>(on_failure_callback)"]
        Crash -->|Triggers callback| Plugin
    end

    subgraph S3["🪣 Amazon S3 Data Lake (s3://airdoctor-data-platform-logs-prod/)"]
        direction TB
        S3Logs["📁 airflow-logs/<br/>Task tracebacks & outputs"]
        S3Cloud["📁 cloud-logging/<br/>Kernel cgroup & DB locks"]
        S3Dbt["📁 dbt-artifacts/<br/>manifest.json & SQL models"]
    end

    Crash -.->|Remote logging sync| S3Logs

    subgraph AgentCore["☁️ AWS AgentCore & LangGraph Cognitive Runtime"]
        direction TB
        Node1["1️⃣ parse_alert<br/>Extract cluster, dag, task, run ID"]
        Node2["2️⃣ investigate_telemetry<br/>Stream S3 logs & correlate signals"]
        Node3["3️⃣ guardrails_evaluation<br/>Classify failure & verify idempotency"]
        
        Router{"🔀 Decision Router<br/>(Conditional Edge)"}
        
        Node4["4️⃣ slack_approval<br/>Dispatch Block Kit card & await sign-off"]
        Node5["5️⃣ execute_remediation<br/>Scale pod, clear task, or open PR"]
        Node6["6️⃣ generate_sre_report<br/>Bedrock (Claude 3.5) RCA summary"]

        Node1 --> Node2
        Node2 --> Node3
        Node3 --> Router
        
        Router -->|High Risk / PR / Ceiling Breach| Node4
        Router -->|Safe / Idempotent Auto-Heal| Node5
        
        Node4 --> Node5
        Node5 --> Node6
    end

    Plugin ==>|HTTP POST Event<br/>(Cluster, DAG, Task)| Node1
    Node2 <==|boto3 stream byte range| S3
    Node6 <==|Prompt & Context| Bedrock["🔮 Amazon Bedrock<br/>(Claude 3.5 Sonnet)"]

    subgraph Targets["⚡ Remediation Targets & SRE Output"]
        AirflowAPI["☸️ Airflow REST API<br/>Clear Task & Patch Pod Spec"]
        GitHub["🐙 GitHub Enterprise<br/>Open & Merge Pull Request"]
        Slack["💬 Slack Channel<br/>Approval Cards & Auto-Heal Audits"]
    end

    Node4 ==>|Interactive Card| Slack
    Node5 ==>|POST /api/v1/dags/.../clear| AirflowAPI
    Node5 ==>|POST /repos/.../pulls| GitHub
    Node6 ==>|Audit Confirmation| Slack
```

---

## 2. LangGraph State Machine & Data Transformations

Every incident flows through the **`AirDoctorState`** typed state machine. Below is the exact data transformation at each node:

| Step / Node | Input State Available | Operations Performed | Output State Produced |
| :--- | :--- | :--- | :--- |
| **`START`** | Raw prompt from webhook | Initialization | `messages: [HumanMessage]` |
| **`parse_alert`** | `messages` | Regex parsing of alert payload coordinates | `cluster`, `dag_id`, `task_id`, `run_id`, `operator` |
| **`investigate_telemetry`** | Coordinates | • Resolves S3 remote log URI<br>• Streams tail 50 lines via `boto3`<br>• Reads Cloud Logging S3 sink<br>• Resolves Git repo from catalog | `s3_log_uri`, `log_trace`, `cloud_logging_event`, `git_repo_url` |
| **`guardrails_evaluation`** | `log_trace`, `operator` | • Classifies failure category<br>• Verifies query idempotency (`MERGE`/`dbt`)<br>• Evaluates policy limits (16GiB ceiling)<br>• If schema drift: opens Draft PR | `failure_category`, `idempotency_level`, `can_auto_remediate`, `remediation_action`, `requires_human_approval`, `pr_url`, `git_diff` |
| **`route_after_guardrails`** | `requires_human_approval` | Evaluates conditional edge | Routes to `slack_approval` OR `execute_remediation` |
| **`slack_approval`** | `pr_url`, `git_diff`, `root_cause` | Dispatches Slack Block Kit card; records verified on-call approval token | `approval_status: "APPROVED"`, `remediation_details` |
| **`execute_remediation`** | `remediation_action` | • Calls Airflow REST API to clear task<br>• If PR: merges PR #403 into `main` | `remediation_details: "SUCCESS"` |
| **`generate_sre_report`** | Full incident state | Calls Amazon Bedrock (Claude 3.5 Sonnet) to write executive RCA report | `final_report: str`, `messages: [AIMessage]` |
| **`END`** | `final_report` | Serializes JSON response to AgentCore caller | HTTP 200 `{ "result": final_report }` |

---

## 3. Telemetry & Control Flow Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor GKE as GKE Airflow Worker
    participant Callback as airdoctor_callback.py
    participant AgentCore as Bedrock AgentCore
    participant S3 as Amazon S3 Data Lake
    participant Bedrock as Amazon Bedrock (Claude 3.5)
    participant Slack as Slack Channel
    participant GitHub as GitHub
    participant AirflowAPI as Airflow REST API

    Note over GKE,S3: 1. Failure & Remote S3 Storage
    GKE->>S3: Remote S3 Logging flushes attempt=1.log
    GKE->>Callback: on_failure_callback triggered
    Callback->>AgentCore: POST event {cluster, dag_id, task_id, run_id}

    Note over AgentCore,S3: 2. S3 Telemetry Retrieval
    activate AgentCore
    AgentCore->>S3: boto3.get_object(airflow-logs/cluster=.../attempt=1.log)
    S3-->>AgentCore: Streams raw log bytes
    AgentCore->>S3: boto3.get_object(cloud-logging/cluster=.../events.json)
    S3-->>AgentCore: Container cgroup / DB lock events

    Note over AgentCore: 3. Guardrails & Policy Evaluation
    AgentCore->>AgentCore: Classify failure (e.g. STRUCTURAL_SCHEMA_DRIFT)
    AgentCore->>AgentCore: Idempotency gate check (dbt: GUARANTEED_IDEMPOTENT)

    alt Scenario A: Structural Code Change (e.g. Schema Drift)
        Note over AgentCore,GitHub: 4A. Pattern 2 (Draft PR First)
        AgentCore->>GitHub: POST /repos/.../pulls (Creates Draft PR #403)
        GitHub-->>AgentCore: Returns pr_url
        AgentCore->>Slack: POST Block Kit Approval Card (with PR link & diff)
        Slack-->>AgentCore: On-call SRE approves via Slack
        AgentCore->>GitHub: PUT /repos/.../pulls/403/merge (Merges to main)
        AgentCore->>AirflowAPI: POST /api/v1/dags/.../clearTaskInstances
        AirflowAPI-->>GKE: Airflow worker re-runs with updated code
    else Scenario B: Autonomous Transient Heal (e.g. Deadlock or OOM)
        Note over AgentCore,AirflowAPI: 4B. Zero Human Intervention
        AgentCore->>AirflowAPI: POST /api/v1/dags/.../clearTaskInstances (with memory override)
        AirflowAPI-->>GKE: Worker pod restarts with 4096Mi
        AgentCore->>Slack: POST Auto-Healed Green Audit Card
    end

    Note over AgentCore,Bedrock: 5. Executive RCA Synthesis
    AgentCore->>Bedrock: ainvoke(summary_prompt + incident_telemetry)
    Bedrock-->>AgentCore: Executive RCA Report
    AgentCore-->>Callback: HTTP 200 Response
    deactivate AgentCore
```

---

## 4. Decision Tree Flowchart (Guardrails Engine)

```mermaid
flowchart TD
    Start([Failure Log Received from S3]) --> Classify{Classify Failure}

    Classify -->|Exit 137 / Memory| OOM[TRANSIENT_OOM]
    Classify -->|Column Missing| Drift[STRUCTURAL_SCHEMA_DRIFT]
    Classify -->|Deadlock 40P01| Lock[TRANSIENT_CONCURRENCY]
    Classify -->|HTTP 429| Rate[TRANSIENT_RATE_LIMIT]
    Classify -->|Sensor 7200s| Sensor[DEPENDENCY_LAG]
    Classify -->|Duplicate Keys| DQ[DATA_QUALITY]

    OOM --> CheckMem{Memory < 16GiB?}
    CheckMem -->|Yes| AutoOOM[Auto-Scale Pod to 4096Mi & Clear Task]
    CheckMem -->|No: Exceeds Ceiling| EscalateHuman[Escalate to Human SRE]

    Lock --> Jitter[Wait 15s Jitter Backoff & Clear Task]
    Rate --> Cooldown[Wait 30s Cooldown & Clear Task]
    Sensor --> CrossRerun[Trigger Upstream DAG & Reset Sensor]

    AutoOOM --> SendGreen[Send Green Auto-Healed Slack Card]
    Jitter --> SendGreen
    Cooldown --> SendGreen
    CrossRerun --> SendGreen

    Drift --> OpenDraftPR[Open GitHub Draft PR #403]
    OpenDraftPR --> SendSlackApproval[Send Yellow Slack Approval Card with PR Link]
    SendSlackApproval --> Approved{SRE Approved?}
    Approved -->|Yes| MergePR[Merge PR #403 into main & Clear Task]
    Approved -->|No| KeepFailed[Keep Pipeline Paused]

    DQ --> BlockRetry[BLOCK Blind Auto-Retry]
    BlockRetry --> AlertStewards[Send Diagnostic SQL Query to Data Stewards]
```
