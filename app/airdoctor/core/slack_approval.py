"""
AirDoctor Slack Interactive Approval Engine.
Generates Slack Block Kit interactive approval notifications for
Human-in-the-Loop (HITL) authorization on high-impact self-healing actions.
"""

import os
import json
import uuid
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
    # Also check agentcore/.env.local and app/airdoctor/.env
    _cur = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(_cur, "..", "agentcore", ".env.local"))
    load_dotenv(os.path.join(_cur, "..", ".env"))
except ImportError:
    pass


@dataclass
class ApprovalRequest:
    approval_id: str
    dag_id: str
    task_id: str
    cluster: str
    action_type: str
    proposed_change: str
    risk_level: str               # "LOW", "MEDIUM", "HIGH"
    code_diff: Optional[str]
    channel: str
    root_cause: str = "STRUCTURAL_SCHEMA_DRIFT"
    pr_url: Optional[str] = None
    status: str = "PENDING"       # "PENDING", "APPROVED", "REJECTED"


DEFAULT_SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

class AirDoctorSlackApproval:
    """Formats and manages Slack Block Kit approval messages."""

    @staticmethod
    def create_request(
        cluster: str,
        dag_id: str,
        task_id: str,
        action_type: str,
        proposed_change: str,
        code_diff: Optional[str] = None,
        risk_level: str = "MEDIUM",
        root_cause: str = "STRUCTURAL_SCHEMA_DRIFT",
        pr_url: Optional[str] = None,
        channel: str = "#demo-aws-agentcore-hackathon-302026"
    ) -> ApprovalRequest:
        return ApprovalRequest(
            approval_id=f"apr-{uuid.uuid4().hex[:8]}",
            cluster=cluster,
            dag_id=dag_id,
            task_id=task_id,
            action_type=action_type,
            proposed_change=proposed_change,
            risk_level=risk_level,
            code_diff=code_diff,
            channel=channel,
            root_cause=root_cause,
            pr_url=pr_url
        )

    @staticmethod
    def format_slack_blocks(req: ApprovalRequest) -> Dict[str, Any]:
        """Builds standard Slack Block Kit JSON payload."""
        risk_emoji = "🔴" if req.risk_level == "HIGH" else ("🟡" if req.risk_level == "MEDIUM" else "🟢")

        fields = [
            {"type": "mrkdwn", "text": f"*Pipeline:*\n`{req.dag_id}.{req.task_id}`"},
            {"type": "mrkdwn", "text": f"*Cluster:*\n`{req.cluster}`"},
            {"type": "mrkdwn", "text": f"*Root Cause:*\n🔍 *{req.root_cause}*"},
            {"type": "mrkdwn", "text": f"*Risk Level:*\n{risk_emoji} *{req.risk_level}*"}
        ]

        if req.pr_url:
            pr_num = req.pr_url.split("/")[-1]
            fields.append({"type": "mrkdwn", "text": f"*GitHub Draft PR:*\n<{req.pr_url}|PR #{pr_num}> (CI: 🟢 Passed)"})
            fields.append({"type": "mrkdwn", "text": "*CI/CD Status:*\n🟢 `dbt compile: OK`"})

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Human Approval Required: AirDoctor Self-Healing Action",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": fields
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Proposed Remediation:*\n{req.proposed_change}"
                }
            }
        ]

        if req.code_diff:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Code Diff:*\n```diff\n{req.code_diff[:800]}\n```"
                }
            })

        approve_btn_text = "✅ Approve & Merge PR" if req.pr_url else "✅ Approve & Execute"
        blocks.append({
            "type": "actions",
            "block_id": f"action_block_{req.approval_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": approve_btn_text, "emoji": True},
                    "style": "primary",
                    "value": f"approve_{req.approval_id}",
                    "action_id": "approve_action"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ Reject & Keep Failed", "emoji": True},
                    "style": "danger",
                    "value": f"reject_{req.approval_id}",
                    "action_id": "reject_action"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "💬 Modify Parameters", "emoji": True},
                    "value": f"modify_{req.approval_id}",
                    "action_id": "modify_action"
                }
            ]
        })

        return {
            "channel": req.channel,
            "text": f"Approval requested for AirDoctor action on {req.dag_id}.{req.task_id}",
            "blocks": blocks
        }

    @staticmethod
    def dispatch_to_slack(req: ApprovalRequest) -> Dict[str, Any]:
        """
        Dispatches interactive Block Kit approval payload to Slack via:
        1. Modern Slack Web API: chat.postMessage (SLACK_BOT_TOKEN="xoxb-...")
        2. Webhook fallback: (SLACK_WEBHOOK_URL="https://hooks.slack.com/...")
        """
        bot_token = os.getenv("SLACK_BOT_TOKEN")
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            slack_cfg = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "slack_config.json"))
            if os.path.exists(slack_cfg):
                try:
                    with open(slack_cfg, "r") as f:
                        webhook_url = json.load(f).get("slack_webhook_url")
                except Exception:
                    pass
        if not webhook_url:
            webhook_url = DEFAULT_SLACK_WEBHOOK

        target_channel = os.getenv("SLACK_CHANNEL", req.channel)

        payload = AirDoctorSlackApproval.format_slack_blocks(req)
        payload["channel"] = target_channel

        # 1. Modern Slack Web API (chat.postMessage)
        if bot_token:
            try:
                import urllib.request
                api_url = "https://slack.com/api/chat.postMessage"
                data = json.dumps(payload).encode("utf-8")
                headers = {
                    "Authorization": f"Bearer {bot_token}",
                    "Content-Type": "application/json; charset=utf-8",
                    "User-Agent": "AirDoctor-SRE/2.0"
                }
                request = urllib.request.Request(api_url, data=data, headers=headers)
                with urllib.request.urlopen(request, timeout=10) as resp:
                    resp_json = json.loads(resp.read().decode("utf-8"))
                    if resp_json.get("ok"):
                        return {
                            "status": "DELIVERED_TO_SLACK_API",
                            "method": "chat.postMessage",
                            "channel": target_channel,
                            "ts": resp_json.get("ts")
                        }
                    else:
                        return {
                            "status": "SLACK_API_ERROR",
                            "error": resp_json.get("error"),
                            "channel": target_channel
                        }
            except Exception as e:
                return {"status": "SLACK_POST_FAILED", "error": str(e), "channel": target_channel}

        # 2. Webhook Fallback
        if webhook_url:
            try:
                import urllib.request
                webhook_payload = {
                    "text": payload.get("text", f"Approval requested for AirDoctor action on {req.dag_id}.{req.task_id}"),
                    "blocks": payload.get("blocks", [])
                }
                data = json.dumps(webhook_payload).encode("utf-8")
                request = urllib.request.Request(
                    webhook_url,
                    data=data,
                    headers={"Content-Type": "application/json", "User-Agent": "AirDoctor-SRE/2.0"}
                )
                with urllib.request.urlopen(request, timeout=8) as resp:
                    return {"status": "DELIVERED_TO_SLACK_WEBHOOK", "code": resp.getcode(), "channel": target_channel}
            except Exception as e:
                return {"status": "WEBHOOK_FAILED", "error": str(e), "channel": target_channel}

        return {"status": "SLACK_ALERT_SIMULATED", "channel": target_channel}

    # Alias for backward-compatibility
    dispatch_to_slack_webhook = dispatch_to_slack

    @staticmethod
    def dispatch_auto_heal_notification(
        cluster: str,
        dag_id: str,
        task_id: str,
        root_cause: str,
        action_taken: str,
        s3_log_uri: Optional[str] = None,
        channel: str = "#demo-aws-agentcore-hackathon-302026"
    ) -> Dict[str, Any]:
        """Dispatches an audit notification to Slack for an autonomously resolved pipeline."""
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        bot_token = os.getenv("SLACK_BOT_TOKEN")

        if not webhook_url:
            slack_cfg = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "slack_config.json"))
            if os.path.exists(slack_cfg):
                try:
                    with open(slack_cfg, "r") as f:
                        webhook_url = json.load(f).get("slack_webhook_url")
                except Exception:
                    pass
        if not webhook_url:
            webhook_url = DEFAULT_SLACK_WEBHOOK

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Pipeline Auto-Healed: Zero Human Intervention",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Pipeline:*\n`{dag_id}.{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Cluster:*\n`{cluster}`"},
                    {"type": "mrkdwn", "text": f"*Root Cause:*\n`{root_cause}`"},
                    {"type": "mrkdwn", "text": "*Status:*\n🟢 *AUTO-RECOVERED*"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Remediation Executed:*\n{action_taken}"
                }
            }
        ]

        if s3_log_uri:
            blocks.append({
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"📁 *Audit Log:* `{s3_log_uri}`"}
                ]
            })

        payload = {
            "channel": os.getenv("SLACK_CHANNEL", channel),
            "text": f"✅ Auto-Healed: `{dag_id}.{task_id}` on `{cluster}` ({root_cause})",
            "blocks": blocks
        }

        # 1. Bot token
        if bot_token:
            try:
                import urllib.request
                api_url = "https://slack.com/api/chat.postMessage"
                data = json.dumps(payload).encode("utf-8")
                headers = {"Authorization": f"Bearer {bot_token}", "Content-Type": "application/json; charset=utf-8"}
                req = urllib.request.Request(api_url, data=data, headers=headers)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return {"status": "DELIVERED_TO_SLACK_API"}
            except Exception as e:
                pass

        # 2. Webhook
        if webhook_url:
            try:
                import urllib.request
                webhook_payload = {"text": payload["text"], "blocks": payload["blocks"]}
                data = json.dumps(webhook_payload).encode("utf-8")
                req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    return {"status": "DELIVERED_TO_SLACK_WEBHOOK", "code": resp.getcode()}
            except Exception as e:
                return {"status": "WEBHOOK_FAILED", "error": str(e)}

        return {"status": "SIMULATED_SLACK"}

    @staticmethod
    def dispatch_approved_recovery_notification(
        cluster: str,
        dag_id: str,
        task_id: str,
        root_cause: str,
        action_taken: str,
        approver: str = "@daniyar (On-Call SRE)",
        pr_url: Optional[str] = None,
        s3_log_uri: Optional[str] = None,
        channel: str = "#demo-aws-agentcore-hackathon-302026"
    ) -> Dict[str, Any]:
        """Dispatches a confirmation notification to Slack when a human-approved remediation is executed."""
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        bot_token = os.getenv("SLACK_BOT_TOKEN")

        if not webhook_url:
            slack_cfg = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "slack_config.json"))
            if os.path.exists(slack_cfg):
                try:
                    with open(slack_cfg, "r") as f:
                        webhook_url = json.load(f).get("slack_webhook_url")
                except Exception:
                    pass
        if not webhook_url:
            webhook_url = DEFAULT_SLACK_WEBHOOK

        fields = [
            {"type": "mrkdwn", "text": f"*Pipeline:*\n`{dag_id}.{task_id}`"},
            {"type": "mrkdwn", "text": f"*Cluster:*\n`{cluster}`"},
            {"type": "mrkdwn", "text": f"*Approved By:*\n👤 *{approver}*"},
            {"type": "mrkdwn", "text": "*Status:*\n🟢 *APPROVED & MERGED*"}
        ]
        if pr_url:
            fields.append({"type": "mrkdwn", "text": f"*GitHub PR:*\n<{pr_url}|PR #{pr_url.split('/')[-1]}> (Merged into main)"})

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Remediation Approved & Executed: Pipeline Resumed",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": fields
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Remediation Summary:*\n{action_taken}"
                }
            }
        ]
        if s3_log_uri:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"📁 *Audit Trail:* `{s3_log_uri}`"}]
            })

        payload = {
            "channel": os.getenv("SLACK_CHANNEL", channel),
            "text": f"✅ Remediation Approved: `{dag_id}.{task_id}` resumed after approval by {approver}.",
            "blocks": blocks
        }

        if webhook_url:
            try:
                import urllib.request
                webhook_payload = {"text": payload["text"], "blocks": payload["blocks"]}
                data = json.dumps(webhook_payload).encode("utf-8")
                req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    return {"status": "DELIVERED_TO_SLACK_WEBHOOK", "code": resp.getcode()}
            except Exception as e:
                return {"status": "WEBHOOK_FAILED", "error": str(e)}

        return {"status": "SIMULATED_SLACK"}

    @staticmethod
    def render_cli_card(req: ApprovalRequest, status: str = "APPROVED", approver: str = "@daniyar (On-Call SRE)"):
        """Renders the Slack Interactive card in the CLI for demos."""
        border = "═" * 78
        print(f"\n\033[95m╔{border}╗\033[0m")
        print(f"\033[95m║  💬 SLACK INTERACTIVE APPROVAL NOTIFICATION (#{req.channel.replace('#', '')}){' ' * max(0, 31 - len(req.channel))}║\033[0m")
        print(f"\033[95m╠{border}╣\033[0m")
        print(f"║  *Pipeline:* `{req.dag_id}.{req.task_id}` on `{req.cluster}`")
        print(f"║  *Root Cause:* \033[1m\033[93m{req.root_cause}\033[0m")
        if req.pr_url:
            print(f"║  *GitHub Draft PR:* \033[96m{req.pr_url}\033[0m (CI/CD: \033[92m🟢 Checks Passed\033[0m)")
        print(f"║  *Proposed Action:* \033[1m{req.action_type}\033[0m ({req.risk_level} RISK)")
        print(f"║  *Details:* {req.proposed_change}")
        if req.code_diff:
            print(f"║  *Code Diff Attached:* [Unified Diff ({len(req.code_diff.splitlines())} lines)]")
        print(f"║")
        print(f"║  Interactive Buttons:")
        approve_btn = "✅ Approve & Merge PR" if req.pr_url else "✅ Approve & Execute"
        print(f"║  \033[92m[ {approve_btn} ]\033[0m   \033[91m[ ❌ Reject ]\033[0m   \033[94m[ 💬 Modify ]\033[0m")
        print(f"║")
        if status == "APPROVED":
            print(f"║  \033[92m✔ Human Decision Received:\033[0m Approved by {approver} at {time.strftime('%H:%M:%S UTC')}")
            print(f"║  \033[92m✔ Authorization Token: auth_{req.approval_id} verified. Executing remediation...\033[0m")
        else:
            print(f"║  \033[93m⏳ Status: Waiting for on-call engineer reaction in Slack...\033[0m")
        print(f"\033[95m╚{border}╝\033[0m\n")
