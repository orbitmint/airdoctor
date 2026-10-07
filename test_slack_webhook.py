#!/usr/bin/env python3
"""
AirDoctor Live Slack Notification Tester.
Sends a real interactive Block Kit approval notification to your Slack channel
via the modern Slack Web API (SLACK_BOT_TOKEN="xoxb-...") or Slack App Webhook.

Usage:
    export SLACK_BOT_TOKEN="xoxb-..."
    export SLACK_CHANNEL="#data-platform-alerts"
    python3 test_slack_webhook.py
"""

import os
import sys

import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "app", "airdoctor"))

from core.slack_approval import AirDoctorSlackApproval

def main():
    bot_token = os.getenv("SLACK_BOT_TOKEN")
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        cfg_path = os.path.join(SCRIPT_DIR, "config", "slack_config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r") as f:
                webhook_url = json.load(f).get("slack_webhook_url")

    channel = os.getenv("SLACK_CHANNEL", "#general")

    if not bot_token and not webhook_url:
        print("\n❌ No Slack credentials found!")
        print("─" * 78)
        print("To send live Slack notifications using the modern Slack Web API:")
        print("1. Go to: https://api.slack.com/apps -> Create New App (From scratch)")
        print("2. Under 'OAuth & Permissions', add Bot Token Scope: 'chat:write'")
        print("3. Click 'Install to Workspace' and copy the Bot User OAuth Token (starts with xoxb-...)")
        print("4. Invite your bot to the channel in Slack: /invite @AirDoctor")
        print("5. Set your environment variables:")
        print('   export SLACK_BOT_TOKEN="xoxb-your-token-here"')
        print(f'   export SLACK_CHANNEL="{channel}"')
        print("6. Run: python3 test_slack_webhook.py")
        print("\n(Alternatively, set SLACK_WEBHOOK_URL to an app webhook URL)")
        print("─" * 78 + "\n")
        return

    method = "Modern Web API (chat.postMessage)" if bot_token else "App Webhook"
    print(f"\n📡 Sending live interactive approval notification via {method} to {channel}...")

    req = AirDoctorSlackApproval.create_request(
        cluster="gke-analytics-europe-west1",
        dag_id="finance_daily_marts",
        task_id="dbt_build_finance",
        action_type="CREATE_GITHUB_PR_AND_MERGE",
        proposed_change="Apply column refactor patch 'fix/airdoctor-schema-drift-account_id' to https://github.com/your-org/analytics-dbt",
        code_diff="""--- a/models/marts/finance/fct_customer_churn_daily.sql
+++ b/models/marts/finance/fct_customer_churn_daily.sql
@@ -16,3 +16,3 @@
-    c.customer_account_id,
+    c.account_id,  -- Auto-patched by AirDoctor""",
        risk_level="MEDIUM",
        channel=channel
    )

    res = AirDoctorSlackApproval.dispatch_to_slack(req)

    if res.get("status") in ["DELIVERED_TO_SLACK_API", "DELIVERED_TO_SLACK_WEBHOOK"]:
        print(f"🎉 SUCCESS! Delivered live Block Kit notification to Slack!")
        print(f"  • Method: {res.get('method', 'Webhook')}")
        print(f"  • Channel: {res.get('channel')}")
        print(f"Check your Slack channel now for the interactive approval card!\n")
    else:
        print(f"⚠️ Dispatch failed: {res}\n")

if __name__ == "__main__":
    main()
