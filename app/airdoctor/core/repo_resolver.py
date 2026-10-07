"""
AirDoctor Git Repository Resolver.
Determines the exact Git repository, branch, and file path to target
when synthesizing code patches and opening automated Pull Requests.

Resolution Hierarchy:
1. GKE Pod / Task Environment (GIT_SYNC_REPO, GIT_SYNC_BRANCH annotations)
2. dbt Manifest metadata in S3 Data Lake (original_file_path, project_name)
3. Airflow DAG Tags & Labels (e.g. repo:analytics-dbt)
4. Enterprise Service Catalog (config/repo_catalog.json)
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger("airdoctor.repo_resolver")

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "repo_catalog.json"))


@dataclass
class ResolvedRepository:
    repo_name: str
    repo_url: str
    default_branch: str
    target_file_path: str
    discovery_method: str
    team_owner: str
    slack_channel: str


class AirDoctorRepoResolver:
    """Dynamically resolves Git repositories for failing Airflow pipelines."""

    def __init__(self, catalog_path: Optional[str] = None):
        self.catalog_path = catalog_path or CONFIG_PATH
        self.catalog = self._load_catalog()

    def _load_catalog(self) -> Dict[str, Any]:
        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, "r") as f:
                return json.load(f).get("repositories", {})
        return {}

    def resolve(
        self,
        dag_id: str,
        task_id: str,
        task_metadata: Optional[Dict[str, Any]] = None,
        dbt_manifest: Optional[Dict[str, Any]] = None,
        failing_file_hint: Optional[str] = None
    ) -> ResolvedRepository:
        """
        Resolves the target repository and branch using the multi-tier resolution engine.
        """
        task_metadata = task_metadata or {}
        env_vars = task_metadata.get("env_vars", {})

        # 1. Tier 1: Check GKE Pod git-sync environment variables
        if "GIT_SYNC_REPO" in env_vars:
            repo_url = env_vars["GIT_SYNC_REPO"]
            branch = env_vars.get("GIT_SYNC_BRANCH", "main")
            return ResolvedRepository(
                repo_name=repo_url.split("/")[-1].replace(".git", ""),
                repo_url=repo_url,
                default_branch=branch,
                target_file_path=failing_file_hint or f"models/{task_id}.sql",
                discovery_method="GKE Pod git-sync Environment (GIT_SYNC_REPO)",
                team_owner="@data-engineering",
                slack_channel="#demo-aws-agentcore-hackathon-302026"
            )

        # 2. Tier 2: Check dbt Manifest lineage in S3
        if dbt_manifest and "nodes" in dbt_manifest:
            for node_id, node_meta in dbt_manifest["nodes"].items():
                if task_id in node_id or (failing_file_hint and node_meta.get("original_file_path") == failing_file_hint):
                    orig_path = node_meta.get("original_file_path", failing_file_hint)
                    return ResolvedRepository(
                        repo_name="analytics-dbt",
                        repo_url="https://github.com/deliveryhero/analytics-dbt",
                        default_branch="main",
                        target_file_path=orig_path,
                        discovery_method="dbt Manifest S3 Lineage (manifest.json)",
                        team_owner="@analytics-engineering",
                        slack_channel="#demo-aws-agentcore-hackathon-302026"
                    )

        # 3. Tier 3: Check Airflow DAG Tags & Metadata
        dag_tags = task_metadata.get("dag_tags", [])
        for tag in dag_tags:
            if tag.startswith("repo:"):
                repo_name = tag.split("repo:")[-1]
                if repo_name in self.catalog:
                    entry = self.catalog[repo_name]
                    return ResolvedRepository(
                        repo_name=entry["repo_name"],
                        repo_url=entry["repo_url"],
                        default_branch=entry.get("default_branch", "main"),
                        target_file_path=failing_file_hint or entry.get("code_subpath", ""),
                        discovery_method="Airflow DAG Tag (repo:*)",
                        team_owner=entry.get("team_owner", "@oncall"),
                        slack_channel=entry.get("slack_channel", "#demo-aws-agentcore-hackathon-302026")
                    )

        # 4. Tier 4: Enterprise Service Catalog Mapping (Matching DAG ID)
        for repo_key, repo_info in self.catalog.items():
            if dag_id in repo_info.get("matching_dags", []):
                file_path = failing_file_hint or os.path.join(
                    repo_info.get("code_subpath", ""),
                    f"{task_id}.sql"
                )
                return ResolvedRepository(
                    repo_name=repo_info["repo_name"],
                    repo_url=repo_info["repo_url"],
                    default_branch=repo_info.get("default_branch", "main"),
                    target_file_path=file_path,
                    discovery_method="Enterprise Service Catalog (config/repo_catalog.json)",
                    team_owner=repo_info.get("team_owner", "@data-platform"),
                    slack_channel=repo_info.get("slack_channel", "#demo-aws-agentcore-hackathon-302026")
                )


        return ResolvedRepository(
            repo_name="analytics-dbt",
            repo_url="https://github.com/deliveryhero/analytics-dbt",
            default_branch="main",
            target_file_path=failing_file_hint or f"models/{task_id}.sql",
            discovery_method="Default Analytics Engineering Repository",
            team_owner="@oncall-data-sre",
            slack_channel="#demo-aws-agentcore-hackathon-302026"
        )
