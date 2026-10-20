"""Publish Fabric item definitions (notebooks, pipelines) from this repo with fabric-cicd.

Infrastructure (workspace, lakehouses, warehouse) is owned by Terraform; this script only
publishes item content. Workspace folders mirror the repo folders (notebooks/framework, ...).

Requires Python >= 3.10 (see requirements-dev.txt).

Usage (from the repo root, after `az login`):
    python deploy/deploy_fabric.py --workspace-id <id> [--environment dev] [--prune]

The workspace id is printed by `terraform output -raw workspace_id` in infra/envs/<env>.
"""

import argparse
import os
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

REPO_ROOT = Path(__file__).resolve().parents[1]
ITEM_TYPES = ["Notebook", "DataPipeline"]


def stray_files() -> list:
    """Files inside item folders that fabric-cicd would upload as definition parts
    (e.g. __pycache__ from running tests). Fabric rejects them."""
    stray = []
    for platform in REPO_ROOT.rglob(".platform"):
        if ".terraform" in platform.parts:
            continue
        for path in platform.parent.rglob("*"):
            if path.is_file() and path.name not in {".platform", "notebook-content.py", "pipeline-content.json"}:
                stray.append(path.relative_to(REPO_ROOT))
    return stray


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-id", default=os.environ.get("FABRIC_WORKSPACE_ID"), help="Target workspace id (or FABRIC_WORKSPACE_ID).")
    parser.add_argument("--environment", default="dev", help="Environment key used by parameter.yml (dev/test/prod).")
    parser.add_argument("--prune", action="store_true", help="Also delete notebooks/pipelines in the workspace that are not in the repo.")
    args = parser.parse_args()
    if not args.workspace_id:
        parser.error("--workspace-id or FABRIC_WORKSPACE_ID is required")
    stray = stray_files()
    if stray:
        parser.error("remove these files from item folders before deploying:\n  " + "\n  ".join(map(str, stray)))

    workspace = FabricWorkspace(
        workspace_id=args.workspace_id,
        environment=args.environment.upper(),
        repository_directory=str(REPO_ROOT),
        item_type_in_scope=ITEM_TYPES,
        token_credential=AzureCliCredential(),
    )
    publish_all_items(workspace)
    if args.prune:
        unpublish_all_orphan_items(workspace)


if __name__ == "__main__":
    main()
