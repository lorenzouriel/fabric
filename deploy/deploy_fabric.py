"""Publish Fabric item definitions (notebooks, pipelines) from this repo with fabric-cicd,
then apply the ingestion config by running nb_framework_bootstrap.

Infrastructure (workspace, lakehouses, warehouse) is owned by Terraform; this script only
publishes item content. Workspace folders mirror the repo folders (notebooks/framework, ...).

Requires Python >= 3.10 (see requirements-dev.txt).

Usage (from the repo root, after `az login`):
    python deploy/deploy_fabric.py --workspace-id <id> [--environment dev] [--prune] [--skip-bootstrap]

The workspace id is printed by `terraform output -raw workspace_id` in infra/envs/<env>.
"""

import argparse
import os
import time
from pathlib import Path

import requests
from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

REPO_ROOT = Path(__file__).resolve().parents[1]
ITEM_TYPES = ["Notebook", "DataPipeline"]
FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
BOOTSTRAP_NOTEBOOK = "nb_framework_bootstrap"


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


def run_notebook(workspace_id: str, notebook_name: str, credential, timeout_minutes: int = 20) -> None:
    """Run a notebook on demand through the Fabric job scheduler API and wait for it."""
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {credential.get_token(FABRIC_SCOPE).token}"

    items = session.get(f"{FABRIC_API}/workspaces/{workspace_id}/items", params={"type": "Notebook"}, timeout=60)
    items.raise_for_status()
    matches = [i for i in items.json()["value"] if i["displayName"] == notebook_name]
    if not matches:
        raise SystemExit(f"{notebook_name} not found in workspace {workspace_id}")

    start = session.post(
        f"{FABRIC_API}/workspaces/{workspace_id}/items/{matches[0]['id']}/jobs/instances",
        params={"jobType": "RunNotebook"}, timeout=60,
    )
    if start.status_code != 202:
        raise SystemExit(f"could not start {notebook_name}: HTTP {start.status_code} {start.text[:300]}")
    status_url = start.headers["Location"]
    print(f"[bootstrap] started {notebook_name}, waiting (Spark start-up takes a minute)...")

    deadline = time.monotonic() + timeout_minutes * 60
    while time.monotonic() < deadline:
        time.sleep(int(start.headers.get("Retry-After", 15)))
        job = session.get(status_url, timeout=60)
        job.raise_for_status()
        state = job.json().get("status")
        if state == "Completed":
            print(f"[bootstrap] {notebook_name} completed: ingestion config applied")
            return
        if state in ("Failed", "Cancelled", "Deduped"):
            reason = job.json().get("failureReason") or {}
            raise SystemExit(f"[bootstrap] {notebook_name} {state}: {reason.get('message', job.text[:500])}")
    raise SystemExit(f"[bootstrap] {notebook_name} did not finish within {timeout_minutes} min; check the Monitoring hub")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-id", default=os.environ.get("FABRIC_WORKSPACE_ID"), help="Target workspace id (or FABRIC_WORKSPACE_ID).")
    parser.add_argument("--environment", default="dev", help="Environment key used by parameter.yml (dev/test/prod).")
    parser.add_argument("--prune", action="store_true", help="Also delete notebooks/pipelines in the workspace that are not in the repo.")
    parser.add_argument("--skip-bootstrap", action="store_true", help=f"Publish only; do not run {BOOTSTRAP_NOTEBOOK}.")
    args = parser.parse_args()
    if not args.workspace_id:
        parser.error("--workspace-id or FABRIC_WORKSPACE_ID is required")
    stray = stray_files()
    if stray:
        parser.error("remove these files from item folders before deploying:\n  " + "\n  ".join(map(str, stray)))

    credential = AzureCliCredential()
    workspace = FabricWorkspace(
        workspace_id=args.workspace_id,
        environment=args.environment.upper(),
        repository_directory=str(REPO_ROOT),
        item_type_in_scope=ITEM_TYPES,
        token_credential=credential,
    )
    publish_all_items(workspace)
    if args.prune:
        unpublish_all_orphan_items(workspace)
    if not args.skip_bootstrap:
        run_notebook(args.workspace_id, BOOTSTRAP_NOTEBOOK, credential)


if __name__ == "__main__":
    main()
