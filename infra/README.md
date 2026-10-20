# Infrastructure (Terraform)

Terraform provisions the Microsoft Fabric platform for Senado 360.

## Ownership boundary

| Managed by Terraform | Managed by Fabric Git integration |
|---|---|
| Capacity lookup, workspaces, lakehouses, warehouse, role assignments, connections, Git link, deployment pipelines, Key Vault, identities | Notebooks, data pipelines, semantic model, reports, SQL objects (tables/views) |

Never manage the same item from both sides.

## Layout

```text
infra/
├── modules/
│   └── fabric_platform/     # workspace + lh_senado_bronze + lh_senado_silver + wh_senado_gold + roles
└── envs/
    └── dev/                 # root module, HCP Terraform workspace "senado360-dev"
```

TEST and PROD will be added as `envs/test` and `envs/prod`, each with its own HCP workspace and state.

## Resources (DEV)

| Resource | Name |
|---|---|
| Capacity | existing, looked up by `capacity_name` (read-only) |
| Workspace | `senado360-dev` |
| Lakehouse | `lh_senado_bronze` (schemas enabled) |
| Lakehouse | `lh_senado_silver` (schemas enabled) |
| Lakehouse | `lh_senado_control` (schemas enabled): ingestion config, watermarks, execution log, landed-file manifest |
| Warehouse | `wh_senado_gold` (collation `Latin1_General_100_CI_AS_KS_WS_SC_UTF8`) |

Lakehouse schemas and warehouse collation are fixed at creation. Changing them later recreates the item and its data.

## Capacity

| Property | Value |
|---|---|
| Type | Fabric trial (SKU `FTL64`, F64-equivalent) |
| Region | Brazil South |
| Tenant | `lorenzouriel394gmail.onmicrosoft.com` |
| Started | 2026-10-19 |
| Expires | ~2026-12-18 (60-day trial) |

Constraints:

- **Expiry:** workspaces on an expired trial become read-only and later lose access to their items. Before expiry, move to an F-SKU: set `capacity_name` to the new capacity and apply. Item data stays in OneLake.
- **No pause/scale control:** trial capacities aren't Azure resources, so they can't be paused, resized, or created by Terraform.
- **AI features:** some Copilot / AI features, possibly including the Fabric Data Agent (E11), are not supported on trial capacities. Verify this before E11.
- **TEST/PROD:** one trial per user. Run TEST/PROD as separate workspaces on the same capacity, or on a paid F-SKU, which can be added to Terraform with `azurerm_fabric_capacity`.
- **SKU guard:** the module rejects non-Fabric SKUs (e.g. Power BI `PP3`) at plan time.

## Prerequisites

1. **Terraform** `>= 1.8`.
2. **Azure CLI**: `winget install -e --id Microsoft.AzureCLI`.
3. **Fabric capacity**: an active Fabric trial or F-SKU capacity. Find its display name in the Fabric admin portal or under workspace settings → License info.
4. **HCP Terraform**:
   - Create (or reuse) an organization at <https://app.terraform.io>.
   - Create a workspace named `senado360-dev` (CLI-driven workflow).
   - Set **Settings → General → Execution Mode = Local**. Runs happen on your machine using your `az login` session; HCP only stores state and locks it. With Remote execution the plan fails, because HCP runners have no Azure credentials.

## First run

```bash
az login                                  # use the account that has access to the capacity
terraform login                           # stores an HCP Terraform API token locally
export TF_CLOUD_ORGANIZATION="<your-hcp-org>"   # PowerShell: $env:TF_CLOUD_ORGANIZATION = "<your-hcp-org>"

cd infra/envs/dev
cp terraform.tfvars.example terraform.tfvars    # set capacity_name (and optional role assignments)

terraform init
terraform plan
terraform apply
```

Run `terraform plan` again afterwards. It should report **No changes**.

The account that runs `apply` becomes workspace Admin automatically. Grant everyone else access through `role_assignments` (prefer Entra groups).

## Roadmap

- **E1.S3 (later):** `fabric_connection` (GitHub) + `fabric_workspace_git`.
- **E13.S2:** Key Vault and source connections (adds `azurerm`); set `enable_workspace_identity = true`.
- **E13.S4:** move to a service principal with GitHub OIDC, switch HCP to remote or agent execution, and add plan-on-PR and apply-on-merge.
