# Naming Conventions

## General rules

- `snake_case` for every Fabric item, table, column, and view.
- Exceptions: workspaces and Terraform/HCP workspaces use `kebab-case` (`senado360-dev`).
- English names in code. Portuguese source terms are kept only where they are proper nouns (`ceaps`, `transparencia`).
- Fabric item names are identical across environments. The environment lives only in the workspace name, so deployment pipelines can pair items by name.

## Fabric items

| Item | Prefix | Example |
|---|---|---|
| Workspace | `<project>-<env>` | `senado360-dev` |
| Lakehouse | `lh_` | `lh_senado_bronze`, `lh_senado_silver` |
| Warehouse | `wh_` | `wh_senado_gold` |
| Data pipeline | `pl_` | `pl_master_ingestion`, `pl_ingest_rest` |
| Notebook | `nb_<layer>_` (layer it writes to) | `nb_silver_senator`, `nb_gold_fact_expense`, `nb_silver_run`, `nb_framework_add_metadata` |
| Semantic model | `sm_` | `sm_senado360` |
| Report | `rpt_` | `rpt_senado360` |
| Data agent | `da_` | `da_senado360` |
| Connection | `conn_<system>` | `conn_transparencia_api`, `conn_github` |

## Notebook folders

Notebooks are grouped by the layer they write to: `notebooks/framework/`, `notebooks/bronze/`, `notebooks/silver/`, `notebooks/gold/`. Each layer has one orchestrator notebook, `nb_<layer>_run`, that executes that layer's entity notebooks in dependency order.

## Bronze layout

```text
Files/<source_system>/<entity>/year=<yyyy>/ingestion_date=<yyyy-mm-dd>/<file>
```

| `source_system` | Source |
|---|---|
| `senado_legislativo` | Senado Federal legislative API |
| `senado_admin` | Senado Federal administrative API |
| `transparencia` | Portal da Transparência |
| `ibge` | IBGE reference data |

Bronze Delta tables: `<source_system>__<entity>` (e.g. `senado_admin__ceaps`).

## Silver tables

Singular entity nouns: `senator`, `senator_mandate`, `senator_vote`, `expense`, `amendment`, `municipality`.

## Gold objects

| Object | Prefix | Example |
|---|---|---|
| Dimension | `dim_` | `dim_senator` |
| Fact | `fact_` | `fact_expense` |
| Analytical view | `vw_` | `vw_senator_expense_summary` |
| AI serving view | `ai_` | `ai_expense_analysis` |

- Surrogate keys: `<entity>_key` (`senator_key`). Business keys: `<entity>_id` (`senator_id`).
- Monetary columns end in `_amount`. Dates end in `_date`, timestamps in `_at` / `_timestamp`. Booleans start with `is_` / `has_`.

## Control and audit tables

All live in `lh_senado_control` (schema `dbo`).

| Table | Purpose |
|---|---|
| `etl_ingestion_config` | Metadata-driven ingestion config |
| `etl_watermark` | Incremental watermarks |
| `etl_execution_log` | Run audit log |
| `etl_landed_file` | Manifest of every file landed in Bronze (lineage, SHA-256) |
| `dq_results` | Data-quality results |
| `recon_results` | Reconciliation results |

## Technical metadata columns

Leading underscore, added by the framework: `_ingestion_timestamp`, `_ingestion_date`, `_source_system`, `_source_entity`, `_pipeline_run_id`, `_source_file`, `_source_url`, `_record_hash`, `_source_updated_at`.

## Power BI measures

Title Case in brackets, unit or `%` suffix where meaningful: `[Total CEAPS]`, `[CEAPS YoY %]`, `[Amendment Payment %]`.

## Terraform

- Resources: `snake_case`. Use `this` for the single instance of a type in a module.
- One HCP Terraform workspace per environment: `senado360-<env>`.
- Variables are described and typed. Secrets are never variables with defaults.
