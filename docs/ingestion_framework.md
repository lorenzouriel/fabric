# Ingestion Framework

Metadata-driven ingestion from official sources into `lh_senado_bronze` (backlog E2).
Source details and API limits: [sources.md](sources.md).

## Architecture

```text
pl_master_ingestion (Data Factory)
  │  params: mode, source_system, entity, backfill_start, backfill_end, key_vault_url
  ▼
Read Ingestion Metadata ── nb_framework_get_config ──► etl_ingestion_config (JSON exit value)
  ▼
Filter Enabled Datasets ── @equals(item().enabled, true)
  ▼
ForEach Dataset (parallel, batchCount 4)
  └─ Switch on source_type
       ├─ REST ─► nb_bronze_ingest_rest ─┐   (on Failed: nb_framework_log_failure ─► Fail)
       ├─ FILE ─► nb_bronze_ingest_file ─┤──► lh_senado_bronze/Files/<source>/<entity>/year=/ingestion_date=/
       └─ other ─► Fail                  │
                                         └──► lh_senado_control: etl_landed_file, etl_execution_log, etl_watermark
```

| Concern | Owner |
|---|---|
| Orchestration, fan-out, parallelism | Pipeline (`pl_master_ingestion`) |
| HTTP, pagination, date windows, retries, throttling | `nb_framework_common` (`HttpClient`, `run_rest_ingestion`) |
| Bulk download, checksum skip, unzip | `nb_framework_common` (`run_file_ingestion`) |
| Audit, lineage, watermarks | `ControlStore` (Delta tables in `lh_senado_control`) |
| Config | `nb_framework_config` (Git) → `nb_framework_bootstrap` → `etl_ingestion_config` |

All logic lives in `nb_framework_common`. The ingestion notebooks are thin wrappers, which
lets `tests/` unit-test the code Fabric runs (35 tests, no Spark needed).

## Items

| Item | Folder | Purpose |
|---|---|---|
| `nb_framework_common` | `notebooks/framework` | Library loaded with `%run` |
| `nb_framework_config` | `notebooks/framework` | Config rows, versioned in Git |
| `nb_framework_bootstrap` | `notebooks/framework` | Creates control tables and upserts config. Idempotent |
| `nb_framework_get_config` | `notebooks/framework` | Returns config rows to the pipeline |
| `nb_framework_log_failure` | `notebooks/framework` | On-failure handler: logs FAILED for errors outside the runners |
| `nb_bronze_ingest_rest` | `notebooks/bronze` | REST runner for one dataset |
| `nb_bronze_ingest_file` | `notebooks/bronze` | FILE runner for one dataset |
| `pl_master_ingestion` | `pipelines` | Orchestrator |

## Control tables (`lh_senado_control.dbo`)

| Table | Grain | Written by |
|---|---|---|
| `etl_ingestion_config` | one row per `source_system` + `entity` | bootstrap (MERGE; rows removed from Git get `enabled = false`) |
| `etl_watermark` | one row per dataset | REST runner, **only after a fully successful run**, never backwards |
| `etl_execution_log` | one row per dataset run | both runners (SUCCESS / FAILED / SKIPPED) |
| `etl_landed_file` | one row per file written to Bronze | both runners: path, source URL, request params, HTTP status, bytes, SHA-256, records, window |

`etl_execution_log` columns: `execution_id`, `pipeline_name`, `pipeline_run_id`, `dataset`,
`source_system`, `entity`, `mode`, `start_timestamp`, `end_timestamp`, `duration_seconds`,
`rows_read`, `rows_written`, `files_written`, `status`, `error_message`, `api_requests`,
`retry_count`, `watermark_before`, `watermark_after`, `deprecation_notice`.

`deprecation_notice` captures the Senado API's `Deprecation` / `Sunset` / `Link` headers, so
a deprecated endpoint shows up in the log before it is switched off.

## Config fields

| Field | Meaning |
|---|---|
| `source_system`, `entity` | Dataset key (`dataset = source_system.entity`) |
| `source_type` | `REST` or `FILE` |
| `base_url`, `endpoint` | Request URL |
| `load_type` | `full` (snapshot every run) or `incremental` (watermark) |
| `request_strategy` | `single` (one call) or `date_window` (one call per window) |
| `date_param_start`, `date_param_end` | Query parameter names for the window (e.g. `dataInicio`, `dataFim`) |
| `window_days` | Max days per call. Windows are also split per calendar year |
| `overlap_days` | Incremental runs restart at `watermark - overlap_days` to catch late corrections |
| `watermark_field` | Record field whose max value becomes the new watermark |
| `backfill_start` | Start date for the first incremental run and default backfill start |
| `record_path` | Dot path to the record list in the response (`""` = top-level list) |
| `request_params` | Extra static query params (JSON) |
| `pagination_type`, `page_param`, `max_pages` | `none` or `page` (stops at the first empty page) |
| `authentication_type`, `auth_header`, `secret_name` | `none` or `api_key_header` (secret read from Key Vault) |
| `rate_limit_per_second` | Client-side throttle per notebook run |
| `landing_path` | Bronze folder, must equal `<source_system>/<entity>` |

## Load behaviour

**Incremental (`mode = incremental`)**

1. Read the watermark. With none, start at `backfill_start`.
2. Request `[watermark - overlap_days, today]` in windows of at most `window_days`, never crossing a calendar year.
3. Land every response unchanged, one file per window (or page).
4. After all windows succeed: record files, commit `max(watermark_field)`, log SUCCESS.
5. On any error: log FAILED with the stack trace. The watermark and file manifest are not touched.

Overlapping windows re-land some records. Silver deduplicates with MERGE on business keys (E6).

**Failures outside the runner (on-failure handler)**

The runners log their own errors. Failures that happen before or outside them (`%run` errors,
Spark start-up, bad parameters, timeouts, cancels) are caught by the pipeline:

```text
<notebook activity> ──(Failed)──► Log … Failure (nb_framework_log_failure) ──(Completed)──► Rethrow … Failure (Fail)
```

- Applied to `Read Ingestion Metadata` (logged as dataset `__pipeline__`), `REST Ingestion` and `File Ingestion`.
- The handler writes one FAILED row with the pipeline's error message, prefixed with the activity name. If the notebook already logged a row for this `pipeline_run_id` and dataset, it writes nothing (no duplicates).
- The `Fail` activity re-raises the original error, so the run is marked **Failed**. A handler on its own would mark it Succeeded. Other datasets in the ForEach keep running.
- Limit: the handler is a notebook. If Spark or the capacity itself is down, it fails too. The run is still marked Failed, but no row is written. External alerting (E13.S1) covers that case.
- A unit test enforces this wiring for every notebook activity in every pipeline.

**Backfill (`mode = backfill`)**

Requests `[backfill_start, backfill_end]` (defaults: config `backfill_start` .. today) regardless
of the watermark. The watermark only moves forward, so a historical backfill never rewinds it.

**Full snapshot**: one request per run, no watermark.

**FILE**: downloads the file. If its SHA-256 is already in `etl_landed_file`, the run is SKIPPED. Otherwise the original file is landed plus every ZIP member under `extracted/<run_id>/`, with bytes untouched (e.g. Latin-1).

## Bronze layout

```text
lh_senado_bronze/Files/<source_system>/<entity>/year=<yyyy>/ingestion_date=<yyyy-mm-dd>/<pipeline_run_id>[_<window_start>_<window_end>][_p<page>].json
```

- `year` = window year for date windows, ingestion year for snapshots and files.
- `ingestion_date` is in **UTC** (Brasília is UTC-3, so late-evening runs land on the next date).
- Manual notebook runs use `manual-<uuid>` as the run id.

## Datasets enabled (first slice)

| Dataset | Load | Calls per run | Verified locally (2026-10-19) |
|---|---|---|---|
| `senado_legislativo.senators` | full | 1 | 81 senators |
| `senado_legislativo.senators_history` | full | 1 | 318 rows (legislatures 56-57) |
| `senado_legislativo.votes` | incremental, 366-day windows per calendar year, 7-day overlap | 1 per window | Full backfill 2019-02-01 → today: 1,146 voting events, 8 calls, 21.9 MB, ~10 s |

## Deploy and run

Prerequisites: `az login`, Terraform applied (creates `lh_senado_control`), Python ≥ 3.10 (fabric-cicd needs it; `uv` can provide one).

```powershell
# 1. Infra (adds lh_senado_control)
cd infra/envs/dev; terraform apply; $ws = terraform output -raw workspace_id; cd ../../..

# 2. Tooling
uv venv --python 3.12 .venv; uv pip install --python .venv -r requirements-dev.txt
.venv\Scripts\python -m pytest

# 3. Publish notebooks + pipeline (workspace folders mirror repo folders)
.venv\Scripts\python deploy\deploy_fabric.py --workspace-id $ws
```

Then, in the Fabric workspace:

4. Run `nb_framework_bootstrap` once. It creates the control tables and loads the config.
5. Run `pl_master_ingestion` with `mode = incremental`. The first run backfills votes from 2019-02-01.
6. Check the results:

```sql
-- lh_senado_control SQL endpoint
SELECT dataset, status, rows_read, files_written, api_requests, retry_count,
       watermark_before, watermark_after, duration_seconds, deprecation_notice
FROM dbo.etl_execution_log ORDER BY start_timestamp DESC;

SELECT * FROM dbo.etl_watermark;
```

Re-run the pipeline: votes should request only the last 7 days plus the overlap, and the watermark stays or moves forward.

## Adding a dataset

1. Add a row to `INGESTION_CONFIG` in `nb_framework_config`.
2. Run `pytest` (config validation runs there).
3. Deploy, then run `nb_framework_bootstrap`.

The next pipeline run picks the dataset up. No pipeline change is needed.
