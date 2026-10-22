# Ingestion Framework

Metadata-driven ingestion from official sources into `lh_senado_bronze` (backlog E2).
Source details and API limits: [sources.md](sources.md).

## Architecture

One notebook per **source** (the connector-per-source model of Airbyte/Fivetran/dlt), driven
by the same config table.

```text
pl_master_ingestion (Data Factory)
  │  params: mode, source_system, entity, backfill_start, backfill_end, force, key_vault_url
  ▼
Read Ingestion Metadata ── nb_framework_get_config ──► sources with enabled datasets (JSON exit value)
  ▼
ForEach Source (parallel, batchCount 4)
  └─ Switch on source_system
       ├─ senado_legislativo ─► nb_bronze_senado_legislativo ─┐  (on Failed: nb_framework_log_failure ─► Fail)
       ├─ senado_admin       ─► nb_bronze_senado_admin       ─┤
       └─ other              ─► Fail (no notebook for source) │
                                                              ▼
            inside each source notebook (run_source):
              shared HttpClient (one rate limit per API)
              for each enabled dataset, in order (fan-outs after their source dataset):
                 not due (frequency)?            → log NOT_DUE
                 snapshot identical to last one? → log SKIPPED, write nothing
                 otherwise                       → land raw files, log, advance watermark
                 failure                         → log FAILED, continue with the next dataset
              refresh etl_dataset_status for its datasets
              any dataset failed → raise at the end (activity fails, handler runs)
                                                              │
         lh_senado_bronze/Files/<source>/<entity>/year=/ingestion_date=/   ◄──┤
         lh_senado_control: etl_execution_log, etl_landed_file,              ◄──┘
                            etl_watermark, etl_dataset_status
```

| Concern | Owner |
|---|---|
| Parallelism across sources | Pipeline (`ForEach Source`) |
| Dataset order, isolation, frequency, shared rate limit | `run_source` in `nb_framework_common` |
| HTTP, pagination, date windows, retries, throttling | `HttpClient`, `run_rest_ingestion` |
| Bulk download, checksum skip, unzip | `run_file_ingestion` |
| Audit, lineage, watermarks, status | `ControlStore` (Delta tables in `lh_senado_control`) |
| Config | `nb_framework_config` (Git) → `deploy_fabric.py` runs `nb_framework_bootstrap` → `etl_ingestion_config` |

All logic lives in `nb_framework_common`. The source notebooks are ~10-line wrappers, which
lets `tests/` unit-test the code Fabric runs (101 tests, no Spark needed).

**Adding a dataset** to an existing source: config row only. **Adding a source**: config rows,
plus a `nb_bronze_<source>` notebook and a case in `Route By Source`. A unit test fails if
a configured source has no notebook or pipeline case.

## Items

| Item | Folder | Purpose |
|---|---|---|
| `nb_framework_common` | `notebooks/framework` | Library loaded with `%run` |
| `nb_framework_config` | `notebooks/framework` | Config rows, versioned in Git |
| `nb_framework_bootstrap` | `notebooks/framework` | Creates control tables and upserts config. Idempotent |
| `nb_framework_get_config` | `notebooks/framework` | Returns the sources with enabled datasets to the pipeline |
| `nb_framework_log_failure` | `notebooks/framework` | On-failure handler: logs FAILED for each dataset of a source that could not log itself |
| `nb_bronze_senado_legislativo` | `notebooks/bronze` | Runs all `senado_legislativo` datasets |
| `nb_bronze_senado_admin` | `notebooks/bronze` | Runs all `senado_admin` datasets |
| `pl_master_ingestion` | `pipelines` | Orchestrator |

Manual runs: open a source notebook and set `entities` (e.g. `votes,matters`), `mode`,
`backfill_start` / `backfill_end` and `force` in its parameters cell.

## Frequency, unchanged snapshots and dataset status

**Frequency.** Each dataset has `frequency` = `daily` (default), `weekly` or `monthly`. In
incremental mode, a dataset runs when its last successful run is older than 90% of the
interval (so a daily 06:00 run still picks up yesterday's 06:05 run). Otherwise it is logged as
`NOT_DUE`. `force = true` and `mode = backfill` run every selected dataset. Weekly today:
`party_affiliations` (318 calls) and `contract_items` (2,525 calls).

**Unchanged snapshots (write-time dedup).** For full snapshots (`single`, `fanout`), the
response is compared (SHA-256) with the dataset's last landed file. If identical, the run is
logged `SKIPPED` ("unchanged") and nothing is written. The API is still called, since comparing
needs the content. Bronze then holds one file per actual change. Fan-out keys still come from
the last file that was written.

**Dataset status (`etl_dataset_status`).** One row per dataset, refreshed by each source run
and by the failure handler: last status, last run, last success (SUCCESS or SKIPPED),
consecutive failures, last error, rows of the last load vs the median of the previous 10 loads
(`volume_flag` = LOW for a zero or a >90% drop on datasets that usually load 50+ rows; **full
snapshots only**: incremental row counts depend on each run's window and are legitimately 0 in
recess, so they show `n/a` and are judged by freshness), and
absolute deadlines `next_due_at`, `warn_at` (1.5× frequency) and `stale_at` (3× frequency),
overridable with `warn_after_hours` / `stale_after_hours`. The deadlines let a query judge
freshness at read time, even if the pipeline stops running:

```sql
-- T-SQL: lh_senado_control → SQL analytics endpoint
SELECT dataset, frequency, last_status, last_success_at, consecutive_failures, volume_flag,
       CASE WHEN last_success_at IS NULL OR SYSUTCDATETIME() > stale_at THEN 'STALE'
            WHEN SYSUTCDATETIME() > warn_at THEN 'WARN' ELSE 'OK' END AS freshness,
       last_error
FROM dbo.etl_dataset_status
ORDER BY CASE WHEN last_success_at IS NULL OR SYSUTCDATETIME() > stale_at THEN 0
              WHEN SYSUTCDATETIME() > warn_at THEN 1 ELSE 2 END, dataset;
```

```sql
-- Spark SQL: notebook cell (Fabric Spark runs in UTC, like the stored deadlines)
SELECT dataset, frequency, last_status, last_success_at, consecutive_failures, volume_flag,
       CASE WHEN last_success_at IS NULL OR current_timestamp() > stale_at THEN 'STALE'
            WHEN current_timestamp() > warn_at THEN 'WARN' ELSE 'OK' END AS freshness,
       last_error
FROM dbo.etl_dataset_status
ORDER BY CASE freshness WHEN 'STALE' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, dataset;
```

## Spark sessions and capacity

Every notebook activity would start its own Spark session. Trial capacities don't queue Spark
jobs; they reject them with **HTTP 430 `TooManyRequestsForCapacity`**. Mitigations:

- **High concurrency for pipelines** is enabled by Terraform (`fabric_spark_workspace_settings.high_concurrency.notebook_pipeline_run_enabled`).
- All notebook activities use `sessionTag = senado360_ingestion`, so they pack into a shared session.
- Notebook activities retry twice, 120 s apart, before the Failed path fires.
- Close interactive notebook sessions (Monitoring hub) before large runs. They also hold capacity.

A unit test enforces the session tag and retries on every notebook activity.

## Control tables (`lh_senado_control.dbo`)

| Table | Grain | Written by |
|---|---|---|
| `etl_ingestion_config` | one row per `source_system` + `entity` | bootstrap (MERGE; rows removed from Git get `enabled = false`) |
| `etl_watermark` | one row per dataset | REST runner, **only after a fully successful run**, never backwards |
| `etl_execution_log` | one row per dataset per run | source runner (SUCCESS / FAILED / SKIPPED / NOT_DUE) and failure handler |
| `etl_landed_file` | one row per file written to Bronze | both runners: path, source URL, request params, HTTP status, bytes, SHA-256, records, window |
| `etl_dataset_status` | one row per dataset | source runner and failure handler (see above) |

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
| `request_strategy` | `single` (one call), `date_window` (one call per window), `lookback` (incremental by "updated in the last N days"), `fanout` (one call per key), `yearly` (one call per year, year in the path) |
| `refresh_years` | `yearly`: how many recent years each incremental run re-reads (default 2) |
| `lookback_param`, `lookback_max_days` | `lookback`: parameter name and API maximum (e.g. `numdias`, 30) |
| `fanout_dataset`, `fanout_record_path`, `fanout_key_path` | `fanout`: source dataset, record path in its files, key path in each record. The endpoint contains `{key}` |
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

Inside a source notebook, each dataset's failure is logged by the runner, and the next dataset
still runs. The notebook raises at the end if any dataset failed. Failures that happen before
or outside the runners (`%run` errors, Spark start-up, bad parameters, timeouts, cancels) are
caught by the pipeline:

```text
<notebook activity> ──(Failed)──► Log … Failure (nb_framework_log_failure) ──(Completed)──► Rethrow … Failure (Fail)
```

- Applied to `Read Ingestion Metadata` (logged as dataset `__pipeline__`) and to every `Ingest <Source>` activity.
- For a source, the handler writes one FAILED row for **each enabled dataset of that source that has no row for this `pipeline_run_id`**, with the pipeline's error prefixed with the activity name. Datasets the notebook already logged are left alone (no duplicates). It then refreshes their `etl_dataset_status` rows.
- The `Fail` activity re-raises the original error, so the run is marked **Failed**. A handler on its own would mark it Succeeded. Other sources in the ForEach keep running.
- Activity retries (2 × 120 s) re-run the source notebook. Datasets that already succeeded in this run are not due any more, so a retry effectively re-runs only what failed (unless `force = true`).
- Limit: the handler is a notebook. If Spark or the capacity itself is down, it fails too. The run is still marked Failed, but no row is written. External alerting (E13.S1) covers that case.
- A unit test enforces this wiring for every notebook activity in every pipeline.

**Backfill (`mode = backfill`)**

Requests `[backfill_start, backfill_end]` (defaults: config `backfill_start` .. today) regardless
of the watermark. The watermark only moves forward, so a historical backfill never rewinds it.

**Lookback (`request_strategy = lookback`)**

For APIs that filter by "updated in the last N days" (Senado `/processo?numdias=N`, max 30):

- First run, or `mode = backfill`: date windows on `date_param_start` / `date_param_end` (e.g. presentation date), as for `date_window`.
- Incremental with a watermark: one request with `lookback_param = (today - watermark) + overlap_days`.
- If that exceeds `lookback_max_days`, the run **fails** with `LookbackGapError` ("gap of N days … run mode=backfill"), and no request is made. Silently capping it would lose updates.

**Fan-out (`request_strategy = fanout`)**

- Keys come from the latest SUCCESS run of `fanout_dataset` that wrote files (`etl_execution_log` + `etl_landed_file`). Each `.json` file is read back, and the distinct values at `fanout_record_path` → `fanout_key_path` become the keys.
- `fanout_dataset` must belong to the same source, so it runs earlier in the same source notebook.
- One request per key (`{key}` in the endpoint), throttled by the source's shared client.
- Output is **one JSON Lines file per run**. Each line is an envelope `{"key", "source_url", "http_status", "payload"}` with the untouched API payload. This avoids hundreds of tiny files.
- A 404 for a key is recorded in its envelope and the run continues. Any other error fails the run.

**Yearly (`request_strategy = yearly`)**

For APIs that return a whole year per call with the year in the path (Senado CEAPS
`/senadores/despesas_ceaps/{year}`) and have no updated-at field:

- First run: every year from `backfill_start` to the current year.
- Incremental: the last `refresh_years` years up to the current one. If runs were missed, it extends back to the watermark year, so a long pause never skips a year.
- Backfill: `backfill_start` .. `backfill_end` years. It may go earlier than the configured start (e.g. CEAPS 2008).
- Watermark type `YEAR` = max of `watermark_field` (e.g. `ano`).
- Past years are re-landed as full snapshots. Silver MERGEs them on the business key, so corrections and deletions in recent years are picked up.

**Full snapshot**: one request per run, no watermark.

**FILE**: downloads the file. If its SHA-256 is already in `etl_landed_file`, the run is SKIPPED. Otherwise the original file is landed plus every ZIP member under `extracted/<run_id>/`, with bytes untouched (e.g. Latin-1).

## Bronze layout

```text
lh_senado_bronze/Files/<source_system>/<entity>/year=<yyyy>/ingestion_date=<yyyy-mm-dd>/<pipeline_run_id>[_<window_start>_<window_end>][_p<page>].json
```

- `year` = window year for date windows, ingestion year for snapshots and files.
- `ingestion_date` is in **UTC** (Brasília is UTC-3, so late-evening runs land on the next date).
- Manual notebook runs use `manual-<uuid>` as the run id.

## Datasets enabled

All `senado_legislativo.*`. Local runs against the live API on 2026-10-19/20.

| Dataset | Strategy | Calls per run | First run (local) |
|---|---|---|---|
| `senators` | single, full | 1 | 81 senators |
| `senators_history` | single, full | 1 | 318 senators (legislatures 56-57), **mandates embedded** (`Mandatos`) |
| `parties` | single, full | 1 | 108 parties (active + extinct) |
| `party_affiliations` | **fanout** over `senators_history` | 318 | 477 affiliations, ~64 s |
| `committees` | single, full | 1 | 217 active committees |
| `committee_composition_permanent` / `_cpi` / `_temporary` | single, full | 1 each | 16 / 2 / 5 committees with blocs, parties, members, roles |
| `votes` | date_window, 7-day overlap | 1 per window | Backfill since 2019-02-01: 1,146 events, 8 calls, 21.9 MB |
| `matters` | **lookback** (`numdias`), backfill by presentation date | 1 (incremental) | Backfill since 2019-02-01: 39,333 matters, 8 calls, 32.4 MB, ~24 s. Incremental (3 days): 124 matters, 1 call |
| `rapporteurships` | date_window by designation date, 30-day overlap | 1 per window | Backfill since 2019-02-01: 13,684 designations, 8 calls, 18.2 MB, ~20 s |

`senado_admin.*` (local run on 2026-10-21):

| Dataset | Strategy | Calls per run | First run (local) |
|---|---|---|---|
| `ceaps` | **yearly**, `refresh_years = 2` | 8 (backfill), then 2 | 149,258 expenses 2019–2026, 65 MB, ~7 s. 192 senators. 247 negative values (refunds/reversals) |
| `contracts` | single, full | 1 | 2,525 contracts |
| `procurement` | single, full | 1 | 2,767 procurement processes |
| `suppliers` | single, full | 1 | 2,944 suppliers |
| `contract_items` | **fanout** over `contracts` | 2,525 | 465 contracts with items (2,986 items), 2,060 × 404, ~8.5 min |

Plenary sessions have no dataset of their own: every voting event already carries the session
(`codigoSessao`, `dataSessao`, `siglaTipoSessao`, `numeroSessao`). Silver derives
`legislative_session` from votes.

## Deploy and run

Prerequisites: `az login`, Terraform applied (creates `lh_senado_control`), Python ≥ 3.10 (fabric-cicd needs it; `uv` can provide one).

```powershell
# 1. Infra (adds lh_senado_control)
cd infra/envs/dev; terraform apply; $ws = terraform output -raw workspace_id; cd ../../..

# 2. Tooling
uv venv --python 3.12 .venv; uv pip install --python .venv -r requirements-dev.txt
.venv\Scripts\python -m pytest

# 3. Publish notebooks + pipeline (workspace folders mirror repo folders),
#    then run nb_framework_bootstrap through the Fabric job API (config applied)
.venv\Scripts\python deploy\deploy_fabric.py --workspace-id $ws
#    --skip-bootstrap: publish only (code change, no config change)
#    --prune: also delete notebooks/pipelines that are no longer in the repo
```

Then, in the Fabric workspace:

4. Run `pl_master_ingestion` with `mode = incremental` (`force = true` to ignore frequencies).
5. Check health with the `etl_dataset_status` query above, and the details with:

```sql
-- lh_senado_control SQL endpoint
SELECT dataset, status, rows_read, files_written, api_requests, retry_count,
       watermark_before, watermark_after, duration_seconds, error_message
FROM dbo.etl_execution_log ORDER BY start_timestamp DESC;
```

## Adding a dataset

1. Add a row to `INGESTION_CONFIG` in `nb_framework_config` (set `frequency` if not daily).
2. Run `pytest` (config validation runs there).
3. Deploy. The deploy script applies the config.

The next pipeline run picks the dataset up. A new **source** also needs a `nb_bronze_<source>`
notebook and a case in `Route By Source` (enforced by a test).
