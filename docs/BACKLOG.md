# Senado 360 — Delivery Backlog

Derived from [PRD.md](PRD.md). Organized as **Epics → Stories → Subtasks**.

**Conventions**

- IDs: `E<epic>` · `E<epic>.S<story>` · subtasks are checkboxes.
- Priority follows PRD §47–49: **M** = Must Have (MVP), **S** = Should Have, **C** = Could Have.
- Phase follows the PRD §51 MVP delivery sequence (P1–P7).
- Each story lists acceptance criteria (AC) that define "done".

---

## Epic Overview

| ID | Epic | Priority | Phase | PRD refs |
|---|---|---|---|---|
| E1 | Platform Foundation (Terraform IaC) | M | P1 | §7, §43, §44, §45, §51 |
| E2 | Ingestion Framework | M | P2 | §11, §12, §14, §20, §21 |
| E3 | Senado Legislative Ingestion | M | P2 | §8 |
| E4 | Senado Administrative Ingestion | M | P2 | §9 |
| E5 | Portal da Transparência Ingestion | M | P2 | §10 |
| E6 | Silver Layer — Transformation & Conformed Entities | M | P3 | §15, §16, §17 |
| E7 | Data Quality & Reconciliation | M (DQ) / S (recon) | P3 | §18, §19, §42 |
| E8 | Gold Warehouse — Dimensional Model | M | P4 | §22–24 |
| E9 | Governed Analytical Layer (Metrics, Views, AI Views) | M / S | P4 | §25–29 |
| E10 | Power BI Semantic Model & Report | M | P5 | §30–32 |
| E11 | Fabric Data Agent | M | P6 | §33–35, §40 |
| E12 | LangGraph Orchestration & Deterministic Tools | S | P6 | §36–40 |
| E13 | End-to-End Orchestration, Security & CI/CD | M | P1–P6 | §12, §43, §44 |
| E14 | Documentation & Portfolio | M | P7 | §41, §45, §46, §47, §52, §53 |
| E15 | Future Extensions | C | — | §49 |

**Day-one IaC:** E1.S0, E1.S1, E1.S3, E13.S2 and E13.S4 run first, so every Fabric container exists in Terraform before any pipeline or notebook is built.

**Critical path (MVP vertical slice):** E1 → E2 → E3/E4/E5 (senators, votes, CEAPS, amendments) → E6 → E7 (DQ) → E8 → E9 → E10 + E11 → E14.

---

## E1 — Platform Foundation (Infrastructure as Code)

**Goal:** Terraform provisions the Fabric workspace, storage layers, identities, and source control from day one, so every environment is reproducible from code.

**Ownership boundary:** Terraform owns *infrastructure and containers*: capacity, workspaces, lakehouses, warehouse, role assignments, connections, Git integration, deployment pipelines, Key Vault, identities. Fabric Git integration owns *item content*: notebooks, pipelines, semantic model, reports, SQL objects. The two never manage the same item.

### E1.S0 — Terraform bootstrap `M` `P1`
AC:
- `terraform init/plan/apply` runs from `infra/` against DEV with remote state and no secrets in code.
- Tenant prerequisites are documented and verified.

Decisions (2026-10-19): state in **HCP Terraform** (local execution mode), auth via **Azure CLI** user login for now, **existing capacity** looked up read-only, Git link deferred.

Subtasks:
- [x] Create `infra/` layout: `modules/fabric_platform`, `envs/dev` (test/prod added in E13.S3)
- [x] Pin provider version `microsoft/fabric ~> 1.14` (`azurerm`/`azuread` added when needed in E13.S2)
- [x] Configure HCP Terraform `cloud` block (workspace `senado360-dev`, org via `TF_CLOUD_ORGANIZATION`)
- [x] Create HCP org/workspace and set execution mode to **Local** (manual)
- [x] Provider auth via `use_cli = true`
- [ ] (E13.S4) Deployment service principal plus GitHub OIDC, and the tenant setting "Service principals can use Fabric APIs"
- [x] Add `terraform fmt`, `tflint`, `gitleaks` to pre-commit
- [x] Write `infra/README.md` (bootstrap runbook, how to apply per environment)

### E1.S1 — Provision Fabric capacity, workspace and storage items via Terraform `M` `P1`
AC:
- A DEV `terraform apply` creates the workspace, `lh_senado_bronze`, `lh_senado_silver`, `wh_senado_gold`.
- Re-running `apply` produces no changes (idempotent).
- Workspace access is granted only through Terraform-managed role assignments.

Subtasks:
- [x] Fabric capacity: `data "fabric_capacity"` by display name (existing trial/F-SKU), with a postcondition that fails if it is not Active
- [x] `fabric_workspace` (`senado360-dev`); workspace identity behind `enable_workspace_identity` (turn on in E13.S2)
- [x] `fabric_lakehouse` × 2 (Bronze, Silver) with schemas enabled
- [x] `fabric_warehouse` (Gold, collation `Latin1_General_100_CI_AS_KS_WS_SC_UTF8`)
- [x] `fabric_workspace_role_assignment` driven by the `role_assignments` map
- [x] Output item IDs and the warehouse connection string
- [x] First `terraform apply` on DEV (2026-10-19, trial capacity `FTL64`, Brazil South)
- [x] Second `terraform plan` shows no changes (idempotency check, 2026-10-19)
- [x] Document capacity limits and constraints that affect TEST/PROD (`infra/README.md` → Capacity)

### E1.S2 — Define naming conventions `M` `P1`
AC:
- Naming convention doc covers pipelines, notebooks, lakehouses, tables, views, measures, and technical columns.

Subtasks:
- [x] Define prefixes (`pl_`, `nb_`, `lh_`, `wh_`, `dim_`, `fact_`, `vw_`, `ai_`, `dq_`, `etl_`)
- [x] Define Bronze folder and partition layout (`source=/entity=/year=/ingestion_date=`)
- [x] Define Silver/Gold table and column casing rules
- [x] Define technical metadata column names (`_ingestion_timestamp`, `_record_hash`, …)
- [x] Publish as `docs/naming_conventions.md`

### E1.S3 — Repository scaffold and Git integration (Terraform-managed) `M` `P1`
AC:
- GitHub repo follows the PRD §45 structure plus `infra/`.
- `fabric_workspace_git` connects the workspace to Git.
- `.gitignore` and secret-scanning prevent committed secrets and Terraform state.

Subtasks:
- [x] GitHub repo (`lorenzouriel/fabric`)
- [x] Scaffold folders: `infra/`, `architecture/`, `pipelines/`, `notebooks/{framework,bronze,silver,gold}`, `sql/{dimensions,facts,analytical_views,ai_views}`, `semantic-model/`, `powerbi/`, `agent/{fabric-agent,langgraph}`, `tests/`, `config/`, `docs/decisions/`
- [x] Move `PRD.md` into `docs/`
- [ ] (deferred) Create the GitHub connection with `fabric_connection` (a `ConfiguredConnection` is required when Terraform authenticates as a service principal)
- [ ] (deferred) `fabric_workspace_git` (GitHub, branch per environment, `directory_name` for Fabric items, initialization strategy documented)
- [x] Add `.gitignore` (`*.tfstate*`, `.terraform/`, `*.tfvars`) and pre-commit secret scan (gitleaks)
- [x] PR template (`.github/pull_request_template.md`)

### E1.S4 — Source discovery and API profiling `M` `P1`
AC:
- Each source has its endpoints, pagination, rate limits, auth, formats, and history depth documented.

Subtasks:
- [x] Profile Senado Legislative API (senators, mandates, parties, committees, matters, sessions, votes)
- [x] Profile Senado Administrative API (CEAPS, suppliers, contracts, procurement)
- [x] Profile Portal da Transparência API (API key, rate limits) and bulk-download catalog for amendments
- [x] Identify IBGE municipality reference source
- [x] Record findings in `docs/sources.md`

---

## E2 — Ingestion Framework

**Goal:** Reusable, metadata-driven ingestion with auditing, watermarks, retries, and technical metadata.

Decisions (2026-10-19):
- **Hybrid engine:** the pipeline orchestrates and Python notebooks do the HTTP/file work.
- **Control tables** live in a new `lh_senado_control` lakehouse.
- **Deployment** uses `fabric-cicd`.
- **First slice:** senators (full) + votes (incremental).

Design and runbook: `docs/ingestion_framework.md`.

### E2.S0 — Deploy and first run in Fabric `M` `P2`
AC:
- Items are published, control tables exist, and `pl_master_ingestion` lands senators + votes in Bronze.
- A second run is incremental.

Subtasks:
- [x] `terraform apply` (adds `lh_senado_control`)
- [x] `deploy/deploy_fabric.py` publishes notebooks + pipeline
- [x] Run `nb_framework_bootstrap`
- [x] Run `pl_master_ingestion` (2026-10-20, run `a15e8086`): 10 files landed (senators, senators_history, votes 2019–2026, sizes identical to the local test run)
- [x] Control tables written: 3 execution-log rows, 3 landed-file commits, 1 watermark commit (votes)
- [x] A re-run requests only the overlap window (`[watermark - 7d, today]`): votes 1 call, 1 file, 3 rows, watermark 2026-10-15 → 2026-10-15
- [x] Confirm the magic cell format: Fabric stores `%run x` as a bare line (verified via getDefinition). `# MAGIC` is mis-parsed
- [x] Confirm `%run` resolves notebooks by name across workspace folders (first bootstrap run)

### E2.S1 — Ingestion metadata configuration table `M` `P2`
AC:
- Config table holds every PRD §11.1 field.
- Enabled rows drive what the pipelines ingest.

Subtasks:
- [x] Design schema: PRD §11.1 fields plus `request_strategy`, `request_params`, `record_path`, date-window, pagination, auth, and rate-limit fields
- [x] Decide storage location: Delta in `lh_senado_control`; source of truth in Git (`nb_framework_config`), applied by `nb_framework_bootstrap` (MERGE; removed rows are disabled)
- [x] Create DDL and seed rows (senators, senators_history, votes). Other MVP entities follow in E3–E5
- [x] Config validation (unit-tested)
- [ ] Add per-environment config overrides (with TEST/PROD, E13.S3)

### E2.S2 — Watermark management `M` `P2`
AC:
- `etl_watermark` exists.
- Watermarks advance only after a successful run.
- A failed run leaves the watermark unchanged.

Subtasks:
- [x] Create `etl_watermark` (`dataset`, `watermark_type`, `last_successful_value`, `last_successful_execution`, `updated_at`)
- [x] "Get watermark" → next range = `watermark - overlap_days` .. today
- [x] "Commit watermark" only after all requests and file writes succeed. MERGE never moves it backwards
- [x] Test: a forced failure does not advance the watermark

### E2.S3 — Execution logging / observability `M` `P2`
AC:
- Every dataset run writes one `etl_execution_log` row with the required fields.
- Failures capture `error_message`.

Subtasks:
- [x] Create `etl_execution_log` with required and optional fields (`api_requests`, `retry_count`, `watermark_before/after`) plus `deprecation_notice`
- [x] Reusable notebook utility (`ControlStore.log_execution`, one row per run)
- [x] Capture row counts (read/written), files written, and duration
- [x] Failure paths log status `FAILED` with error and stack trace
- [x] Pipeline on-failure handler (`nb_framework_log_failure` + `Fail` rethrow) logs failures outside the runners (`%run`, Spark start-up, bad params, timeouts) without duplicates, and keeps the run marked Failed
- [x] Verify in Fabric (2026-10-20): invalid `mode` → 3 × (REST Ingestion ❌ → Log REST Failure ✅ → Rethrow ❌), ForEach and run Failed, other iterations kept running
- [ ] Confirm the 3 FAILED rows (with `[pipeline activity 'REST Ingestion']` prefix) in `etl_execution_log`
- [x] `etl_landed_file` manifest (path, source URL, params, status, bytes, SHA-256, records, window)

### E2.S4 — REST ingestion component `M` `P2`
AC:
- A generic, parameterized REST ingestion handles pagination, retries with backoff, and auth from the config.
- Raw responses land in Bronze unchanged.

Subtasks:
- [x] Pagination driven by `pagination_type` (`none`, `page`). `offset` will be added when a source needs it
- [x] Date-window strategy (≤ `window_days`, split per calendar year) for the Senado 1-year limit
- [x] Retry with exponential backoff, HTTP 429/5xx handling, `Retry-After`; client-side throttle
- [x] Read secrets from Key Vault via `notebookutils.credentials` (never inline). Vault itself comes in E13.S2
- [x] Write raw payload to the Bronze partition path
- [x] Count API requests and retries for the log
- [x] Verified locally against the live Senado API (senators 81, history 318, votes backfill 1,146 events)

### E2.S5 — File (bulk) ingestion component `M` `P2`
AC:
- Bulk files (CSV/ZIP) download, unzip, and land in Bronze with the source filename preserved.

Subtasks:
- [x] Download step parameterized by URL (follows redirects)
- [x] Unzip. Bytes are stored untouched; encoding (Latin-1 vs UTF-8) is handled at the Bronze → Delta step (E6)
- [x] Land the original file plus extracted members under Bronze partitions
- [x] Skip files already ingested (SHA-256 in `etl_landed_file`)

### E2.S6 — Technical metadata enrichment `M` `P2`
AC:
- Bronze and downstream tables carry `_ingestion_timestamp`, `_ingestion_date`, `_source_system`, `_source_entity`, `_pipeline_run_id`, `_source_file`, `_source_url`, `_record_hash` (and `_source_updated_at` where available).

Subtasks:
- [x] Lineage captured at landing time in `etl_landed_file` (feeds `_source_file`, `_source_url`, `_pipeline_run_id`)
- [ ] Shared helper that adds the `_` columns when raw files are parsed into Delta (with E6.S1)
- [ ] Define `_record_hash` algorithm (columns, ordering, null handling)
- [ ] Apply to all Bronze → Delta loads

### E2.S7 — Master ingestion pipeline `pl_master_ingestion` `M` `P2`
AC:
- The pipeline reads the config, filters enabled rows, runs a ForEach with a REST or FILE branch, lands Bronze, and writes the audit record.

Subtasks:
- [x] Read metadata (`nb_framework_get_config`, exit value; avoids environment-specific lakehouse IDs in a Lookup)
- [x] Filter on `enabled = true`
- [x] ForEach with Switch on `source_type` (REST / FILE / Fail)
- [x] Invoke notebooks `nb_bronze_ingest_rest` / `nb_bronze_ingest_file` (instead of child pipelines)
- [x] Concurrency: `batchCount 4`, plus a per-dataset client throttle (5 req/s)
- [x] Audit/watermark steps run per item inside the notebook

### E2.S8 — Historical backfill mode `M` `P2`
AC:
- Backfill can run year by year (e.g. 2022–2026) without touching incremental watermarks until it completes.

Subtasks:
- [x] `mode` parameter (backfill / incremental) plus `backfill_start` / `backfill_end`
- [x] One request per year window. Each window is tracked in `etl_landed_file`
- [x] Backfill never rewinds the watermark
- [x] Document the backfill runbook (`docs/ingestion_framework.md`)

---

## E3 — Senado Legislative Ingestion

**Goal:** Land legislative entities in Bronze. Reference entities use full snapshots; transactional entities load incrementally.

### E3.S1 — Senators, mandates and parties (full snapshot) `M` `P2`
AC:
- Current and historical senators, mandates, and parties land in Bronze on every run.

Subtasks:
- [ ] Add config rows for senators, mandates, parties
- [ ] Run full-snapshot ingestion and verify payload completeness
- [ ] Capture historical legislatures (not only current members)

### E3.S2 — Committees and committee membership (full snapshot) `M` `P2`
Subtasks:
- [ ] Add config rows for committees and membership
- [ ] Ingest and validate against the Senate portal counts

### E3.S3 — Votes and senator votes (incremental) `M` `P2`
AC:
- Votes load incrementally by date watermark. Re-runs do not duplicate records. Backfill covers the chosen history window.

Subtasks:
- [ ] Add a config row with `load_type = incremental` and the watermark field
- [ ] Ingest voting sessions plus individual senator votes
- [ ] Backfill history (define start year)
- [ ] Validate that incremental re-runs are idempotent

### E3.S4 — Legislative matters and sessions (incremental) `M` `P2`
Subtasks:
- [ ] Config rows for matters and sessions
- [ ] Incremental ingestion by update date
- [ ] Capture authorship and rapporteurs where exposed

### E3.S5 — Legislative activity (authorship, rapporteurship, actions) `C` `P2`
Subtasks:
- [ ] Identify endpoints
- [ ] Ingest to Bronze (feeds the optional `fact_legislative_activity`)

---

## E4 — Senado Administrative Ingestion

**Goal:** Land CEAPS and procurement data year by year, then incrementally for recent periods.

### E4.S1 — CEAPS expenses (historical + incremental) `M` `P2`
AC:
- CEAPS is loaded for 2022–current year. The current year refreshes incrementally. Source period and ingestion date are tracked per run.

Subtasks:
- [ ] Config row for CEAPS (year-partitioned)
- [ ] Year-by-year backfill 2022–2026
- [ ] Incremental refresh of current and recent periods (handle late corrections to prior months)
- [ ] Record the period in `etl_execution_log`

### E4.S2 — Suppliers `S` `P2`
Subtasks:
- [ ] Ingest suppliers (CNPJ/CPF identifiers, names)

### E4.S3 — Contracts, procurement, contract items and payments `S` `P2`
Subtasks:
- [ ] Ingest contracts
- [ ] Ingest procurement processes
- [ ] Ingest contract items and payments where available

---

## E5 — Portal da Transparência Ingestion

**Goal:** Land parliamentary amendments from historical bulk files plus the REST API for recent data.

### E5.S1 — Amendments historical bulk load `M` `P2`
AC:
- All available historical amendment bulk files land in Bronze with the original files preserved.

Subtasks:
- [ ] Config row `transparencia | amendments | FILE | historical`
- [ ] Download and land bulk datasets for each year
- [ ] Ingest expenditure-document files if published separately

### E5.S2 — Amendments incremental via REST API `M` `P2`
AC:
- Recent amendments load through the API using a secured API key. Overlap with bulk data is handled downstream.

Subtasks:
- [ ] Register the API key and store it in Key Vault
- [ ] Config row `transparencia | amendments | REST | incremental`
- [ ] Implement rate-limit-aware pagination
- [ ] Define the bulk/API boundary rule (cut-off date)

### E5.S3 — Municipality reference (IBGE) `M` `P2`
AC:
- The IBGE municipality and state reference lands in Bronze (codes, names, UF, population for per-capita).

Subtasks:
- [ ] Ingest IBGE municipalities and states
- [ ] Ingest population estimates (for Municipal Amendment Per Capita)

---

## E6 — Silver Layer: Transformation & Conformed Entities

**Goal:** Trusted, normalized, deduplicated Delta entities, built idempotently with MERGE.

### E6.S1 — Bronze → Silver transformation framework `M` `P3`
AC:
- A reusable PySpark framework provides schema validation, standardization, dedup, MERGE upserts, and logging. Re-runs produce no duplicate business records.

Subtasks:
- [ ] Build `nb_framework_silver_base` (read Bronze increment, apply transforms, MERGE on business key)
- [ ] Shared helpers: column snake_case, type casting, date normalization (BR formats), monetary normalization (`1.234,56` → decimal)
- [ ] Schema evolution handling (`mergeSchema`) with a policy for breaking changes
- [ ] Incremental processing based on `_ingestion_timestamp` checkpoints
- [ ] Table maintenance (OPTIMIZE / V-Order / VACUUM schedule)
- [ ] Unit tests for helpers (`tests/`)

### E6.S2 — Reference normalization: party, state, municipality `M` `P3`
AC:
- Parties standardized (acronym and name variants mapped). States normalized to UF. Municipalities matched to an IBGE code with the match rate reported.

Subtasks:
- [ ] Silver `party` with a mapping table for historical acronym changes
- [ ] Silver `state`
- [ ] Silver `municipality` with IBGE matching (accent/case-insensitive, UF-scoped)
- [ ] Report and quarantine unmatched municipalities

### E6.S3 — Senator entities: senator, senator_mandate `M` `P3`
Subtasks:
- [ ] Silver `senator` (business key = Senate code)
- [ ] Silver `senator_mandate` (start/end, UF, party affiliation over time)
- [ ] Handle senators with multiple mandates and party switches

### E6.S4 — Committees: committee, committee_member `M` `P3`
Subtasks:
- [ ] Silver `committee`
- [ ] Silver `committee_member` with role and period

### E6.S5 — Voting: legislative_session, vote, senator_vote `M` `P3`
AC:
- `senator_id + vote_id` is unique. Vote positions are normalized to a controlled vocabulary (Yes/No/Abstention/Absent/Obstruction/…).

Subtasks:
- [ ] Silver `legislative_session`
- [ ] Silver `vote` (voting event)
- [ ] Silver `senator_vote` with the normalized vote-position vocabulary
- [ ] Deduplicate duplicate API results

### E6.S6 — Legislative matter `M` `P3`
Subtasks:
- [ ] Silver `legislative_matter` (type, number, year, summary, status)
- [ ] Link authorship and rapporteurs

### E6.S7 — CEAPS: expense, expense_category, supplier `M` `P3`
AC:
- Amounts are decimal and non-negative (refunds handled explicitly). Categories are standardized. Suppliers keyed by normalized CNPJ/CPF.

Subtasks:
- [ ] Silver `expense`
- [ ] Silver `expense_category` (map category text variants)
- [ ] Silver `supplier` (clean document IDs, mask CPF if required)
- [ ] Define refund/negative value treatment

### E6.S8 — Amendments: amendment, amendment_payment `M` `P3`
AC:
- Bulk and API sources merge without double-counting. Authorized, committed, liquidated, and paid values are kept as separate columns. Each amendment links to a parliamentarian and a municipality.

Subtasks:
- [ ] Silver `amendment`
- [ ] Silver `amendment_payment` / expenditure documents
- [ ] Bulk-vs-API reconciliation and dedup rule
- [ ] Parliamentarian name → senator matching (entity resolution, document the match rate)
- [ ] Municipality → IBGE linkage

### E6.S9 — Contracts & procurement `S` `P3`
Subtasks:
- [ ] Silver `contract`
- [ ] Silver `procurement`

### E6.S10 — `nb_silver_run` orchestration notebook `M` `P3`
Subtasks:
- [ ] Run entity transformations in dependency order
- [ ] Log each entity to `etl_execution_log`

---

## E7 — Data Quality & Reconciliation

**Goal:** Explicit, logged quality rules across completeness, uniqueness, validity, referential integrity, and freshness. Aggregates reconcile against reference sources.

### E7.S1 — DQ rule framework and `dq_results` `M` `P3`
AC:
- Rules are declared as configuration. Each run writes `dq_results` rows with PASS/WARNING/FAIL and threshold-based statuses.

Subtasks:
- [ ] Create `dq_results` (PRD §19 schema)
- [ ] Rule config format (table, rule_name, rule_type, expression, warn/fail thresholds)
- [ ] Rule engine notebook `nb_dq_validation`
- [ ] Define the pipeline behavior on FAIL (stop the Gold load vs continue with a warning)

### E7.S2 — Implement MVP rules `M` `P3`
Subtasks:
- [ ] Completeness: `senator_id`, `vote_id`, expense keys NOT NULL
- [ ] Uniqueness: `senator_id + vote_id`; expense business key; amendment key
- [ ] Validity: `expense_amount >= 0`, valid dates, valid UF, valid vote positions
- [ ] Referential integrity: facts → senator, municipality, category
- [ ] Freshness: latest source date and latest successful ingestion per critical dataset

### E7.S3 — DQ monitoring view/report `S` `P3`
Subtasks:
- [ ] Gold view over `dq_results` + `etl_execution_log`
- [ ] Operational page in Power BI (or a separate report)

### E7.S4 — Reconciliation framework `S` `P3`
AC:
- Key aggregates compare to Base dos Dados and/or source totals. Discrepancies are logged and documented.

Subtasks:
- [ ] Define reconciliation tests: count(senators), count(votes), count(expenses), sum(expense_amount), count(amendments), sum(paid_amount)
- [ ] Pull Base dos Dados reference aggregates
- [ ] Results table `recon_results` with tolerance and status
- [ ] Document discrepancies in `docs/data_quality.md`

---

## E8 — Gold Warehouse: Dimensional Model

**Goal:** A star schema in `wh_senado_gold` with documented grains.

### E8.S1 — Core dimensions `M` `P4`
AC:
- All initial dimensions exist with surrogate keys and unknown members. Each dimension's SCD policy is documented.

Subtasks:
- [ ] `dim_date` (calendar + legislative year attributes)
- [ ] `dim_senator` (decide SCD type for party/UF changes)
- [ ] `dim_party`
- [ ] `dim_state`
- [ ] `dim_municipality` (with population)
- [ ] `dim_expense_category`
- [ ] `dim_legislative_matter`
- [ ] `dim_committee`
- [ ] DDL in `sql/dimensions/`

### E8.S2 — `fact_vote` `M` `P4`
AC:
- Grain is one senator vote in one voting event. Keys resolve to dimensions. Party key reflects party at the vote date.

Subtasks:
- [ ] DDL and load (`vote_key`, `senator_key`, `matter_key`, `date_key`, `party_key`, `vote_position`)
- [ ] Point-in-time party lookup

### E8.S3 — `fact_expense` `M` `P4`
AC:
- Grain is one published parliamentary expense.

Subtasks:
- [ ] DDL and load (`expense_key`, `senator_key`, `expense_category_key`, `supplier_key`, `date_key`, `amount`, `document_number`)

### E8.S4 — `fact_amendment` `M` `P4`
AC:
- The grain is explicitly decided and documented (e.g. amendment × municipality × year). Authorized, committed, liquidated, and paid amounts are additive at that grain.

Subtasks:
- [ ] Decide the grain from source availability (record as an ADR or in the data dictionary)
- [ ] DDL and load with senator, municipality, state, date, and amendment-type keys

### E8.S5 — Later dimensions `S` `P4`
Subtasks:
- [ ] `dim_supplier`
- [ ] `dim_program`
- [ ] `dim_amendment_type`
- [ ] `dim_vote_position`

### E8.S6 — `fact_legislative_activity` `C` `P4`
Subtasks:
- [ ] Model events (authored matter, rapporteurship, committee participation, action)

### E8.S7 — `nb_gold_run` load process `M` `P4`
Subtasks:
- [ ] Load dims, then facts, idempotently
- [ ] Log to `etl_execution_log`

---

## E9 — Governed Analytical Layer

**Goal:** Implement each canonical metric once and reuse it from both BI and AI.

### E9.S1 — Metric catalogue `M` `P4`
AC:
- `docs/metric_catalog.md` defines every metric with its formula, grain, filters, caveats, and owning object (SQL view and/or DAX measure).

Subtasks:
- [ ] Expenses: CEAPS Total, Monthly Average, YoY Growth, YTD Growth, Historical Median, Peer Percentile, Category Share
- [ ] Voting: Participation Rate, Vote Alignment Rate, Party Alignment Rate, Shared Vote Count
- [ ] Amendments: Authorized, Committed, Liquidated, Paid, Payment Rate, Municipal Amendment Per Capita
- [ ] Define equivalent-period rule for YoY when the current year is incomplete
- [ ] Machine-readable version (`config/metrics.yaml`) for agent `get_metric_definition()`

### E9.S2 — Senator analytical views `M` `P4`
Subtasks:
- [ ] `vw_senator_profile`
- [ ] `vw_senator_expense_summary`
- [ ] `vw_senator_vote_summary`
- [ ] `vw_senator_amendment_summary`

### E9.S3 — Expense comparison views `M` `P4`
Subtasks:
- [ ] `vw_expense_historical_baseline`
- [ ] `vw_expense_peer_comparison` (peer percentile is S)

### E9.S4 — Vote similarity `S` `P4`
AC:
- Similarity = same votes / votes where both participated. Abstention treatment is documented. Absences are excluded. A minimum shared-vote threshold applies. Periods are comparable.

Subtasks:
- [ ] Decide and document abstention/absence/obstruction treatment
- [ ] Define the minimum shared-vote threshold
- [ ] Build `vw_vote_similarity` (pairwise senator matrix, parameterized period)
- [ ] Build `vw_party_vote_alignment`
- [ ] Add the "not proof of alliance" caveat to metadata

### E9.S5 — Amendment geography & execution views `M` `P4`
Subtasks:
- [ ] `vw_municipality_amendment_summary` (with per-capita)
- [ ] `vw_state_amendment_summary`
- [ ] `vw_amendment_execution` (authorized → committed → liquidated → paid funnel)

### E9.S6 — Expense anomaly analysis `S` `P4`
AC:
- Flags use "statistically unusual" wording only. The MVP method is the senator/category historical P95.

Subtasks:
- [ ] MVP: P95 per senator/category view
- [ ] Alternative: robust Z-score (median/MAD)
- [ ] Document the method and neutral-language output labels
- [ ] (C) Isolation Forest prototype

### E9.S7 — AI serving views `M` `P4`
AC:
- AI views have descriptive column names, documented fields, controlled joins, and canonical metrics. The agent has no Bronze or Silver access.

Subtasks:
- [ ] `ai_senator_profile`
- [ ] `ai_expense_analysis`
- [ ] `ai_vote_analysis`
- [ ] `ai_amendment_analysis`
- [ ] `ai_municipality_funding`
- [ ] Column descriptions (extended properties / data dictionary)
- [ ] Include `last_refresh` / source columns to support provenance

### E9.S8 — BI ↔ AI consistency tests `M` `P4`
AC:
- Automated checks confirm the AI views and semantic-model measures return identical values for a set of reference queries.

Subtasks:
- [ ] Define reference queries (e.g. CEAPS total for Senator X in 2026)
- [ ] Compare SQL view results with DAX measure results
- [ ] Add to the test suite / post-load checks

---

## E10 — Power BI Semantic Model & Report

**Goal:** A narrative BI product on a semantic model aligned with the governed layer.

### E10.S1 — Semantic model `M` `P5`
AC:
- Relationships, hierarchies, formatting, and business descriptions are defined. The model is saved to Git (TMDL/PBIP).

Subtasks:
- [ ] Choose storage mode (Direct Lake vs import) and document it
- [ ] Relationships and hierarchies (date, geography state → municipality)
- [ ] Table/column descriptions from the data dictionary
- [ ] Hide technical columns
- [ ] Save in `semantic-model/` as TMDL

### E10.S2 — Core measures `M` `P5`
Subtasks:
- [ ] `[Total CEAPS]`, `[CEAPS YoY]` (equivalent-period logic)
- [ ] `[Voting Participation %]`, `[Vote Alignment %]`
- [ ] `[Authorized Amendments]`, `[Paid Amendments]`, `[Amendment Payment %]`
- [ ] Remaining catalogue measures
- [ ] Measure descriptions referencing the metric catalogue

### E10.S3 — Overview page `M` `P5`
Subtasks:
- [ ] National summary of senators, voting, expenses, amendments
- [ ] Source/period annotations

### E10.S4 — Senator Profile page `M` `P5`
AC:
- Selecting a senator shows votes, expenses, and amendments reliably (PRD §52).

Subtasks:
- [ ] Political profile, voting participation, expense composition, historical spending, amendment allocation, key observations
- [ ] Drill-through target from the other pages

### E10.S5 — Voting Networks page `M` `P5`
Subtasks:
- [ ] Senator similarity visual (matrix/network)
- [ ] Party alignment and clusters
- [ ] Methodology tooltip and caveat

### E10.S6 — Public Spending page `M` `P5`
Subtasks:
- [ ] CEAPS trends, categories, peer comparison
- [ ] Statistical anomaly visual with neutral labels

### E10.S7 — Amendment Map page `M` `P5`
Subtasks:
- [ ] Municipality map and geographic distribution
- [ ] Parliamentarian attribution
- [ ] Execution funnel

### E10.S8 — Interaction & transparency standards `M` `P5`
Subtasks:
- [ ] Every important visual shows source, period, and metric definition
- [ ] Guided questions and annotations instead of heavy slicers
- [ ] Methodology page

---

## E11 — Fabric Data Agent

**Goal:** Governed conversational analytics over Gold and AI views.

### E11.S1 — Configure Data Agent and data sources `M` `P6`
AC:
- The agent connects only to the Gold analytical views and AI views.

Subtasks:
- [ ] Create the Fabric Data Agent
- [ ] Attach AI views and selected Gold views (no Bronze/Silver)
- [ ] Add source/table/column descriptions

### E11.S2 — Agent instructions and guardrails `M` `P6`
Subtasks:
- [ ] Domain glossary (CEAPS definition, amendment value stages)
- [ ] Guardrail instructions (anomaly ≠ misconduct, similarity ≠ coordination, absence ≠ negligence, authorized ≠ delivered, payment ≠ impact)
- [ ] YoY equivalent-period instruction
- [ ] Neutral-language and source-attribution instructions
- [ ] Version instructions in `agent/fabric-agent/`

### E11.S3 — Example queries `M` `P6`
Subtasks:
- [ ] Add PRD §34 question patterns with reference SQL
- [ ] Add a "How was this calculated?" pattern

### E11.S4 — Answer validation suite `M` `P6`
AC:
- Each validated question pattern matches the BI model value. "How much did Senator X spend in 2026?" equals `[Total CEAPS]`.

Subtasks:
- [ ] Build a golden Q&A set with expected values from the semantic model
- [ ] Run, score, and record results in `tests/agent/`
- [ ] Guardrail red-team prompts (e.g. "Is Senator X corrupt?") return a neutral response

---

## E12 — LangGraph Orchestration & Deterministic Tools `S`

**Goal:** Multi-step reasoning with deterministic tools, provenance, and guardrails on top of the Data Agent.

### E12.S1 — Fabric Data Agent connectivity (MCP) `S` `P6`
Subtasks:
- [ ] Expose or consume the Data Agent through MCP
- [ ] `query_fabric_agent()` tool with auth via service identity

### E12.S2 — Deterministic analytical tools `S` `P6`
AC:
- Calculations run in the tools, not in the LLM. Each tool returns its data plus provenance metadata.

Subtasks:
- [ ] `get_senator_profile()`
- [ ] `get_expense_history()`
- [ ] `get_expense_peer_percentile()`
- [ ] `get_vote_similarity()`
- [ ] `get_amendment_distribution()`
- [ ] `get_municipality_summary()`
- [ ] `get_metric_definition()` (reads the metric catalogue)
- [ ] `get_data_provenance()` (source, object, metric, filters, last refresh)
- [ ] Unit tests per tool

### E12.S3 — Graph: router, planner, synthesis `S` `P6`
Subtasks:
- [ ] Intent router (simple / deterministic metric / methodology / complex)
- [ ] Planner for multi-tool questions
- [ ] Synthesis node
- [ ] State schema and checkpointing

### E12.S4 — Guardrail check node `S` `P6`
Subtasks:
- [ ] Classify statements as source fact / calculated metric / statistical observation / model inference
- [ ] Block or rewrite accusatory or causal political claims
- [ ] Tests with adversarial prompts

### E12.S5 — Provenance display `S` `P6`
AC:
- Important answers include source, analytical object, metric, filters, and last refresh. A "How was this calculated?" follow-up works.

Subtasks:
- [ ] Provenance block format
- [ ] Follow-up methodology handling via conversation memory

### E12.S6 — Conversational UI `S` `P6`
Subtasks:
- [ ] Minimal chat UI (e.g. Streamlit/Chainlit) showing the answer, provenance, and tool trace
- [ ] Demo-ready deployment (local or a lightweight host)

---

## E13 — End-to-End Orchestration, Security & CI/CD

### E13.S1 — `pl_master_data_platform` `M` `P3–P5`
AC:
- One pipeline runs ingestion → `nb_silver_run` → DQ → `nb_gold_run` → semantic refresh, with failure handling and alerts.

Subtasks:
- [ ] Chain child pipelines and notebooks
- [ ] Conditional stop on DQ FAIL
- [ ] Semantic model refresh activity
- [ ] Failure notification (email/Teams)
- [ ] Schedule (daily/weekly) and document it in `docs/orchestration.md`

### E13.S2 — Secrets & identity (Terraform-managed) `M` `P1–P2`
AC:
- Key Vault, identities, and access policies/RBAC come from Terraform. Secret *values* are set out-of-band and never appear in code or state outputs.

Subtasks:
- [ ] `azurerm_key_vault` with RBAC authorization, plus a role assignment for the Fabric workspace identity (Key Vault Secrets User)
- [ ] Create the `transparencia-api-key` secret placeholder. Set the value manually or from a CI secret, and add `lifecycle { ignore_changes = [value] }`
- [ ] Fabric connections for source APIs (`fabric_connection`) where pipelines need them
- [ ] Separate service principals for deployment (Terraform) and runtime (pipelines/agent), each least-privilege
- [ ] Verify no secrets exist in notebooks, Git, or Terraform outputs (scan + `sensitive = true`)
- [ ] Restrict agent access to curated resources

### E13.S3 — Environment strategy and deployment pipelines `M` (design) / `S` (implementation)
AC:
- The DEV/TEST/PROD target model is documented and expressed as Terraform environments, even if only DEV is applied.

Subtasks:
- [ ] Document target deployment model (Git branches ↔ workspaces ↔ `infra/envs/*`)
- [ ] `envs/test` and `envs/prod` tfvars (separate state per environment)
- [ ] `fabric_deployment_pipeline` with stages bound to the DEV/TEST/PROD workspaces (if capacity allows)
- [ ] Deployment rules / parameterization for environment-specific config (lakehouse IDs, connection IDs from Terraform outputs)
- [ ] Post-deployment data population runbook
- [ ] CI checks (lint notebooks/SQL, unit tests) via GitHub Actions

### E13.S4 — Terraform CI/CD workflow `M` `P1`
AC:
- PRs run `fmt`/`validate`/`tflint`/`plan` and post the plan. Merges to `main` apply DEV. TEST/PROD applies need manual approval through GitHub Environments.

Subtasks:
- [ ] GitHub Actions workflow `infra.yml` with OIDC login (no stored secrets)
- [ ] Plan on PR with a plan comment; apply on merge
- [ ] GitHub Environments with required reviewers for TEST/PROD
- [ ] Scheduled `terraform plan` drift detection (alerts on changes made in the portal)
- [ ] Optional security scan (`checkov` / `tfsec`)

---

## E14 — Documentation & Portfolio

### E14.S1 — Architecture diagrams `M` `P7`
Subtasks:
- [ ] `solution-architecture.png`
- [ ] `data-flow.png`
- [ ] `agent-architecture.png`

### E14.S2 — Architecture Decision Records `M` `P7`
Subtasks:
- [ ] ADR-001 official government sources
- [ ] ADR-002 medallion architecture
- [ ] ADR-003 separate Bronze/Silver lakehouses
- [ ] ADR-004 Gold Fabric Warehouse
- [ ] ADR-005 metadata-driven ingestion
- [ ] ADR-006 bulk + API for Transparência
- [ ] ADR-007 Fabric Data Agent
- [ ] ADR-008 LangGraph orchestration
- [ ] ADR-009 no Eventstream
- [ ] ADR-010 AI serving views
- [ ] ADR-011 Terraform for infrastructure; Fabric Git integration for item content (ownership boundary)
- [ ] (extra) fact_amendment grain, vote-similarity treatment, SCD policy

### E14.S3 — Technical documentation `M` `P7`
Subtasks:
- [ ] `docs/data_dictionary.md`
- [ ] `docs/metric_catalog.md` (from E9.S1)
- [ ] `docs/data_quality.md`
- [ ] `docs/orchestration.md`
- [ ] `docs/sources.md` with source transparency and attribution (Senado, Transparência, Base dos Dados, IBGE)
- [ ] Methodology document (vote similarity, anomalies, YoY, caveats)

### E14.S4 — README `M` `P7`
Subtasks:
- [ ] Portfolio narrative (PRD §53)
- [ ] Architecture overview, tech stack, how to reproduce
- [ ] Success-criteria walkthrough (source → Bronze → Silver → Gold)

### E14.S5 — Screenshots, demo and publication `M` `P7`
Subtasks:
- [ ] Power BI screenshots per page
- [ ] Agent Q&A screenshots, including a provenance example
- [ ] Demo recording
- [ ] LinkedIn / Data Glow Up #48 post

---

## E15 — Future Extensions `C`

One story per item, unscheduled:

- E15.S1 Automated weekly Senate summary
- E15.S2 Social-media content generation
- E15.S3 Downloadable senator profile
- E15.S4 Natural-language chart generation
- E15.S5 Vector search over legislative documents
- E15.S6 Legislation full-text integration
- E15.S7 Ontology layer
- E15.S8 Advanced entity resolution
- E15.S9 Advanced anomaly models
- E15.S10 Public web application

---

## Open Decisions Surfaced by the PRD

These are unresolved in the PRD and block specific stories:

| # | Decision | Blocks |
|---|---|---|
| 1 | `fact_amendment` grain | E8.S4, E9.S5 |
| 2 | Abstention / obstruction treatment and min shared-vote threshold for similarity | E9.S4, E10.S5 |
| 3 | SCD policy for `dim_senator` (party/UF changes) | E8.S1, E8.S2 |
| 4 | Bulk vs API cut-off for amendments and dedup rule | E5.S2, E6.S8 |
| 5 | Matching amendment parliamentarian names to Senate senator IDs | E6.S8 |
| 6 | Pipeline behavior on DQ FAIL (block vs warn) | E7.S1, E13.S1 |
| 7 | History start year for votes (CEAPS is 2022+) | E3.S3 |
| 8 | Semantic model storage mode (Direct Lake vs import) | E10.S1 |
| 9 | Metadata table storage (Warehouse vs Lakehouse vs config file) | E2.S1 |
| 10 | Treatment of negative CEAPS values / refunds | E6.S7, E7.S2 |
| 11 | Capacity: Terraform-managed `azurerm_fabric_capacity` (Azure subscription, pay-as-you-go cost) vs existing trial capacity referenced as a data source | E1.S1 |
| 12 | Whether the Fabric Data Agent can be provisioned via Terraform (check current provider support; otherwise create it in the portal and sync it to Git) | E11.S1 |
| 13 | Git branch ↔ environment mapping and `initialization_strategy` (PreferGit vs PreferWorkspace) | E1.S3, E13.S3 |
| 14 | Amendments have no *authorized* value (only committed/liquidated/paid). Redefine *Authorized Amount* and *Payment Rate* (e.g. paid / committed) or add a budget source. See `docs/sources.md` §3 | E9.S1, E9.S5, E10.S2 |
| 15 | Amendment ingestion: full monthly bulk snapshot (recommended) vs bulk-history + API-incremental (PRD §10) | E5.S1, E5.S2, ADR-006 |
| 16 | Amendment geography: 60% of amendments have no municipality (`MÚLTIPLO`/`Nacional`/state). Map only the 40% with an IBGE code, or use recipient municipality from `PorFavorecido` | E6.S8, E8.S4, E10.S7 |
| 17 | Whether amendments a senator authored earlier as a federal deputy count toward their profile | E6.S8 |
