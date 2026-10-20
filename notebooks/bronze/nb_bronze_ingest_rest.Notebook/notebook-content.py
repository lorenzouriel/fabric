# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_ingest_rest
#
# Lands raw REST responses in `lh_senado_bronze` for one dataset, records lineage in
# `etl_landed_file`, logs the run in `etl_execution_log`, and advances `etl_watermark`
# only after everything succeeded.
#
# Called by `pl_master_ingestion` with `config_json`. For a manual run, leave
# `config_json` empty and set `dataset` (e.g. `senado_legislativo.votes`).

# PARAMETERS CELL ********************

config_json = ""
dataset = ""
pipeline_name = "manual"
pipeline_run_id = "manual"
mode = "incremental"
backfill_start = ""
backfill_end = ""
key_vault_url = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_framework_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace = current_workspace_name()
store = ControlStore(spark, workspace)

if config_json:
    cfg = IngestionConfig.from_dict(json.loads(config_json))
else:
    matches = [r for r in store.read_config() if f"{r['source_system']}.{r['entity']}" == dataset]
    if not matches:
        raise ConfigError(f"dataset '{dataset}' not found in {TABLE_CONFIG}")
    cfg = IngestionConfig.from_dict(matches[0])

if cfg.source_type != "REST":
    raise ConfigError(f"{cfg.dataset} is {cfg.source_type}, not REST")
if mode not in MODES:
    raise ValueError(f"mode must be one of {sorted(MODES)}, got '{mode}'")

run = RunContext(
    pipeline_name=pipeline_name,
    pipeline_run_id=pipeline_run_id if pipeline_run_id != "manual" else f"manual-{uuid.uuid4()}",
    mode=mode,
    backfill_start=backfill_start,
    backfill_end=backfill_end,
)

result = run_rest_ingestion(
    cfg,
    run,
    store,
    OneLakeWriter(workspace),
    secret_provider=key_vault_secret_provider(key_vault_url) if key_vault_url else None,
)
print(json.dumps(result, default=str, indent=2))
notebookutils.notebook.exit(json.dumps(result, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
