# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_ingest_file
#
# Downloads one bulk file, skips it when the same content (SHA-256) was already landed,
# otherwise lands the original and every extracted ZIP member in `lh_senado_bronze`.
# Source bytes are stored untouched (e.g. Latin-1 CSV stays Latin-1).
#
# Called by `pl_master_ingestion` with `config_json`. For a manual run, leave
# `config_json` empty and set `dataset`.

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

if cfg.source_type != "FILE":
    raise ConfigError(f"{cfg.dataset} is {cfg.source_type}, not FILE")
if mode not in MODES:
    raise ValueError(f"mode must be one of {sorted(MODES)}, got '{mode}'")

run = RunContext(
    pipeline_name=pipeline_name,
    pipeline_run_id=pipeline_run_id if pipeline_run_id != "manual" else f"manual-{uuid.uuid4()}",
    mode=mode,
)

result = run_file_ingestion(cfg, run, store, OneLakeWriter(workspace))
print(json.dumps(result, default=str, indent=2))
notebookutils.notebook.exit(json.dumps(result, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
