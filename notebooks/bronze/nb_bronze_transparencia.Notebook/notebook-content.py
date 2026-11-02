# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_bronze_transparencia
#
# Source: CGU, Portal da Transparência (parliamentary amendments).
# Runs every enabled `transparencia` dataset from `etl_ingestion_config`, in order, with one
# shared HTTP client (the API allows 90 requests/min during the day).
#
# - `amendments`: the bulk ZIP (all years). Skipped when the file is unchanged.
# - `amendments_api`: REST API, current year. Needs the API key: without `key_vault_url`, it is
#   read from `lh_senado_control/Files/_secrets/transparencia-api-key`.
# - Datasets that are not due (their `frequency`) are logged as NOT_DUE. `force = true` runs all.
# - A failed dataset is logged and the next one still runs; the notebook fails at the end.

# PARAMETERS CELL ********************

pipeline_name = "manual"
pipeline_run_id = "manual"
mode = "incremental"
backfill_start = ""
backfill_end = ""
force = "false"
entities = ""
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

result = run_source_notebook(
    "transparencia", spark,
    pipeline_name=pipeline_name, pipeline_run_id=pipeline_run_id, mode=mode,
    backfill_start=backfill_start, backfill_end=backfill_end,
    force=force, entities=entities, key_vault_url=key_vault_url,
)
notebookutils.notebook.exit(json.dumps(result, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
